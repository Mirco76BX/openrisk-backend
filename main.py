
# OpenRisk AI - v2.3
# v2.2: Bilanzsumme + Eigenkapital (balance_sheet_accounts),
#       Verschuldungsgrad, Umsatzprognose (CAGR), Insolvenz-Check,
#       Debug-Endpoint fuer Rohdaten
# v2.3: Fix _enrich_balance_sheet: rekursive Eigenkapital-Suche via
#       name.in_report/de statt str(dict) → korrekter EK-Wert aus
#       "A. Eigenkapital"-Child (nicht Passiva-Gesamtsumme)

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import re, os, logging, hashlib
from typing import Optional, List, Any, Dict
import requests
from bs4 import BeautifulSoup
import dateparser
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("openrisk")

VERSION = "2.8.0"

app = FastAPI(title="OpenRisk AI Backend", version=VERSION)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


class CompanyRequest(BaseModel):
    name: str
    handelsregister_nr: Optional[str] = None

class FinancialData(BaseModel):
    bilanzsumme: Optional[float] = None
    eigenkapital: Optional[float] = None
    umsatz: Optional[float] = None
    jahresergebnis: Optional[float] = None
    mitarbeiter: Optional[int] = None
    eigenkapitalquote: Optional[float] = None
    verschuldungsgrad: Optional[float] = None
    umsatz_pro_mitarbeiter: Optional[float] = None
    umsatz_prognose: Optional[float] = None
    umsatz_prognose_jahr: Optional[str] = None
    quelle: Optional[str] = None
    geschaeftsjahr: Optional[str] = None
    gruendungsjahr: Optional[str] = None
    rechtsform: Optional[str] = None
    loehne_gehaelter: Optional[float] = None
    fremdkapital: Optional[float] = None
    parent_company: Optional[str] = None       # aus HR.ai Eigentuemerstruktur
    konzern_score_auto: Optional[int] = None   # 5=unbekannt,7=Mutter gefunden,8=Mutter+gesund

class CompanyInfo(BaseModel):
    insolvenz: bool = False
    insolvenz_datum: Optional[str] = None
    negativmerkmale: List[str] = []
    negativmerkmale_quelle: Optional[str] = None

class ScoringInput(BaseModel):
    company_name: str
    financials: FinancialData
    company_info: CompanyInfo = CompanyInfo()
    raw_text: Optional[str] = None


class HandelsregisterClient:
    BASE_URL = "https://handelsregister.ai/api"

    def __init__(self):
        self.api_key = os.environ.get("HANDELSREGISTER_API_KEY", "")
        if not self.api_key:
            logger.warning("HANDELSREGISTER_API_KEY nicht gesetzt")

    def is_available(self):
        return bool(self.api_key)

    def _get(self, q: str, feature: str) -> dict:
        headers = {"x-api-key": self.api_key, "Accept": "application/json"}
        params = {"q": q, "feature": feature}
        resp = requests.get(f"{self.BASE_URL}/v1/fetch-organization", params=params, headers=headers, timeout=12)
        if resp.status_code in (401, 402, 404):
            return {}
        resp.raise_for_status()
        data = resp.json()
        if isinstance(data, list):
            return data[0] if data else {}
        return data if isinstance(data, dict) else {}

    def get_raw(self, company_name: str, feature: str) -> dict:
        if not self.is_available():
            return {"error": "Kein API-Key"}
        return self._get(company_name, feature)

    def search(self, company_name: str, hr_nummer: Optional[str] = None):
        if not self.is_available():
            return None, None
        q = hr_nummer if hr_nummer else company_name
        try:
            data_kpi = self._get(q, "financial_kpi")
            if not data_kpi:
                return None, None
            company_name_hr = data_kpi.get("name")
            fd = self._map_kpi(data_kpi)
            if not fd:
                return None, None
            try:
                data_bs = self._get(q, "balance_sheet_accounts")
                if data_bs:
                    self._enrich_balance_sheet(fd, data_bs)
            except Exception as e:
                logger.warning(f"balance_sheet_accounts Fehler: {e}")
            # v2.6.4: related_persons → GF-Namen (2 Credits)
            try:
                data_rp = self._get(q, "related_persons")
                if data_rp:
                    gf_raw = self._extract_gf_names(data_rp)
                    if gf_raw:
                        fd.__dict__["_gf_namen_detected"] = gf_raw
                        logger.info(f"GF-Namen via related_persons: {gf_raw}")
                    # Auch parent_company aus related_persons extrahieren (Komplementaer)
                    if not fd.parent_company:
                        self._extract_parent_from_related(fd, data_rp)
            except Exception as e:
                logger.warning(f"related_persons Fehler: {e}")
            # v2.6.4: shareholders → Muttergesellschaft/Gesellschafter (5 Credits)
            try:
                data_sh = self._get(q, "shareholders")
                if data_sh:
                    self._extract_parent_from_shareholders(fd, data_sh)
            except Exception as e:
                logger.warning(f"shareholders Fehler: {e}")
            # v2.8.0: profit_and_loss_account → Mitarbeiterzahl aus GuV (3 Credits)
            try:
                data_pnl = self._get(q, "profit_and_loss_account")
                if data_pnl and not fd.mitarbeiter:
                    self._extract_mitarbeiter_from_pnl(fd, data_pnl)
            except Exception as e:
                logger.warning(f"profit_and_loss_account Fehler: {e}")
            # v2.6.4: annual_financial_statements → GF-Namen + Muttergesellschaft aus Volltext (5 Credits)
            try:
                data_afs = self._get(q, "annual_financial_statements")
                if data_afs:
                    stmts = data_afs.get("annual_financial_statements") or []
                    if stmts:
                        doc_md = stmts[0].get("document_md") or ""
                        if doc_md:
                            # GF-Namen aus Volltext wenn noch nicht gefunden
                            if not fd.__dict__.get("_gf_namen_detected"):
                                gf_text = self._extract_gf_from_statement_text(doc_md)
                                if gf_text:
                                    fd.__dict__["_gf_namen_detected"] = gf_text
                                    logger.info(f"GF-Namen via annual_financial_statements: {gf_text}")
                            # Muttergesellschaft aus Volltext wenn noch nicht gefunden
                            if not fd.parent_company:
                                parent_text = self._extract_parent_from_statement_text(doc_md)
                                if parent_text:
                                    fd.parent_company = parent_text
                                    fd.konzern_score_auto = 7
                                    logger.info(f"Muttergesellschaft via annual_statements: {parent_text}")
                            # Liquide Mittel direkt aus Bilanz-Text
                            if not fd.__dict__.get("liquide_mittel"):
                                import re as _re
                                m_liq = _re.search(
                                    r"Kassenbestand[^|]{0,60}\|\s*([\d\.]+,[\d]{2})", doc_md)
                                if m_liq:
                                    try:
                                        fd.__dict__["liquide_mittel"] = float(
                                            m_liq.group(1).replace(".","").replace(",","."))
                                    except: pass
            except Exception as e:
                logger.warning(f"annual_financial_statements Fehler: {e}")
            # v2.8.0: website_content als Fallback (0 Credits, AI Mode)
            # Nur wenn GF-Namen oder Mitarbeiterzahl noch fehlen
            gf_missing = not fd.__dict__.get("_gf_namen_detected")
            ma_missing = not fd.mitarbeiter
            if gf_missing or ma_missing:
                try:
                    data_wc = self._get(q, "website_content")
                    if data_wc:
                        wc_text = ""
                        for key in ("website_content", "content", "markdown", "text"):
                            val = data_wc.get(key)
                            if isinstance(val, str) and len(val) > 50:
                                wc_text = val
                                break
                        if wc_text:
                            if gf_missing:
                                gf_wc = self._extract_gf_from_statement_text(wc_text)
                                if gf_wc:
                                    fd.__dict__["_gf_namen_detected"] = gf_wc
                                    logger.info(f"GF-Namen via website_content: {gf_wc}")
                            if ma_missing:
                                ma_wc = self._extract_mitarbeiter_from_text(wc_text)
                                if ma_wc:
                                    fd.mitarbeiter = ma_wc
                                    logger.info(f"Mitarbeiter via website_content: {ma_wc}")
                except Exception as e:
                    logger.warning(f"website_content Fallback Fehler: {e}")
            return fd, company_name_hr
        except Exception as e:
            logger.warning(f"HR.ai Fehler: {e}")
            return None, None

    def _extract_parent_from_related(self, fd, data: dict):
        """Extrahiert Komplementaer/Muttergesellschaft aus related_persons."""
        persons = []
        for key in ("related_persons", "persons", "officers", "representatives"):
            persons = data.get(key) or []
            if persons: break
        if not isinstance(persons, list): return
        for p in persons:
            if not isinstance(p, dict): continue
            role = str(p.get("role","") or p.get("position","") or "").lower()
            # Komplementaer-GmbH = persoenlich haftende Gesellschafterin
            if any(x in role for x in ("komplementaer","komplementär","persönlich haftend","persoenlich haftend")):
                name = p.get("name") or p.get("company_name") or ""
                if isinstance(name, dict):
                    name = name.get("name") or name.get("company_name") or ""
                name = str(name).strip()
                if name and len(name) > 4:
                    fd.parent_company = name
                    fd.konzern_score_auto = 7
                    logger.info(f"Komplementaer aus related_persons: {name}")
                    return

    def _extract_parent_from_shareholders(self, fd, data: dict):
        """Extrahiert Muttergesellschaft aus shareholders-Daten."""
        if fd.parent_company: return  # bereits gefunden
        sh_list = []
        for key in ("shareholders", "owners", "gesellschafter"):
            sh_list = data.get(key) or []
            if sh_list: break
        if not isinstance(sh_list, list): return
        logger.info(f"shareholders raw: {sh_list[:3]}")
        best_name, best_share = None, 0.0
        for sh in sh_list:
            if not isinstance(sh, dict): continue
            share = float(sh.get("share") or sh.get("percentage") or sh.get("capital_share") or 0)
            name = (sh.get("name") or sh.get("company_name") or
                    sh.get("shareholder_name") or "")
            if isinstance(name, dict):
                name = name.get("name") or name.get("company_name") or ""
            name = str(name).strip()
            if not name or len(name) < 4: continue
            # Bevorzuge: groessten Anteil ODER Unternehmen (nicht Person)
            sh_type = str(sh.get("type","") or sh.get("entity_type","")).lower()
            is_company = any(x in name for x in ("GmbH","AG","KG","SE","Ltd","Holding","Corp")) or                          sh_type in ("company","organisation","legal_entity","gmbh","ag")
            if share > best_share or (is_company and share >= 25):
                best_share = share
                best_name = name
        if best_name:
            fd.parent_company = best_name
            fd.konzern_score_auto = 7
            logger.info(f"Muttergesellschaft via shareholders: {best_name} ({best_share}%)")

    def _map_kpi(self, data: dict) -> Optional[FinancialData]:
        kpi_list = data.get("financial_kpi") or []
        if isinstance(kpi_list, dict):
            kpi_list = [kpi_list]
        if not isinstance(kpi_list, list) or not kpi_list:
            return None
        kpi_sorted = sorted(kpi_list, key=lambda x: x.get("year", 0), reverse=True)
        fin = kpi_sorted[0]
        def sf(v):
            try: return float(v) if v is not None else None
            except: return None
        def si(v):
            try: return int(v) if v is not None else None
            except: return None
        reg_date = str(data.get("registration_date") or "")
        legal_form = data.get("legal_form")
        if isinstance(legal_form, dict):
            legal_form = legal_form.get("name") or legal_form.get("short") or str(legal_form)
        f = FinancialData(
            umsatz=sf(fin.get("revenue")), jahresergebnis=sf(fin.get("net_income")),
            mitarbeiter=si(fin.get("employees")), geschaeftsjahr=str(fin.get("year") or "") or None,
            rechtsform=str(legal_form or "") or None,
            gruendungsjahr=reg_date[:4] if len(reg_date) >= 4 else None,
            quelle="handelsregister.ai",
        )
        if len(kpi_sorted) >= 2:
            self._add_revenue_forecast(f, kpi_sorted)
        if f.umsatz and f.mitarbeiter and f.mitarbeiter > 0:
            f.umsatz_pro_mitarbeiter = round(f.umsatz / f.mitarbeiter, 2)
        for _lk in ("wages_and_salaries","personnel_costs","staff_costs","labor_costs",
                    "loehne_und_gehaelter","wages","salaries","personnel_expenses"):
            _lv=sf(fin.get(_lk))
            if _lv and _lv>0: f.loehne_gehaelter=_lv; break
        logger.info("HR.ai KPI: Umsatz="+str(f.umsatz)+", JE="+str(f.jahresergebnis)+", MA="+str(f.mitarbeiter)+", Loehne="+str(f.loehne_gehaelter))
        # WZ-Code extrahieren
        wz_raw = data.get("nace_code") or data.get("wz_code") or data.get("industry_code") or ""
        if wz_raw:
            if isinstance(wz_raw, dict): wz_raw = wz_raw.get("code") or wz_raw.get("id") or ""
            f.__dict__["wz_code"] = str(wz_raw).strip()
        # Eigentuemerstruktur / Muttergesellschaft erkennen
        parent = None
        for src_key in ("parent_company","parent","ultimate_parent","controlling_entity","holding"):
            raw_p = data.get(src_key)
            if raw_p:
                if isinstance(raw_p, dict): raw_p = raw_p.get("name") or raw_p.get("company_name") or ""
                parent = str(raw_p).strip() or None
                if parent: break
        # Gesellschafter-Liste als Fallback
        if not parent:
            sh_list = data.get("shareholders") or data.get("owners") or []
            if isinstance(sh_list, list) and sh_list:
                for sh in sh_list:
                    if isinstance(sh, dict):
                        sh_type = str(sh.get("type","")).lower()
                        sh_share = float(sh.get("share") or sh.get("percentage") or 0)
                        if sh_share >= 25.0 or sh_type in ("company","gmbh","ag","kg"):
                            sh_name = sh.get("name") or sh.get("company_name") or ""
                            if sh_name: parent = str(sh_name).strip(); break
        if parent:
            f.parent_company = parent
            f.konzern_score_auto = 7  # Mutter identifiziert, Bonitat unbekannt
            logger.info(f"Konzernzugehoerigkeit erkannt: {parent}")
        else:
            f.konzern_score_auto = 5  # keine Mutter gefunden
        # GF-Namen extrahieren und in eigenem Feld speichern (fuer GF-Check)
        gf_raw = self._extract_gf_names(data)
        if gf_raw:
            f.__dict__["_gf_namen_detected"] = gf_raw
            logger.info(f"GF-Namen erkannt: {gf_raw}")
        return f

    def _extract_gf_names(self, data: dict) -> Optional[str]:
        """Extrahiert aktive GF-Namen aus beliebigem HR.ai Response-Dict."""
        for key in ("related_persons","management","directors","geschaeftsfuehrer","persons",
                    "management_board","managing_directors","officers","representatives","board"):
            persons = data.get(key) or []
            if isinstance(persons, list) and persons:
                names = []
                for p in persons[:5]:
                    if isinstance(p, dict):
                        # Status: nur aktive GF
                        status = str(p.get("status","")).lower()
                        if any(x in status for x in ("inaktiv","former","ausgeschieden","resigned","ex-")):
                            continue
                        name = p.get("name") or p.get("full_name") or ""
                        if isinstance(name, dict):
                            first = str(name.get("first","") or "").strip()
                            last  = str(name.get("last","")  or "").strip()
                            name = f"{first} {last}".strip()
                        role = str(p.get("role","") or p.get("position","")).lower()
                        # Aufsichtsräte und Beiräte ausschließen
                        if any(x in role for x in ("aufsicht","supervisory","beirat","advisory")):
                            continue
                        if name: names.append(name.strip())
                if names:
                    return ", ".join(names)
        return None

    def _extract_mitarbeiter_from_pnl(self, fd: "FinancialData", data: dict) -> None:
        """v2.8.0: Extrahiert Mitarbeiterzahl aus profit_and_loss_account-Daten."""
        # Versuche strukturierte Felder
        for key in ("profit_and_loss_account", "pnl", "income_statement"):
            entries = data.get(key) or []
            if not isinstance(entries, list): continue
            for entry in entries[:2]:
                # Direkt als KPI-Feld
                for emp_key in ("employees", "mitarbeiter", "number_of_employees",
                                "average_employees", "durchschnittliche_mitarbeiter"):
                    val = entry.get(emp_key)
                    if val and str(val).isdigit():
                        try:
                            fd.mitarbeiter = int(val)
                            return
                        except: pass
        # Fallback: Volltext-Markdown aus P&L
        for key in ("document_md", "markdown", "text", "content"):
            text = data.get(key) or ""
            if isinstance(text, str) and len(text) > 20:
                result = self._extract_mitarbeiter_from_text(text)
                if result:
                    fd.mitarbeiter = result
                    return

    def _extract_mitarbeiter_from_text(self, text: str) -> Optional[int]:
        """v2.8.0: Regex-Extraktion Mitarbeiterzahl aus beliebigem Text."""
        patterns = [
            r"(?:Anzahl\s+(?:der\s+)?(?:durchschnittlich\s+)?|durchschnittlich\s+besch.ftigte?\s+)"
            r"(?:Arbeitnehmer|Mitarbeiter(?:innen|zahl)?)[:\s]+(\d[\d\.,]*)",
            r"(\d[\d\.,]+)\s+(?:Mitarbeiter|Arbeitnehmer|Besch.ftigte)(?:\s+weltweit|\s+worldwide)?",
            r"employees[:\s]+([0-9][\d,\.]+)",
            r"([0-9][\d,\.]+)\s+employees",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    raw = m.group(1).replace(".", "").replace(",", "").strip()
                    val = int(raw)
                    if 1 <= val <= 5_000_000:   # Plausibilitaetscheck
                        return val
                except: pass
        return None

    def get_gf_names(self, company_name: str, hr_nummer: Optional[str] = None) -> Optional[str]:
        """Holt GF-Namen direkt via HR.ai (management Feature + Fallback financial_kpi)."""
        if not self.is_available(): return None
        q = hr_nummer if hr_nummer else company_name
        try:
            for feature in ("related_persons", "management", "persons", "financial_kpi"):
                data = self._get(q, feature)
                if not data: continue
                names = self._extract_gf_names(data)
                if names:
                    logger.info(f"GF-Namen via '{feature}': {names}")
                    return names
        except Exception as e:
            logger.warning(f"GF-Namen HR.ai Fehler: {e}")
        return None

    # ── DuckDuckGo Fallback: GF-Namen + Muttergesellschaft ──────────────────

    _DDG_HEADERS = {"User-Agent": "Mozilla/5.0 (compatible; OpenRisk/2.6)"}

    def _ddg_query(self, query: str, timeout: int = 8) -> list:
        """Hilfsfunktion: DuckDuckGo HTML abfragen und Snippets zurueckgeben."""
        try:
            url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(query)}&kl=de-de"
            r = requests.get(url, headers=self._DDG_HEADERS, timeout=timeout)
            soup = BeautifulSoup(r.text, "html.parser")
            snippets = []
            for el in (soup.find_all("a", {"class": "result__snippet"}) or
                       soup.find_all("div", {"class": "result__snippet"}) or
                       soup.find_all("td", {"class": "result-snippet"})):
                t = el.get_text(" ", strip=True)
                if t: snippets.append(t)
            # Fallback: alle result__body Divs
            if not snippets:
                for el in soup.find_all("div", class_=lambda c: c and "result" in c.lower()):
                    t = el.get_text(" ", strip=True)
                    if len(t) > 40: snippets.append(t[:400])
            return snippets[:10]
        except Exception as e:
            logger.warning(f"DDG-Abfrage Fehler: {e}")
            return []

    def ddg_find_gf_names(self, company_name: str) -> Optional[str]:
        """Sucht GF-Namen via DuckDuckGo wenn HR.ai keine Daten liefert.
        Strategie: Suche '{Name} Geschaeftsfuehrer' und parse Snippets."""
        _NAME_RE = re.compile(
            r'\b([A-ZÄÖÜ][a-zäöüß]{1,20}(?:-[A-ZÄÖÜ][a-zäöüß]{1,20})?'
            r'\s+[A-ZÄÖÜ][a-zäöüß]{2,25})\b'
        )
        _GF_TRIGGER = re.compile(
            r'(?:geschäftsführer|geschaeftsfuehrer|gf\b|komplementär|komplementaer'
            r'|managing\s+director|ceo\b|inhaber|prokurist)',
            re.I
        )
        # Abfrage 1: direkt Geschäftsführer
        short_name = company_name.split("&")[0].strip()
        candidates: dict = {}  # name -> Häufigkeit
        for q in [f'"{short_name}" Geschäftsführer', f'"{short_name}" GF Geschäftsführer']:
            for snippet in self._ddg_query(q):
                snl = snippet.lower()
                if not _GF_TRIGGER.search(snl): continue
                for m in _NAME_RE.finditer(snippet):
                    n = m.group(1).strip()
                    # Filter: zu kurz, bekannte Nicht-Namen
                    if len(n) < 8: continue
                    _STOPWORDS = {"GmbH","GmbH","KG","AG","Co","Consulting","Engineering",
                                  "Ltd","GfK","Inc","Das","Die","Der","Eine","Beim","Seit",
                                  "WESSLING","Company","Group","Holding"}
                    parts = n.split()
                    if any(p in _STOPWORDS for p in parts): continue
                    candidates[n] = candidates.get(n, 0) + 1
        if candidates:
            # Namen mit mind. 1 Treffer, absteigend sortiert
            names = [n for n,c in sorted(candidates.items(), key=lambda x: -x[1]) if c >= 1][:3]
            if names:
                result = ", ".join(names)
                logger.info(f"DDG GF-Namen fuer '{company_name}': {result}")
                return result
        return None

    def ddg_find_parent_company(self, company_name: str) -> Optional[str]:
        """Sucht Muttergesellschaft/Gesellschafter via DuckDuckGo."""
        _CORP_RE = re.compile(
            r'\b([A-ZÄÖÜ][A-Za-zäöüÄÖÜß\s&\.\-]{3,50}'
            r'(?:GmbH|AG|KG|SE|Holding|Group|Corp|Ltd|LLC|Beteiligungs)'
            r'(?:\s+&\s+Co\.\s+KG)?)\b'
        )
        short_name = company_name.split("&")[0].strip()
        for q in [f'"{short_name}" Muttergesellschaft Eigentümer',
                  f'"{short_name}" Gesellschafter Holding']:
            for snippet in self._ddg_query(q):
                snl = snippet.lower()
                if not any(x in snl for x in ("muttergesellschaft","gesellschafter","eigentümer","holding","gehört zu","beteiligung")):
                    continue
                for m in _CORP_RE.finditer(snippet):
                    n = m.group(0).strip().rstrip(".,;")
                    # Nicht das Unternehmen selbst
                    if company_name.split()[0].lower() in n.lower(): continue
                    if len(n) > 8:
                        logger.info(f"DDG Muttergesellschaft fuer '{company_name}': {n}")
                        return n
        return None

    def _extract_gf_from_statement_text(self, text: str):
        """Extrahiert GF-Namen aus Jahresabschluss-Volltext (Anhang-Abschnitt)."""
        import re as _re
        # Muster: "waren: Herr Florian Wessling, Muenster Herr Daniel Luellmann. Bremen"
        # Extrahiere Namen mit "Herr" / "Frau" Prefix
        gf_section = _re.search(
            r"Gesch.ftsf.hrer[^:]{0,100}:([^*]{0,600}?)(?:Prokuristen|Beirat|Aufsichtsrat|\*\*|\n\n)",
            text, _re.I | _re.S)
        if gf_section:
            raw = gf_section.group(1)
            names = _re.findall(r"(?:Herr|Frau)\s+([A-Z][\w\-]+\s+[A-Z][\w\-]+)", raw)
            if names:
                result = ", ".join(names[:4])
                return result
        return None

    def _extract_parent_from_statement_text(self, text: str):
        """Extrahiert Muttergesellschaft/Kommanditistin aus Jahresabschluss-Text."""
        import re as _re
        # Muster 1: "alleinigen Kommanditistin, WESSLING GmbH (AG Steinfurt HRB 1953)"
        m = _re.search(
            r"(?:alleinigen?|einzigen?)\s+Kommanditist(?:in)?[,\s]+([A-Z][\w\s&\.\-]{3,60}?GmbH(?:\s+&\s+Co\.\s+KG)?)",
            text, _re.I)
        if m: return m.group(1).strip().rstrip(",(")
        # Muster 2: "Mutterunternehmens, der WESSLING Holding GmbH & Co. KG"
        m = _re.search(
            r"Mutterunternehmen[s,\s]{1,5}(?:der\s+)?([A-Z][\w\s&\.\-]{3,60}?(?:GmbH|AG|KG|SE)(?:\s+&\s+Co\.\s+KG)?)",
            text, _re.I)
        if m: return m.group(1).strip().rstrip(",(")
        # Muster 3: "Kommanditeinlage ... Kommanditistin WESSLING GmbH"
        m = _re.search(
            r"Kommanditist(?:in)?\s+(?:ist\s+)?(?:die\s+)?([A-Z][\w\s&\.\-]{3,60}?(?:GmbH|AG|KG|SE)(?:\s+&\s+Co\.\s+KG)?)",
            text, _re.I)
        if m: return m.group(1).strip().rstrip(",(")
        return None


    def get_publications(self, q: str) -> Optional[List[Any]]:
        """Holt Unternehmens-Bekanntmachungen aus HR.ai (5 Credits).
        Liefert: Datum, Typ, Titel der Bundesanzeiger-Veroeffentlichungen.
        Nuetzlich fuer: Transparenz-Check, Rechtsform-Aenderungen, Eigentuemerhistorie."""
        if not self.is_available(): return None
        try:
            data = self._get(q, "publications")
            if not data: return None
            pubs = data.get("publications") or data.get("announcements") or []
            if not isinstance(pubs, list): return None
            result = []
            for p in pubs[:25]:
                if not isinstance(p, dict): continue
                result.append({
                    "date": str(p.get("date") or p.get("publication_date") or ""),
                    "type": str(p.get("type") or p.get("category") or p.get("kind") or ""),
                    "title": str(p.get("title") or p.get("subject") or p.get("name") or ""),
                    "source": str(p.get("source") or p.get("publisher") or "Bundesanzeiger"),
                })
            logger.info(f"publications: {len(result)} Eintraege fuer '{q}'")
            return result if result else None
        except Exception as e:
            logger.warning(f"get_publications Fehler: {e}")
            return None

    def get_news(self, q: str) -> Optional[List[Any]]:
        """Holt aktuelle Pressemeldungen aus HR.ai (10 Credits).
        Liefert: Titel, Datum, Quelle, URL der Pressemitteilungen.
        Nuetzlich fuer: Oeffentlichkeitsbild, M&A-Aktivitaeten, Personalwechsel."""
        if not self.is_available(): return None
        try:
            data = self._get(q, "news")
            if not data: return None
            articles = (data.get("news") or data.get("articles") or
                       data.get("press_releases") or data.get("media") or [])
            if not isinstance(articles, list): return None
            result = []
            for a in articles[:15]:
                if not isinstance(a, dict): continue
                result.append({
                    "date": str(a.get("date") or a.get("published_at") or a.get("publication_date") or ""),
                    "title": str(a.get("title") or a.get("headline") or a.get("subject") or ""),
                    "source": str(a.get("source") or a.get("publisher") or a.get("outlet") or ""),
                    "url": str(a.get("url") or a.get("link") or ""),
                    "summary": str(a.get("summary") or a.get("excerpt") or a.get("body") or "")[:300],
                })
            logger.info(f"news: {len(result)} Artikel fuer '{q}'")
            return result if result else None
        except Exception as e:
            logger.warning(f"get_news Fehler: {e}")
            return None

    def _enrich_balance_sheet(self, f: FinancialData, data: dict):
        bs_years = data.get("balance_sheet_accounts") or []
        if not isinstance(bs_years, list) or not bs_years:
            return
        bs_years_sorted = sorted(bs_years, key=lambda x: x.get("year", 0), reverse=True)
        accounts = bs_years_sorted[0].get("balance_sheet_accounts") or []
        if not accounts:
            return

        def sf(v):
            try: return float(v) if v is not None else None
            except: return None

        def get_label(acc):
            n = acc.get("name") or {}
            if isinstance(n, dict):
                return (n.get("in_report") or n.get("de") or n.get("en") or "").lower()
            return str(n).lower()

        def find_equity(items):
            for item in items:
                lbl = get_label(item)
                val = sf(item.get("value"))
                # Direkt-Treffer auf oberster Ebene
                if lbl.startswith("a. eigen") or lbl == "eigenkapital":
                    return val
                # Rekursiv in children suchen
                children = item.get("children") or []
                for child in children:
                    c_lbl = get_label(child)
                    if c_lbl.startswith("a. eigen") or c_lbl == "eigenkapital" or "eigenkapital" in c_lbl:
                        return sf(child.get("value"))
                # Tiefer rekursieren wenn noch keine Treffer
                result = find_equity(children)
                if result is not None:
                    return result
            return None

        # accounts[0] = Aktiva → Bilanzsumme
        bilanzsumme = sf(accounts[0].get("value")) if accounts else None
        # Eigenkapital rekursiv aus Passiva-Seite holen
        eigenkapital = find_equity(accounts)

        logger.info(
            f"HR.ai BS: Bilanzsumme={bilanzsumme}, EK={eigenkapital}, "
            f"top_labels={[get_label(a)[:25] for a in accounts]}"
        )
        if bilanzsumme:
            f.bilanzsumme = bilanzsumme
        if eigenkapital is not None:
            f.eigenkapital = eigenkapital
        self._calc_ratios(f)

    @staticmethod
    def _calc_ratios(f: FinancialData):
        if f.eigenkapital is not None and f.bilanzsumme and f.bilanzsumme > 0:
            f.eigenkapitalquote = round(f.eigenkapital / f.bilanzsumme * 100, 2)
            fremdkapital = f.bilanzsumme - f.eigenkapital
            if f.eigenkapital > 0:
                f.verschuldungsgrad = round(fremdkapital / f.eigenkapital, 2)

    def _add_revenue_forecast(self, f: FinancialData, kpi_sorted: list):
        try:
            current_year = datetime.now().year
            latest_year = kpi_sorted[0].get("year", 0)
            if latest_year >= current_year:
                return
            revenues = [(k["year"], float(k["revenue"])) for k in kpi_sorted
                        if k.get("year") and k.get("revenue") and float(k.get("revenue", 0)) > 0]
            if not revenues:
                return
            revenues_asc = sorted(revenues, key=lambda x: x[0])
            if len(revenues_asc) == 1:
                f.umsatz_prognose = round(revenues_asc[0][1], 2)
                f.umsatz_prognose_jahr = str(current_year)
                return
            y_start, r_start = revenues_asc[0]
            y_end, r_end = revenues_asc[-1]
            years_diff = y_end - y_start
            if years_diff <= 0 or r_start <= 0:
                return
            cagr = (r_end / r_start) ** (1 / years_diff) - 1
            cagr = max(-0.30, min(0.50, cagr))
            prognose = r_end * ((1 + cagr) ** (current_year - latest_year))
            f.umsatz_prognose = round(prognose, 2)
            f.umsatz_prognose_jahr = str(current_year)
            logger.info(f"Umsatzprognose {current_year}: {f.umsatz_prognose:,.0f} EUR (CAGR {cagr:.1%})")
        except Exception as e:
            logger.warning(f"Umsatzprognose Fehler: {e}")


class InsolvenzChecker:
    URL = "https://www.insolvenzbekanntmachungen.de/cgi-bin/bl_recherche.pl"

    def check(self, company_name: str) -> CompanyInfo:
        info = CompanyInfo(negativmerkmale_quelle="insolvenzbekanntmachungen.de")
        try:
            search_term = company_name.split(" ")[0]
            payload = {"Ger_Name": search_term, "Ger_Ort": "", "Land": "0",
                       "Gericht": "", "Art": "2", "Absatz": "0",
                       "select_Registergericht": "0", "button2": "Suchen"}
            headers = {"User-Agent": "Mozilla/5.0 (compatible; OpenRisk/2.2)", "Accept-Language": "de-DE,de;q=0.9"}
            r = requests.post(self.URL, data=payload, headers=headers, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
            table = soup.find("table", {"class": "result"}) or soup.find("table")
            if not table:
                return info
            rows = table.find_all("tr")[1:]
            name_tokens = [t.lower() for t in company_name.split() if len(t) > 3]
            for row in rows:
                cells = row.find_all("td")
                if len(cells) < 2:
                    continue
                row_text = " ".join(c.get_text(strip=True) for c in cells)
                if sum(1 for t in name_tokens if t in row_text.lower()) >= 2:
                    info.insolvenz = True
                    dm = re.search(r"\d{2}\.\d{2}\.\d{4}", row_text)
                    if dm:
                        info.insolvenz_datum = dm.group(0)
                    info.negativmerkmale.append(f"Insolvenzverfahren: {row_text[:120]}")
                    break
            if not info.insolvenz:
                logger.info(f"Insolvenzcheck {company_name!r}: Kein Eintrag")
        except Exception as e:
            logger.warning(f"Insolvenzcheck Fehler: {e}")
        return info

    def check_persons(self, gf_namen: str) -> int:
        """Prueft GF-Namen auf persoenliche Insolvenz. Gibt gf_score 0-10 zurueck.
        Kein Treffer = 9, 1 Treffer = 2, Fehler = 7 (neutral)."""
        if not gf_namen: return 7
        namen = [n.strip() for n in gf_namen.split(",") if n.strip()]
        if not namen: return 7
        treffer = 0
        for name in namen[:3]:  # max 3 GF pruefen
            try:
                parts = name.split()
                if len(parts) < 2: continue
                payload = {"Ger_Name": parts[-1], "Ger_Ort": "", "Land": "0",
                           "Gericht": "", "Art": "4",  # Art=4: Verbraucher/Restschuldbefreiung
                           "Absatz": "0", "select_Registergericht": "0", "button2": "Suchen"}
                headers = {"User-Agent": "Mozilla/5.0 (compatible; OpenRisk/2.5)", "Accept-Language": "de-DE,de;q=0.9"}
                r = requests.post(self.URL, data=payload, headers=headers, timeout=10)
                soup = BeautifulSoup(r.text, "html.parser")
                table = soup.find("table", {"class": "result"}) or soup.find("table")
                if not table: continue
                rows = table.find_all("tr")[1:]
                name_tokens = [t.lower() for t in parts if len(t) > 2]
                for row in rows:
                    cells = row.find_all("td")
                    if len(cells) < 2: continue
                    row_text = " ".join(c.get_text(strip=True) for c in cells).lower()
                    if sum(1 for t in name_tokens if t in row_text) >= 2:
                        treffer += 1; break
            except Exception as e:
                logger.warning(f"GF-Insolvenzcheck Fehler ({name}): {e}")
        if treffer == 0: return 9   # Kein Treffer = gut
        if treffer == 1: return 2   # 1 Treffer = kritisch
        return 0                    # Mehrere Treffer = sehr kritisch

    def check_persons_extended(self, gf_namen: str, company_name: str = "") -> dict:
        """Erweiterter GF-Check: Insolvenz (insolvenzbekanntmachungen.de) +
        Presse-Screening (DuckDuckGo HTML) fuer Negativmerkmale.
        Score-Logik: Start 9 (clean), Abzuege je Befund.
        Returns {"score": 0-10, "details": [...], "quellen": [...]}
        """
        if not gf_namen: return {"score": 7, "details": ["Keine GF-Namen"], "quellen": []}
        namen = [n.strip() for n in gf_namen.split(",") if n.strip()]
        if not namen: return {"score": 7, "details": [], "quellen": []}
        score = 9  # Ausgangspunkt: keine Negativmerkmale bekannt
        details = []
        quellen = set()
        for name in namen[:3]:
            parts = name.split()
            if len(parts) < 2:
                details.append(f"{name}: zu kurz fuer Suche (Vorname + Nachname erforderlich)")
                continue
            # ── A: Insolvenz-Historien zaehlen (Firmen + persoenlich) ───────────
            try:
                # Suche mit Art=2 (Unternehmensinsolvenz), zaehle alle Treffer fuer diese Person
                insolv_count = 0
                for art, art_label in [("2", "Unternehmensinsolvenz"), ("4", "Restschuldbefreiung")]:
                    payload_i = {"Ger_Name": parts[-1], "Ger_Ort": "", "Land": "0",
                                 "Gericht": "", "Art": art, "Absatz": "0",
                                 "select_Registergericht": "0", "button2": "Suchen"}
                    headers_i = {"User-Agent": "Mozilla/5.0 (compatible; OpenRisk/2.5)",
                                 "Accept-Language": "de-DE,de;q=0.9"}
                    r_i = requests.post(self.URL, data=payload_i, headers=headers_i, timeout=10)
                    soup_i = BeautifulSoup(r_i.text, "html.parser")
                    table_i = soup_i.find("table", {"class": "result"}) or soup_i.find("table")
                    if not table_i: continue
                    rows_i = table_i.find_all("tr")[1:]
                    name_tokens_i = [t.lower() for t in name.split() if len(t) > 2]
                    for row_i in rows_i:
                        cells_i = row_i.find_all("td")
                        if len(cells_i) < 2: continue
                        row_text_i = " ".join(c.get_text(strip=True) for c in cells_i).lower()
                        if sum(1 for t in name_tokens_i if t in row_text_i) >= 2:
                            insolv_count += 1
                            details.append(f"FUND ({art_label}): {name!r} in Insolvenzbekanntmachung gefunden")
                quellen.add("insolvenzbekanntmachungen.de")
                if insolv_count == 0:
                    details.append(f"OK: Kein Insolvenz-Eintrag fuer {name!r}")
                elif insolv_count == 1:
                    score = min(score, 4)
                    details.append(f"HINWEIS: 1 Insolvenz-Eintrag fuer {name!r} (einmalig, nicht ungewoehnlich)")
                else:
                    score = min(score, 2)
                    details.append(f"ALARM: {insolv_count} Insolvenz-Eintraege fuer {name!r} — serielle Insolvenzhistorie")
            except Exception as e:
                logger.warning(f"GF-Insolvenz {name}: {e}")
            # ── B: Presse-Screening via DuckDuckGo HTML ────────────────────────
            # Negativ-Suchbegriffe in zwei Laeufen (schwer / mittel)
            _NEG_SCHWER = ["insolvenz betrug", "strafverfahren verurteilt", "haftbefehl"]
            _NEG_MITTEL = ["insolvenz geschaeftsfuehrer", "negative presse"]
            _headers = {"User-Agent": "Mozilla/5.0 (compatible; OpenRisk/2.5)"}
            for suchbegriff, gewicht in [*[(t, "schwer") for t in _NEG_SCHWER],
                                          *[(t, "mittel") for t in _NEG_MITTEL]]:
                try:
                    # Mit Unternehmenskontext: schraenkt auf relevante Person ein
                    ctx = f' "{company_name.split()[0]}"' if company_name else ""
                    q = requests.utils.quote(f'"{name}"{ctx} {suchbegriff}')
                    url = f"https://html.duckduckgo.com/html/?q={q}&kl=de-de"
                    r = requests.get(url, headers=_headers, timeout=8)
                    soup = BeautifulSoup(r.text, "html.parser")
                    # Ergebnis-Snippets extrahieren
                    result_divs = soup.find_all("div", {"class": "result__body"}) or []
                    result_links = soup.find_all("a", {"class": "result__url"}) or []
                    # Snippet-Validierung: Nachname muss im Kontext des Suchbegriffs stehen
                    nachname = parts[-1].lower()
                    confirmed_hits = 0
                    all_snippets = [d.get_text(" ", strip=True).lower() for d in (result_divs or [])]
                    for snippet in all_snippets:
                        # Beide Terme muessen im selben Snippet vorkommen
                        if nachname in snippet and any(t in snippet for t in suchbegriff.split()):
                            confirmed_hits += 1
                    quellen.add("DuckDuckGo-Presse")
                    # Hoehere Schwellen: 5 bestaetigte Snippets fuer schwer, 7 fuer mittel
                    threshold = 5 if gewicht == "schwer" else 7
                    if confirmed_hits >= threshold:
                        if gewicht == "schwer":
                            score = min(score, 2)
                            details.append(f"KRITISCH: Presse-Treffer {name!r} + {suchbegriff!r} ({confirmed_hits} validierte Snippets)")
                        else:
                            score = min(score, 5)
                            details.append(f"HINWEIS: Presse-Treffer {name!r} + {suchbegriff!r} ({confirmed_hits} Snippets)")
                        break
                    else:
                        details.append(f"OK: Kein belastbarer Presse-Fund fuer {name!r} + {suchbegriff!r} ({confirmed_hits} Snippet-Treffer < {threshold})")
                except Exception as e:
                    logger.warning(f"GF-Presse {name} / {suchbegriff}: {e}")
        final_score = max(0, score)
        # Alarm: mehrfache Insolvenzhistorie oder kritischer Presse-Fund
        alarm_entries = [d for d in details if "ALARM:" in d or ("KRITISCH:" in d and "Presse" in d)]
        alarm = len(alarm_entries) > 0
        alarm_text = "; ".join(alarm_entries) if alarm else ""
        logger.info(f"GF-Extended-Check: Score={final_score}, Alarm={alarm}, Details={details}")
        return {"score": final_score, "details": details,
                "quellen": sorted(quellen), "alarm": alarm, "alarm_text": alarm_text}


class BundesanzeigerScraper:
    BASE_URL = "https://www.bundesanzeiger.de"
    HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
               "Accept-Language": "de-DE,de;q=0.9", "Referer": "https://www.bundesanzeiger.de/"}

    def _session(self):
        s = requests.Session()
        s.cookies["cc"] = "1628606977-805e172265bfdbde-10"
        s.headers.update(self.HEADERS)
        try:
            s.get(self.BASE_URL, timeout=15)
            s.get(f"{self.BASE_URL}/pub/de/start?0", timeout=15)
        except:
            pass
        return s

    def _fetch_content(self, s, url):
        try:
            r = s.get(url, timeout=30)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")
            pub = soup.find("div", {"class": "publication_container"})
            return pub.get_text(separator=" ", strip=True) if pub else None
        except:
            return None

    def get_reports(self, company_name: str, max_reports=5):
        s = self._session()
        url = (f"{self.BASE_URL}/pub/de/start?0-2.-top%7Econtent%7Epanel-left%7Ecard-form="
               f"&fulltext={requests.utils.quote(company_name)}&area_select=&search_button=Suchen")
        try:
            r = s.get(url, timeout=30)
            soup = BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            raise ConnectionError(f"Bundesanzeiger nicht erreichbar: {e}")
        wrapper = soup.find("div", {"class": "result_container"})
        if not wrapper:
            return {}
        entries = []
        for row in wrapper.find_all("div", {"class": "row"}):
            info_div = row.find("div", {"class": "info"})
            if not info_div:
                continue
            link = info_div.find("a")
            if not link:
                continue
            href = link.get("href", "")
            if href and not href.startswith("http"):
                href = self.BASE_URL + href
            date_el = row.find("div", {"class": "date"})
            date_str = date_el.contents[0].strip() if date_el and date_el.contents else ""
            date_p = dateparser.parse(date_str, languages=["de"])
            co_el = row.find("div", {"class": "first"})
            co = co_el.contents[0].strip() if co_el and co_el.contents else ""
            entries.append({"name": link.contents[0].strip() if link.contents else "",
                            "date": date_p, "company": co, "content_url": href})
        def prio(e):
            n = e["name"].lower()
            return 0 if "jahresabschluss" in n else 1 if "konzernabschluss" in n else 2 if "bilanz" in n else 9
        entries = sorted(entries, key=prio)[:max_reports]
        import json as _j
        results = {}
        for e in entries:
            text = self._fetch_content(s, e["content_url"])
            if not text:
                continue
            h = hashlib.md5(_j.dumps({"n": e["name"], "c": e["company"]}, sort_keys=True).encode()).hexdigest()
            results[h] = {**e, "report": text}
        return results


class FinancialTextParser:
    PATTERNS = {
        "bilanzsumme": [r"Bilanzsumme\s*[:\.]?\s*(?:EUR|€)?\s*([0-9]{2,3}(?:\.[0-9]{3})+(?:,[0-9]{2})?)"],
        "eigenkapital": [r"(?:Summe\s+)?Eigenkapital\s*[:\.]?\s*(?:EUR|€)?\s*([0-9][0-9.,]+)"],
        "umsatz": [r"Umsatzerlöse?\s*[:\.]?\s*(?:EUR|€)?\s*([0-9][0-9.,]+)"],
        "jahresergebnis": [r"Jahresüberschuss\s*[:\.]?\s*(?:EUR|€)?\s*(-?[0-9][0-9.,]+)",
                           r"Jahresfehlbetrag\s*[:\.]?\s*(?:EUR|€)?\s*(-?[0-9][0-9.,]+)"],
        "mitarbeiter": [r"(?:Anzahl\s+)?Mitarbeiter(?:innen|zahl)?\s*[:\.]?\s*(\d[\d.,]*)"],
    }
    def parse(self, text: str) -> FinancialData:
        f = FinancialData()
        f.geschaeftsjahr = self._year(text)
        teur = bool(re.search(r"(?:TEUR|T€)", text, re.I))
        for field, pats in self.PATTERNS.items():
            for pat in pats:
                m = re.search(pat, text, re.I | re.M)
                if m:
                    v = self._num(m.group(1))
                    if v is not None:
                        if teur and field != "mitarbeiter": v *= 1000
                        if field == "mitarbeiter": v = int(v)
                        setattr(f, field, v)
                    break
        HandelsregisterClient._calc_ratios(f)
        return f
    def _num(self, s):
        if not s: return None
        s = s.strip()
        neg = s.endswith("-")
        if neg: s = s[:-1]
        s = s.replace(".", "").replace(",", ".") if "," in s else s.replace(".", "")
        try:
            v = float(s); return -abs(v) if neg else v
        except: return None
    def _year(self, text):
        m = re.search(r"(?:zum|per)\s+31\.\s*12\.\s*(\d{4})", text, re.I)
        if m: return m.group(1)
        yrs = re.findall(r"(20\d{2})", text)
        if yrs:
            from collections import Counter
            return Counter(yrs).most_common(1)[0][0]
        return None


    # ── GF-Namen aus Freitext (Bundesanzeiger / Jahresabschluss) ─────────────
    _GF_PATTERNS = [
        # Jahresabschluss-Format: "Geschaeftsfuehrer ... waren: Herr Florian Wessling, Muenster Herr Daniel Luellmann"
        r"(?:Gesch.ftsf.hrer|Prokuristen?)(?:[^:]{0,80})?:\s*((?:Herr|Frau)\s+[A-Z][\w\-]{1,20}\s+[A-Z][\w\-]{2,25}(?:[^\n]{0,80}(?:Herr|Frau)\s+[A-Z][\w\-]{1,20}\s+[A-Z][\w\-]{2,25})*)",
        # "Geschaeftsfuehrer: Max Mustermann, Lisa Schmidt"
        r"Gesch.ftsf.hrer(?:in)?(?:\s*(?:der\s+Gesellschaft)?)?[:\s]+([A-Z][\w\-]{1,20}(?:\s+[A-Z][\w\-]{2,25}){1,3}(?:\s*,\s*[A-Z][\w\-]{1,20}(?:\s+[A-Z][\w\-]{2,25}){1,3})*)",
        # "vertreten durch ihre Geschaeftsfuehrer Max Mustermann"
        r"(?:vertreten\s+durch|durch\s+(?:ihre|seinen?|ihren?))\s+(?:(?:Gesch.ftsf.hrer|Komplement.r)[:\s]+)?([A-Z][\w\-]{1,20}(?:\s+[A-Z][\w\-]{2,25}){1,3})",
        # "Alleiniger Geschaeftsfuehrer ist Max Mustermann"
        r"(?:Alleiniger?\s+)?Gesch.ftsf.hrer\s+ist\s+([A-Z][\w\-]{1,20}(?:\s+[A-Z][\w\-]{2,25}){1,3})",
        # "Max Mustermann (Geschaeftsfuehrer)"
        r"([A-Z][\w\-]{1,20}\s+[A-Z][\w\-]{2,25}(?:\s+[A-Z][\w\-]{2,25})?)\s*[\(\[]\s*(?:Gesch.ftsf.hrer|GF\b|Managing)",
    ]
    _GF_STOPWORDS = frozenset([
        "GmbH","KG","AG","SE","Ltd","Inc","Corp","Consulting","Engineering","Holding","Group",
        "Management","Services","Solutions","Technology","Deutschland","Germany","Verwaltungs",
        "Gesellschaft","Kommanditgesellschaft","Aktiengesellschaft","Jahresabschluss","Lagebericht",
        "Bundesanzeiger","Bilanz","Jahresbericht","Steinfurt","Altenberge","Nordrhein","Westfalen",
    ])

    def extract_gf_names_from_text(self, text):
        # type: (str) -> Optional[str]
        """Extrahiert GF-Namen aus Freitext (Bundesanzeiger-Jahresabschluss, Lagebericht etc.)"""
        if not text: return None
        found_names = []
        seen = set()
        for pat in self._GF_PATTERNS:
            for m in re.finditer(pat, text, re.M | re.UNICODE):
                raw = m.group(1).strip()
                candidates = [c.strip() for c in re.split(r'\s*,\s*|\s+und\s+', raw)]
                for cand in candidates:
                    parts = cand.split()
                    if len(parts) < 2: continue
                    if any(p in self._GF_STOPWORDS for p in parts): continue
                    if len(cand) < 6: continue
                    if not all(p[0].isupper() for p in parts if p): continue
                    key = cand.lower()
                    if key not in seen:
                        seen.add(key)
                        found_names.append(cand)
        if found_names:
            result = ", ".join(found_names[:4])
            import logging; logging.getLogger("openrisk").info("GF-Namen aus BA-Text: %s", result)
            return result
        return None


hr_client = HandelsregisterClient()
ba_scraper = BundesanzeigerScraper()
text_parser = FinancialTextParser()
insolvenz_checker = InsolvenzChecker()


def _merge(primary: FinancialData, fallback: FinancialData) -> FinancialData:
    m = FinancialData(
        bilanzsumme=primary.bilanzsumme or fallback.bilanzsumme,
        eigenkapital=primary.eigenkapital if primary.eigenkapital is not None else fallback.eigenkapital,
        umsatz=primary.umsatz or fallback.umsatz,
        jahresergebnis=primary.jahresergebnis if primary.jahresergebnis is not None else fallback.jahresergebnis,
        mitarbeiter=primary.mitarbeiter or fallback.mitarbeiter,
        geschaeftsjahr=primary.geschaeftsjahr or fallback.geschaeftsjahr,
        gruendungsjahr=primary.gruendungsjahr, rechtsform=primary.rechtsform,
        umsatz_prognose=primary.umsatz_prognose, umsatz_prognose_jahr=primary.umsatz_prognose_jahr,
        quelle=f"{primary.quelle} + {fallback.quelle}",
    )
    HandelsregisterClient._calc_ratios(m)
    if m.umsatz and m.mitarbeiter and m.mitarbeiter > 0:
        m.umsatz_pro_mitarbeiter = round(m.umsatz / m.mitarbeiter, 2)
    return m


@app.get("/")
async def root():
    return {"status": "ok", "service": "OpenRisk AI Backend", "version": VERSION,
            "primary": "handelsregister.ai" if hr_client.is_available() else "Bundesanzeiger"}

@app.get("/api/health")
async def health():
    return {"status": "ok", "version": VERSION,
            "handelsregister_api": "aktiv" if hr_client.is_available() else "kein API-Key"}

@app.post("/api/company/lookup", response_model=ScoringInput)
async def lookup_company(request: CompanyRequest):
    name = request.name.strip()
    if not name:
        raise HTTPException(status_code=400, detail="Firmenname fehlt.")
    hr_data, hr_name = None, None
    if hr_client.is_available():
        hr_data, hr_name = hr_client.search(name, hr_nummer=request.handelsregister_nr)
    ba_data, raw_text, company_found = None, None, hr_name or name
    if hr_data is None or hr_data.umsatz is None:
        try:
            reports = ba_scraper.get_reports(name)
        except ConnectionError:
            if hr_data is None:
                raise HTTPException(status_code=503, detail="Keine Datenquelle erreichbar.")
            reports = {}
        if reports:
            best = None
            for kws in [["jahresabschluss"], ["bilanz", "konzernabschluss", "lagebericht"]]:
                for r in reports.values():
                    if any(k in r.get("name", "").lower() for k in kws):
                        if best is None or (r.get("date") and best.get("date") and r["date"] > best["date"]):
                            best = r
                if best: break
            if not best and reports:
                best = list(reports.values())[0]
            if best:
                raw_text = best.get("report", "")
                if not hr_name:
                    company_found = best.get("company", name)
                ba_data = text_parser.parse(raw_text)
                ba_data.quelle = "Bundesanzeiger"
    if hr_data and ba_data:
        financials = _merge(hr_data, ba_data)
    elif hr_data:
        financials = hr_data
    elif ba_data:
        financials = ba_data
    else:
        raise HTTPException(status_code=404, detail=f"Keine Daten fuer {name!r} gefunden.")
    company_info = insolvenz_checker.check(company_found)
    logger.info(f"{company_found}: Umsatz={financials.umsatz}, BS={financials.bilanzsumme}, "
                f"EK-Q={financials.eigenkapitalquote}%, VG={financials.verschuldungsgrad}, "
                f"Prognose={financials.umsatz_prognose}, Insolvenz={company_info.insolvenz}")
    return ScoringInput(company_name=company_found, financials=financials,
                        company_info=company_info, raw_text=raw_text[:2000] if raw_text else None)

@app.get("/api/debug/raw-hr")
async def debug_raw_hr(name: str, feature: str = "balance_sheet_accounts"):
    if not hr_client.is_available():
        return {"error": "Kein HANDELSREGISTER_API_KEY"}
    return {"feature": feature, "query": name, "response": hr_client.get_raw(name, feature)}


# --- SCORING ENGINE v2.1 ---

import math as _math

_GEW = {"insolvenz":10,"eigenkapitalquote":10,"verschuldungsgrad":4,"liquiditaet":7,
        "ergebnismarge":7,"verlustentwicklung":7,"kosten_pro_ma":5,"zahlungsweise":20,
        "branchenrisiko":5,"branchenvergleich_peer":4,"investorenstruktur":4,
        "konzernstruktur":3,"gf_bonitaet":4,"rechtsform":2,"unternehmensalter":1,
        "mitarbeiterzahl":1,"umsatz_pro_ma":1,"presse":5}  # sum=100 (18 dims, Zahlung=20%)

_LABELS = {"insolvenz":"Insolvenz / Negativmerkmale","eigenkapitalquote":"Eigenkapitalquote (bereinigt)",
           "verschuldungsgrad":"Verschuldungsgrad (FK/EK)","liquiditaet":"Liquiditaet I. Grades",
           "ergebnismarge":"Ergebnismarge","verlustentwicklung":"Verlustentwicklung",
           "kosten_pro_ma":"Kosten / Mitarbeiter","branchenrisiko":"Branchenrisiko",
           "branchenvergleich_peer":"Branchenvergleich (Peer-Perzentil)","investorenstruktur":"Investorenstruktur",
           "konzernstruktur":"Konzernstruktur / Gesellschafter","gf_bonitaet":"GF-Bonitaet (Personencheck)",
           "rechtsform":"Rechtsform","unternehmensalter":"Unternehmensalter","mitarbeiterzahl":"Mitarbeiterzahl",
           "umsatz_pro_ma":"Umsatz / Mitarbeiter","presse":"Presse / Sentiment",
           "zahlungsweise":"Zahlungsrisiko (KPI-abgeleitet)"}

# WZ-Branchen-Referenzdaten (Medianwerte fuer Peer-Vergleich)
# Quelle: Destatis/Bundesbank Unternehmensstatistik, eigene Kalibrierung
_WZ_REFS = {
    "71.12":  {"ek_med":12.0,"vg_med":7.0,"marge_med":2.5,"pd":1.84,"name":"Ingenieurbueros (WZ 71.12)"},
    "71":     {"ek_med":15.0,"vg_med":6.0,"marge_med":3.0,"pd":1.75,"name":"Architektur/Ingenieurbueros (WZ 71)"},
    "62":     {"ek_med":32.0,"vg_med":2.5,"marge_med":10.0,"pd":1.10,"name":"IT-Dienstleistungen (WZ 62)"},
    "63":     {"ek_med":28.0,"vg_med":3.0,"marge_med":8.0,"pd":1.20,"name":"IT-Infodienste (WZ 63)"},
    "41":     {"ek_med":20.0,"vg_med":8.0,"marge_med":5.0,"pd":2.50,"name":"Hochbau (WZ 41)"},
    "42":     {"ek_med":18.0,"vg_med":9.0,"marge_med":4.0,"pd":2.80,"name":"Tiefbau (WZ 42)"},
    "43":     {"ek_med":14.0,"vg_med":10.0,"marge_med":3.5,"pd":3.20,"name":"Ausbaugewerbe (WZ 43)"},
    "45":     {"ek_med":16.0,"vg_med":8.0,"marge_med":2.5,"pd":2.10,"name":"KFZ-Handel/-Reparatur (WZ 45)"},
    "46":     {"ek_med":15.0,"vg_med":9.0,"marge_med":1.8,"pd":2.30,"name":"Grosshandel (WZ 46)"},
    "47":     {"ek_med":18.0,"vg_med":6.0,"marge_med":2.5,"pd":1.90,"name":"Einzelhandel (WZ 47)"},
    "55":     {"ek_med":12.0,"vg_med":12.0,"marge_med":3.0,"pd":3.50,"name":"Beherbergung (WZ 55)"},
    "56":     {"ek_med":10.0,"vg_med":14.0,"marge_med":2.5,"pd":4.00,"name":"Gastronomie (WZ 56)"},
    "68":     {"ek_med":35.0,"vg_med":5.0,"marge_med":15.0,"pd":1.50,"name":"Grundstueck/Wohnungswesen (WZ 68)"},
    "69":     {"ek_med":28.0,"vg_med":3.0,"marge_med":12.0,"pd":1.00,"name":"Rechts-/Steuerberatung (WZ 69)"},
    "70":     {"ek_med":25.0,"vg_med":4.0,"marge_med":10.0,"pd":1.20,"name":"Unternehmensberatung (WZ 70)"},
    "72":     {"ek_med":40.0,"vg_med":2.0,"marge_med":8.0,"pd":0.90,"name":"Forschung/Entwicklung (WZ 72)"},
    "73":     {"ek_med":22.0,"vg_med":4.0,"marge_med":8.0,"pd":1.30,"name":"Werbung/Marktforschung (WZ 73)"},
    "74":     {"ek_med":20.0,"vg_med":5.0,"marge_med":7.0,"pd":1.50,"name":"Sonstige wirtsch. DL (WZ 74)"},
    "77":     {"ek_med":18.0,"vg_med":8.0,"marge_med":6.0,"pd":2.00,"name":"Vermietung (WZ 77)"},
    "85":     {"ek_med":20.0,"vg_med":5.0,"marge_med":4.0,"pd":1.20,"name":"Bildung (WZ 85)"},
    "86":     {"ek_med":22.0,"vg_med":5.0,"marge_med":3.5,"pd":1.00,"name":"Gesundheitswesen (WZ 86)"},
    "default":{"ek_med":18.0,"vg_med":5.5,"marge_med":3.5,"pd":1.88,"name":"Deutschland Gesamt"},
}

def _get_wz_ref(wz_code):
    """Lookup WZ-Referenzwerte. Probiert exakt, dann 2-stellig, dann default."""
    if not wz_code:
        return _WZ_REFS["default"]
    wz = str(wz_code).strip()
    if wz in _WZ_REFS:
        return _WZ_REFS[wz]
    # 2-stellig (z.B. "71" aus "71.12.1")
    parts = wz.split(".")
    if parts[0] in _WZ_REFS:
        return _WZ_REFS[parts[0]]
    # Praefix-Match
    for k in _WZ_REFS:
        if k != "default" and wz.startswith(k):
            return _WZ_REFS[k]
    return _WZ_REFS["default"]

class ScoringRequest(BaseModel):
    company_name: str
    rechtsform: Optional[str] = "GmbH"
    bilanzsumme: Optional[float] = None
    eigenkapital: Optional[float] = None
    fremdkapital: Optional[float] = None
    umsatz: Optional[float] = None
    jahresergebnis: Optional[float] = None
    mitarbeiter: Optional[int] = None
    loehne_gehaelter: Optional[float] = None
    fluessige_mittel: Optional[float] = None
    forderungen: Optional[float] = None
    kurzfristiges_fk: Optional[float] = None
    gruendungsjahr: Optional[int] = None
    branche_risiko: Optional[str] = "medium"
    investoren_score: Optional[int] = 5
    presse_score: Optional[int] = 5
    insolvenz: Optional[bool] = False
    negativmerkmale_anzahl: Optional[int] = 0
    ausschuettungen_avg: Optional[float] = None
    wz_code: Optional[str] = None           # WZ-Klassifikationscode fuer Branchenvergleich
    gf_score: Optional[int] = 5             # GF-Bonitaet 0-10 (5=unbekannt/nicht geprueft, 7=ok, 9-10=top)
    konzern_score: Optional[int] = 5        # Konzernstruktur 0-10 (5=unbekannt)
    gf_namen: Optional[str] = None          # GF-Namen fuer PersonenInsolvenzCheck (kommagetrennt)
    konzern_info: Optional[str] = None       # Name Muttergesellschaft (wird im Label angezeigt)

class DimensionScore(BaseModel):
    name: str; label_de: str; score_0_10: int; gewichtung_pct: int; beitrag: float; info: str

class HardThresholdItem(BaseModel):
    kennzahl: str; wert: float; schwellenwert: str; risikostufe: str; beschreibung: str

class ScoringResult(BaseModel):
    company_name: str; rechtsform: str
    eigenkapitalquote_pct: Optional[float] = None
    eigenkapital_bereinigt: Optional[float] = None
    verschuldungsgrad: Optional[float] = None
    ergebnismarge_pct: Optional[float] = None
    umsatz_pro_ma: Optional[float] = None
    kosten_pro_ma: Optional[float] = None
    liquiditaet_1: Optional[float] = None
    dimensionen: List[DimensionScore] = []
    rohscore_0_100: float = 0.0; bonitaetsindex: int = 0
    risikoklasse: str = ""; pd_pct: float = 0.0; pd_label: str = ""
    hard_thresholds: List[HardThresholdItem] = []
    kapitalstruktur_risiko: str = "NORMAL"
    ek_bereinigt_angewendet: bool = False; ek_bereinigung_betrag: float = 0.0
    zahlungsweise_bi_optimistisch: Optional[int] = None
    zahlungsweise_bi_wahrscheinlich: Optional[int] = None
    zahlungsweise_bi_pessimistisch: Optional[int] = None
    zahlungsweise_band_note: str = ""
    zahlungsweise_probability_buckets: List[Any] = []     # 5-Bucket Wahrscheinlichkeitsverteilung
    zahlungsweise_expected_bi: Optional[int] = None       # Probability-weighted Erwartungswert BI
    zahlungsweise_score_rationale: str = ""               # Ausfuehrliche Begruendung inkl. Quellen
    konzern_mutter: Optional[str] = None
    konzern_zahlungsmodifikator_faktor: Optional[float] = None  # v2.5.7: Mod-Faktor auf z_prob (1.0=neutral)
    konzern_zahlungsmodifikator_info: str = ""                  # v2.5.7: Erklärungstext
    gf_check_score: Optional[int] = None                  # GF-Bonitaet: berechneter Score
    gf_check_details: List[str] = []                       # Befunde
    gf_check_quellen: List[str] = []                       # gepruef. Quellen
    gf_alarm: bool = False                                  # TRUE wenn mehrfache Insolvenzhistorie
    gf_alarm_text: str = ""                                 # Alarm-Begruendung fuer Bericht

def _is_kg(rf): return "co. kg" in rf.lower() or "co.kg" in rf.lower()
def _pd(s):
    try: return round(100.0/(1.0+_math.exp(-0.0216*(s-475))),2)
    except: return 50.0
def _rk(s):
    for lo,hi,rk,lb in [(100,149,"A","Sehr gut"),(150,199,"B","Gut"),(200,249,"C","Befriedigend"),(250,299,"D","Ausreichend"),(300,349,"E","Erhoehtes Risiko"),(350,449,"F","Kritisch"),(450,549,"G","Sehr kritisch"),(550,600,"H","Hoechstes Ausfallrisiko")]:
        if lo<=s<=hi: return f"{rk} - {lb}"
    return "H - Hoechstes Ausfallrisiko"
def _bereinige(rf,ek,bs,je,avg):
    if not _is_kg(rf): return ek,0.0,False
    if je is not None and bs>0 and je<-(bs*0.02): return ek,0.0,False
    b=avg if (avg and avg>0) else bs*(0.10 if (bs>0 and ek/bs<0.05) else 0.08 if (bs>0 and ek/bs<0.15) else 0.05)
    return ek+b,b,True

_ZAHLUNG_MAX_P = 0.60  # Normierung: P=0.60 -> Score=0; Praxis-Max ~0.34 -> Score~4/10

def _zahlung_prob(ep, vg, liq, mg, je, umsatz):
    """P(Zahlungsproblem) aus Bilanzkennzahlen. Startet bei 0.0.
    Steigt mit KPI-Verschlechterung via: P = 1 - prod(1 - w_i * s_i)
    Faktoren: EK-Quote, Verschuldungsgrad, Liquiditaet, Ergebnismarge, Verlust/Umsatz"""
    factors=[]
    if ep is not None:
        factors.append((0.10, max(0.0,min(1.0,(15.0-ep)/15.0))))    # 0 bei EK>=15%, 1 bei EK<=0%
    if vg is not None and vg>0:
        factors.append((0.10, max(0.0,min(1.0,(vg-2.0)/23.0))))     # 0 bei VG<=2, 1 bei VG>=25
    if liq is not None:
        factors.append((0.08, max(0.0,min(1.0,(1.5-liq)/1.4))))     # 0 bei Liq>=1.5, 1 bei Liq<=0.1
    if mg is not None:
        factors.append((0.07, max(0.0,min(1.0,(2.0-mg)/12.0))))     # 0 bei Marge>=2%, 1 bei Marge<=-10%
    if je is not None and umsatz and umsatz>0:
        r=je/umsatz*100
        factors.append((0.05, max(0.0,min(1.0,-r/5.0)) if r<0 else 0.0))  # 0 bei JE>=0
    if not factors: return 0.0
    p=1.0
    for w,s in factors: p*=(1.0-w*s)
    return round(1.0-p,4)

def _dim(k,rf,ep,vg,liq,mg,je,kpm,br,inv,ma,upm,gj,ins,nm,ps,wz=None,gf=7,kz=5):
    kg=_is_kg(rf)
    if k=="insolvenz":
        if ins: return 0,"Insolvenz"
        if nm>=3: return 2,f"{nm}NM"
        if nm>=1: return 6,f"{nm}NM"
        return 10,"OK"
    if k=="eigenkapitalquote":
        if ep is None: return 5,"?"
        t=[(30,10),(15 if kg else 20,8),(10,6),(5,3 if not kg else 4),(0,1 if not kg else 2)]
        for th,sc in t:
            if ep>=th: return sc,f"EK {ep:.1f}%"
        return 0,f"EK {ep:.1f}%"
    if k=="verschuldungsgrad":
        if vg is None: return 5,"?"
        if vg<0: return 0,f"VG{vg:.1f}"
        for th,sc in [(1,10),(2,8),(5,6),(10,4),(20,2)]:
            if vg<th: return sc,f"VG{vg:.1f}"
        return 1,f"VG{vg:.1f}"
    if k=="liquiditaet":
        if liq is None: return 5,"?"
        for th,sc in [(0.5,10),(0.2,7),(0.1,4)]: 
            if liq>=th: return sc,f"Liq{liq:.2f}"
        return 2,f"Liq{liq:.2f}"
    if k=="ergebnismarge":
        if mg is None: return 5,"?"
        for th,sc in [(5,10),(1,7),(0,5),(-5,2)]:
            if mg>=th: return sc,f"M{mg:.1f}%"
        return 0,f"M{mg:.1f}%"
    if k=="verlustentwicklung":
        if je is None: return 5,"?"
        if je>0: return 10,f"G{je:,.0f}"
        if je>-10000: return 6,"kl.V"
        return 2,f"V{je:,.0f}"
    if k=="kosten_pro_ma":
        if kpm is None: return 5,"?"
        kk=kpm/1000
        for th,sc in [(40,10),(60,8),(80,5),(100,3)]:
            if kk<th: return sc,f"{kk:.0f}k/MA"
        return 1,f"{kk:.0f}k/MA"
    if k=="branchenrisiko": return {"low":(10,"Low"),"medium":(6,"Med"),"high":(3,"High")}.get((br or "medium").lower(),(5,"?"))
    if k=="investorenstruktur": return max(0,min(10,inv or 5)),f"I{inv}"
    if k=="rechtsform":
        r=rf.lower()
        if "ag" in r and "co" not in r: return 9,"AG"
        if "gmbh" in r and "co" not in r: return 9,"GmbH"
        if "co. kg" in r or "co.kg" in r: return 8,"KG"
        if "kg" in r: return 6,"KG"
        if "einzel" in r: return 3,"EU"
        return 7,rf
    if k=="unternehmensalter":
        if gj is None: return 5,"?"
        from datetime import datetime; a=datetime.now().year-gj
        for th,sc in [(20,10),(10,8),(5,6),(3,3)]:
            if a>th: return sc,f"{a}J"
        return 1,f"{a}J"
    if k=="mitarbeiterzahl":
        if not ma: return 5,"?"
        for th,sc in [(250,10),(50,8),(20,6),(5,4)]:
            if ma>th: return sc,f"{ma}MA"
        return 2,f"{ma}MA"
    if k=="umsatz_pro_ma":
        if upm is None: return 5,"?"
        for th,sc in [(500000,9),(200000,7),(100000,5)]:
            if upm>th: return sc,f"{upm/1000:.0f}k/MA"
        return 2,f"{upm/1000:.0f}k/MA"
    if k=="presse": return max(0,min(10,ps or 5)),f"P{ps}"
    if k=="branchenvergleich_peer":
        # Kein Doppelzaehlen: EK/VG/Marge sind schon in eigenen Dims.
        # Peer misst ausschliesslich: Branchen-PD vs. nationaler Durchschnitt (Makro-Risiko-Kontext).
        # Unabhaengige Information: In welcher Branche operiert das Unternehmen relativ zum Markt?
        ref=_get_wz_ref(wz)
        nat_pd=_WZ_REFS["default"]["pd"]   # 1.88% nationaler Schnitt
        ind_pd=ref["pd"]
        rel=(nat_pd-ind_pd)/nat_pd          # positiv=Branche besser als Schnitt
        sc=max(2,min(8,round(5+rel*4)))    # range 2-8, ±3 max
        return sc,f"Branchen-PD={ind_pd:.2f}% vs D-Schnitt={nat_pd:.2f}% ({ref['name'][:18]})"
    if k=="gf_bonitaet":
        sc=max(0,min(10,gf if gf is not None else 7))
        lbl="Personencheck OK" if sc>=8 else "Personencheck neutral" if sc>=5 else "Personencheck kritisch"
        return sc,lbl
    if k=="konzernstruktur":
        sc=max(0,min(10,kz if kz is not None else 5))
        lbl="Eigentuemer sauber" if sc>=8 else "Struktur unbekannt" if sc==5 else "Konzernrisiko erhoeht"
        return sc,lbl
    return 5,"?"
def _ht(ep,vg,rf):
    kg=_is_kg(rf); ht=[]; mr="NORMAL"
    if ep is not None:
        h=3.0 if kg else 5.0; e=8.0 if kg else 10.0
        if ep<h: ht.append(HardThresholdItem(kennzahl="EK-Quote",wert=round(ep,2),schwellenwert=f"<{h}%",risikostufe="HOCH",beschreibung="Substanzverlust")); mr="HOCH"
        elif ep<e: ht.append(HardThresholdItem(kennzahl="EK-Quote",wert=round(ep,2),schwellenwert=f"<{e}%",risikostufe="ERHOEHT",beschreibung="Kritisch")); mr=mr if mr=="HOCH" else "ERHOEHT"
    if vg is not None and vg>0:
        if vg>20: ht.append(HardThresholdItem(kennzahl="Verschuldungsgrad",wert=round(vg,1),schwellenwert=">20",risikostufe="HOCH",beschreibung="Extremes Leverage")); mr="HOCH"
        elif vg>10: ht.append(HardThresholdItem(kennzahl="Verschuldungsgrad",wert=round(vg,1),schwellenwert=">10",risikostufe="ERHOEHT",beschreibung="Hohes Leverage")); mr=mr if mr=="HOCH" else "ERHOEHT"
    return ht,mr

_ALPHA_CONFIRMED = 0.20   # Anteil "guter" Unternehmen mit externer Bestätigung pünktlich
_ALPHA_AUSFALL   = 0.30   # Anteil "problematischer" Unternehmen mit bestätigtem Ausfall

# v2.5.7: Konzern-Zahlungsmodifikator
# Konzern-Score 0-10 → Multiplikator auf z_prob (P(Zahlungsproblem))
# 5 = neutral/unbekannt (kein Einfluss), >5 = Rückhalt reduziert Risiko, <5 = Mutter belastet
_KONZERN_MOD = {0: 1.55, 1: 1.40, 2: 1.28, 3: 1.18, 4: 1.08,
                5: 1.00,
                6: 0.92, 7: 0.84, 8: 0.76, 9: 0.70, 10: 0.64}

def _konzern_zahlung_mod(kz_score: int) -> float:
    """Gibt den Multiplikator für z_prob basierend auf Konzern-Score zurück.
    Kalibrierung: Score 10 (starker gesunder Konzern) reduziert P(Zahlungsproblem) um ~36%.
    Score 0 (insolvente/kritische Mutter) erhöht um ~55%. Score 5 = neutral.
    Quelle: Creditreform Konzernhaftungsanalyse, Moody's Parent-Subsidiary Credit Linkage (2022).
    """
    return _KONZERN_MOD.get(int(max(0, min(10, kz_score))), 1.00)

def _zahlung_buckets(z_prob: float, z_sc: int, bi_opt: int, bi_pess: int, bi_modal: int) -> dict:
    """Kalibrierte 5-Bucket-Wahrscheinlichkeitsverteilung für Zahlungsweise.
    P(schlechter + Ausfall) = z_prob (Kalibrierungsanker aus Bilanzanalyse).
    Funktioniert für beliebige KPI-Profile: je höher z_prob, desto mehr Masse auf bad buckets.
    """
    p = max(0.001, min(0.999, z_prob))
    # Gruppe A (kein Problem): 1-p
    p_puenktlich = round((1-p) * _ALPHA_CONFIRMED, 4)
    p_besser     = round((1-p) * (1-_ALPHA_CONFIRMED) * 0.60, 4)
    p_modal      = round((1-p) * (1-_ALPHA_CONFIRMED) * 0.40, 4)
    # Gruppe B (Problem vorhanden): p
    p_schlechter = round(p * (1 - _ALPHA_AUSFALL), 4)
    p_ausfall    = round(p * _ALPHA_AUSFALL, 4)
    # Normieren
    total = p_puenktlich + p_besser + p_modal + p_schlechter + p_ausfall
    pv = [p_puenktlich/total, p_besser/total, p_modal/total, p_schlechter/total, p_ausfall/total]
    # BI-Midpoints
    bi_besser_mid  = round(bi_opt + (bi_modal - bi_opt) * 0.35)
    bi_modal_mid   = round(bi_opt + (bi_pess - bi_opt) * 0.50)
    bi_schlecht_mid= round(bi_modal + (bi_pess - bi_modal) * 0.65)
    buckets = [
        {"label": "Bestätigt pünktlich",    "z_sc_range": "10",  "bi": bi_opt,         "probability": round(pv[0],3), "bucket_id": "optimistisch"},
        {"label": "Besser als Schätzung",   "z_sc_range": "7–9", "bi": bi_besser_mid,  "probability": round(pv[1],3), "bucket_id": "besser"},
        {"label": "KPI-konform (modal)",     "z_sc_range": "4–6", "bi": bi_modal_mid,   "probability": round(pv[2],3), "bucket_id": "modal"},
        {"label": "Schlechter als Schätzg.","z_sc_range": "1–3", "bi": bi_schlecht_mid,"probability": round(pv[3],3), "bucket_id": "schlechter"},
        {"label": "Bestätigt Ausfall",       "z_sc_range": "0",   "bi": bi_pess,        "probability": round(pv[4],3), "bucket_id": "pessimistisch"},
    ]
    e_bi = round(sum(b["bi"] * b["probability"] for b in buckets))
    return {"buckets": buckets, "expected_bi": e_bi,
            "modal_bi": bi_modal, "p_zahlungsproblem": round(p,4),
            "p_bad_total": round(pv[3]+pv[4],3), "p_good_total": round(pv[0]+pv[1]+pv[2],3)}

def _zahlung_rationale(z_prob: float, z_sc: int, ep, vg, liq, mg, je, umsatz, buckets_result: dict) -> str:
    """Erzeugt prägnante, quellengestützte Scoring-Begründung für Zahlungsrisiko-Dimension."""
    p_pct = round(z_prob * 100, 1)
    e_bi  = buckets_result["expected_bi"]
    p_bad = round(buckets_result["p_bad_total"] * 100, 1)
    p_good= round(buckets_result["p_good_total"] * 100, 1)
    # Treiber identifizieren
    treiber = []
    if ep is not None and ep < 15: treiber.append(f"EK-Quote {ep:.1f}% (Schwellenwert: 15%)")
    if vg is not None and vg > 2:  treiber.append(f"Verschuldungsgrad {vg:.1f}x (Schwellenwert: 2x)")
    if liq is not None and liq < 1.5: treiber.append(f"Liquidität I. Grades {liq:.2f} (Schwellenwert: 1,5)")
    if mg is not None and mg < 2:  treiber.append(f"Ergebnismarge {mg:.1f}% (Schwellenwert: 2%)")
    if je is not None and umsatz and umsatz > 0 and je < 0:
        treiber.append(f"Jahresverlust ({je/umsatz*100:.1f}% des Umsatzes)")
    treiber_str = "; ".join(treiber) if treiber else "keine kritischen Schwellenwert-Unterschreitungen"
    # Empirische Einordnung nach P-Niveau
    if p_pct < 10:
        empirisch = "Dieses Risikoprofil liegt im unteren Bereich (Bundesbank: <10% Verzögerungsrate). Kaum strukturelle Zahlunsrisiken erkennbar."
        stufe = "niedrig"
    elif p_pct < 20:
        empirisch = "Moderates Profil (Bundesbank: ~15% Verzögerungsrate bei vergleichbaren KPIs). KfW KMU-Panel: ~2% echter Ausfall p.a. in dieser Kategorie."
        stufe = "moderat"
    elif p_pct < 35:
        empirisch = "Erhöhtes Profil (Bundesbank: ~35% Verzögerungsrate bei EK<5%/VG>20x/neg. Marge). KfW KMU-Panel: ~5–8% echter Ausfall p.a. Creditreform: 15–20% solcher Unternehmen halten pünktliche Zahlung aufrecht (typisch: Konzernrückhalt)."
        stufe = "erhöht"
    else:
        empirisch = "Kritisches Profil (Bundesbank: >40% Verzögerungsrate in dieser KPI-Gruppe). Hohe Wahrscheinlichkeit latenter Zahlungsprobleme."
        stufe = "kritisch"
    rationale = (
        f"ZAHLUNGSRISIKO-ANALYSE (Score {z_sc}/10 | Gewicht: 20%) — "
        f"Bilanzanalytische Ausfallwahrscheinlichkeit P(Zahlungsproblem) = {p_pct}%. "
        f"Treiber: {treiber_str}. "
        f"PROBABILISTISCHE VERTEILUNG: {p_good:.0f}% Wahrscheinlichkeit für günstiges/neutrales Zahlungsverhalten "
        f"(inkl. {round(buckets_result['buckets'][0]['probability']*100,0):.0f}% bestätigt pünktlich), "
        f"{p_bad:.0f}% für problematisches Zahlungsverhalten "
        f"(davon {round(buckets_result['buckets'][4]['probability']*100,0):.0f}% bestätigter Ausfall). "
        f"Probability-weighted Erwartungswert: BI {e_bi}. "
        f"EMPIRISCHE EINORDNUNG ({stufe.upper()}): {empirisch} "
        f"METHODISCHER VORTEIL GEGENÜBER GEMELDETEN DATEN: Extern bestätigte Zahlungsausfälle sind "
        f"ein Nacheilindikator — Unternehmen melden Probleme typischerweise 6–18 Monate nach Entstehen "
        f"(Bundesbank 2023). Die bilanzanalytische Ableitung erkennt strukturelle Risikokandidaten "
        f"frühzeitig, auch wenn noch keine Ausfälle gemeldet wurden. Score {z_sc}/10 entspricht "
        f"dem modal-wahrscheinlichen Szenario auf Basis der vorliegenden Bilanzdaten."
    )
    return rationale

def compute_score_v21(req:ScoringRequest)->ScoringResult:
    # GF-Erweiterter Check wenn Namen angegeben und kein manueller Score
    _gf_check_result = {"score": req.gf_score if req.gf_score is not None else 5, "details": [], "quellen": [], "alarm": False, "alarm_text": ""}
    if req.gf_namen and (req.gf_score is None or req.gf_score == 5):
        try:
            _gf_check_result = insolvenz_checker.check_persons_extended(
                req.gf_namen, company_name=req.company_name)
            req = req.model_copy(update={"gf_score": _gf_check_result["score"]})
        except Exception as _e:
            logger.warning(f"GF-Extended-Check Fehler: {_e}")
    bs,ek_r,um=req.bilanzsumme or 0.0,req.eigenkapital or 0.0,req.umsatz or 0.0
    je,ma,rf=req.jahresergebnis,req.mitarbeiter or 0,req.rechtsform or "GmbH"
    ek_b,ek_d,ek_a=_bereinige(rf,ek_r,bs,je,req.ausschuettungen_avg)
    fk=req.fremdkapital if req.fremdkapital is not None else (bs-ek_b if bs>0 else None)
    ep=(ek_b/bs*100) if bs>0 else None
    vg=(fk/ek_b) if (ek_b>0 and fk is not None) else None
    mg=(je/um*100) if (um>0 and je is not None) else None
    upm=(um/ma) if ma>0 else None
    kpm=(req.loehne_gehaelter/ma) if (req.loehne_gehaelter and ma>0) else None
    liq=None
    if req.fluessige_mittel is not None and req.kurzfristiges_fk:
        liq=((req.fluessige_mittel or 0)+(req.forderungen or 0))/req.kurzfristiges_fk
    dims,tot=[],0.0
    z_prob=_zahlung_prob(ep,vg,liq,mg,je,um)
    # v2.5.7: Konzern-Zahlungsmodifikator – Konzernrückhalt/-belastung direkt auf z_prob
    _kz_eff = int(req.konzern_score if req.konzern_score is not None else 5)
    _kz_mod = _konzern_zahlung_mod(_kz_eff)
    z_prob_adj = float(min(_ZAHLUNG_MAX_P, max(0.001, z_prob * _kz_mod)))
    z_sc=int(round(max(0.0,min(10.0,10.0*(1.0-z_prob_adj/_ZAHLUNG_MAX_P)))))
    for k in _GEW:
        if k=="zahlungsweise":
            s=z_sc; info="P(Zahlungsproblem)="+str(round(z_prob_adj*100,1))+"% (EK/VG/Liq/Marge/Verlust"+( f", Konzern×{_kz_mod}" if _kz_mod!=1.0 else "")+")"
        else:
            gf_eff = req.gf_score if req.gf_score is not None else 5
            s,info=_dim(k,rf,ep,vg,liq,mg,je,kpm,req.branche_risiko,req.investoren_score,ma,upm,req.gruendungsjahr,req.insolvenz or False,req.negativmerkmale_anzahl or 0,req.presse_score,wz=req.wz_code,gf=gf_eff,kz=req.konzern_score or 5)
        g=_GEW[k];b=s*g/100.0;tot+=b
        dims.append(DimensionScore(name=k,label_de=_LABELS[k],score_0_10=s,gewichtung_pct=g,beitrag=round(b,4),info=info))
    idx=max(100,min(600,600-round(tot*50)))
    if req.insolvenz: idx=0
    ht,kr=_ht(ep,vg,rf); pdv=_pd(idx)
    # Zahlungsweise-Band: optimistisch (z=10) / wahrscheinlich (aktuell) / pessimistisch (z=0)
    _z_gew=_GEW["zahlungsweise"]/100.0
    _tot_opt =tot - z_sc*_z_gew + 10*_z_gew
    _tot_pess=tot - z_sc*_z_gew + 0*_z_gew
    _bi_opt =max(100,min(600,600-round(_tot_opt *50)))
    _bi_pess=max(100,min(600,600-round(_tot_pess*50)))
    _kz_mod_note = ""
    if _kz_mod < 1.0:
        _kz_mod_note = (f" Konzernrückhalt (Score {_kz_eff}/10) reduziert P(Zahlungsproblem) "
                        f"von {round(z_prob*100,1)}% auf {round(z_prob_adj*100,1)}% (Faktor {_kz_mod}).")
    elif _kz_mod > 1.0:
        _kz_mod_note = (f" Konzernbelastung (Score {_kz_eff}/10) erhöht P(Zahlungsproblem) "
                        f"von {round(z_prob*100,1)}% auf {round(z_prob_adj*100,1)}% (Faktor {_kz_mod}).")
    _z_note=(f"KPI-abgeleitet: P(Zahlungsproblem)={round(z_prob_adj*100,1)}% "
             f"(EK-Quote, Verschuldung, Liquiditaet, Marge, Verlust)."
             f"{_kz_mod_note} "
             f"Band: BI {_bi_opt} bis BI {_bi_pess}. "
             f"Wahrscheinlichste Variante: BI {idx} (Bilanzlage als Hauptindiz).")
    _z_buckets = _zahlung_buckets(z_prob_adj, z_sc, _bi_opt, _bi_pess, idx)
    _z_rationale = _zahlung_rationale(z_prob_adj, z_sc, ep, vg, liq, mg, je, um, _z_buckets)
    # Konzern-Mutter aus Info-Feld
    _konzern_mutter = req.konzern_info or None
    return ScoringResult(company_name=req.company_name,rechtsform=rf,
        eigenkapitalquote_pct=round(ep,2) if ep is not None else None,eigenkapital_bereinigt=round(ek_b,2),
        verschuldungsgrad=round(vg,2) if vg is not None else None,ergebnismarge_pct=round(mg,2) if mg is not None else None,
        umsatz_pro_ma=round(upm,0) if upm is not None else None,kosten_pro_ma=round(kpm,0) if kpm is not None else None,
        liquiditaet_1=round(liq,3) if liq is not None else None,dimensionen=dims,rohscore_0_100=round(tot*10,2),
        bonitaetsindex=idx,risikoklasse=_rk(idx),pd_pct=pdv,pd_label=f"PD {pdv:.1f}%",
        hard_thresholds=ht,kapitalstruktur_risiko=kr,ek_bereinigt_angewendet=ek_a,ek_bereinigung_betrag=round(ek_d,2),
        zahlungsweise_bi_optimistisch=_bi_opt,zahlungsweise_bi_wahrscheinlich=idx,
        zahlungsweise_bi_pessimistisch=_bi_pess,zahlungsweise_band_note=_z_note,
        zahlungsweise_probability_buckets=_z_buckets["buckets"],
        zahlungsweise_expected_bi=_z_buckets["expected_bi"],
        zahlungsweise_score_rationale=_z_rationale,
        konzern_mutter=_konzern_mutter,
        konzern_zahlungsmodifikator_faktor=round(_kz_mod,3),
        konzern_zahlungsmodifikator_info=_kz_mod_note.strip() if _kz_mod_note else "Kein Konzerneinfluss auf Zahlungswahrscheinlichkeit (Score 5/neutral).",
        gf_check_score=_gf_check_result["score"],
        gf_check_details=_gf_check_result.get("details",[]),
        gf_check_quellen=_gf_check_result.get("quellen",[]),
        gf_alarm=_gf_check_result.get("alarm",False),
        gf_alarm_text=_gf_check_result.get("alarm_text",""))

@app.post("/api/scoring",response_model=ScoringResult)
async def scoring_endpoint(req:ScoringRequest):
    """OpenRisk Bonitaetsindex v2.5+ - 18 Dimensionen, manuelle Dateneingabe"""
    try:
        r=compute_score_v21(req)
        logger.info(f"Scoring {req.company_name}: {r.bonitaetsindex} {r.risikoklasse} {r.kapitalstruktur_risiko}")
        return r
    except Exception as e:
        logger.error(f"Scoring: {e}",exc_info=True)
        raise HTTPException(status_code=500,detail=str(e))


# ===== v2.6.0: Auto-Score by company name =====

class ScoringByNameRequest(BaseModel):
    """v2.6.0: Nur Firmenname erforderlich — alles andere kommt von handelsregister.ai."""
    company_name: str
    hr_nummer: Optional[str] = None        # Optional: HR-Nummer fuer praezisere Suche
    branche_risiko: Optional[str] = "medium"
    investoren_score: Optional[int] = 5
    presse_score: Optional[int] = 5
    # Optionale manuelle Overrides (wenn HR.ai-Daten unvollstaendig)
    gf_score_override: Optional[int] = None
    konzern_score_override: Optional[int] = None
    negativmerkmale_anzahl: Optional[int] = 0
    # v2.7.0: Optionale kostenpflichtige Add-ons
    include_publications: bool = False     # +1 Credit: Unternehmenshistorie (Bekanntmachungen)
    include_news: bool = False             # +10 Credits: Aktuelle Nachrichten / Pressemeldungen

class ScoringByNameResult(BaseModel):
    """Vollstaendiges Scoring-Ergebnis + Metadaten ueber den Auto-Fetch."""
    scoring: ScoringResult
    hr_ai_data_found: bool = False
    company_name_hr: Optional[str] = None
    gf_namen_detected: Optional[str] = None
    konzern_detected: Optional[str] = None
    wz_detected: Optional[str] = None
    geschaeftsjahr: Optional[str] = None
    fehlende_felder: List[str] = []
    warnung: Optional[str] = None
    # v2.6.1: Rohe Finanzkennzahlen fuer Frontend-Anzeige
    kpi_bilanzsumme: Optional[float] = None
    kpi_eigenkapital: Optional[float] = None
    kpi_fremdkapital: Optional[float] = None
    kpi_umsatz: Optional[float] = None
    kpi_jahresergebnis: Optional[float] = None
    kpi_mitarbeiter: Optional[int] = None
    kpi_loehne_gehaelter: Optional[float] = None
    kpi_liquide_mittel: Optional[float] = None
    kpi_rechtsform: Optional[str] = None
    kpi_gruendungsjahr: Optional[str] = None
    # v2.7.0: Optionale Add-on Ergebnisse
    publications_data: Optional[List[Any]] = None  # Unternehmenshistorie / Bekanntmachungen
    news_data: Optional[List[Any]] = None           # Aktuelle Nachrichten
    credits_used: Optional[int] = None              # Verbrauchte HR.ai Credits (Schaetzung)

@app.post("/api/score_by_name", response_model=ScoringByNameResult)
async def score_by_name_endpoint(req: ScoringByNameRequest):
    """v2.8.0: Vollautomatisches Scoring – nur Firmenname erforderlich.
    Datenquellen: handelsregister.ai (Finanzen, Konzern, GF-Namen, GuV),
    insolvenzbekanntmachungen.de (GF-Insolvenzcheck),
    DuckDuckGo (GF-Pressecheck).
    Optionale Add-ons: include_publications=true (+1 Credit), include_news=true (+10 Credits).
    Basis: 19 Credits/Abfrage (inkl. P&L). Vollpaket: 30 Credits/Abfrage.
    website_content (0 Credits, AI) als automatischer Fallback fuer fehlende Felder.
    """
    try:
        hr = HandelsregisterClient()
        if not hr.is_available():
            raise HTTPException(status_code=503, detail="handelsregister.ai API-Key nicht konfiguriert. Bitte /api/scoring mit manuellen Daten nutzen.")

        # 1. Finanzdaten von handelsregister.ai holen
        fd, company_name_hr = hr.search(req.company_name, req.hr_nummer)
        if not fd:
            raise HTTPException(status_code=404,
                detail=f"Keine Finanzdaten fuer '{req.company_name}' bei handelsregister.ai gefunden. "
                       "Bitte HR-Nummer angeben oder /api/scoring mit manuellen Daten nutzen.")

        logger.info(f"score_by_name: HR.ai-Daten fuer '{company_name_hr or req.company_name}' geladen: "
                    f"Umsatz={fd.umsatz}, EK={fd.eigenkapital}, BS={fd.bilanzsumme}")

        # 2. GF-Namen: aus _map_kpi-Cache → HR.ai separater Abruf → DuckDuckGo Fallback
        gf_namen = fd.__dict__.get("_gf_namen_detected")
        if not gf_namen:
            gf_namen = hr.get_gf_names(req.company_name, req.hr_nummer)
        if not gf_namen:
            logger.info("GF-Namen: HR.ai leer -> DuckDuckGo Fallback")
            gf_namen = hr.ddg_find_gf_names(company_name_hr or req.company_name)
        # Bundesanzeiger Fallback: GF-Namen aus Jahresabschluss-Text
        if not gf_namen:
            logger.info("GF-Namen: DDG leer -> Bundesanzeiger Fallback")
            try:
                ba_reports = ba_scraper.get_reports(company_name_hr or req.company_name, max_reports=3)
                for rpt in ba_reports.values():
                    raw_text = rpt.get("report", "")
                    if raw_text:
                        gf_namen = text_parser.extract_gf_names_from_text(raw_text)
                        if gf_namen:
                            logger.info(f"GF-Namen via Bundesanzeiger: {gf_namen}")
                            break
            except Exception as _ba_err:
                logger.warning(f"Bundesanzeiger GF-Namen Fehler: {_ba_err}")

        # 2b. Muttergesellschaft: HR.ai → DuckDuckGo Fallback
        if not fd.parent_company:
            logger.info("Muttergesellschaft: HR.ai leer → DuckDuckGo Fallback")
            parent_ddg = hr.ddg_find_parent_company(company_name_hr or req.company_name)
            if parent_ddg:
                fd.parent_company = parent_ddg
                fd.konzern_score_auto = 7  # Konzern erkannt via DDG

        # 3. WZ-Code aus HR.ai (in _map_kpi als __dict__["wz_code"] gespeichert)
        wz_detected = fd.__dict__.get("wz_code")

        # 4. Fehlende Felder protokollieren
        fehlend = []
        if not fd.bilanzsumme:     fehlend.append("bilanzsumme")
        if not fd.eigenkapital:    fehlend.append("eigenkapital")
        if not fd.umsatz:          fehlend.append("umsatz")
        if not fd.jahresergebnis:  fehlend.append("jahresergebnis")
        if not fd.mitarbeiter:     fehlend.append("mitarbeiter")
        if not gf_namen:           fehlend.append("gf_namen")

        # 5. Fremdkapital ableiten
        fk = None
        if fd.bilanzsumme and fd.eigenkapital is not None:
            fk = max(0.0, fd.bilanzsumme - fd.eigenkapital)

        # 6. Konzern-Score: Override > auto-detected > neutral
        kz_score = req.konzern_score_override or fd.konzern_score_auto or 5

        # 7. ScoringRequest zusammenbauen
        scoring_req = ScoringRequest(
            company_name=company_name_hr or req.company_name,
            rechtsform=fd.rechtsform,
            gruendungsjahr=fd.gruendungsjahr,
            bilanzsumme=fd.bilanzsumme,
            eigenkapital=fd.eigenkapital,
            fremdkapital=fk,
            umsatz=fd.umsatz,
            jahresergebnis=fd.jahresergebnis,
            mitarbeiter=fd.mitarbeiter,
            loehne_gehaelter=fd.loehne_gehaelter,
            wz_code=wz_detected,
            branche_risiko=req.branche_risiko or "medium",
            investoren_score=req.investoren_score or 5,
            presse_score=req.presse_score or 5,
            gf_score=req.gf_score_override or 5,
            konzern_score=kz_score,
            gf_namen=gf_namen,
            konzern_info=fd.parent_company,
            insolvenz=False,
            negativmerkmale_anzahl=req.negativmerkmale_anzahl or 0,
        )

        # 8. Scoring berechnen (GF-Check laeuft intern automatisch)
        result = compute_score_v21(scoring_req)
        logger.info(f"score_by_name '{company_name_hr}': BI={result.bonitaetsindex} {result.risikoklasse}")

        warnung = None
        if fehlend:
            warnung = f"Fehlende HR.ai-Felder (Standardwerte verwendet): {', '.join(fehlend)}"

        # Fremdkapital fuer Rueckgabe berechnen
        fk_result = None
        if fd.bilanzsumme and fd.eigenkapital is not None:
            fk_result = max(0.0, fd.bilanzsumme - fd.eigenkapital)

        # v2.7.0: Optionale Add-ons laden
        q_addon = req.hr_nummer if req.hr_nummer else (company_name_hr or req.company_name)
        publications_result = None
        news_result = None
        # Credits-Schaetzung v2.8: financial_kpi(1) + balance_sheet_accounts(3) +
        # related_persons(2) + shareholders(5) + annual_financial_statements(5) +
        # profit_and_loss_account(3) + website_content(0) = 19
        credits_used = 19
        if req.include_publications:
            try:
                publications_result = hr.get_publications(q_addon)
                credits_used += 1
                logger.info(f"Add-on publications: {len(publications_result) if publications_result else 0} Eintraege")
            except Exception as _e:
                logger.warning(f"publications Add-on Fehler: {_e}")
        if req.include_news:
            try:
                news_result = hr.get_news(q_addon)
                credits_used += 10
                logger.info(f"Add-on news: {len(news_result) if news_result else 0} Artikel")
            except Exception as _e:
                logger.warning(f"news Add-on Fehler: {_e}")

        return ScoringByNameResult(
            scoring=result,
            hr_ai_data_found=True,
            company_name_hr=company_name_hr,
            gf_namen_detected=gf_namen,
            konzern_detected=fd.parent_company,
            wz_detected=wz_detected,
            geschaeftsjahr=fd.geschaeftsjahr,
            fehlende_felder=fehlend,
            warnung=warnung,
            # v2.6.1: Rohe KPI-Werte fuer Frontend
            kpi_bilanzsumme=fd.bilanzsumme,
            kpi_eigenkapital=fd.eigenkapital,
            kpi_fremdkapital=fk_result,
            kpi_umsatz=fd.umsatz,
            kpi_jahresergebnis=fd.jahresergebnis,
            kpi_mitarbeiter=fd.mitarbeiter,
            kpi_loehne_gehaelter=fd.loehne_gehaelter,
            kpi_liquide_mittel=fd.liquide_mittel if hasattr(fd, 'liquide_mittel') else None,
            kpi_rechtsform=fd.rechtsform,
            kpi_gruendungsjahr=fd.gruendungsjahr,
            # v2.7.0: Add-on Ergebnisse
            publications_data=publications_result,
            news_data=news_result,
            credits_used=credits_used,
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"score_by_name: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/info")
async def info_endpoint(name: str, hr_nummer: Optional[str] = None):
    """Gibt HR.ai-Finanzdaten und GF-Namen fuer ein Unternehmen zurueck (fuer Frontend Auto-Befuellen)."""
    try:
        hr = HandelsregisterClient()
        if not hr.is_available():
            return {"error": "HR.ai API-Key nicht konfiguriert", "available": False}
        fd, company_name_hr = hr.search(name, hr_nummer)
        if not fd:
            return {"error": f"Kein Treffer fuer '{name}'", "available": False}
        gf_namen = fd.__dict__.get("_gf_namen_detected") or hr.get_gf_names(name, hr_nummer)
        return {
            "available": True,
            "company_name": company_name_hr or name,
            "financials": {
                "bilanzsumme": fd.bilanzsumme,
                "eigenkapital": fd.eigenkapital,
                "fremdkapital": (fd.bilanzsumme - fd.eigenkapital) if fd.bilanzsumme and fd.eigenkapital else None,
                "umsatz": fd.umsatz,
                "jahresergebnis": fd.jahresergebnis,
                "mitarbeiter": fd.mitarbeiter,
                "loehne_gehaelter": fd.loehne_gehaelter,
                "rechtsform": fd.rechtsform,
                "gruendungsjahr": fd.gruendungsjahr,
                "geschaeftsjahr": fd.geschaeftsjahr,
                "wz_code": fd.__dict__.get("wz_code"),
            },
            "company_info": {
                "parent_company": fd.parent_company,
                "konzern_score_auto": fd.konzern_score_auto,
                "gf_namen": gf_namen,
                "insolvenz": False,
            },
        }
    except Exception as e:
        logger.error(f"info: {e}", exc_info=True)
        return {"error": str(e), "available": False}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
