
# OpenRisk AI - v2.10.29
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

VERSION = "2.10.36"

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
    gruendungsjahr_quelle: Optional[str] = None   # v2.9: "afs_text"|"registration"|"estimated"
    rechtsform: Optional[str] = None
    loehne_gehaelter: Optional[float] = None
    fremdkapital: Optional[float] = None
    parent_company: Optional[str] = None       # aus HR.ai Eigentuemerstruktur
    parent_company_anteil: Optional[float] = None  # v2.10.31: Beteiligungsquote in % (aus shareholders)
    konzern_score_auto: Optional[int] = None   # 5=unbekannt,6=<50%,7=≥50%,8=≥75%,9=≥95%
    # v2.9: P&L-Kennzahlen aus profit_and_loss_account
    bruttoergebnis: Optional[float] = None           # Gross Profit
    brutto_marge_pct: Optional[float] = None         # Bruttoergebnis / Umsatz
    fae_kosten: Optional[float] = None               # F&E-Kosten
    fae_quote_pct: Optional[float] = None            # F&E / Umsatz
    personalaufwand: Optional[float] = None          # Personalaufwand (falls vorhanden)
    personalaufwand_quote_pct: Optional[float] = None
    umsatz_vorjahr: Optional[float] = None           # fuer Wachstumsrate
    umsatz_wachstum_pct: Optional[float] = None      # YoY Umsatzwachstum
    miet_leasing: Optional[float] = None             # v2.10.23: Off-Balance Leasingverpflichtungen
    # v2.10.32: Neu extrahierte KPIs aus Bilanz + GuV
    vorraete: Optional[float] = None                 # Vorräte/Inventory (Bilanz Aktiva)
    langfristiges_fk: Optional[float] = None         # Langfristiges Fremdkapital (Bilanz Passiva)
    zinsaufwand: Optional[float] = None              # Zinsaufwand (GuV)
    abschreibungen: Optional[float] = None           # Abschreibungen (GuV)
    ebitda: Optional[float] = None                   # EBITDA = JE + Abschreibungen + Zinsaufwand (abgeleitet)
    zinsdeckungsgrad: Optional[float] = None         # EBIT / Zinsaufwand (abgeleitet)

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
        if resp.status_code == 401:
            logger.error(f"HR.ai 401 UNAUTHORIZED — API-Key ungültig! q={q!r}")
            return {}
        if resp.status_code == 402:
            logger.error(f"HR.ai 402 PAYMENT REQUIRED — Credits aufgebraucht! q={q!r}")
            return {}
        if resp.status_code == 404:
            logger.debug(f"HR.ai 404 nicht gefunden: q={q!r}, feature={feature!r}")
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


    def _search_companies(self, query: str, limit: int = 10) -> list:
        """v2.10.4: DDG-first Unternehmenssuche (0 Credits).
        Primär: DuckDuckGo HTML-Suche → 0 Credits.
        Fallback: HR.ai fetch-organization wenn DDG leer."""
        # ── Step 1: DDG (immer kostenlos) ────────────────────────────────────
        ddg_results = self._ddg_search_companies(query, limit=limit)
        if ddg_results:
            logger.info(f"search_companies '{query}': {len(ddg_results)} DDG-Treffer")
            return ddg_results

        # ── Step 2: HR.ai Fallback (nur wenn DDG leer + API-Key vorhanden) ──
        if not self.is_available():
            return []
        headers = {"x-api-key": self.api_key, "Accept": "application/json"}
        seen_names: set = set()
        results = []
        for q in self._name_variants(query)[:3]:
            try:
                params = {"q": q, "feature": "financial_kpi"}
                resp = requests.get(f"{self.BASE_URL}/v1/fetch-organization",
                                    params=params, headers=headers, timeout=12)
                if resp.status_code in (401, 402, 404):
                    continue
                resp.raise_for_status()
                data = resp.json()
                items = data if isinstance(data, list) else ([data] if isinstance(data, dict) else [])
                for item in items[:limit]:
                    if not isinstance(item, dict):
                        continue
                    result = self._map_search_result(item, fallback_name=q)
                    if result and result.name.lower() not in seen_names:
                        seen_names.add(result.name.lower())
                        results.append(result)
                        if len(results) >= limit:
                            break
            except Exception as e:
                logger.warning(f"_search_companies HR.ai Fehler ({q}): {e}")
            if results:
                break
        logger.info(f"search_companies '{query}': {len(results)} HR.ai-Treffer")
        return results

    def _ddg_search_companies(self, query: str, limit: int = 8) -> list:
        """v2.10.5: DuckDuckGo-Unternehmenssuche — 0 Credits, rein web-basiert.
        Nur Einträge MIT HR-Nummer; Duplikate (gleiche HR-Nr.) werden verdichtet."""
        import re as _re
        RECHTSFORMEN = [
            "GmbH & Co. KG", "GmbH & Co KG", "GmbH", "AG", "SE", "KGaA",
            "KG", "UG", "OHG", "GbR", "e.V.", "eG", "mbH",
        ]
        RF_PAT = _re.compile(
            r'\b(' + '|'.join(_re.escape(r) for r in RECHTSFORMEN) + r')\.?\b',
            _re.IGNORECASE
        )
        HR_PAT = _re.compile(r'\b(HRB|HRA)\s*(\d{3,8})\b', _re.IGNORECASE)
        # Stadt-Muster in absteigender Zuverlässigkeit:
        # 1) "Amtsgericht München"  2) "Sitz: München"
        # 3) "München HRB/HRA"     4) "HRB 1234 München"  5) "· München ·"
        CITY_PATS = [
            _re.compile(r'Amtsgericht\s+([A-ZÄÖÜ][a-zäöüß]{2,25}(?:[\s\-][A-ZÄÖÜ][a-zäöüß]{2,20})?)', _re.I),
            _re.compile(r'Sitz[:\s]+([A-ZÄÖÜ][a-zäöüß]{2,25}(?:[\s\-][A-ZÄÖÜ][a-zäöüß]{2,20})?)'),
            _re.compile(r'([A-ZÄÖÜ][a-zäöüß]{2,25}(?:[\s\-][A-ZÄÖÜ][a-zäöüß]{2,20})?)\s+(?:HRB|HRA)\s*\d', _re.I),
            _re.compile(r'(?:HRB|HRA)\s*\d{3,8}\s+([A-ZÄÖÜ][a-zäöüß]{2,25})', _re.I),
            _re.compile(r'·\s*([A-ZÄÖÜ][a-zäöüß]{2,25})\s*·'),
        ]
        # Seiten-Titel-Präfixe die kein Firmenname sind
        TITLE_JUNK = _re.compile(
            r'^(?:Handelsregisterauszug\s+(?:von\s+)?|Unternehmensregister\s+'
            r'|Firmenprofil\s+|Eintrag\s+im\s+Handelsregister\s+(?:von\s+)?'
            r'|Jahresabschluss\s+|Bilanz\s+)',
            _re.IGNORECASE
        )

        search_q = f"{query} Handelsregister"
        results = []
        seen_names: set = set()
        seen_hr: set = set()      # Deduplizierung nach HR-Nummer
        try:
            url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(search_q)}&kl=de-de"
            r = requests.get(url, headers=self._DDG_HEADERS, timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")

            for result_div in soup.find_all("div", class_=_re.compile(r"result(?!__snippet)")):
                title_el = (result_div.find("a", class_="result__a") or
                            result_div.find("h2"))
                snippet_el = (result_div.find("a", class_="result__snippet") or
                              result_div.find("div", class_="result__snippet"))
                if not title_el:
                    continue
                title = title_el.get_text(" ", strip=True)
                snippet = snippet_el.get_text(" ", strip=True) if snippet_el else ""
                combined = title + "  " + snippet

                # ── HR-Nummer zuerst prüfen — kein HR-Eintrag = überspringen ──
                hr_m = HR_PAT.search(combined)
                if not hr_m:
                    continue   # v2.10.5: nur Einträge mit HR-Nummer
                hr_nummer = f"{hr_m.group(1).upper()} {hr_m.group(2)}"
                if hr_nummer in seen_hr:
                    continue   # v2.10.5: Duplikat nach HR-Nummer verdichten
                seen_hr.add(hr_nummer)

                # ── Firmenname aus Titel ──────────────────────────────────────
                # Seiten-Titel-Präfixe entfernen ("Handelsregisterauszug von ...")
                clean_title = TITLE_JUNK.sub("", title).strip()
                rf_m = RF_PAT.search(clean_title)
                if rf_m:
                    raw = clean_title[:rf_m.end()].strip().rstrip("·|–-:,")
                    company = _re.sub(r'^[\d\s·|–\-:,]+', '', raw).strip()
                    rechtsform = rf_m.group(1)
                else:
                    if query.lower() not in clean_title.lower():
                        continue
                    company = _re.sub(r'^[\d\s·|–\-:,]+', '', clean_title).strip()
                    company = " ".join(company.split()[:6])
                    rechtsform = None

                if not company or len(company) < 3:
                    continue
                if company.lower() in seen_names:
                    continue
                seen_names.add(company.lower())

                # ── Stadt: erste Übereinstimmung aus priorisierten Mustern ────
                city = None
                for cp in CITY_PATS:
                    c_m = cp.search(combined)
                    if c_m:
                        city = c_m.group(1).strip()
                        break

                results.append(CompanySearchResult(
                    name=company, city=city,
                    rechtsform=rechtsform, hr_nummer=hr_nummer
                ))
                if len(results) >= limit:
                    break

        except Exception as e:
            logger.warning(f"DDG company search Fehler: {e}")
        return results

    def _map_search_result(self, data: dict, fallback_name: str = "") -> Optional["CompanySearchResult"]:
        """Mappt HR.ai-Response auf CompanySearchResult.
        v2.10.4: name-Feld kann String ODER Dict sein; fallback_name als Notanker."""
        # ── Name: HR.ai gibt manchmal {"name_1": "SAP", "name_2": "SE"} zurück ──
        name_raw = data.get("name") or ""
        if isinstance(name_raw, dict):
            name = " ".join(str(v) for v in name_raw.values() if v).strip()
        else:
            name = str(name_raw).strip()
        # Fallback: Suchquery wenn kein Name in HR.ai-Daten
        if not name or len(name) < 2:
            name = fallback_name.strip()
        if not name or len(name) < 2:
            return None
        # Rechtsform
        lf = data.get("legal_form") or {}
        rf = (lf.get("short") or lf.get("name") or str(lf) if isinstance(lf, dict)
              else str(lf or "")).strip()
        # Stadt / Ort
        city = self._extract_city(data)
        # HR-Nummer
        hr_nr = (data.get("registration_number") or data.get("hr_number") or
                 data.get("handelsregister_number") or data.get("register_number") or "")
        if isinstance(hr_nr, dict):
            hr_nr = hr_nr.get("number") or hr_nr.get("id") or ""
        # Umsatz-Hint aus KPIs
        kpi_list = data.get("financial_kpi") or []
        umsatz = None
        if isinstance(kpi_list, list) and kpi_list:
            try:
                umsatz = float(sorted(kpi_list, key=lambda x: x.get("year",0),
                                      reverse=True)[0].get("revenue") or 0) or None
            except: pass
        reg_date = str(data.get("registration_date") or "")[:10] or None
        year = None
        if isinstance(kpi_list, list) and kpi_list:
            try:
                year = str(sorted(kpi_list, key=lambda x: x.get("year",0),
                                  reverse=True)[0].get("year") or "")
            except: pass
        return CompanySearchResult(
            name=name, city=city, rechtsform=rf or None,
            hr_nummer=str(hr_nr) if hr_nr else None,
            registration_date=reg_date, umsatz_hint=umsatz,
            geschaeftsjahr=year)

    def _extract_city(self, data: dict) -> Optional[str]:
        """Extrahiert Stadtname aus HR.ai-Response."""
        for key in ("city","ort","registered_city","location","address"):
            val = data.get(key)
            if val:
                if isinstance(val, dict):
                    val = val.get("city") or val.get("ort") or val.get("name") or ""
                city = str(val).strip()
                if city and len(city) > 1:
                    return city
        # Aus registered_office
        ro = data.get("registered_office") or data.get("office") or {}
        if isinstance(ro, dict):
            city = ro.get("city") or ro.get("ort") or ro.get("location") or ""
            if city:
                return str(city).strip()
        return None


    @staticmethod
    def _name_variants(company_name: str) -> list:
        """v2.10.2: Erzeugt Such-Varianten für robusteres HR.ai-Matching.
        Reihenfolge: exakt → ohne Rechtsform → erste Wörter → Kurzform"""
        name = company_name.strip()
        variants = [name]
        # Rechtsform-Suffixe entfernen
        import re as _re
        cleaned = _re.sub(
            r'\s*\b(SE|AG|GmbH(?:\s*&\s*Co\.?\s*KG)?|KGaA|KG|GbR|OHG|e\.?V\.?|'
            r'UG|Ltd|LLC|Corp|Inc|Holding|Group|Gruppe)\b\.?\s*$',
            '', name, flags=_re.IGNORECASE).strip()
        if cleaned and cleaned != name:
            variants.append(cleaned)
        # Erste 2 Wörter (z. B. "SAP SE" → "SAP")
        words = name.split()
        if len(words) >= 2:
            variants.append(words[0])
        # Ohne Sonderzeichen / Umlaute normalisieren
        norm = name.replace("ä","ae").replace("ö","oe").replace("ü","ue").replace("ß","ss")
        if norm != name:
            variants.append(norm)
        # Deduplizieren, Reihenfolge beibehalten
        seen = set()
        result = []
        for v in variants:
            if v and v not in seen:
                seen.add(v); result.append(v)
        return result

    def search(self, company_name: str, hr_nummer: Optional[str] = None):
        if not self.is_available():
            return None, None
        # v2.10.20: Immer Namensvarianten verwenden — HR.ai findet per Name zuverlässiger
        # als per HR-Nummer ("HRB 719915" gibt bei HR.ai oft leeres/falsches Dict zurück)
        queries = self._name_variants(company_name)
        # v2.10.19: Prüfe ob echte Finanzdaten vorhanden (nicht nur leeres/Error-Dict)
        _KPI_KEYS = {"bilanzsumme", "eigenkapital", "umsatz", "jahresergebnis",
                     "total_assets", "equity", "revenue", "net_income", "name"}
        data_kpi = None
        used_q = company_name
        for q in queries:
            raw = self._get(q, "financial_kpi")
            if raw and any(k in raw for k in _KPI_KEYS):
                data_kpi = raw
                used_q = q
                logger.info(f"HR.ai financial_kpi gefunden mit Query: {q!r}")
                break
            elif raw:
                logger.debug(f"HR.ai financial_kpi: Query {q!r} lieferte Daten ohne KPI-Felder: {list(raw.keys())[:5]}")
        if not data_kpi:
            logger.warning(f"HR.ai: kein Treffer für alle Varianten von {company_name!r}: {queries}")
            return None, None
        try:
            company_name_hr = data_kpi.get("name")
            fd = self._map_kpi(data_kpi)
            if not fd:
                # Firma gefunden, aber keine Finanzdaten (kein Jahresabschluss veroeffentlicht)
                return None, company_name_hr
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
            # v2.8.0/v2.9.0: profit_and_loss_account → P&L-KPIs + Mitarbeiterzahl (3 Credits)
            try:
                data_pnl = self._get(q, "profit_and_loss_account")
                if data_pnl:
                    self._extract_pnl_kpis(fd, data_pnl)          # v2.9: Bruttomarge, F&E etc.
                    if not fd.mitarbeiter:
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
                            # v2.9: Echtes Gruendungsjahr aus Volltext (ueberschreibt reg_date)
                            gj_text = self._extract_gruendungsjahr_from_text(doc_md, fd.gruendungsjahr)
                            if gj_text:
                                fd.gruendungsjahr = gj_text
                                fd.gruendungsjahr_quelle = "afs_text"
                                logger.info(f"Gruendungsjahr aus AFS-Text: {gj_text}")
                            elif fd.gruendungsjahr:
                                fd.gruendungsjahr_quelle = "registration"
                            # v2.9: Mitarbeiterzahl aus Volltext falls noch nicht gefunden
                            if not fd.mitarbeiter:
                                ma_afs = self._extract_mitarbeiter_from_text(doc_md)
                                if ma_afs:
                                    fd.mitarbeiter = ma_afs
                                    logger.info(f"Mitarbeiter aus AFS-Text: {ma_afs}")
                            # GF-Namen aus Volltext wenn noch nicht gefunden
                            if not fd.__dict__.get("_gf_namen_detected"):
                                gf_text = self._extract_gf_from_statement_text(doc_md)
                                if gf_text:
                                    fd.__dict__["_gf_namen_detected"] = gf_text
                                    logger.info(f"GF-Namen via annual_financial_statements: {gf_text}")
                            # v2.10.34: Vorjahresumsatz aus AFS-Vergleichsspalte (§ 265 HGB)
                            if fd.umsatz and fd.umsatz_vorjahr is None:
                                prev_umsatz = self._extract_umsatz_vorjahr_from_text(doc_md, fd.umsatz)
                                if prev_umsatz and prev_umsatz > 0:
                                    fd.umsatz_vorjahr = prev_umsatz
                                    raw_growth = (fd.umsatz - prev_umsatz) / prev_umsatz * 100
                                    if prev_umsatz < fd.umsatz * 0.25:
                                        fd.umsatz_wachstum_pct = round(min(raw_growth * 0.2, 50.0), 1)
                                        fd.__dict__["umsatz_wachstum_hinweis"] = "Rumpfgeschäftsjahr"
                                    elif raw_growth > 100:
                                        fd.umsatz_wachstum_pct = round(min(raw_growth * 0.5, 80.0), 1)
                                        fd.__dict__["umsatz_wachstum_hinweis"] = "Anlaufdynamik"
                                    else:
                                        fd.umsatz_wachstum_pct = round(raw_growth, 1)
                                    logger.info(f"Wachstum via AFS-Text: {prev_umsatz:,.0f}→{fd.umsatz:,.0f} = {fd.umsatz_wachstum_pct:+.1f}%")
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
        """Extrahiert Muttergesellschaft + Beteiligungsquote aus shareholders-Daten.
        v2.10.33: KEIN früher Exit wenn Mutter bereits bekannt — shareholders-Endpunkt liefert
        die präziseste Beteiligungsquote und muss immer ausgewertet werden, um konzern_score_auto
        korrekt zu setzen (z.B. wenn related_persons schon Namen fand, aber keine %)."""
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
            is_company = any(x in name for x in ("GmbH","AG","KG","SE","Ltd","Holding","Corp")) or \
                         sh_type in ("company","organisation","legal_entity","gmbh","ag")
            if share > best_share or (is_company and share >= 25):
                best_share = share
                best_name = name
        if best_name:
            # v2.10.33: Immer aktualisieren (überschreibt ggf. vorherigen Platzhalter ohne %)
            fd.parent_company = best_name
            fd.parent_company_anteil = best_share if best_share > 0 else None
            # v2.10.31: Konzern-Score gestuft nach Beteiligungsquote
            # Quelle: Moody's Parent-Subsidiary Credit Linkage — Mehrheit = Haftungsübernahme
            if best_share >= 95:
                fd.konzern_score_auto = 9   # Kerngesellschaft / Vollkonsolidierung → ×0.70
            elif best_share >= 75:
                fd.konzern_score_auto = 8   # Qualifizierte Mehrheit → ×0.76
            elif best_share >= 50:
                fd.konzern_score_auto = 7   # Einfache Mehrheit → ×0.84
            elif best_share >= 25:
                fd.konzern_score_auto = 6   # Sperrminorität → ×0.92
            else:
                fd.konzern_score_auto = 5   # Minderheitsbeteiligung → kein Rückhalt
            logger.info(f"Muttergesellschaft via shareholders: {best_name} ({best_share}%) → konzern_score_auto={fd.konzern_score_auto}")
        elif fd.parent_company and (fd.konzern_score_auto is None or fd.konzern_score_auto == 7):
            # Keine shareholders-Daten, aber Mutter bereits bekannt (z.B. via related_persons)
            # → Anteil unbekannt, moderater Score bleibt (wird nicht verschlechtert)
            logger.info(f"shareholders leer, Mutter bereits bekannt: {fd.parent_company} → konzern_score_auto bleibt {fd.konzern_score_auto}")

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
            # v2.10.24: Vorjahresdaten → YoY Umsatzwachstum mit Dämpfung
            try:
                prev = kpi_sorted[1]
                prev_rev = sf(prev.get("revenue"))
                if prev_rev and prev_rev > 0 and f.umsatz and f.umsatz > 0:
                    f.umsatz_vorjahr = prev_rev
                    raw_growth = (f.umsatz - prev_rev) / prev_rev * 100
                    # Dämpfung: Rumpfgeschäftsjahr / Anlaufdynamik
                    # Wenn Vorjahresumsatz < 25% des aktuellen → Rumpfgeschäftsjahr (z.B. nur 4 Monate)
                    # → Wachstum ist strukturell bedingt, nicht organisch
                    if prev_rev < f.umsatz * 0.25:
                        # Rumpfjahr: Wachstum auf Annualisierungsbasis schätzen
                        # (Vorjahr war vermutlich nur ~3-6 Monate → annualisiert ~2-4x)
                        monate_faktor = f.umsatz / prev_rev / 12  # implizite Monatsbasis
                        gedaempft = min(raw_growth * 0.2, 50.0)   # max. 50% anzeigen
                        f.umsatz_wachstum_pct = round(gedaempft, 1)
                        f.__dict__["umsatz_wachstum_hinweis"] = "Rumpfgeschäftsjahr"
                        logger.info(f"Wachstum gedämpft (Rumpfgeschäftsjahr): {raw_growth:.0f}% → {gedaempft:.1f}%")
                    elif raw_growth > 100:
                        # Anlaufdynamik: Erstes volles Jahr nach Gründung → halbieren
                        gedaempft = round(min(raw_growth * 0.5, 80.0), 1)
                        f.umsatz_wachstum_pct = gedaempft
                        f.__dict__["umsatz_wachstum_hinweis"] = "Anlaufdynamik"
                        logger.info(f"Wachstum gedämpft (Anlaufdynamik): {raw_growth:.0f}% → {gedaempft:.1f}%")
                    else:
                        f.umsatz_wachstum_pct = round(raw_growth, 1)
                    logger.info(f"Umsatzwachstum YoY: {prev_rev:,.0f}→{f.umsatz:,.0f} = {f.umsatz_wachstum_pct:+.1f}%")
            except Exception as e:
                logger.debug(f"YoY-Wachstum Fehler: {e}")
        if f.umsatz and f.mitarbeiter and f.mitarbeiter > 0:
            f.umsatz_pro_mitarbeiter = round(f.umsatz / f.mitarbeiter, 2)
        for _lk in ("wages_and_salaries","personnel_costs","staff_costs","labor_costs",
                    "loehne_und_gehaelter","wages","salaries","personnel_expenses"):
            _lv=sf(fin.get(_lk))
            if _lv and _lv>0: f.loehne_gehaelter=_lv; break
        # v2.10.23: Miet-/Leasingverpflichtungen (Off-Balance) → adjustiertes FK
        for _ll in ("lease_liabilities","leasing","miet_leasing","operating_leases",
                    "finance_leases","right_of_use_assets","nutzungsrechte"):
            _lv = sf(fin.get(_ll))
            if _lv and _lv > 0:
                f.miet_leasing = _lv
                logger.info(f"Miet-/Leasing: {_lv:,.0f} EUR")
                break
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
        # Gesellschafter-Liste als Fallback (mit Anteil-Extraktion)
        if not parent:
            sh_list = data.get("shareholders") or data.get("owners") or []
            if isinstance(sh_list, list) and sh_list:
                for sh in sh_list:
                    if isinstance(sh, dict):
                        sh_type = str(sh.get("type","")).lower()
                        sh_share = float(sh.get("share") or sh.get("percentage") or 0)
                        if sh_share >= 25.0 or sh_type in ("company","gmbh","ag","kg"):
                            sh_name = sh.get("name") or sh.get("company_name") or ""
                            if sh_name:
                                parent = str(sh_name).strip()
                                f.parent_company_anteil = sh_share if sh_share > 0 else None
                                break
        if parent:
            f.parent_company = parent
            # v2.10.31/32: konzern_score_auto gestuft nach Beteiligungsquote
            _ant = f.parent_company_anteil or 0
            if _ant >= 95:   f.konzern_score_auto = 9
            elif _ant >= 75: f.konzern_score_auto = 8
            elif _ant >= 50: f.konzern_score_auto = 7
            elif _ant >= 25: f.konzern_score_auto = 6
            else:            f.konzern_score_auto = 7  # Mutter bekannt, Anteil unbekannt → moderat
            logger.info(f"Konzernzugehoerigkeit erkannt: {parent} ({_ant}%) → konzern_score={f.konzern_score_auto}")
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
            persons_raw = data.get(key) or []
            # HR.ai liefert related_persons als {"current": [...], "former": [...]}
            if isinstance(persons_raw, dict):
                persons = persons_raw.get("current") or persons_raw.get("active") or []
            else:
                persons = persons_raw
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
                        # Rolle aus label (HR.ai) oder role-Feld ermitteln
                        label = str(p.get("label","")).lower()
                        role_obj = p.get("role") or {}
                        if isinstance(role_obj, dict):
                            role = str(role_obj.get("de",{}).get("long","") or role_obj.get("en",{}).get("long","")).lower()
                        else:
                            role = str(role_obj).lower()
                        role_combined = label + " " + role + " " + str(p.get("position","")).lower()
                        # Aufsichtsräte und Beiräte ausschließen
                        if any(x in role_combined for x in ("aufsicht","supervisory","beirat","advisory")):
                            continue
                        # Fuehrungsrollen einschliessen (GF, Partner, Komplementaer, Inhaber, Vorstand)
                        # Bei GmbH & Co. KG sind Partner die fuehrenden Personen
                        if name: names.append(name.strip())
                if names:
                    return ", ".join(names)
        return None

    def _extract_gruendungsjahr_from_text(self, text: str, reg_year: Optional[str]) -> Optional[str]:
        """v2.9.0: Echtes Gruendungsjahr aus AFS-Text — ueberschreibt HR-Registrierungsdatum."""
        patterns = [
            r"(?:wurde\s+)?(?:im\s+Jahr\s+)?(\d{4})\s+(?:als\s+\w+\s+)?gegr[uü]ndet",
            r"gegr[uü]ndet(?:\s+im\s+Jahr|\s+in)?\s+(\d{4})",
            r"Gr[uü]ndung(?:sjahr)?\s*(?:im\s+Jahr\s+|:\s*)?(\d{4})",
            r"seit\s+(?:dem\s+Jahr\s+)?(\d{4})\s+(?:ist|sind|besteht|entwickelt|bietet)",
            r"founded\s+in\s+(\d{4})",
            r"incorporated\s+in\s+(\d{4})",
            r"seit\s+(?:ihrer\s+Gr[uü]ndung\s+(?:im\s+Jahr\s+)?)(\d{4})",
        ]
        for pat in patterns:
            m = re.search(pat, text, re.IGNORECASE)
            if m:
                try:
                    year = int(m.group(1))
                    # Muss plausibel sein: zwischen 1800 und heute, UND aelter als HR-Datum
                    if 1800 <= year <= 2025:
                        if reg_year is None or year < int(reg_year):
                            return str(year)
                except: pass
        return None

    def _extract_pnl_kpis(self, fd: "FinancialData", data: dict) -> None:
        """v2.9.0: Extrahiert P&L-Kennzahlen aus profit_and_loss_account-Struktur."""
        entries = data.get("profit_and_loss_account") or []
        if not isinstance(entries, list) or not entries:
            return
        # Neuestes Jahr bevorzugen
        entry = sorted(entries, key=lambda x: x.get("year", 0), reverse=True)[0]
        accounts = entry.get("profit_and_loss_accounts") or []

        # Mapping: deutsche/englische Label-Fragmente → Zielfeld
        LABEL_MAP = {
            "bruttoergebnis": "bruttoergebnis",
            "gross profit":   "bruttoergebnis",
            "forschung":      "fae_kosten",
            "entwicklung":    "fae_kosten",
            "research":       "fae_kosten",
            "personalaufwand":"personalaufwand",
            "lohn":           "personalaufwand",
            "gehalt":         "personalaufwand",
            "personnel":      "personalaufwand",
            # v2.10.32: Zinsaufwand
            "zinsaufwand":    "zinsaufwand",
            "zinsen und ähnliche aufwendungen": "zinsaufwand",
            "zinsen":         "zinsaufwand",
            "interest expense": "zinsaufwand",
            "finance costs":  "zinsaufwand",
            "financial expenses": "zinsaufwand",
            # v2.10.32: Abschreibungen
            "abschreibung":   "abschreibungen",
            "depreciation":   "abschreibungen",
            "amortisation":   "abschreibungen",
            "amortization":   "abschreibungen",
            "abschreibungen auf": "abschreibungen",
        }

        def _walk(items):
            for item in items:
                name_obj = item.get("name", {})
                label = (name_obj.get("de") or name_obj.get("in_report") or
                         name_obj.get("en") or "").lower()
                val = item.get("value")
                if val is not None:
                    for fragment, field in LABEL_MAP.items():
                        if fragment in label:
                            if getattr(fd, field) is None:
                                try:
                                    setattr(fd, field, float(val))
                                except: pass
                            break
                _walk(item.get("children", []))

        _walk(accounts)

        # Abgeleitete Quoten berechnen
        if fd.bruttoergebnis and fd.umsatz and fd.umsatz > 0:
            fd.brutto_marge_pct = round(fd.bruttoergebnis / fd.umsatz * 100, 1)
        if fd.fae_kosten and fd.umsatz and fd.umsatz > 0:
            fd.fae_quote_pct = round(abs(fd.fae_kosten) / fd.umsatz * 100, 1)
        if fd.personalaufwand and fd.umsatz and fd.umsatz > 0:
            fd.personalaufwand_quote_pct = round(abs(fd.personalaufwand) / fd.umsatz * 100, 1)
        # v2.10.32: EBITDA = JE + Abschreibungen + Zinsaufwand (Vereinfachung ohne Steuern)
        # Robuste Berechnung: Abschreibungen und Zinsaufwand können negativ (Aufwand) oder positiv gebucht sein
        _je = fd.jahresergebnis or 0
        _afa = abs(fd.abschreibungen) if fd.abschreibungen else 0
        _zins = abs(fd.zinsaufwand) if fd.zinsaufwand else 0
        if fd.abschreibungen or fd.zinsaufwand:
            fd.ebitda = round(_je + _afa + _zins, 2)
            logger.info(f"EBITDA: {_je:+,.0f} + AFA {_afa:,.0f} + Zins {_zins:,.0f} = {fd.ebitda:,.0f}")
        # v2.10.32: Zinsdeckungsgrad = EBIT / Zinsaufwand (EBIT ≈ JE + Zinsaufwand)
        # Aussagekräftig ab Zinsaufwand > 0; Richtwert: <1.5x = kritisch, >3x = gut
        if fd.zinsaufwand and abs(fd.zinsaufwand) > 0 and fd.jahresergebnis is not None:
            ebit_approx = _je + _zins  # Näherung (ohne Steuern, da nicht extrahiert)
            fd.zinsdeckungsgrad = round(ebit_approx / _zins, 2)
            logger.info(f"Zinsdeckungsgrad (EBIT-Näherung): {fd.zinsdeckungsgrad:.2f}x")

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


    def _extract_umsatz_vorjahr_from_text(self, text: str, umsatz_aktuell: float) -> Optional[float]:
        """v2.10.34: Extrahiert Vorjahresumsatz aus AFS-Volltext (Vergleichsspalte).
        Deutsche Jahresabschlüsse müssen gesetzlich Vorjahreszahlen ausweisen (§ 265 HGB).
        Das Dokument enthält typischerweise eine Tabelle:
          | Umsatzerlöse | 17.330.352,19 | 15.890.000,00 |
        Oder: Umsatzerlöse  17.330.352  15.890.000
        """
        import re as _re

        def _parse_de_number(s: str) -> Optional[float]:
            """Parst deutsche Zahlenformate: 1.234.567,89 oder 1234567.89"""
            s = s.strip().replace(" ", "")
            try:
                if "," in s:
                    s = s.replace(".", "").replace(",", ".")
                return float(s)
            except:
                return None

        # Kandidaten-Zeilen mit Umsatzerlöse suchen
        UMSATZ_PATTERN = r"(?:Umsatzerlöse|Umsatzerl[oö]se|Umsatz|revenue|net\s+sales|net\s+revenue)"

        # Muster 1: Tabellen-Format mit | Trennzeichen
        # | Umsatzerlöse | 17.330.352,19 | 15.890.000,00 |
        m = _re.search(
            UMSATZ_PATTERN + r"[^|\n]{0,30}\|\s*([\d\.,]+)\s*\|\s*([\d\.,]+)",
            text, _re.I)
        if m:
            v1 = _parse_de_number(m.group(1))
            v2 = _parse_de_number(m.group(2))
            # v1 sollte dem aktuellen Umsatz entsprechen, v2 = Vorjahr
            if v1 and v2 and v2 > 0:
                # Plausibilitätsprüfung: Vorjahr sollte im Bereich 20%–500% des aktuellen liegen
                ratio = v2 / umsatz_aktuell if umsatz_aktuell > 0 else 0
                if 0.2 <= ratio <= 5.0:
                    logger.info(f"Vorjahresumsatz aus AFS-Tabelle (|): {v2:,.0f} (ratio={ratio:.2f})")
                    return v2

        # Muster 2: Zwei Zahlen nebeneinander in einer Zeile nach dem Label
        # Umsatzerlöse    17.330.352    15.890.000
        m = _re.search(
            UMSATZ_PATTERN + r"[^\n]{0,40}?([\d]{1,3}(?:[\.\s][\d]{3})+(?:,\d{2})?)\s+(?:EUR\s+)?([\d]{1,3}(?:[\.\s][\d]{3})+(?:,\d{2})?)",
            text, _re.I)
        if m:
            v1 = _parse_de_number(m.group(1).replace(" ", "."))
            v2 = _parse_de_number(m.group(2).replace(" ", "."))
            if v1 and v2 and v2 > 0:
                ratio = v2 / umsatz_aktuell if umsatz_aktuell > 0 else 0
                if 0.2 <= ratio <= 5.0:
                    logger.info(f"Vorjahresumsatz aus AFS-Zeilenformat: {v2:,.0f} (ratio={ratio:.2f})")
                    return v2

        # Muster 3: "im Vorjahr X EUR" oder "Vj. X" Kontext
        m = _re.search(
            r"(?:im\s+Vorjahr|Vj\.|Vorjahr(?:es)?(?:betrag)?)[:\s]+(?:EUR\s+|T€\s+|TEUR\s+)?([\d]{1,3}(?:[\.\s][\d]{3})*(?:,\d{2})?)",
            text, _re.I)
        if m:
            v2 = _parse_de_number(m.group(1).replace(" ", "."))
            if v2 and v2 > 0:
                # TEUR-Check: wenn Wert << aktueller Umsatz → evtl. in TEUR
                if umsatz_aktuell > 0 and v2 < umsatz_aktuell * 0.01:
                    v2 *= 1000  # Umrechnung TEUR → EUR
                ratio = v2 / umsatz_aktuell if umsatz_aktuell > 0 else 0
                if 0.2 <= ratio <= 5.0:
                    logger.info(f"Vorjahresumsatz aus AFS 'im Vorjahr'-Muster: {v2:,.0f}")
                    return v2

        return None

    def _extract_liquidity_from_bs(self, f: "FinancialData", accounts: list) -> None:
        """v2.9.1: Extrahiert liquide Mittel und kurzfristige Verbindlichkeiten aus Bilanz-Tree.
        v2.10.26: Auch Forderungen aus LuL für DSO-Berechnung."""
        CASH_FRAGMENTS = [
            "kassenbestand", "zahlungsmittel", "flüssige mittel", "liquide mittel",
            "guthaben bei kreditinstituten", "bankguthaben",
            "cash and cash equivalents", "cash equivalents",
        ]
        CURRENT_LIAB_FRAGMENTS = [
            "kurzfristige verbindlichkeiten", "verbindlichkeiten kurzfristig",
            "current liabilities", "restlaufzeit bis zu einem jahr",
            "restlaufzeit bis 1 jahr", "kurzfristig",
        ]
        # v2.10.26: Forderungen aus Lieferungen und Leistungen (für DSO)
        RECEIVABLES_FRAGMENTS = [
            "forderungen aus lieferungen und leistungen",
            "forderungen aus lieferungen",
            "forderungen lul",
            "forderungen l+l",
            "trade receivables",
            "accounts receivable",
            "forderungen",   # Fallback: breiter, aber nach spezifischeren Treffern
        ]
        # v2.10.32: Vorräte / Inventory (Umlaufvermögen, für Working Capital)
        INVENTORY_FRAGMENTS = [
            "vorräte", "vorratsbestand", "roh- hilfs- und betriebsstoffe",
            "unfertige erzeugnisse", "fertige erzeugnisse",
            "inventories", "inventory", "stock", "raw materials",
        ]
        # v2.10.32: Langfristiges Fremdkapital (FK-Fälligkeitsstruktur)
        LONG_TERM_LIAB_FRAGMENTS = [
            "langfristige verbindlichkeiten", "verbindlichkeiten langfristig",
            "long-term liabilities", "long term liabilities",
            "non-current liabilities", "noncurrent liabilities",
            "langfristige schulden", "restlaufzeit mehr als ein jahr",
        ]
        # v2.10.30: Konzernverbindlichkeiten (Eigenkapitalersatz bei KG mit Konzernrückhalt)
        INTERCOMPANY_LIAB_FRAGMENTS = [
            "verbindlichkeiten gegenüber verbundenen unternehmen",
            "verbindlichkeiten verbundene unternehmen",
            "liabilities to affiliated companies",
            "liabilities to related companies",
            "due to affiliated companies",
            "intercompany liabilities",
        ]

        def _get_lbl(item):
            n = item.get("name") or {}
            if isinstance(n, dict):
                return (n.get("in_report") or n.get("de") or n.get("en") or "").lower()
            return str(n).lower()

        def _walk(items, fragments, holder):
            for item in items:
                lbl = _get_lbl(item)
                if any(fr in lbl for fr in fragments):
                    val = item.get("value")
                    if val is not None and holder[0] is None:
                        try:
                            v = float(val)
                            if v > 0:
                                holder[0] = v
                                return
                        except: pass
                _walk(item.get("children", []), fragments, holder)

        # v2.10.26: Für Forderungen zuerst spezifisch suchen, dann breit
        def _walk_receivables(items, holder):
            # Runde 1: Spezifische Treffer (LuL, Trade Receivables)
            _walk(items, RECEIVABLES_FRAGMENTS[:5], holder)
            # Runde 2: Fallback auf generisches "forderungen" wenn nichts gefunden
            if holder[0] is None:
                _walk(items, ["forderungen"], holder)

        cash_h = [None]; fk_h = [None]; recv_h = [None]; ic_h = [None]
        inv_h = [None]; ltfk_h = [None]  # v2.10.32: Vorräte + langfristiges FK
        _walk(accounts, CASH_FRAGMENTS, cash_h)
        _walk(accounts, CURRENT_LIAB_FRAGMENTS, fk_h)
        _walk_receivables(accounts, recv_h)
        _walk(accounts, INTERCOMPANY_LIAB_FRAGMENTS, ic_h)
        _walk(accounts, INVENTORY_FRAGMENTS, inv_h)          # v2.10.32
        _walk(accounts, LONG_TERM_LIAB_FRAGMENTS, ltfk_h)   # v2.10.32

        if cash_h[0] is not None and not f.__dict__.get("liquide_mittel"):
            f.__dict__["liquide_mittel"] = cash_h[0]
            logger.info(f"Liquide Mittel aus BS-Tree: {cash_h[0]:,.0f}")
        if fk_h[0] is not None and not f.__dict__.get("kurzfristiges_fk"):
            f.__dict__["kurzfristiges_fk"] = fk_h[0]
            logger.info(f"Kurzfristiges FK aus BS-Tree: {fk_h[0]:,.0f}")
        if recv_h[0] is not None and not f.__dict__.get("forderungen"):
            f.__dict__["forderungen"] = recv_h[0]
            logger.info(f"Forderungen aus BS-Tree: {recv_h[0]:,.0f}")
        # v2.10.30: Konzernverbindlichkeiten speichern (wird in score_by_name für KG-Bereinigung genutzt)
        if ic_h[0] is not None and not f.__dict__.get("konzernverbindlichkeiten"):
            f.__dict__["konzernverbindlichkeiten"] = ic_h[0]
            logger.info(f"Konzernverbindlichkeiten aus BS-Tree: {ic_h[0]:,.0f}")
        # v2.10.32: Vorräte + langfristiges FK speichern
        if inv_h[0] is not None and f.vorraete is None:
            f.vorraete = inv_h[0]
            logger.info(f"Vorräte aus BS-Tree: {inv_h[0]:,.0f}")
        if ltfk_h[0] is not None and f.langfristiges_fk is None:
            f.langfristiges_fk = ltfk_h[0]
            logger.info(f"Langfristiges FK aus BS-Tree: {ltfk_h[0]:,.0f}")

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
        # v2.9.1: Liquide Mittel und kurzfristiges FK aus Bilanz-Tree
        self._extract_liquidity_from_bs(f, accounts)

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

    # ── Wikipedia + DDG Enrichment-Methoden (v2.10.13) ──────────────────────

    def _wiki_enrich(self, company_name: str) -> dict:
        """v2.10.14: Wikipedia + Wikidata als primäre Enrichment-Quelle.
        Gibt dict zurück: {employees, founded_year, ceo_names, shareholders, wiki_text}.
        Kein DDG, kein Rate-Limit. 3 HTTP-Calls: Wikipedia Q-ID → Wikidata → Wikipedia-Extrakt."""
        import urllib.parse
        result = {"employees": None, "founded_year": None, "ceo_names": [],
                  "shareholders": [], "wiki_text": ""}

        # v2.10.22: Robuste Variantenbildung — strippt mehrteilige Rechtsformen
        # z.B. "WESSLING Consulting Engineering GmbH & Co. KG" → "WESSLING Consulting Engineering" → "WESSLING"
        _RF_PAT = re.compile(
            r'\s*(?:GmbH\s*&\s*Co\.?\s*KGa?A?|AG\s*&\s*Co\.?\s*KGa?A?|'
            r'GmbH\s*&\s*Co\.?\s*OHG|SE\s*&\s*Co\.?\s*KG|'
            r'GmbH|AG|SE|KGa?A?|OHG|UG(?:\s*\(haftungsbeschränkt\))?|'
            r'Plc\.?|Inc\.?|Corp\.?|Ltd\.?|S\.A\.|N\.V\.|B\.V\.)\s*$',
            re.I)
        variants = [company_name]
        stripped = _RF_PAT.sub("", company_name).strip().rstrip("&,. ")
        if stripped and stripped != company_name:
            variants.append(stripped)
        # Kurzvariante: erstes bedeutendes Wort (ignoriert generische Deskriptoren)
        _GENERIC = {"consulting", "engineering", "services", "solutions", "group",
                    "holding", "management", "international", "deutschland", "germany"}
        words = [w for w in (stripped or company_name).split()
                 if len(w) >= 4 and w.lower() not in _GENERIC]
        short = words[0] if words else None
        if short and short not in variants and short != company_name:
            variants.append(short)

        wiki_title = None
        wikidata_qid = None

        # ── Schritt 1: Wikipedia-Artikel + Q-ID per pageprops ──────────────────
        # v2.10.29: alle Varianten versuchen (nicht nur [:2]) damit "WESSLING" auch getroffen wird
        for lang in ("en", "de"):
            for name in variants:
                title = urllib.parse.quote(name.replace(" ", "_"))
                url = (f"https://{lang}.wikipedia.org/w/api.php"
                       f"?action=query&prop=pageprops|extracts&exintro=true&explaintext=true"
                       f"&titles={title}&format=json&redirects=1")
                try:
                    r = requests.get(url, headers={"User-Agent": "OpenRiskBot/1.0"}, timeout=6)
                    if r.status_code != 200:
                        continue
                    data = r.json()
                    pages = data.get("query", {}).get("pages", {})
                    for page in pages.values():
                        if page.get("pageid", -1) < 0:
                            continue
                        extract = page.get("extract", "")
                        if extract and len(extract) > 80:
                            result["wiki_text"] += " " + extract
                            if not wikidata_qid:
                                wikidata_qid = page.get("pageprops", {}).get("wikibase_item")
                            wiki_title = title
                            break
                except Exception as e:
                    logger.debug(f"Wikipedia {lang} Fehler '{name}': {e}")
            if wiki_title:
                break  # EN reicht, DE nur als Fallback

        # ── Schritt 2: Wikidata für Gründungsjahr + CEO ────────────────────────
        if wikidata_qid:
            try:
                wd_url = (f"https://www.wikidata.org/w/api.php"
                          f"?action=wbgetentities&ids={wikidata_qid}"
                          f"&format=json&languages=de|en&props=claims")
                rw = requests.get(wd_url, headers={"User-Agent": "OpenRiskBot/1.0"}, timeout=6)
                claims = rw.json().get("entities", {}).get(wikidata_qid, {}).get("claims", {})

                # P571 = inception (Gründungsdatum)
                for c in claims.get("P571", []):
                    time_str = (c.get("mainsnak", {})
                                 .get("datavalue", {})
                                 .get("value", {})
                                 .get("time", ""))
                    m = re.match(r'\+(\d{4})', time_str)
                    if m:
                        result["founded_year"] = int(m.group(1))
                        break

                # P169 = current CEO (ohne Enddatum P582)
                ceo_qids = []
                for c in claims.get("P169", []):
                    if "P582" in c.get("qualifiers", {}):
                        continue  # hat Enddatum → ehemaliger CEO
                    val = c.get("mainsnak", {}).get("datavalue", {}).get("value", {})
                    if isinstance(val, dict) and "id" in val:
                        ceo_qids.append(val["id"])

                # P1084 (executives) als Alternative
                for c in claims.get("P1037", []):  # director/manager
                    if "P582" in c.get("qualifiers", {}):
                        continue
                    val = c.get("mainsnak", {}).get("datavalue", {}).get("value", {})
                    if isinstance(val, dict) and "id" in val:
                        ceo_qids.append(val["id"])

                # P127 = owned by (Aktionäre mit P1107 = Anteil in Dezimal)
                owner_qids = []
                owner_pcts: dict = {}
                seen_owner_qids: set = set()  # Deduplizierung nach Q-ID
                for c in claims.get("P127", []):
                    if "P582" in c.get("qualifiers", {}):
                        continue  # Enddatum → ehemaliger Eigentümer
                    val = c.get("mainsnak", {}).get("datavalue", {}).get("value", {})
                    if not isinstance(val, dict) or "id" not in val:
                        continue
                    eid = val["id"]
                    if eid in seen_owner_qids:
                        continue  # Duplikat überspringen
                    seen_owner_qids.add(eid)
                    owner_qids.append(eid)
                    # P1107 = Anteil als Dezimalzahl (0.07 = 7%)
                    pct_list = c.get("qualifiers", {}).get("P1107", [])
                    if pct_list:
                        amount = (pct_list[0].get("datavalue", {})
                                  .get("value", {}).get("amount", ""))
                        try:
                            owner_pcts[eid] = round(float(amount.lstrip("+")) * 100, 2)
                        except: pass

                # Namen für CEO-Q-IDs + Owner-Q-IDs gemeinsam auflösen
                all_resolve = list(dict.fromkeys(ceo_qids[:6] + owner_qids[:8]))
                if all_resolve:
                    ids_str = "|".join(all_resolve[:14])
                    nm_url = (f"https://www.wikidata.org/w/api.php"
                              f"?action=wbgetentities&ids={ids_str}"
                              f"&format=json&languages=en|de&props=labels")
                    rn = requests.get(nm_url, headers={"User-Agent": "OpenRiskBot/1.0"}, timeout=5)
                    name_map: dict = {}
                    for eid, ent in rn.json().get("entities", {}).items():
                        labels = ent.get("labels", {})
                        nm = (labels.get("en", {}).get("value")
                              or labels.get("de", {}).get("value", ""))
                        if nm:
                            name_map[eid] = nm
                    # CEO-Namen
                    for eid in ceo_qids:
                        if eid in name_map:
                            result["ceo_names"].append(name_map[eid])
                    # Aktionäre: mit % wenn vorhanden, sonst ohne
                    for eid in owner_qids:
                        nm = name_map.get(eid)
                        if not nm:
                            continue
                        pct = owner_pcts.get(eid)
                        if pct:
                            result["shareholders"].append(f"{nm} ({pct:.2f}%)")
                        else:
                            result["shareholders"].append(nm)

            except Exception as e:
                logger.warning(f"Wikidata Fehler '{company_name}': {e}")

        # ── Schritt 3: Mitarbeiter aus Wikipedia-Text extrahieren ──────────────
        _NUM = re.compile(
            r'(\d[\d\.,]{0,9})\s*(?:Mitarbeiter|Beschäftigte|Angestellte|employees|headcount)',
            re.I)
        _NUM_REV = re.compile(
            r'(?:approximately|about|etwa|rund|über|more than|around)\s+(\d[\d,\.]{1,9})'
            r'\s+(?:employees|Mitarbeiter)',
            re.I)
        cands = []
        for pat in (_NUM, _NUM_REV):
            for m in pat.finditer(result["wiki_text"]):
                try:
                    raw = m.group(1).replace(".", "").replace(",", "").strip()
                    val = int(raw)
                    if 10 <= val <= 5_000_000:
                        cands.append(val)
                except: pass
        if cands:
            result["employees"] = max(cands)

        logger.info(
            f"_wiki_enrich '{company_name}': "
            f"MA={result['employees']}, GJ={result['founded_year']}, "
            f"CEO={result['ceo_names']}, text={len(result['wiki_text'])}ch"
        )
        return result

    def ddg_find_mitarbeiter(self, company_name: str, _wiki_cache: dict = None) -> tuple:
        """v2.10.13: Wikipedia/Wikidata-first, DDG-Fallback."""
        data = _wiki_cache if _wiki_cache is not None else self._wiki_enrich(company_name)
        if data["employees"]:
            fmt = f"{data['employees']:,}".replace(",", ".")
            return fmt, "Wikipedia"

        # DDG Fallback — v2.10.22: auch Kurznamen versuchen
        _NUM = re.compile(
            r'(\d[\d\.,]{0,9})\s*(?:Mitarbeiter|Beschäftigte|Angestellte|employees|headcount)',
            re.I)
        _RF_S = re.compile(
            r'\s*(?:GmbH\s*&\s*Co\.?\s*KGa?A?|AG\s*&\s*Co\.?\s*KGa?A?|GmbH\s*&\s*Co\.?\s*OHG|'
            r'SE\s*&\s*Co\.?\s*KG|GmbH|AG|SE|KGa?A?|OHG|UG|Plc\.?|Inc\.?|Corp\.?|Ltd\.?)\s*$', re.I)
        short_name = _RF_S.sub("", company_name).strip().rstrip("&,. ").split()[0] if company_name else company_name
        cands = []
        queries = [f'"{company_name}" Mitarbeiter', f'"{company_name}" employees',
                   f'"{short_name}" Mitarbeiter', f'"{short_name}" employees']
        for q in queries:
            for snip in self._ddg_query(q):
                for m in _NUM.finditer(snip):
                    try:
                        raw = m.group(1).replace(".", "").replace(",", "").strip()
                        val = int(raw)
                        if 10 <= val <= 5_000_000:
                            cands.append(val)
                    except: pass
            if cands:
                break
        if cands:
            best = max(cands)
            return f"{best:,}".replace(",", "."), "DuckDuckGo"
        return None, "nicht gefunden"

    def ddg_find_vorstand_names(self, company_name: str, rechtsform: str = "",
                                 _wiki_cache: dict = None) -> tuple:
        """v2.10.13: Wikidata P169 (aktueller CEO) als primäre Quelle, DDG-Fallback."""
        data = _wiki_cache if _wiki_cache is not None else self._wiki_enrich(company_name)
        if data["ceo_names"]:
            result = ", ".join(data["ceo_names"][:4])
            return result, "Wikidata"

        # DDG Fallback
        is_ag_se = any(x in (rechtsform or "").lower() for x in ("ag", "se", "kgaa", "plc"))
        role_label = "Vorstand" if is_ag_se else "Geschäftsführer"
        _NAME = re.compile(
            r'\b([A-ZÄÖÜ][a-zäöüß]{1,20}(?:-[A-ZÄÖÜ][a-zäöüß]{1,20})?\s+[A-ZÄÖÜ][a-zäöüß]{2,25})\b')
        _ROLE = re.compile(
            r'(?:CEO|CFO|CTO|Vorstand|chief executive|chief financial|geschäftsführ|Vorsitzend)',
            re.I)
        _STOP = {"GmbH", "AG", "SE", "KG", "Holding", "Group", "Inc", "Corp",
                 "Das", "Die", "Der", "Seit", "Von", "New", "North", "South"}
        # v2.10.22: Kurznamen für bessere DDG-Treffer bei Mittelstandsfirmen
        _RF_S2 = re.compile(
            r'\s*(?:GmbH\s*&\s*Co\.?\s*KGa?A?|AG\s*&\s*Co\.?\s*KGa?A?|GmbH\s*&\s*Co\.?\s*OHG|'
            r'SE\s*&\s*Co\.?\s*KG|GmbH|AG|SE|KGa?A?|OHG|UG|Plc\.?|Inc\.?|Corp\.?|Ltd\.?)\s*$', re.I)
        short_cn = _RF_S2.sub("", company_name).strip().rstrip("&,. ").split()[0] if company_name else company_name
        cands = {}
        for q in (f'"{company_name}" {role_label} CEO', f'"{short_cn}" {role_label} Geschäftsführer'):
            for snip in self._ddg_query(q):
                if not _ROLE.search(snip):
                    continue
                for m in _NAME.finditer(snip):
                    n = m.group(1).strip()
                    if len(n) < 8 or any(p in _STOP for p in n.split()):
                        continue
                    cands[n] = cands.get(n, 0) + 1
        if cands:
            names = [n for n, c in sorted(cands.items(), key=lambda x: -x[1]) if c >= 1][:4]
            if names:
                return ", ".join(names), "DuckDuckGo"
        return None, "nicht gefunden"

    def ddg_find_gruendungsjahr(self, company_name: str, current_hr_year: Optional[str] = None,
                                 _wiki_cache: dict = None) -> tuple:
        """v2.10.13: Wikidata P571 (inception) als primäre Quelle, DDG-Fallback."""
        data = _wiki_cache if _wiki_cache is not None else self._wiki_enrich(company_name)
        if data["founded_year"]:
            yr = data["founded_year"]
            if current_hr_year is None or yr <= int(current_hr_year):
                return yr, "Wikidata"

        # DDG Fallback
        _YEAR = re.compile(
            r'(?:gegr[üu]ndet|gr[üu]ndung|founded|incorporated|established)\s*'
            r'(?:in\s+)?(?:[A-Za-z]+\s+\d{1,2},?\s*)?(\d{4})',
            re.I)
        # v2.10.22: auch Kurznamen probieren
        _RF_S3 = re.compile(
            r'\s*(?:GmbH\s*&\s*Co\.?\s*KGa?A?|AG\s*&\s*Co\.?\s*KGa?A?|GmbH\s*&\s*Co\.?\s*OHG|'
            r'SE\s*&\s*Co\.?\s*KG|GmbH|AG|SE|KGa?A?|OHG|UG|Plc\.?|Inc\.?|Corp\.?|Ltd\.?)\s*$', re.I)
        short_gj = _RF_S3.sub("", company_name).strip().rstrip("&,. ").split()[0] if company_name else company_name
        for q in (f'"{company_name}" gegründet founded', f'"{short_gj}" gegründet Gründungsjahr'):
            for snip in self._ddg_query(q):
                m = _YEAR.search(snip)
                if m:
                    try:
                        yr = int(m.group(1))
                        if 1800 <= yr <= 2024:
                            if current_hr_year is None or yr <= int(current_hr_year):
                                return yr, "DuckDuckGo"
                    except: pass
        return None, "nicht gefunden"

    def ddg_find_investoren(self, company_name: str, rechtsform: str = "",
                             _wiki_cache: dict = None) -> tuple:
        """v2.10.14: Aktionärsstruktur — Strategie nach Rechtsform:
        AG/SE/KGaA: Wikidata P127 (WpHG-Meldepflicht) → primär, zuverlässig
        GmbH/KG:    Gesellschafter selten öffentlich → DDG als Versuch
        UG:         Kaum öffentlich → leer lassen (kein DDG-Versuch)
        """
        rf_upper = (rechtsform or "").upper()
        is_listed  = any(x in rf_upper for x in ("AG", "SE", "KGAA", "PLC"))
        is_gmbh_kg = any(x in rf_upper for x in ("GMBH", "KG", "OHG", "PARTG"))
        is_ug      = "UG" in rf_upper

        # ── 1. Wikidata P127 für AG/SE/KGaA ────────────────────────────────────
        if is_listed:
            data = _wiki_cache if _wiki_cache is not None else self._wiki_enrich(company_name)
            if data.get("shareholders"):
                result = "; ".join(data["shareholders"][:6])
                logger.info(f"Wikidata Aktionäre '{company_name}': {result}")
                return result, "Wikidata (öffentliche Meldungen)"

        # ── 2. UG: keine öffentlichen Daten erwartet → sofort leer ─────────────
        if is_ug:
            return None, "Nicht öffentlich verfügbar"

        # ── 3. GmbH/KG: DDG-Versuch (manchmal Impressum-Daten) ─────────────────
        _PCT = re.compile(
            r'([\w][A-Za-zÄÖÜäöüß\s&\.\-]{2,45}?)\s*[:\(]\s*(\d{1,3}[,\.]\d{1,2})\s*%',
            re.I)
        _FREE = re.compile(
            r'(?:Streubesitz|Free\s*Float)[^\d]{0,20}(\d{1,3}[,\.]\d{1,2})\s*%', re.I)
        investors: list = []
        seen: set = set()
        _STOP = {"Die", "Der", "Das", "Eine", "Seit", "Beim", "Nach", "Über",
                 "Stand", "Anteil", "Prozent", "Quelle", "Weitere", "Mehr"}

        def _add(name: str, pct: str):
            name = name.strip().rstrip(" ,.(")
            if len(name) < 4 or name.lower() in seen: return
            if any(w in _STOP for w in name.split()): return
            seen.add(name.lower())
            investors.append(f"{name} ({pct.replace(',', '.')}%)")

        def _scan(snippets: list):
            for snip in snippets:
                fm = _FREE.search(snip)
                if fm: _add("Streubesitz", fm.group(1))
                for m in _PCT.finditer(snip): _add(m.group(1), m.group(2))

        # v2.10.29: Kurznamen für bessere DDG-Treffer (wie in ddg_find_vorstand_names)
        _RF_INV = re.compile(
            r'\s*(?:GmbH\s*&\s*Co\.?\s*KGa?A?|AG\s*&\s*Co\.?\s*KGa?A?|GmbH\s*&\s*Co\.?\s*OHG|'
            r'SE\s*&\s*Co\.?\s*KG|GmbH|AG|SE|KGa?A?|OHG|UG|Plc\.?|Inc\.?|Corp\.?|Ltd\.?)\s*$', re.I)
        _GENERIC_INV = {"consulting","engineering","services","solutions","group",
                        "holding","management","international","deutschland","germany"}
        _words_inv = [w for w in _RF_INV.sub("", company_name).strip().rstrip("&,. ").split()
                      if len(w) >= 4 and w.lower() not in _GENERIC_INV]
        short_inv = _words_inv[0] if _words_inv else company_name.split()[0]

        if is_listed:
            # AG/SE: Wikidata hat nichts gefunden → DDG-Fallback
            _scan(self._ddg_query(f'"{company_name}" Aktionärsstruktur Hauptaktionäre'))
        elif is_gmbh_kg:
            # Kurznamen zuerst (bessere Trefferquote bei Mittelstand)
            _scan(self._ddg_query(f'"{short_inv}" Gesellschafter Eigentümer Anteil'))
            if not investors:
                _scan(self._ddg_query(f'"{company_name}" Gesellschafter Eigentümer'))

        if investors:
            result = "; ".join(investors[:6])
            logger.info(f"DDG Investoren '{company_name}' [{rechtsform}]: {result}")
            return result, "DuckDuckGo (öffentliche Meldungen)"
        return None, "Nicht öffentlich verfügbar"


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

@app.get("/api/hr_status")
async def hr_status_endpoint():
    """v2.10.17: Diagnose-Endpunkt — zeigt echten HTTP-Status von handelsregister.ai."""
    key = hr_client.api_key
    if not key:
        return {"api_key_set": False, "status": "NO_KEY"}
    try:
        import requests as _req
        resp = _req.get(
            f"{hr_client.BASE_URL}/v1/fetch-organization",
            params={"q": "SAP SE", "feature": "financial_kpi"},
            headers={"x-api-key": key, "Accept": "application/json"},
            timeout=8
        )
        return {
            "api_key_set": True,
            "http_status": resp.status_code,
            "status": {
                200: "OK — Credits vorhanden, Daten gefunden",
                401: "UNAUTHORIZED — API-Key ungültig",
                402: "PAYMENT_REQUIRED — Credits aufgebraucht",
                404: "NOT_FOUND — Key gültig, aber Unternehmen nicht in DB",
            }.get(resp.status_code, f"UNBEKANNT ({resp.status_code})"),
            "response_preview": resp.text[:200] if resp.status_code != 200 else "(OK)"
        }
    except Exception as e:
        return {"api_key_set": True, "status": "ERROR", "detail": str(e)}

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
        if hr_name:
            raise HTTPException(
                status_code=404,
                detail=(
                    f"Firma '{hr_name}' im Handelsregister gefunden, aber noch keine "
                    f"Jahresabschluesse veroeffentlicht. Bitte Daten manuell ueber "
                    f"/api/scoring eingeben."
                )
            )
        raise HTTPException(status_code=404, detail=f"Keine Finanzdaten fuer {name!r} bei handelsregister.ai gefunden. Bitte HR-Nummer angeben oder /api/scoring mit manuellen Daten nutzen.")
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

# ── v2.10.0: Enrichment-Modelle ──────────────────────────────────────────────

class CompanySearchResult(BaseModel):
    """v2.10.3: Ein Treffer aus der Unternehmenssuche."""
    name: str
    city: Optional[str] = None
    rechtsform: Optional[str] = None
    hr_nummer: Optional[str] = None       # für exakten HR.ai-Lookup
    registration_date: Optional[str] = None
    umsatz_hint: Optional[float] = None   # grobe Größenordnung für Sortierung
    geschaeftsjahr: Optional[str] = None

class CompanySearchResponse(BaseModel):
    query: str
    results: List[CompanySearchResult]
    count: int


class EnrichmentField(BaseModel):
    value: Optional[Any] = None
    source: str = "nicht gefunden"   # "handelsregister.ai" | "DuckDuckGo" | "Jahresabschluss-Text" | ...
    confidence: str = "niedrig"       # "hoch" | "mittel" | "niedrig"

class EnrichmentResult(BaseModel):
    """v2.10.0: Recherche-Ergebnis vor dem Scoring — zur Bestätigung durch den Nutzer."""
    company_name_hr: Optional[str] = None
    rechtsform: Optional[str] = None
    geschaeftsjahr: Optional[str] = None
    # Finanzdaten (HR.ai, read-only)
    umsatz: Optional[float] = None
    jahresergebnis: Optional[float] = None
    bilanzsumme: Optional[float] = None
    eigenkapital: Optional[float] = None
    # Anreicherte / überprüfbare Felder
    mitarbeiter: EnrichmentField = EnrichmentField()
    fuehrungspersonen: EnrichmentField = EnrichmentField()   # Vorstand / GF
    gruendungsjahr: EnrichmentField = EnrichmentField()
    investorenstruktur: EnrichmentField = EnrichmentField()
    liquide_mittel: EnrichmentField = EnrichmentField()

class EnrichmentRequest(BaseModel):
    company_name: str
    hr_nummer: Optional[str] = None
    rechtsform_hint: Optional[str] = None       # z.B. "SE", "GmbH" → für korrektes Vorstand/GF-Label
    registration_date: Optional[str] = None     # v2.10.10: aus Suchergebnis, Fallback für Gründungsjahr


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
    # v2.9.1: Dimensionen ohne valide Daten (Gewicht=0, wird auf andere umverteilt)
    skip_dimensions: Optional[List[str]] = None
    # v2.10.28: Für perspektiv-spezifische Empfehlungen
    miet_leasing: Optional[float] = None          # Off-Balance Leasingverpflichtungen
    umsatz_wachstum_pct: Optional[float] = None   # YoY Umsatzwachstum
    # v2.10.32: Neu extrahierte KPIs aus Bilanz + GuV
    vorraete: Optional[float] = None              # Vorräte (für Working Capital / Current Ratio)
    langfristiges_fk: Optional[float] = None      # Langfristiges FK (FK-Fälligkeitsstruktur)
    ebitda: Optional[float] = None               # EBITDA (operativer Cash-Flow-Proxy)
    zinsdeckungsgrad: Optional[float] = None      # EBIT / Zinsaufwand (Schuldentragfähigkeit)

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
    # v2.10.25: 4 Sub-Scores + Handlungsempfehlungen
    sub_scores: Optional[Any] = None                       # {finanzstaerke, zahlungsverhalten, marktposition, unternehmensqualitaet}
    empfehlungen: Optional[Any] = None                     # [{kategorie, empfehlung, begruendung, prioritaet, icon}]

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

def _groessen_modifikator(umsatz, ma=None):
    """v2.10.21: Größenklassen-Modifikator auf P(Zahlungsproblem).
    Quelle: KfW KMU-Panel + Bundesbank MFI-Statistik.
    Großunternehmen haben strukturell niedrigere Ausfallraten als Mittelstand.
    Kalibrierung: KMU ~2% p.a.; Mittelstand oben ~0.8%; Großunternehmen ~0.3%; Konzerne ~0.1%.
    Returns: Multiplikator (1.0 = kein Einfluss; <1.0 = Reduktion).
    Primär: Umsatz. Sekundär: Mitarbeiterzahl (wenn Umsatz fehlt)."""
    if umsatz and umsatz >= 5_000_000_000:   return 0.12  # >5 Mrd → Konzern/DAX
    if umsatz and umsatz >= 500_000_000:     return 0.28  # >500 Mio → Großunternehmen
    if umsatz and umsatz >= 50_000_000:      return 0.55  # >50 Mio → oberer Mittelstand
    if ma and ma >= 10_000:                  return 0.12  # Fallback MA
    if ma and ma >= 1_000:                   return 0.28
    if ma and ma >= 250:                     return 0.55
    return 1.0  # KMU / Kleinstunternehmen

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
# v2.10.34: Stärker kalibriert — 100%-Töchter (Score 9) profitieren deutlich mehr.
# Begründung: Bei ≥95% Mutter-Anteil übernimmt Konzern faktisch Haftung (Patronat, Gewinnabführung).
# Standalone-EK-Quote von GmbH & Co. KG-Töchtern ist strukturell verzerrt durch Konzernfinanzierung.
# Quelle: Moody's Parent-Subsidiary Credit Linkage (2022), Creditreform Konzernbonitätsanalyse.
_KONZERN_MOD = {0: 1.55, 1: 1.40, 2: 1.28, 3: 1.18, 4: 1.08,
                5: 1.00,
                6: 0.90, 7: 0.80, 8: 0.68, 9: 0.58, 10: 0.50}

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

def _rating_equivalenz(bi: int) -> list:
    """v2.10.23: Ratingäquivalenz-Tabelle für einen gegebenen Bonitätsindex (100-600).
    Mappt auf gängige Bankratings und Agenturen-Skalen.
    Quellen: Commerzbank KMU-Rating, Sparkassen-Rating, S&P/Moody's/Fitch KMU-Mapping."""
    table = [
        # (bi_von, bi_bis, openrisk, sp,      moodys,  fitch,  commerzbank,  sparkasse,  bundesbank)
        (100, 149, "A",  "AAA–AA", "Aaa–Aa", "AAA–AA", "1",   "1–2",   "sehr gut"),
        (150, 199, "B",  "A",      "A",       "A",      "2",   "3–4",   "gut"),
        (200, 249, "C",  "BBB",    "Baa",     "BBB",    "3",   "5–6",   "befriedigend"),
        (250, 299, "D",  "BB+",    "Ba1",     "BB+",    "4",   "7–8",   "ausreichend"),
        (300, 349, "E",  "BB–B+",  "Ba2–Ba3", "BB–B+",  "4–5", "9–10",  "erhöhtes Risiko"),
        (350, 449, "F",  "B",      "B",       "B",      "5–6", "11–12", "kritisch"),
        (450, 549, "G",  "CCC",    "Caa",     "CCC",    "6",   "13–14", "sehr kritisch"),
        (550, 600, "H",  "CC–D",   "Ca–C",    "CC–D",   "6",   "15–16", "höchstes Risiko"),
    ]
    for lo, hi, or_g, sp, mo, fi, cb, spk, bb in table:
        if lo <= bi <= hi:
            return [
                {"skala": "OpenRisk", "rating": or_g, "beschreibung": f"Score {bi}/600"},
                {"skala": "S&P (äquivalent)", "rating": sp, "beschreibung": ""},
                {"skala": "Moody's (äquivalent)", "rating": mo, "beschreibung": ""},
                {"skala": "Fitch (äquivalent)", "rating": fi, "beschreibung": ""},
                {"skala": "Commerzbank KMU", "rating": cb, "beschreibung": "Risikoklasse"},
                {"skala": "Sparkassen-Rating", "rating": spk, "beschreibung": "Ratingklasse"},
                {"skala": "Bundesbank-Kategorie", "rating": bb, "beschreibung": ""},
            ]
    return []

def _calc_sub_scores(dims: list) -> dict:
    """v2.10.27: 4 Stakeholder-Perspektiv-Scores aus den 18 Dimensionen.
    Gleiche Firma — unterschiedlicher Score je nach Betrachtungswinkel des Anfragenden.
    Methodik: Perspektiv-spezifische Multiplikatoren auf die Basis-Gewichtung jeder Dimension.
    Score 10 → BI 100 (exzellent), Score 0 → BI 600 (höchstes Risiko).
    """
    # Multiplikatoren je Perspektive: welche Dimensionen sind für wen wie wichtig?
    # 1.0 = Standardgewichtung, >1 = übergewichtet, <1 = untergewichtet
    PERSPEKTIVEN = {
        "lieferant": {
            "label": "Für Lieferanten",
            "icon": "🏭",
            "fokus": "Zahlungsverhalten & Liquidität",
            "kernfrage": "Zahlt der Kunde pünktlich und zuverlässig?",
            "multiplier": {
                # Kernrelevanz: Zahlungsverhalten
                "zahlungsweise":           3.0,
                "insolvenz":               3.0,
                "liquiditaet":             2.5,
                "verschuldungsgrad":       1.5,
                "eigenkapitalquote":       1.5,
                "verlustentwicklung":      1.5,
                "ergebnismarge":           1.0,
                # Weniger relevant für Lieferanten
                "branchenrisiko":          0.8,
                "unternehmensalter":       0.8,
                "rechtsform":              0.6,
                "konzernstruktur":         0.6,
                "branchenvergleich_peer":  0.5,
                "gf_bonitaet":             0.5,
                "mitarbeiterzahl":         0.4,
                "investorenstruktur":      0.4,
                "presse":                  0.5,
                "kosten_pro_ma":           0.5,
                "umsatz_pro_ma":           0.5,
            },
        },
        "investor": {
            "label": "Für Investoren",
            "icon": "💰",
            "fokus": "Profitabilität & Wachstum",
            "kernfrage": "Ist das Unternehmen profitabel, wachstumsstark und gut geführt?",
            "multiplier": {
                # Kernrelevanz: Ertragskraft und Struktur
                "ergebnismarge":           3.0,
                "eigenkapitalquote":       2.5,
                "umsatz_pro_ma":           2.0,
                "investorenstruktur":      2.0,
                "branchenvergleich_peer":  2.0,
                "verschuldungsgrad":       2.0,
                "verlustentwicklung":      2.0,
                "branchenrisiko":          1.5,
                "konzernstruktur":         1.5,
                "gf_bonitaet":             1.5,
                "unternehmensalter":       1.2,
                "mitarbeiterzahl":         1.0,
                "kosten_pro_ma":           1.5,
                # Weniger relevant für Investoren
                "liquiditaet":             0.8,
                "rechtsform":              0.6,
                "presse":                  1.0,
                "zahlungsweise":           0.5,  # nur indirekt relevant
                "insolvenz":               1.5,  # Ausschlussrisiko
            },
        },
        "leasinggeber": {
            "label": "Für Leasinggeber",
            "icon": "🏗️",
            "fokus": "Schuldentragfähigkeit & Stabilität",
            "kernfrage": "Kann das Unternehmen Leasingraten langfristig bedienen?",
            "multiplier": {
                # Kernrelevanz: Bilanzstärke und Schuldentragfähigkeit
                "eigenkapitalquote":       3.0,
                "verschuldungsgrad":       3.0,
                "liquiditaet":             2.5,
                "insolvenz":               2.5,
                "ergebnismarge":           2.0,
                "verlustentwicklung":      2.0,
                "unternehmensalter":       1.5,
                "rechtsform":              1.5,
                "zahlungsweise":           1.5,
                "konzernstruktur":         1.5,
                "mitarbeiterzahl":         1.0,
                "branchenrisiko":          1.0,
                # Weniger relevant
                "gf_bonitaet":             0.8,
                "investorenstruktur":      0.6,
                "branchenvergleich_peer":  0.6,
                "umsatz_pro_ma":           0.8,
                "kosten_pro_ma":           0.8,
                "presse":                  0.4,
            },
        },
        "kunde": {
            "label": "Für Kunden",
            "icon": "🤝",
            "fokus": "Lieferzuverlässigkeit & Bestand",
            "kernfrage": "Ist der Lieferant langfristig zuverlässig und wird er liefern können?",
            "multiplier": {
                # Kernrelevanz: Existenz, Verlässlichkeit und finanzielle Gesundheit
                # v2.10.34: zahlungsweise erhöht — finanzielle Schwäche = Lieferausfall-Risiko
                "insolvenz":               3.0,
                "unternehmensalter":       2.5,
                "konzernstruktur":         2.0,   # Konzernrückhalt = Lieferkontinuität
                "mitarbeiterzahl":         2.0,
                "verlustentwicklung":      1.8,   # Verluste gefährden Lieferfähigkeit
                "zahlungsweise":           1.5,   # v2.10.34: hoch (Insolvenzproxy = Lieferausfall)
                "eigenkapitalquote":       1.5,
                "branchenrisiko":          1.5,
                "verschuldungsgrad":       1.3,   # v2.10.34: erhöht (Überschuldung = Existenzrisiko)
                "branchenvergleich_peer":  1.2,
                "presse":                  1.2,
                "liquiditaet":             1.2,   # v2.10.34: erhöht (Liquiditätskrise → Lieferstopp)
                "rechtsform":              1.0,
                "gf_bonitaet":             1.0,
                "ergebnismarge":           0.8,   # v2.10.34: erhöht (Verlustgeschäft = Risiko)
                # Weniger relevant für Kunden
                "investorenstruktur":      0.5,
                "umsatz_pro_ma":           0.5,
                "kosten_pro_ma":           0.5,
            },
        },
    }

    dim_map = {d.name: d for d in dims}
    result = {}

    for pk, pv in PERSPEKTIVEN.items():
        total_w, total_ws = 0.0, 0.0
        for dim_name, d in dim_map.items():
            if d.gewichtung_pct <= 0:
                continue   # skip-Dimensionen (keine Daten)
            mult = pv["multiplier"].get(dim_name, 1.0)
            eff_w = d.gewichtung_pct * mult
            total_ws += d.score_0_10 * eff_w
            total_w  += eff_w

        if total_w == 0:
            avg_score = 5.0
        else:
            avg_score = total_ws / total_w   # 0–10

        bi_equiv = int(round(600 - avg_score * 50))
        bi_equiv = max(100, min(600, bi_equiv))
        rk = _rk(bi_equiv)

        result[pk] = {
            "label":               pv["label"],
            "icon":                pv["icon"],
            "fokus":               pv["fokus"],
            "kernfrage":           pv["kernfrage"],
            "score_0_10":          round(avg_score, 1),
            "bonitaetsindex_equiv": bi_equiv,
            "risikoklasse":        rk,
        }

    return result


def _calc_empfehlungen(bi: int, sub_scores: dict, req: "ScoringRequest") -> dict:
    """v2.10.28: Perspektiv-spezifische Handlungsempfehlungen für alle 4 Stakeholder.
    Gibt ein Dict zurück: {lieferant: [...], investor: [...], leasinggeber: [...], kunde: [...]}.
    Jede Perspektive nutzt ihren eigenen Sub-Score-BI für die Schwellenwerte."""

    umsatz       = req.umsatz or 0
    ek           = req.eigenkapital or 0
    forderungen  = req.forderungen or 0
    miet_leasing = req.miet_leasing or 0
    wachstum     = req.umsatz_wachstum_pct   # kann None sein
    # v2.10.32: Neue KPIs aus Bilanz + GuV
    ebitda         = req.ebitda           # EBITDA in EUR (kann None sein)
    zinsdeckung    = req.zinsdeckungsgrad  # EBIT/Zinsaufwand (kann None sein)
    langfr_fk      = req.langfristiges_fk  # Langfristiges FK in EUR (kann None sein)
    vorraete       = req.vorraete          # Vorräte in EUR (kann None sein)

    # Perspektiv-spezifische BI-Werte aus sub_scores (Fallback: globaler BI)
    bi_l  = (sub_scores.get("lieferant")    or {}).get("bonitaetsindex_equiv", bi)
    bi_i  = (sub_scores.get("investor")     or {}).get("bonitaetsindex_equiv", bi)
    bi_lg = (sub_scores.get("leasinggeber") or {}).get("bonitaetsindex_equiv", bi)
    bi_k  = (sub_scores.get("kunde")        or {}).get("bonitaetsindex_equiv", bi)

    def _e(kategorie, icon, empfehlung, begruendung, prioritaet, wert=None):
        return {"kategorie": kategorie, "icon": icon, "empfehlung": empfehlung,
                "begruendung": begruendung, "prioritaet": prioritaet,
                "wert": wert or empfehlung}

    # ══════════════════════════════════════════════════════════════════════════
    # 🏭 LIEFERANT — 8 Empfehlungskategorien
    # ══════════════════════════════════════════════════════════════════════════
    lieferant = []

    # 1. Zahlungsziel (DSO-basiert wenn Forderungen vorhanden, sonst BI)
    if forderungen > 0 and umsatz > 0:
        dso = round(forderungen / umsatz * 365)
        empf_tage = max(14, min(90, round(dso * 0.5)))
        lieferant.append(_e(
            "Zahlungsziel", "📅",
            f"Empfohlenes Zahlungsziel: {empf_tage} Tage netto",
            (f"DSO-Analyse: Offene Forderungen {forderungen:,.0f} EUR = "
             f"{forderungen/umsatz*100:.0f}% des Umsatzes → Zahlungszyklus ~{dso} Tage. "
             f"Empfehlung: max. 50% des DSO als Zahlungsziel gewähren."),
            "hoch" if dso > 60 else "mittel", f"{empf_tage} Tage",
        ))
    else:
        zt_map = [
            (150, "60–90 Tage möglich",        "Sehr gute Bonität — langes Zahlungsziel vertretbar.",           "niedrig"),
            (250, "30–60 Tage Standard",        "Gute bis solide Bonität — marktübliche Konditionen.",           "niedrig"),
            (350, "14–30 Tage empfohlen",       "Erhöhtes Risiko — kürzere Fristen obligatorisch.",              "mittel"),
            (450, "7–14 Tage / Skonto prüfen",  "Kritische Bonität — kurze Fristen, Skonto als Anreiz.",         "hoch"),
            (601, "Vorkasse",                   "Höchstes Risiko — nur Vorkasse oder erweiterter Eigentumsvorbehalt.", "hoch"),
        ]
        for thr, emf, beg, pri in zt_map:
            if bi_l < thr:
                lieferant.append(_e("Zahlungsziel", "📅", emf, beg, pri)); break

    # 2. Kreditlimit
    if umsatz > 0:
        faktor_map = [(150,0.05),(250,0.03),(300,0.02),(350,0.01),(450,0.003),(601,0.0)]
        faktor = 0.0
        for thr, f in faktor_map:
            if bi_l < thr: faktor = f; break
        limit_umsatz = umsatz * faktor
        limit_ek     = ek * 0.30 if ek > 0 else float("inf")
        kreditlimit  = round(min(limit_umsatz, limit_ek) / 1000) * 1000
        if kreditlimit > 0:
            lieferant.append(_e(
                "Kreditlimit", "💶",
                f"Max. Kreditlimit: {kreditlimit:,.0f} EUR",
                (f"Berechnung: {faktor*100:.1f}% des Jahresumsatzes ({umsatz:,.0f} EUR), "
                 f"gedeckelt auf 30% Eigenkapital. Risikoklasse {_rk(bi_l)[:1]}."),
                "hoch" if bi_l > 350 else "mittel", f"{kreditlimit:,.0f} EUR",
            ))
        else:
            lieferant.append(_e(
                "Kreditlimit", "🚫", "Kein Kreditlimit empfohlen",
                "Bonitätsscore zu niedrig — kein ungesichertes Kreditengagement.", "hoch", "0 EUR",
            ))

    # 3. Sicherheiten — Eskalationsleiter: keine → Selbstauskunft → Bürgschaft → Vorkasse
    sich_map = [
        (250, "Keine Sicherheiten erforderlich",
              "Sehr gute Bonität — Standardrisiko akzeptabel.", "niedrig"),
        (350, "Selbstauskunft: aktuellen Jahresabschluss oder BWA anfordern",
              "Solide Bonität — Bonitätsnachweis als Voraussetzung für Kreditgewährung.", "niedrig"),
        (450, "GF-Bürgschaft oder Anzahlung 25–30 %",
              "Erhöhtes Risiko — persönliche Haftung des GF oder substantielle Teilsicherung.", "mittel"),
        (601, "Vorkasse + erweiterter Eigentumsvorbehalt",
              "Hohes Risiko — kein ungesichertes Warenkredit-Engagement.", "hoch"),
    ]
    for thr, emf, beg, pri in sich_map:
        if bi_l < thr:
            lieferant.append(_e("Sicherheiten", "🔒", emf, beg, pri)); break

    # 4. Monitoring-Frequenz
    mon_map = [
        (200, "Alle 2 Jahre",              "Sehr gute Bonität — Routineprüfung ausreichend.",                  "niedrig"),
        (300, "Jährlich",                  "Gute Bonität — jährliche Aktualisierung empfohlen.",               "niedrig"),
        (350, "Halbjährlich",              "Erhöhtes Risiko — engmaschigeres Monitoring sinnvoll.",            "mittel"),
        (450, "Quartalsweise",             "Kritische Bonität — aktives Kennzahlen-Monitoring.",               "hoch"),
        (601, "Monatlich / kontinuierlich","Sehr kritisch — laufende Beobachtung, sofortige Reaktion.",       "hoch"),
    ]
    for thr, emf, beg, pri in mon_map:
        if bi_l < thr:
            lieferant.append(_e("Monitoring", "📡", f"Überprüfungsintervall: {emf}", beg, pri, emf)); break

    # 5. Skonto (bei mittlerem Risiko als Anreiz für schnelle Zahlung)
    if 250 <= bi_l <= 400 and umsatz > 0:
        skonto_vorteil = round(umsatz * 0.02 * 0.3 / 1000) * 1000
        lieferant.append(_e(
            "Skonto", "💡",
            "Skonto anbieten: 2 % bei Zahlung innerhalb 10 Tagen",
            (f"Bei erhöhtem Risiko schafft Skonto einen Anreiz für schnelle Zahlung. "
             f"Geschätzter Liquiditätsvorteil bei Inanspruchnahme: ~{skonto_vorteil:,.0f} EUR/Jahr."),
            "mittel", "2 % / 10 Tage",
        ))

    # 6. Warenkreditversicherung
    if bi_l >= 300 and umsatz > 100_000:
        praemie = round(umsatz * 0.003 / 1000) * 1000
        lieferant.append(_e(
            "Absicherung", "🛡️",
            "Warenkreditversicherung prüfen",
            (f"Risikoklasse {_rk(bi_l)[:1]} — Warenkreditversicherung schützt bei Zahlungsausfall. "
             f"Typische Prämie: ~{praemie:,.0f} EUR/Jahr (ca. 0,2–0,5 % des abgesicherten Umsatzes)."),
            "hoch" if bi_l >= 400 else "mittel", f"~{praemie:,.0f} EUR/Jahr",
        ))

    # 7. Mahnstufen-Strategie
    mahn_map = [
        (250, "Erste Mahnung nach 30 Tagen Verzug",
              "Sehr gute Bonität — kulante Mahnstrategie vertretbar.", "niedrig"),
        (350, "Erste Mahnung nach 14 Tagen Verzug",
              "Mittlere Bonität — zeitnahes Mahnwesen empfohlen.", "mittel"),
        (450, "Erste Mahnung nach 7 Tagen Verzug",
              "Kritische Bonität — Zahlungsverzug sofort adressieren.", "hoch"),
        (601, "Sofortmahnung + Inkasso-Bereitschaft",
              "Höchstes Risiko — proaktives Mahnwesen, Inkasso-Dienstleister vorbereiten.", "hoch"),
    ]
    for thr, emf, beg, pri in mahn_map:
        if bi_l < thr:
            lieferant.append(_e("Mahnstufen", "⏰", emf, beg, pri)); break

    # 8. Factoring (bei hohem DSO + erhöhtem Risiko)
    if forderungen > 0 and umsatz > 0:
        dso_val = forderungen / umsatz * 365
        if dso_val > 60 and bi_l >= 300:
            lieferant.append(_e(
                "Factoring", "🔄",
                "Factoring prüfen: Forderungsverkauf zur Liquiditätssicherung",
                (f"DSO von ~{round(dso_val)} Tagen bindet erheblich Liquidität. "
                 f"Beim Factoring übernimmt ein Factor das Ausfallrisiko und zahlt sofort aus. "
                 f"Typische Kosten: 0,5–2 % des Forderungsvolumens."),
                "mittel", f"~{forderungen:,.0f} EUR Volumen",
            ))

    # ══════════════════════════════════════════════════════════════════════════
    # 💰 INVESTOR — 5 Empfehlungskategorien
    # ══════════════════════════════════════════════════════════════════════════
    investor = []

    # 1. Investitionseinschätzung
    inv_map = [
        (200, "Strong Buy",    "Exzellente Bonität — sehr attraktives Risiko-Rendite-Profil.",              "niedrig"),
        (270, "Buy",           "Sehr gute Kennzahlen — Investment empfohlen.",                              "niedrig"),
        (330, "Watch",         "Solide Basis, einzelne Risikofaktoren beobachten.",                         "mittel"),
        (400, "Hold / Reduce", "Erhöhtes Risiko — bestehende Positionen überprüfen.",                      "mittel"),
        (500, "Avoid",         "Kritische Lage — kein Neueinstieg empfohlen.",                              "hoch"),
        (601, "Strong Avoid",  "Sehr hohes Ausfall-/Totalverlustrisiko — sofortige Überprüfung nötig.",    "hoch"),
    ]
    for thr, emf, beg, pri in inv_map:
        if bi_i < thr:
            investor.append(_e("Investitionseinschätzung", "📈", emf, beg, pri)); break

    # 2. Wachstumseinschätzung
    if wachstum is not None:
        if wachstum >= 15:
            wachs_text = f"Starkes Wachstum (+{wachstum:.1f}% YoY) — hohes Skalierungspotenzial."
            wachs_prio = "niedrig"
        elif wachstum >= 5:
            wachs_text = f"Moderates Wachstum (+{wachstum:.1f}% YoY) — stabiler Entwicklungspfad."
            wachs_prio = "niedrig"
        elif wachstum >= 0:
            wachs_text = f"Stagnation ({wachstum:+.1f}% YoY) — Wachstumsstrategie prüfen."
            wachs_prio = "mittel"
        else:
            wachs_text = f"Umsatzrückgang ({wachstum:+.1f}% YoY) — strukturelle Ursachen analysieren."
            wachs_prio = "hoch"
        investor.append(_e(
            "Wachstumseinschätzung", "📊",
            f"Umsatzwachstum YoY: {wachstum:+.1f}%",
            wachs_text, wachs_prio, f"{wachstum:+.1f}%",
        ))

    # 3. Kapitalstruktur-Risiko
    if req.fremdkapital and ek > 0:
        vg = (req.fremdkapital or 0) / ek
        if vg > 4:
            investor.append(_e(
                "Kapitalstruktur", "⚠️",
                f"Verschuldungsgrad kritisch: {vg:.1f}x Eigenkapital",
                (f"Ein Verschuldungsgrad von {vg:.1f}x übersteigt branchenübliche Grenzen deutlich. "
                 f"Hohe Zins- und Refinanzierungsrisiken — Due Diligence zur Schuldenstruktur empfohlen."),
                "hoch", f"{vg:.1f}x EK",
            ))
        elif vg > 2:
            investor.append(_e(
                "Kapitalstruktur", "📋",
                f"Verschuldungsgrad erhöht: {vg:.1f}x Eigenkapital",
                f"Verschuldungsgrad von {vg:.1f}x — im mittleren Risikobereich. Schuldenentwicklung beobachten.",
                "mittel", f"{vg:.1f}x EK",
            ))

    # 3b. EBITDA-Marge / Ertragskraft (v2.10.32)
    if ebitda is not None and umsatz > 0:
        ebitda_marge = ebitda / umsatz * 100
        if ebitda_marge >= 15:
            investor.append(_e(
                "EBITDA-Ertragskraft", "💹",
                f"Starke operative Ertragskraft: EBITDA-Marge {ebitda_marge:.1f}%",
                (f"EBITDA von {ebitda:,.0f} EUR entspricht {ebitda_marge:.1f}% des Umsatzes — "
                 f"exzellente Cash-Generierung, hohe Schuldentragfähigkeit."),
                "niedrig", f"{ebitda_marge:.1f}% EBITDA-Marge",
            ))
        elif ebitda_marge >= 8:
            investor.append(_e(
                "EBITDA-Ertragskraft", "💹",
                f"Solide operative Ertragskraft: EBITDA-Marge {ebitda_marge:.1f}%",
                (f"EBITDA von {ebitda:,.0f} EUR ({ebitda_marge:.1f}% Marge) — "
                 f"ausreichende Cash-Generierung für Schuldendienst und Investitionen."),
                "niedrig", f"{ebitda_marge:.1f}% EBITDA-Marge",
            ))
        elif ebitda_marge >= 3:
            investor.append(_e(
                "EBITDA-Ertragskraft", "⚠️",
                f"Niedrige EBITDA-Marge: {ebitda_marge:.1f}%",
                (f"EBITDA von {ebitda:,.0f} EUR ({ebitda_marge:.1f}% Marge) — "
                 f"geringe Puffer für Kostenanstieg oder Umsatzrückgang. Kosteneffizienz prüfen."),
                "mittel", f"{ebitda_marge:.1f}% EBITDA-Marge",
            ))
        else:
            investor.append(_e(
                "EBITDA-Ertragskraft", "🚨",
                f"Kritische EBITDA-Marge: {ebitda_marge:.1f}%",
                (f"EBITDA von {ebitda:,.0f} EUR ({ebitda_marge:.1f}% Marge) — "
                 f"kaum operative Cash-Generierung. Schuldendienst und Investitionen gefährdet."),
                "hoch", f"{ebitda_marge:.1f}% EBITDA-Marge",
            ))

    # 4. Due Diligence Prioritäten (ab mittlerem Risiko)
    if bi_i >= 280:
        investor.append(_e(
            "Due Diligence", "🔍",
            "Vertiefte Prüfung empfohlen",
            (f"Bei Risikoklasse {_rk(bi_i)[:1]} sollte die Due Diligence folgende Bereiche priorisieren: "
             f"Ertragskontinuität, Forderungsstruktur, Off-Balance-Verpflichtungen (Leasing), "
             f"Schlüsselpersonen-Abhängigkeit und Kundenkonzentration."),
            "mittel" if bi_i < 350 else "hoch",
        ))

    # 5. Exit-Risiken (ab erhöhtem Risiko)
    if bi_i >= 350:
        investor.append(_e(
            "Exit-Risiken", "🚨",
            "Exit-Strategie und Liquiditätspfad definieren",
            (f"Risikoklasse {_rk(bi_i)[:1]}: Branchenrisiko und Marktstellung erschweren einen schnellen Exit. "
             f"Klare Trigger-Events für Veräußerung oder Restrukturierung vorab definieren."),
            "hoch",
        ))

    # ══════════════════════════════════════════════════════════════════════════
    # 🏗️ LEASINGGEBER — 5 Empfehlungskategorien
    # ══════════════════════════════════════════════════════════════════════════
    leasinggeber = []

    # 1. Maximale monatliche Leasingrate (% vom Jahresumsatz)
    if umsatz > 0:
        lr_map = [
            (250, 0.020, "Sehr gute Bonität"),
            (300, 0.015, "Gute Bonität"),
            (350, 0.010, "Solide Bonität"),
            (400, 0.005, "Erhöhtes Risiko"),
            (601, 0.002, "Kritische Bonität"),
        ]
        lr_faktor = 0.002
        lr_label  = "Kritische Bonität"
        for thr, f, lbl in lr_map:
            if bi_lg < thr: lr_faktor = f; lr_label = lbl; break
        max_rate_monatlich = round(umsatz * lr_faktor / 12 / 100) * 100
        leasinggeber.append(_e(
            "Max. Leasingrate", "💳",
            f"Empfohlene max. Monatsrate: {max_rate_monatlich:,.0f} EUR",
            (f"{lr_label} — Leasingbelastung auf {lr_faktor*100:.1f}% des Jahresumsatzes begrenzen "
             f"({umsatz:,.0f} EUR Umsatz → max. {umsatz*lr_faktor:,.0f} EUR/Jahr Gesamtleasingrate)."),
            "hoch" if bi_lg >= 400 else "mittel",
            f"{max_rate_monatlich:,.0f} EUR/Monat",
        ))

    # 2. Kautionshöhe (in Anzahl Monatsraten)
    kaution_map = [
        (300, 0, "Sehr gute Bonität — keine Kaution erforderlich."),
        (350, 1, "Gute Bonität — eine Monatsrate als Sicherheit."),
        (400, 3, "Erhöhtes Risiko — drei Monatsraten als Kaution."),
        (601, 6, "Hohes Risiko — sechs Monatsraten als Kaution zwingend."),
    ]
    for thr, monate, beg in kaution_map:
        if bi_lg < thr:
            if monate == 0:
                leasinggeber.append(_e("Kaution", "💰", "Keine Kaution erforderlich", beg, "niedrig", "0 Monatsraten"))
            else:
                leasinggeber.append(_e(
                    "Kaution", "💰",
                    f"Kaution: {monate} Monatsrate(n)",
                    beg, "mittel" if monate <= 2 else "hoch",
                    f"{monate} Monatsraten",
                ))
            break

    # 3. Empfohlene maximale Laufzeit
    laufzeit_map = [
        (250, 72, "Exzellente Bonität — lange Laufzeiten ohne erhöhtes Risiko."),
        (300, 60, "Sehr gute Bonität — Standardlaufzeit möglich."),
        (350, 48, "Solide Bonität — mittlere Laufzeit empfohlen."),
        (400, 24, "Erhöhtes Risiko — kurze Bindung schützt vor Ausfall."),
        (601, 12, "Kritische Bonität — nur kurzfristige Verträge eingehen."),
    ]
    for thr, monate, beg in laufzeit_map:
        if bi_lg < thr:
            leasinggeber.append(_e(
                "Laufzeit", "📆",
                f"Empfohlene max. Laufzeit: {monate} Monate",
                beg, "niedrig" if monate >= 48 else ("mittel" if monate >= 24 else "hoch"),
                f"Max. {monate} Monate",
            ))
            break

    # 4. GF-Bürgschaft (ab BI 300 — schlechtere Bonität erfordert persönliche Haftung)
    if bi_lg >= 400:
        leasinggeber.append(_e(
            "GF-Bürgschaft", "✍️",
            "Persönliche GF-Bürgschaft zwingend",
            (f"Risikoklasse {_rk(bi_lg)[:1]}: Unternehmensrating nicht ausreichend als alleinige Sicherheit. "
             f"Persönliche Haftung des/der Geschäftsführer(s) ist Grundvoraussetzung für Vertragsabschluss."),
            "hoch",
        ))
    elif bi_lg >= 300:
        leasinggeber.append(_e(
            "GF-Bürgschaft", "✍️",
            "Persönliche GF-Bürgschaft empfohlen",
            (f"Risikoklasse {_rk(bi_lg)[:1]}: Zur Absicherung des Leasingengagements wird eine "
             f"persönliche GF-Bürgschaft empfohlen — insbesondere bei längeren Laufzeiten."),
            "mittel",
        ))

    # 5. Bestehende Leasingbelastung prüfen
    if miet_leasing > 0 and umsatz > 0:
        leasing_quote = miet_leasing / umsatz * 100
        prio_lg = "hoch" if leasing_quote > 5 else ("mittel" if leasing_quote > 2 else "niedrig")
        leasinggeber.append(_e(
            "Bestehende Leasingbelastung", "📋",
            f"Aktuelle Miet-/Leasingverpflichtungen: {miet_leasing:,.0f} EUR/Jahr",
            (f"Die bestehende Off-Balance-Belastung beträgt {leasing_quote:.1f}% des Umsatzes. "
             f"{'Hohe Vorbelastung — neues Leasingengagement kritisch prüfen.' if leasing_quote > 5 else 'Neue Leasingrate in Gesamtbelastung einrechnen.'}"),
            prio_lg, f"{leasing_quote:.1f}% des Umsatzes",
        ))

    # 6. Zinsdeckungsgrad — kritisch für Schuldendienst (v2.10.32)
    if zinsdeckung is not None:
        if zinsdeckung < 1.0:
            leasinggeber.append(_e(
                "Zinsdeckungsgrad", "🚨",
                f"Kritischer Zinsdeckungsgrad: {zinsdeckung:.1f}x",
                (f"EBIT deckt den Zinsaufwand nicht vollständig ({zinsdeckung:.1f}x). "
                 f"Leasingengagement sehr kritisch prüfen — Ausfall des Schuldendiensts möglich."),
                "hoch", f"{zinsdeckung:.1f}x",
            ))
        elif zinsdeckung < 1.5:
            leasinggeber.append(_e(
                "Zinsdeckungsgrad", "⚠️",
                f"Niedriger Zinsdeckungsgrad: {zinsdeckung:.1f}x",
                (f"EBIT deckt den Zinsaufwand nur knapp ({zinsdeckung:.1f}x — Richtwert: > 1.5x). "
                 f"Kaum Puffer für Mehrbelastung durch neue Leasingverpflichtungen."),
                "hoch", f"{zinsdeckung:.1f}x",
            ))
        elif zinsdeckung < 3.0:
            leasinggeber.append(_e(
                "Zinsdeckungsgrad", "📋",
                f"Ausreichender Zinsdeckungsgrad: {zinsdeckung:.1f}x",
                (f"EBIT übersteigt den Zinsaufwand um das {zinsdeckung:.1f}-Fache — "
                 f"solider Puffer, zusätzliche Leasingbelastung vertretbar."),
                "mittel", f"{zinsdeckung:.1f}x",
            ))
        else:
            leasinggeber.append(_e(
                "Zinsdeckungsgrad", "✅",
                f"Guter Zinsdeckungsgrad: {zinsdeckung:.1f}x",
                (f"EBIT übersteigt den Zinsaufwand um das {zinsdeckung:.1f}-Fache (Richtwert: > 3x) — "
                 f"komfortable Schuldentragfähigkeit, Leasingverpflichtungen gut abgedeckt."),
                "niedrig", f"{zinsdeckung:.1f}x",
            ))

    # ══════════════════════════════════════════════════════════════════════════
    # 🤝 KUNDE — 5 Empfehlungskategorien
    # ══════════════════════════════════════════════════════════════════════════
    kunde = []

    # 1. Lieferantenqualifizierung
    qual_map = [
        (300, "Freigabe empfohlen",
              "Stabile Bonität — zuverlässiger Lieferant mit geringem Ausfallrisiko.", "niedrig"),
        (400, "Bedingte Freigabe — Alternativlieferant aufbauen",
              "Erhöhtes Risiko — Lieferant kann kurzfristig ausfallen. Dual Sourcing einleiten.", "mittel"),
        (601, "Nicht freigeben",
              "Kritisches Insolvenz-/Ausfallrisiko — kein strategischer Lieferant ohne Absicherung.", "hoch"),
    ]
    for thr, emf, beg, pri in qual_map:
        if bi_k < thr:
            kunde.append(_e("Lieferantenqualifizierung", "✅", emf, beg, pri)); break

    # 2. Dual Sourcing
    if bi_k >= 350:
        kunde.append(_e(
            "Dual Sourcing", "🔀",
            "Zweiten Lieferanten qualifizieren",
            (f"Bei Risikoklasse {_rk(bi_k)[:1]} ist eine Abhängigkeit von diesem Lieferanten riskant. "
             f"Einen alternativen Lieferanten für kritische Materialien/Leistungen qualifizieren."),
            "mittel" if bi_k < 450 else "hoch",
        ))

    # 3. Anzahlungsrisiko
    if bi_k >= 300:
        max_anzahlung = "10 %" if bi_k < 400 else "0 % (nur Zahlung nach Lieferung)"
        kunde.append(_e(
            "Anzahlungsrisiko", "💸",
            f"Maximale Anzahlung: {max_anzahlung}",
            (f"Risikoklasse {_rk(bi_k)[:1]}: Bei Insolvenz des Lieferanten vor Lieferung droht Verlust "
             f"geleisteter Anzahlungen. Zahlungen nach Lieferungsnachweis strukturieren."),
            "mittel" if bi_k < 400 else "hoch", max_anzahlung,
        ))

    # 4. Empfohlene Vertragslaufzeit
    vl_map = [
        (300, "Bis 36 Monate",   "Stabile Bonität — mittelfristige Verträge vertretbar.", "niedrig"),
        (400, "Bis 12 Monate",   "Erhöhtes Risiko — kurze Laufzeiten, regelmäßige Verlängerungsprüfung.", "mittel"),
        (601, "Maximal 6 Monate","Kritische Bonität — nur kurzfristige Liefervereinbarungen.", "hoch"),
    ]
    for thr, emf, beg, pri in vl_map:
        if bi_k < thr:
            kunde.append(_e("Vertragslaufzeit", "📅", emf, beg, pri, emf)); break

    # 5. Vertragsklauseln
    if bi_k >= 300:
        klauseln = ["Insolvenz-Lösungsrecht (Kündigung bei Insolvenzantrag)"]
        if bi_k >= 350:
            klauseln.append("Lieferbürgschaft oder Erfüllungsgarantie")
        if bi_k >= 400:
            klauseln.append("Change-of-Control-Klausel")
        kunde.append(_e(
            "Vertragsklauseln", "📝",
            "Schutzklauseln in Liefervertrag aufnehmen",
            f"Empfohlene Klauseln bei Risikoklasse {_rk(bi_k)[:1]}: {', '.join(klauseln)}.",
            "mittel" if bi_k < 400 else "hoch",
        ))

    return {
        "lieferant":    lieferant,
        "investor":     investor,
        "leasinggeber": leasinggeber,
        "kunde":        kunde,
    }


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
        # v2.10.32: Vorräte mit 50% gewichtet (weniger liquide als Forderungen/Cash)
        _vorraete_liq = (req.vorraete or 0) * 0.5
        liq=((req.fluessige_mittel or 0)+(req.forderungen or 0)+_vorraete_liq)/req.kurzfristiges_fk
    z_prob=_zahlung_prob(ep,vg,liq,mg,je,um)
    # v2.10.21: Größenklassen-Modifikator (KfW-kalibriert) — Entity-Level MA (korrekte Bewertung)
    # v2.10.31: Konzernrückhalt wird separat via konzern_score/_konzern_zahlung_mod abgebildet
    _gm_mod = _groessen_modifikator(um, ma)
    _gm_label = None
    z_prob = round(z_prob * _gm_mod, 4)
    # v2.5.7: Konzern-Zahlungsmodifikator – Konzernrückhalt/-belastung direkt auf z_prob
    _kz_eff = int(req.konzern_score if req.konzern_score is not None else 5)
    _kz_mod = _konzern_zahlung_mod(_kz_eff)
    z_prob_adj = float(min(_ZAHLUNG_MAX_P, max(0.001, z_prob * _kz_mod)))
    z_sc=int(round(max(0.0,min(10.0,10.0*(1.0-z_prob_adj/_ZAHLUNG_MAX_P)))))

    # v2.9.1: Dimensionen ohne valide Daten überspringen + Gewichte umverteilen
    _skip = set(req.skip_dimensions or [])
    _skip_w = sum(_GEW.get(k, 0) for k in _skip)
    _scale = 100.0 / (100.0 - _skip_w) if 0 < _skip_w < 99 else 1.0

    dims, tot = [], 0.0
    for k in _GEW:
        if k == "zahlungsweise":
            s = z_sc
            _gm_note = (f", Größe×{_gm_mod}" + (f"[{_gm_label}]" if _gm_label else "")) if _gm_mod!=1.0 else ""
            info = "P(Zahlungsproblem)="+str(round(z_prob_adj*100,1))+"% (EK/VG/Liq/Marge/Verlust"+_gm_note+(f", Konzern×{_kz_mod}" if _kz_mod!=1.0 else "")+")"
        else:
            gf_eff = req.gf_score if req.gf_score is not None else 5
            s,info=_dim(k,rf,ep,vg,liq,mg,je,kpm,req.branche_risiko,req.investoren_score,ma,upm,req.gruendungsjahr,req.insolvenz or False,req.negativmerkmale_anzahl or 0,req.presse_score,wz=req.wz_code,gf=gf_eff,kz=req.konzern_score or 5)
        if k in _skip:
            # Dimension ohne Daten: Gewicht 0, kein Beitrag, Info-Hinweis
            _info_skip = ("k.A. — " + info) if (info and info not in ("?","")) else "k.A. (keine Daten)"
            dims.append(DimensionScore(name=k, label_de=_LABELS[k], score_0_10=s,
                                       gewichtung_pct=0, beitrag=0.0, info=_info_skip))
            continue
        g_eff = _GEW[k] * _scale
        b = s * g_eff / 100.0
        tot += b
        dims.append(DimensionScore(name=k, label_de=_LABELS[k], score_0_10=s,
                                   gewichtung_pct=round(g_eff), beitrag=round(b,4), info=info))
    idx=max(100,min(600,600-round(tot*50)))
    if req.insolvenz: idx=0
    ht,kr=_ht(ep,vg,rf); pdv=_pd(idx)
    # Zahlungsweise-Band: optimistisch (z=10) / wahrscheinlich (aktuell) / pessimistisch (z=0)
    _z_gew=_GEW["zahlungsweise"] * _scale / 100.0  # v2.9.1: skaliertes Gewicht
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
        gf_alarm_text=_gf_check_result.get("alarm_text",""),
        sub_scores=(_ss := _calc_sub_scores(dims)),
        empfehlungen=_calc_empfehlungen(idx, _ss, req))

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
    # v2.10.0: Pre-enriched Overrides (aus /api/enrich_company Bestätigungsschritt)
    mitarbeiter_override: Optional[int] = None           # bestätigte Mitarbeiterzahl
    gf_namen_override: Optional[str] = None              # bestätigte Vorstand/GF-Namen
    gruendungsjahr_override: Optional[int] = None        # bestätigtes Gründungsjahr
    fluessige_mittel_override: Optional[float] = None    # bestätigte liquide Mittel
    # v2.10.30: Gruppen-MA aus Step-1-Enrichment (Wikipedia), wenn größer als HR.ai-MA
    mitarbeiter_gruppe_override: Optional[int] = None    # Konzerngruppen-MA (bspw. Wikipedia 1.600 vs HR.ai 184)

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
    kpi_gruendungsjahr_quelle: Optional[str] = None  # v2.9: "afs_text"|"registration"
    # v2.9.0: P&L-Kennzahlen
    kpi_brutto_marge_pct: Optional[float] = None      # Bruttoergebnis / Umsatz
    kpi_fae_quote_pct: Optional[float] = None         # F&E-Kosten / Umsatz
    kpi_personalaufwand_quote_pct: Optional[float] = None
    kpi_umsatz_wachstum_pct: Optional[float] = None   # YoY
    kpi_umsatz_vorjahr: Optional[float] = None         # v2.10.23: Vorjahresumsatz
    kpi_miet_leasing: Optional[float] = None           # v2.10.23: Off-Balance Leasing
    kpi_forderungen: Optional[float] = None            # v2.10.26: Forderungen LuL für DSO
    # v2.10.30: Strukturhinweise für transparente Datenbasis
    hinweis_konzernbereinigung: Optional[str] = None   # Hinweis wenn Konzernverbindlichkeiten aus kfk herausgerechnet
    hinweis_gruppe_ma: Optional[str] = None            # (v2.10.30, deprecated — nicht mehr befüllt)
    kpi_parent_company_anteil: Optional[float] = None  # v2.10.31: Beteiligungsquote Hauptgesellschafter in %
    # v2.10.35: Empfehlung Mutter-Scoring
    empfehlung_mutter_scoring: Optional[dict] = None   # {name, anteil_pct, hinweis, konzern_score_auto}
    # v2.10.32: Erweiterte Bilanz- + GuV-Kennzahlen
    kpi_vorraete: Optional[float] = None               # Vorräte aus Bilanz-Tree
    kpi_langfristiges_fk: Optional[float] = None      # Langfristiges FK aus Bilanz-Tree
    kpi_zinsaufwand: Optional[float] = None            # Zinsaufwand aus GuV
    kpi_abschreibungen: Optional[float] = None         # Abschreibungen aus GuV
    kpi_ebitda: Optional[float] = None                 # Abgeleitet: JE + AFA + Zins
    kpi_zinsdeckungsgrad: Optional[float] = None       # Abgeleitet: EBIT-Näherung / Zinsaufwand
    # v2.10.36: Konzernbereinigter Score (Gesellschafterdarlehen / KV als EK)
    kpi_konzernverbindlichkeiten: Optional[float] = None  # Verbindlichkeiten ggü. verbundenen Unternehmen (€)
    scoring_konzernbereinigt: Optional[dict] = None        # {bi, risikoklasse, pd_pct, eq_pct, vg, betrag, hinweis}
    # v2.10.23: Ratingäquivalenz-Tabelle
    rating_equivalenz: Optional[List[Any]] = None      # Mapping auf Bankratings / Agenturen
    # v2.7.0: Optionale Add-on Ergebnisse
    publications_data: Optional[List[Any]] = None  # Unternehmenshistorie / Bekanntmachungen
    news_data: Optional[List[Any]] = None           # Aktuelle Nachrichten
    credits_used: Optional[int] = None              # Verbrauchte HR.ai Credits (Schaetzung)




@app.get("/api/search_companies", response_model=CompanySearchResponse)
async def search_companies_endpoint(q: str, limit: int = 10):
    """v2.10.3: Unternehmenssuche — gibt Liste passender Unternehmen zurück.
    Schritt 0 im 3-Schritt-Flow: Eingabe → Auswahl → Enrichment → Scoring.
    Kostenfrei (nur 1 financial_kpi-Abfrage für die Suche)."""
    q = q.strip()
    if not q or len(q) < 2:
        raise HTTPException(status_code=400, detail="Suchanfrage zu kurz (min. 2 Zeichen).")
    hr = HandelsregisterClient()
    if not hr.is_available():
        raise HTTPException(status_code=503, detail="handelsregister.ai nicht verfügbar.")
    results = hr._search_companies(q, limit=min(limit, 20))
    return CompanySearchResponse(query=q, results=results, count=len(results))

@app.post("/api/enrich_company")
async def enrich_company_endpoint(req: EnrichmentRequest):
    """v2.10.8: DDG-only enrichment (0 Credits). Mit vollem Traceback-Logging."""
    from fastapi.responses import JSONResponse
    import traceback as _tb
    try:
        name = req.company_name.strip()
        rf   = req.rechtsform_hint or ""

        # Alle Felder als einfaches Dict aufbauen — kein Pydantic-Modell im Response
        def field(value=None, source="nicht gefunden", confidence="niedrig"):
            return {"value": value, "source": source, "confidence": confidence}

        result = {
            "company_name_hr": name,
            "rechtsform": rf,
            "mitarbeiter":       field(),
            "fuehrungspersonen": field(),
            "gruendungsjahr":    field(),
            "investorenstruktur":field(),
            "liquide_mittel":    field(source="Wird beim Scoring aus Bilanz ermittelt"),
        }

        # v2.10.13: Wikipedia + Wikidata einmalig abrufen, Cache an alle Methoden weitergeben
        wiki = hr_client._wiki_enrich(name)

        val, src = hr_client.ddg_find_mitarbeiter(name, _wiki_cache=wiki)
        if val:
            result["mitarbeiter"] = field(val, src, "mittel")

        gf_val, gf_src = hr_client.ddg_find_vorstand_names(name, rf, _wiki_cache=wiki)
        if gf_val:
            result["fuehrungspersonen"] = field(gf_val, gf_src, "mittel")

        yr_val, yr_src = hr_client.ddg_find_gruendungsjahr(name, _wiki_cache=wiki)
        if yr_val:
            result["gruendungsjahr"] = field(yr_val, yr_src, "mittel")
        elif req.registration_date:
            # v2.10.10: Fallback auf HR-Eintragungsdatum wenn Wiki nichts findet
            reg_year = str(req.registration_date)[:4]
            result["gruendungsjahr"] = field(
                reg_year, f"HR-Eintragung ({req.registration_date[:10]})", "niedrig")

        inv_val, inv_src = hr_client.ddg_find_investoren(name, rf, _wiki_cache=wiki)
        if inv_val:
            result["investorenstruktur"] = field(inv_val, inv_src, "mittel")
        else:
            result["investorenstruktur"] = field(None, inv_src, "niedrig")

        logger.info(f"enrich_company '{name}': MA={result['mitarbeiter']['value']}, "
                    f"GF={result['fuehrungspersonen']['value']}, "
                    f"GJ={result['gruendungsjahr']['value']}")
        return JSONResponse(content=result)

    except Exception as exc:
        err = _tb.format_exc()
        logger.error(f"enrich_company FEHLER: {err}")
        return JSONResponse(status_code=500, content={"error": str(exc), "trace": err})

@app.post("/api/score_by_name")  # kein response_model — Pydantic v2 + Optional[Any] Bug
async def score_by_name_endpoint(req: ScoringByNameRequest):
    """v2.8.0: Vollautomatisches Scoring – nur Firmenname erforderlich.
    Datenquellen: handelsregister.ai (Finanzen, Konzern, GF-Namen, GuV),
    insolvenzbekanntmachungen.de (GF-Insolvenzcheck),
    DuckDuckGo (GF-Pressecheck).
    Optionale Add-ons: include_publications=true (+1 Credit), include_news=true (+10 Credits).
    Basis: 19 Credits/Abfrage (inkl. P&L). Vollpaket: 30 Credits/Abfrage.
    website_content (0 Credits, AI) als automatischer Fallback fuer fehlende Felder.
    """
    import traceback as _tb
    from fastapi.responses import JSONResponse as _JR
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

        # v2.10.1: Enrichment-Overrides als FALLBACK — HR.ai-Daten haben immer Vorrang
        # Regel: Override wird nur genutzt wenn HR.ai für dieses Feld nichts geliefert hat
        if req.mitarbeiter_override and req.mitarbeiter_override > 0 and not fd.mitarbeiter:
            fd.mitarbeiter = req.mitarbeiter_override
            logger.info(f"Mitarbeiter-Fallback (DDG): {fd.mitarbeiter}")
        elif fd.mitarbeiter:
            logger.info(f"Mitarbeiter aus HR.ai (gewinnt über DDG): {fd.mitarbeiter}")
        if req.gruendungsjahr_override and (not fd.gruendungsjahr or fd.gruendungsjahr_quelle == "registration"):
            fd.gruendungsjahr = str(req.gruendungsjahr_override)
            fd.gruendungsjahr_quelle = "web_enrichment"
            logger.info(f"Gründungsjahr-Fallback (DDG): {fd.gruendungsjahr}")
        if req.fluessige_mittel_override and req.fluessige_mittel_override > 0 and not fd.__dict__.get("liquide_mittel"):
            fd.__dict__["liquide_mittel"] = req.fluessige_mittel_override
            logger.info(f"Liquide-Mittel-Fallback (DDG): {req.fluessige_mittel_override}")

        # 2. GF-Namen: aus _map_kpi-Cache → HR.ai separater Abruf → DuckDuckGo Fallback
        # v2.10.1: GF-Namen: HR.ai hat Vorrang; DDG-Override nur wenn HR.ai nichts liefert
        gf_namen_hr = fd.__dict__.get("_gf_namen_detected")
        if gf_namen_hr:
            gf_namen = gf_namen_hr
            logger.info(f"GF-Namen aus HR.ai (gewinnt über DDG): {gf_namen}")
        elif req.gf_namen_override:
            gf_namen = req.gf_namen_override
            logger.info(f"GF-Namen-Fallback (DDG-Enrichment): {gf_namen}")
        else:
            gf_namen = None
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

        # 2c. v2.10.31: Konzernverbindlichkeiten aus kurzfristigem FK herausrechnen (GmbH & Co. KG)
        # Verbindlichkeiten gegenüber verbundenen Unternehmen sind faktisch Eigenkapitalersatz
        # und sollten bei Liquiditätsberechnung nicht als echtes kurzfristiges FK zählen.
        _kfk_raw = fd.__dict__.get("kurzfristiges_fk")
        _konz_vbl = fd.__dict__.get("konzernverbindlichkeiten")
        if (_kfk_raw and _konz_vbl and _is_kg(fd.rechtsform or "GmbH") and fd.parent_company):
            _kfk_adj = max(0.0, _kfk_raw - _konz_vbl)
            logger.info(f"v2.10.30 Kurzfr. FK bereinigt: {_kfk_raw:,.0f} - Konzernvbl. {_konz_vbl:,.0f} = {_kfk_adj:,.0f}")
            fd.__dict__["kurzfristiges_fk"] = _kfk_adj
            fd.__dict__["kurzfristiges_fk_hinweis"] = (
                f"Konzernverbindlichkeiten ({_konz_vbl:,.0f} €) als Eigenkapitalersatz herausgerechnet "
                f"(GmbH & Co. KG mit Konzernrückhalt)."
            )

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

        # 7. v2.9.1: Dimensionen ohne valide Daten identifizieren
        _skip_dims: List[str] = []
        _loehne = fd.loehne_gehaelter
        _ma = fd.mitarbeiter or 0
        if not _loehne or not _ma:
            _skip_dims.append("kosten_pro_ma")
        if not _ma:
            _skip_dims.extend(["mitarbeiterzahl", "umsatz_pro_ma"])
        if not gf_namen:
            _skip_dims.append("gf_bonitaet")   # kein Personencheck möglich
        if (req.investoren_score or 5) == 5:
            _skip_dims.append("investorenstruktur")  # kein Investoren-Override → default neutral
        if fd.gruendungsjahr_quelle == "registration":
            _skip_dims.append("unternehmensalter")   # HR-Datum ≠ echtes Gründungsjahr
        if kz_score == 5 and not fd.parent_company:
            _skip_dims.append("konzernstruktur")     # Struktur unbekannt
        logger.info(f"v2.9.1 skip_dims: {_skip_dims}")

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
            fluessige_mittel=fd.__dict__.get("liquide_mittel"),   # v2.9.1: aus BS-Tree
            kurzfristiges_fk=fd.__dict__.get("kurzfristiges_fk"),  # v2.9.1: aus BS-Tree
            forderungen=fd.__dict__.get("forderungen"),             # v2.10.26: aus BS-Tree für DSO
            miet_leasing=fd.miet_leasing,                          # v2.10.28: für Leasinggeber-Empfehlung
            umsatz_wachstum_pct=fd.umsatz_wachstum_pct,           # v2.10.28: für Investor-Empfehlung
            vorraete=fd.__dict__.get("vorraete"),                   # v2.10.32: aus BS-Tree
            langfristiges_fk=fd.__dict__.get("langfristiges_fk"),  # v2.10.32: aus BS-Tree
            ebitda=fd.__dict__.get("ebitda"),                       # v2.10.32: abgeleitet JE+AFA+Zins
            zinsdeckungsgrad=fd.__dict__.get("zinsdeckungsgrad"),   # v2.10.32: EBIT-Näherung / Zinsaufwand
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
            skip_dimensions=_skip_dims,   # v2.9.1: Dimensionen ohne Daten
        )

        # 8. Scoring berechnen (GF-Check laeuft intern automatisch)
        result = compute_score_v21(scoring_req)
        logger.info(f"score_by_name '{company_name_hr}': BI={result.bonitaetsindex} {result.risikoklasse}")

        # v2.10.36: Konzernbereinigter Score — Gesellschafterdarlehen / Konzernverbindlichkeiten als EK
        # Modell-Philosophie: Verbindlichkeiten ggü. verbundenen Unternehmen sind wirtschaftlich
        # Eigenkapitalersatz (§ 39 InsO) und werden nie kurzfristig fällig gestellt.
        _konz_vbl_amount = fd.__dict__.get("konzernverbindlichkeiten")
        scoring_konzernbereinigt = None
        if (_konz_vbl_amount and _konz_vbl_amount > 0
                and fd.parent_company
                and fd.eigenkapital is not None
                and fd.bilanzsumme):
            _ek_adj  = (fd.eigenkapital or 0.0) + _konz_vbl_amount
            _fk_adj  = max(0.0, (fk or 0.0) - _konz_vbl_amount)
            scoring_req_adj = scoring_req.model_copy(update={
                "eigenkapital": _ek_adj,
                "fremdkapital": _fk_adj,
            })
            try:
                result_adj = compute_score_v21(scoring_req_adj)
                _eq_ist  = result.eigenkapitalquote_pct or 0.0
                _eq_adj  = result_adj.eigenkapitalquote_pct or 0.0
                _vg_ist  = result.verschuldungsgrad
                _vg_adj  = result_adj.verschuldungsgrad
                scoring_konzernbereinigt = {
                    "bonitaetsindex":          result_adj.bonitaetsindex,
                    "risikoklasse":            result_adj.risikoklasse,
                    "pd_pct":                  result_adj.pd_pct,
                    "eigenkapitalquote_pct":   result_adj.eigenkapitalquote_pct,
                    "verschuldungsgrad":       result_adj.verschuldungsgrad,
                    "konzernverbindlichkeiten_betrag": _konz_vbl_amount,
                    "bonitaetsindex_standard": result.bonitaetsindex,
                    "risikoklasse_standard":   result.risikoklasse,
                    "hinweis": (
                        f"Gesellschafterdarlehen / Konzernverbindlichkeiten ({_konz_vbl_amount:,.0f} €) "
                        f"werden als wirtschaftliches Eigenkapital behandelt "
                        f"(§ 39 InsO Eigenkapitalersatz — faktisch nie kurzfristig fällig). "
                        f"Eigenkapitalquote: {_eq_ist:.1f}% → {_eq_adj:.1f}%. "
                        f"Verschuldungsgrad: "
                        + (f"{_vg_ist:.1f}x → {_vg_adj:.1f}x." if (_vg_ist is not None and _vg_adj is not None)
                           else "verbessert.")
                    ),
                    "methodik": "Szenario 2: KV-als-EK (nur Bilanzstrukturbereinigung, kein Insolvenz-Override)",
                }
                logger.info(f"v2.10.36 Konzernbereinigter Score: BI={result_adj.bonitaetsindex} "
                            f"({result_adj.risikoklasse}) vs. Standard BI={result.bonitaetsindex}; "
                            f"KV={_konz_vbl_amount:,.0f} €")
            except Exception as _adj_err:
                logger.warning(f"v2.10.36 Konzernbereinigung Fehler: {_adj_err}")
        else:
            _konz_vbl_amount = None  # kein Betrag → kein bereinigter Score

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

        # v2.10.15: JSONResponse statt response_model — Pydantic v2 + Optional[Any] Bug
        from fastapi.responses import JSONResponse
        obj = ScoringByNameResult(
            scoring=result,
            hr_ai_data_found=True,
            company_name_hr=company_name_hr,
            gf_namen_detected=gf_namen,
            konzern_detected=fd.parent_company,
            wz_detected=wz_detected,
            geschaeftsjahr=fd.geschaeftsjahr,
            fehlende_felder=fehlend,
            warnung=warnung,
            kpi_bilanzsumme=fd.bilanzsumme,
            kpi_eigenkapital=fd.eigenkapital,
            kpi_fremdkapital=fk_result,
            kpi_umsatz=fd.umsatz,
            kpi_jahresergebnis=fd.jahresergebnis,
            kpi_mitarbeiter=fd.mitarbeiter,
            kpi_loehne_gehaelter=fd.loehne_gehaelter,
            kpi_liquide_mittel=fd.liquide_mittel if hasattr(fd, 'liquide_mittel') else None,
            kpi_forderungen=fd.__dict__.get("forderungen"),         # v2.10.26
            hinweis_konzernbereinigung=fd.__dict__.get("kurzfristiges_fk_hinweis"),  # v2.10.30
            kpi_parent_company_anteil=fd.parent_company_anteil,      # v2.10.31: Beteiligungsquote
            # v2.10.35: Empfehlung Mutter-Scoring
            empfehlung_mutter_scoring=(
                {
                    "name": fd.parent_company,
                    "anteil_pct": fd.parent_company_anteil,
                    "konzern_score_auto": fd.konzern_score_auto,
                    "hinweis": (
                        f"Scoring der Muttergesellschaft empfohlen: '{fd.parent_company}' "
                        + (f"({fd.parent_company_anteil:.0f}% Anteil) " if fd.parent_company_anteil else "(Anteil unbekannt) ")
                        + "beeinflusst den Konzernrückhalt-Modifier direkt. "
                        + "Ein starkes Mutter-Rating verbessert den Risiko-Score der Tochter."
                    ),
                }
                if fd.parent_company else None
            ),
            # v2.10.32: Erweiterte Bilanz- + GuV-Kennzahlen
            kpi_vorraete=fd.__dict__.get("vorraete"),
            kpi_langfristiges_fk=fd.__dict__.get("langfristiges_fk"),
            kpi_zinsaufwand=fd.__dict__.get("zinsaufwand"),
            kpi_abschreibungen=fd.__dict__.get("abschreibungen"),
            kpi_ebitda=fd.__dict__.get("ebitda"),
            kpi_zinsdeckungsgrad=fd.__dict__.get("zinsdeckungsgrad"),
            # v2.10.36: Konzernbereinigter Score
            kpi_konzernverbindlichkeiten=_konz_vbl_amount,
            scoring_konzernbereinigt=scoring_konzernbereinigt,
            kpi_rechtsform=fd.rechtsform,
            kpi_gruendungsjahr=fd.gruendungsjahr,
            kpi_gruendungsjahr_quelle=fd.gruendungsjahr_quelle,
            kpi_brutto_marge_pct=fd.brutto_marge_pct,
            kpi_fae_quote_pct=fd.fae_quote_pct,
            kpi_personalaufwand_quote_pct=fd.personalaufwand_quote_pct,
            kpi_umsatz_wachstum_pct=fd.umsatz_wachstum_pct,
            kpi_umsatz_vorjahr=fd.umsatz_vorjahr,
            kpi_miet_leasing=fd.miet_leasing,
            rating_equivalenz=_rating_equivalenz(result.bonitaetsindex or 300),
            publications_data=publications_result,
            news_data=news_result,
            credits_used=credits_used,
        )
        return JSONResponse(content=obj.model_dump(mode="json"))

    except HTTPException:
        raise
    except Exception as e:
        err = _tb.format_exc()
        logger.error(f"score_by_name FEHLER: {err}")
        from fastapi.responses import JSONResponse as _JR
        return _JR(status_code=500, content={"error": str(e), "detail": str(e), "trace": err})


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

# ──────────────────────────────────────────────────────────────────────────────
# SELBST-SCORING-LINK  (v2.11.0)
# Drei Endpunkte:
#   POST /api/invite-upload        → erzeugt signierten Upload-Link
#   GET  /api/upload/{token}       → HTML-Landingpage fuer gescortes Unternehmen
#   POST /api/upload-financials/{token} → nimmt Datei entgegen, parst, re-scored
#
# Env-Variablen (in Railway setzen):
#   UPLOAD_SECRET   – JWT-Signatur-Geheimnis (beliebiger langer String)
#   UPLOAD_API_KEY  – API-Key fuer POST /api/invite-upload (nur Frontend kennt ihn)
# ──────────────────────────────────────────────────────────────────────────────

import jwt as _jwt
import datetime as _dt
from fastapi import UploadFile, File, Header
from fastapi.responses import HTMLResponse

_UPLOAD_SECRET  = os.environ.get("UPLOAD_SECRET", "fair-score-upload-secret-change-me")
_UPLOAD_API_KEY = os.environ.get("UPLOAD_API_KEY", "")

def _create_upload_token(entity_id: str, company_name: str) -> str:
    payload = {
        "entity_id":    entity_id,
        "company_name": company_name,
        "purpose":      "upload",
        "exp":          _dt.datetime.utcnow() + _dt.timedelta(days=30),
        "iat":          _dt.datetime.utcnow(),
    }
    return _jwt.encode(payload, _UPLOAD_SECRET, algorithm="HS256")

def _decode_upload_token(token: str) -> dict:
    try:
        return _jwt.decode(token, _UPLOAD_SECRET, algorithms=["HS256"])
    except _jwt.ExpiredSignatureError:
        raise HTTPException(status_code=410, detail="Dieser Upload-Link ist abgelaufen (30 Tage). Bitte neuen Link anfordern.")
    except _jwt.InvalidTokenError:
        raise HTTPException(status_code=400, detail="Ungültiger Upload-Link.")


class InviteUploadRequest(BaseModel):
    entity_id:    str
    company_name: str

@app.post("/api/invite-upload")
async def invite_upload(req: InviteUploadRequest, x_upload_api_key: Optional[str] = Header(None)):
    """Erzeugt einen signierten Upload-Link fuer ein gescortes Unternehmen.
    Erfordert Header: X-Upload-Api-Key (muss UPLOAD_API_KEY env-Variable entsprechen).
    """
    if _UPLOAD_API_KEY and x_upload_api_key != _UPLOAD_API_KEY:
        raise HTTPException(status_code=401, detail="Ungültiger API-Key.")
    if not req.entity_id or not req.company_name:
        raise HTTPException(status_code=400, detail="entity_id und company_name erforderlich.")
    token = _create_upload_token(req.entity_id, req.company_name)
    base_url = os.environ.get("PUBLIC_BASE_URL", "https://openrisk-backend-production.up.railway.app")
    link = f"{base_url}/api/upload/{token}"
    logger.info(f"Upload-Link erstellt fuer '{req.company_name}' (entity_id={req.entity_id})")
    return {"token": token, "link": link, "expires_days": 30}


@app.get("/api/upload/{token}", response_class=HTMLResponse)
async def upload_landing_page(token: str):
    """HTML-Landingpage fuer das gescorte Unternehmen – zeigt Upload-Formular."""
    payload = _decode_upload_token(token)
    company_name = payload.get("company_name", "Ihr Unternehmen")

    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>fair-score · Jahresabschluss hochladen</title>
<style>
  * {{ box-sizing: border-box; margin: 0; padding: 0; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
         background: #f7f8fa; color: #1a1a2e; min-height: 100vh;
         display: flex; align-items: center; justify-content: center; padding: 24px; }}
  .card {{ background: white; border-radius: 16px; padding: 40px; max-width: 540px;
           width: 100%; box-shadow: 0 4px 24px rgba(0,0,0,0.08); }}
  .logo {{ display: flex; align-items: center; gap: 8px; margin-bottom: 32px; }}
  .logo-icon {{ width: 36px; height: 36px; }}
  .logo-text {{ font-size: 22px; font-weight: 700; }}
  .logo-text span:first-child {{ color: #1a3f7c; }}
  .logo-text span:last-child  {{ color: #3ecf8e; }}
  h1 {{ font-size: 22px; font-weight: 700; color: #1a3f7c; margin-bottom: 8px; }}
  .company {{ font-size: 16px; color: #3ecf8e; font-weight: 600; margin-bottom: 16px; }}
  p  {{ font-size: 14px; color: #5e6472; line-height: 1.6; margin-bottom: 20px; }}
  .info-box {{ background: #f0faf5; border-left: 3px solid #3ecf8e; border-radius: 8px;
               padding: 14px 16px; margin-bottom: 28px; font-size: 13px; color: #1a3f7c; line-height: 1.5; }}
  .drop-zone {{ border: 2px dashed #d1d5db; border-radius: 12px; padding: 36px 24px;
                text-align: center; cursor: pointer; transition: all 0.2s;
                background: #fafafa; margin-bottom: 20px; }}
  .drop-zone:hover, .drop-zone.drag-over {{ border-color: #3ecf8e; background: #f0faf5; }}
  .drop-zone svg {{ margin-bottom: 12px; }}
  .drop-zone p {{ margin: 0; color: #9ca3af; font-size: 14px; }}
  .drop-zone strong {{ color: #374151; }}
  #file-input {{ display: none; }}
  #file-name {{ font-size: 13px; color: #3ecf8e; margin-top: 8px; font-weight: 600; }}
  .btn {{ width: 100%; padding: 14px; background: #3ecf8e; color: white; border: none;
          border-radius: 10px; font-size: 16px; font-weight: 700; cursor: pointer;
          transition: background 0.2s; }}
  .btn:hover {{ background: #2ba87a; }}
  .btn:disabled {{ background: #d1d5db; cursor: not-allowed; }}
  .result {{ display: none; border-radius: 12px; padding: 20px; margin-top: 20px; }}
  .result.success {{ background: #f0faf5; border: 1px solid #3ecf8e; }}
  .result.error   {{ background: #fff5f5; border: 1px solid #f87171; }}
  .result h2 {{ font-size: 18px; margin-bottom: 8px; }}
  .result.success h2 {{ color: #1a3f7c; }}
  .result.error   h2 {{ color: #dc2626; }}
  .score-box {{ background: #1a3f7c; color: white; border-radius: 10px;
                padding: 16px; text-align: center; margin-top: 16px; }}
  .score-val {{ font-size: 42px; font-weight: 800; color: #3ecf8e; }}
  .score-lbl {{ font-size: 14px; opacity: 0.8; margin-top: 4px; }}
  .dsgvo {{ font-size: 11px; color: #9ca3af; text-align: center; margin-top: 20px; line-height: 1.5; }}
  .spinner {{ display: none; text-align: center; margin: 12px 0; color: #5e6472; font-size: 14px; }}
</style>
</head>
<body>
<div class="card">
  <div class="logo">
    <svg class="logo-icon" viewBox="0 0 40 40" fill="none" xmlns="http://www.w3.org/2000/svg">
      <circle cx="20" cy="20" r="20" fill="#1a3f7c"/>
      <rect x="10" y="26" width="4" height="8" rx="1" fill="#3ecf8e"/>
      <rect x="16" y="20" width="4" height="14" rx="1" fill="#3ecf8e"/>
      <rect x="22" y="14" width="4" height="20" rx="1" fill="#3ecf8e"/>
      <rect x="28" y="8"  width="4" height="26" rx="1" fill="#3ecf8e" opacity="0.7"/>
      <circle cx="32" cy="8" r="3" fill="white"/>
      <path d="M30 8 L34 8 L32 5 Z" fill="white"/>
    </svg>
    <div class="logo-text"><span>fair-</span><span>score</span></div>
  </div>

  <h1>Jahresabschluss hochladen</h1>
  <div class="company">{company_name}</div>

  <p>Ihr Unternehmen wurde über fair-score bonitätsgeprüft. Einzelne Kennzahlen konnten nicht automatisch ermittelt werden, da diese für Ihre Rechtsform nicht veröffentlichungspflichtig sind.</p>

  <div class="info-box">
    📋 <strong>So funktioniert es:</strong><br>
    Laden Sie Ihren aktuellen Jahresabschluss hoch (PDF oder Excel). Wir berechnen daraus einen vollständigen Bonitätsindex — <strong>kostenlos</strong> für Sie. Ihre Daten werden ausschließlich für dieses Scoring verwendet.
  </div>

  <form id="upload-form">
    <div class="drop-zone" id="drop-zone" onclick="document.getElementById('file-input').click()">
      <svg width="40" height="40" viewBox="0 0 24 24" fill="none" stroke="#9ca3af" stroke-width="1.5">
        <path d="M21 15v4a2 2 0 01-2 2H5a2 2 0 01-2-2v-4"/>
        <polyline points="17 8 12 3 7 8"/>
        <line x1="12" y1="3" x2="12" y2="15"/>
      </svg>
      <p><strong>Datei hier ablegen</strong> oder klicken zum Auswählen</p>
      <p style="margin-top:6px">PDF oder Excel · max. 20 MB</p>
      <div id="file-name"></div>
    </div>
    <input type="file" id="file-input" name="file" accept=".pdf,.xlsx,.xls">
    <div class="spinner" id="spinner">⏳ Jahresabschluss wird analysiert...</div>
    <button type="submit" class="btn" id="submit-btn" disabled>📊 Jahresabschluss hochladen & Scoring starten</button>
  </form>

  <div class="result" id="result"></div>

  <p class="dsgvo">🔒 Datenschutz: Ihre Daten werden gemäß DSGVO Art. 6 Abs. 1 lit. f verarbeitet und nicht an Dritte weitergegeben. Herausgeber: Anno 76 GmbH · <a href="https://fair-score.de" style="color:#3ecf8e">fair-score.de</a></p>
</div>

<script>
const token = "{token}";
const dropZone = document.getElementById("drop-zone");
const fileInput = document.getElementById("file-input");
const submitBtn = document.getElementById("submit-btn");
const fileNameEl = document.getElementById("file-name");
const resultEl = document.getElementById("result");
const spinner = document.getElementById("spinner");

fileInput.addEventListener("change", () => {{
  if (fileInput.files[0]) {{
    fileNameEl.textContent = "✅ " + fileInput.files[0].name;
    submitBtn.disabled = false;
  }}
}});

dropZone.addEventListener("dragover", e => {{ e.preventDefault(); dropZone.classList.add("drag-over"); }});
dropZone.addEventListener("dragleave", () => dropZone.classList.remove("drag-over"));
dropZone.addEventListener("drop", e => {{
  e.preventDefault();
  dropZone.classList.remove("drag-over");
  fileInput.files = e.dataTransfer.files;
  if (fileInput.files[0]) {{
    fileNameEl.textContent = "✅ " + fileInput.files[0].name;
    submitBtn.disabled = false;
  }}
}});

document.getElementById("upload-form").addEventListener("submit", async e => {{
  e.preventDefault();
  if (!fileInput.files[0]) return;
  submitBtn.disabled = true;
  spinner.style.display = "block";
  resultEl.style.display = "none";

  const formData = new FormData();
  formData.append("file", fileInput.files[0]);

  try {{
    const res = await fetch("/api/upload-financials/" + token, {{
      method: "POST", body: formData
    }});
    const data = await res.json();
    spinner.style.display = "none";

    if (res.ok && data.bonitaetsindex) {{
      const klasse = data.risikoklasse || "";
      resultEl.className = "result success";
      resultEl.innerHTML = `
        <h2>✅ Scoring abgeschlossen!</h2>
        <p style="color:#5e6472;font-size:14px">Ihr Jahresabschluss wurde erfolgreich analysiert.</p>
        <div class="score-box">
          <div class="score-val">${{data.bonitaetsindex}}</div>
          <div class="score-lbl">Bonitätsindex (max. 600) · ${{klasse}}</div>
        </div>
        <p style="margin-top:16px;font-size:13px;color:#5e6472">
          Das vollständige Scoring wurde dem anfragenden Unternehmen automatisch mitgeteilt.
          <br><a href="https://fair-score.de" style="color:#3ecf8e">Eigenes fair-score-Konto erstellen →</a>
        </p>`;
    }} else {{
      resultEl.className = "result error";
      const msg = data.detail || data.error || "Unbekannter Fehler.";
      resultEl.innerHTML = `<h2>⚠️ Fehler</h2><p style="color:#5e6472;font-size:14px">${{msg}}</p>
        <p style="font-size:13px;color:#9ca3af;margin-top:8px">Bitte prüfen Sie das Dateiformat (PDF oder Excel) und versuchen Sie es erneut.</p>`;
      submitBtn.disabled = false;
    }}
    resultEl.style.display = "block";
  }} catch(err) {{
    spinner.style.display = "none";
    resultEl.className = "result error";
    resultEl.innerHTML = `<h2>⚠️ Verbindungsfehler</h2><p style="color:#5e6472;font-size:14px">Bitte Seite neu laden und erneut versuchen.</p>`;
    resultEl.style.display = "block";
    submitBtn.disabled = false;
  }}
}});
</script>
</body>
</html>"""
    return HTMLResponse(content=html)


@app.post("/api/upload-financials/{token}")
async def upload_financials(token: str, file: UploadFile = File(...)):
    """Nimmt Jahresabschluss-Datei entgegen, parst sie, fuehrt Re-Scoring durch.
    Datei wird nur im Arbeitsspeicher verarbeitet — keine persistente Speicherung.
    """
    payload = _decode_upload_token(token)
    entity_id    = payload.get("entity_id", "")
    company_name = payload.get("company_name", "Unbekannt")

    # Dateigroesse pruefen (max 20 MB)
    content = await file.read()
    if len(content) > 20 * 1024 * 1024:
        raise HTTPException(status_code=413, detail="Datei zu grooss (max. 20 MB).")

    filename = file.filename or ""
    logger.info(f"Upload empfangen: '{filename}' ({len(content)} Bytes) fuer '{company_name}' (entity_id={entity_id})")

    # Datei parsen — text_parser unterstuetzt PDF-Text und einfache Textformate
    raw_text = ""
    try:
        if filename.lower().endswith(".pdf"):
            # PDF: Bytes direkt an pdfminer/pdfplumber uebergeben falls verfuegbar,
            # sonst als Text dekodieren (funktioniert bei digital erstellten PDFs)
            try:
                import pdfminer.high_level as _pdfhl
                import io as _io
                raw_text = _pdfhl.extract_text(_io.BytesIO(content))
            except ImportError:
                # Fallback: roher Text
                raw_text = content.decode("utf-8", errors="ignore")
        elif filename.lower().endswith((".xlsx", ".xls")):
            # Excel: openpyxl lesen
            try:
                import openpyxl as _xl
                import io as _io
                wb = _xl.load_workbook(_io.BytesIO(content), data_only=True)
                lines = []
                for ws in wb.worksheets:
                    for row in ws.iter_rows(values_only=True):
                        line = "\t".join(str(c) if c is not None else "" for c in row)
                        if line.strip():
                            lines.append(line)
                raw_text = "\n".join(lines)
            except ImportError:
                raw_text = content.decode("utf-8", errors="ignore")
        else:
            raw_text = content.decode("utf-8", errors="ignore")
    except Exception as parse_err:
        logger.error(f"Datei-Parse-Fehler: {parse_err}")
        raise HTTPException(status_code=422, detail=f"Datei konnte nicht gelesen werden: {parse_err}. Bitte als PDF oder Excel hochladen.")

    if not raw_text or len(raw_text.strip()) < 50:
        raise HTTPException(status_code=422, detail="Datei konnte nicht als Text extrahiert werden. Bitte ein digital erstelltes PDF (kein Scan) oder Excel hochladen.")

    logger.info(f"Extrahierter Text: {len(raw_text)} Zeichen")

    # Finanzdaten aus Text parsen
    try:
        uploaded_fd = text_parser.parse(raw_text)
        uploaded_fd.quelle = "upload"
    except Exception as tp_err:
        logger.error(f"text_parser Fehler: {tp_err}")
        raise HTTPException(status_code=422, detail="Jahresabschluss konnte nicht ausgewertet werden. Bitte prüfen Sie das Format.")

    # Plausibilitaetspruefung: Bilanzsumme aus Upload vs. HR.ai
    try:
        hr_check = HandelsregisterClient()
        if hr_check.is_available() and entity_id:
            data_kpi = hr_check._get(entity_id, "financial_kpi")
            hr_bs = None
            if data_kpi:
                kpi_list = data_kpi.get("financial_kpi") or []
                if kpi_list:
                    hr_bs = kpi_list[0].get("active_total")
            if hr_bs and uploaded_fd.bilanzsumme:
                ratio = max(hr_bs, uploaded_fd.bilanzsumme) / min(hr_bs, uploaded_fd.bilanzsumme)
                if ratio > 5:
                    logger.warning(f"Plausibilitaet: Upload-Bilanzsumme {uploaded_fd.bilanzsumme} vs HR.ai {hr_bs} (Faktor {ratio:.1f}) — moegliche falsche Datei")
                else:
                    logger.info(f"Plausibilitaet OK: Bilanzsumme Upload={uploaded_fd.bilanzsumme}, HR.ai={hr_bs}, Faktor={ratio:.2f}")
    except Exception as plaus_err:
        logger.warning(f"Plausibilitaetspruefung Fehler (nicht kritisch): {plaus_err}")

    # Re-Scoring: bestehende HR.ai-Daten mit Upload-Daten zusammenfuehren
    try:
        hr_fd, _ = HandelsregisterClient().search(company_name) if HandelsregisterClient().is_available() else (None, None)
        if hr_fd:
            merged = _merge(hr_fd, uploaded_fd)
        else:
            merged = uploaded_fd

        company_info = insolvenz_checker.check(company_name)
        gf_namen = merged.__dict__.get("_gf_namen_detected") or ""

        scoring_req = ScoringRequest(
            company_name  = company_name,
            umsatz        = merged.umsatz or 0,
            jahresergebnis= merged.jahresergebnis or 0,
            eigenkapital  = merged.eigenkapital or 0,
            bilanzsumme   = merged.bilanzsumme or 0,
            mitarbeiter   = merged.mitarbeiter or 0,
            verschuldungsgrad = merged.verschuldungsgrad or 0,
            fremdkapital  = (merged.bilanzsumme - merged.eigenkapital) if merged.bilanzsumme and merged.eigenkapital else 0,
            umsatz_vorjahr= merged.umsatz_vorjahr,
            insolvenz     = company_info.insolvenz,
            gf_namen      = gf_namen,
            rechtsform    = merged.rechtsform or "",
        )
        result = await scoring_endpoint(scoring_req)
        result_dict = result if isinstance(result, dict) else result.model_dump()

        logger.info(
            f"Upload-Scoring abgeschlossen: '{company_name}' | "
            f"BI={result_dict.get('bonitaetsindex')} | "
            f"Klasse={result_dict.get('risikoklasse')} | "
            f"Umsatz={merged.umsatz} | EK={merged.eigenkapital}"
        )
        return result_dict

    except HTTPException:
        raise
    except Exception as score_err:
        logger.error(f"Re-Scoring Fehler: {score_err}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Scoring-Fehler: {score_err}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
