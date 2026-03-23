
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

VERSION = "2.5.1"

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
            return fd, company_name_hr
        except Exception as e:
            logger.warning(f"HR.ai Fehler: {e}")
            return None, None

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
        return f

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
    gf_score: Optional[int] = 7             # GF-Bonitaet 0-10 (7=neutral/unbekannt)
    konzern_score: Optional[int] = 5        # Konzernstruktur 0-10 (5=unbekannt)
    gf_namen: Optional[str] = None          # GF-Namen fuer PersonenInsolvenzCheck (kommagetrennt)

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
def compute_score_v21(req:ScoringRequest)->ScoringResult:
    # GF-Insolvenzcheck wenn Namen angegeben und kein manueller Score
    if req.gf_namen and (req.gf_score is None or req.gf_score == 7):
        try:
            req = req.model_copy(update={"gf_score": insolvenz_checker.check_persons(req.gf_namen)})
        except Exception as _e:
            logger.warning(f"GF-Insolvenzcheck Fehler: {_e}")
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
    z_sc=int(round(max(0.0,min(10.0,10.0*(1.0-z_prob/_ZAHLUNG_MAX_P)))))
    for k in _GEW:
        if k=="zahlungsweise":
            s=z_sc; info="P(Zahlungsproblem)="+str(round(z_prob*100,1))+"% (EK/VG/Liq/Marge/Verlust)"
        else:
            s,info=_dim(k,rf,ep,vg,liq,mg,je,kpm,req.branche_risiko,req.investoren_score,ma,upm,req.gruendungsjahr,req.insolvenz or False,req.negativmerkmale_anzahl or 0,req.presse_score,wz=req.wz_code,gf=req.gf_score or 7,kz=req.konzern_score or 5)
        g=_GEW[k];b=s*g/100.0;tot+=b
        dims.append(DimensionScore(name=k,label_de=_LABELS[k],score_0_10=s,gewichtung_pct=g,beitrag=round(b,4),info=info))
    idx=max(100,min(600,600-round(tot*50)))
    if req.insolvenz: idx=0
    ht,kr=_ht(ep,vg,rf); pdv=_pd(idx)
    return ScoringResult(company_name=req.company_name,rechtsform=rf,
        eigenkapitalquote_pct=round(ep,2) if ep is not None else None,eigenkapital_bereinigt=round(ek_b,2),
        verschuldungsgrad=round(vg,2) if vg is not None else None,ergebnismarge_pct=round(mg,2) if mg is not None else None,
        umsatz_pro_ma=round(upm,0) if upm is not None else None,kosten_pro_ma=round(kpm,0) if kpm is not None else None,
        liquiditaet_1=round(liq,3) if liq is not None else None,dimensionen=dims,rohscore_0_100=round(tot*10,2),
        bonitaetsindex=idx,risikoklasse=_rk(idx),pd_pct=pdv,pd_label=f"PD {pdv:.1f}%",
        hard_thresholds=ht,kapitalstruktur_risiko=kr,ek_bereinigt_angewendet=ek_a,ek_bereinigung_betrag=round(ek_d,2))

@app.post("/api/scoring",response_model=ScoringResult)
async def scoring_endpoint(req:ScoringRequest):
    """OpenRisk Bonitaetsindex v2.5 - 18 Dimensionen, Branchenvergleich, GF-Bonitaet, Konzernstruktur"""
    try:
        r=compute_score_v21(req)
        logger.info(f"Scoring {req.company_name}: {r.bonitaetsindex} {r.risikoklasse} {r.kapitalstruktur_risiko}")
        return r
    except Exception as e:
        logger.error(f"Scoring: {e}",exc_info=True)
        raise HTTPException(status_code=500,detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
