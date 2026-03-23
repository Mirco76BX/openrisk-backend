
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

VERSION = "2.3.0"

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
        logger.info(f"HR.ai KPI: Umsatz={f.umsatz}, JE={f.jahresergebnis}, MA={f.mitarbeiter}, Jahr={f.geschaeftsjahr}")
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.environ.get("PORT", 8000)))
