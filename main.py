# OpenRisk AI - Automatische Datenpipeline

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import re
import os
import logging
from typing import Optional

import requests
from bs4 import BeautifulSoup
import dateparser

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("openrisk")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

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
    umsatz_pro_mitarbeiter: Optional[float] = None
    quelle: Optional[str] = None
    geschaeftsjahr: Optional[str] = None

class ScoringInput(BaseModel):
    company_name: str
    financials: FinancialData
    raw_text: Optional[str] = None

# ============================================================
# Bundesanzeiger Scraper (eigene Implementierung)
# Ersetzt das deutschland-Package, das bei fehlenden Ergebnissen crasht
# ============================================================

class BundesanzeigerScraper:
    """Robuster Bundesanzeiger-Scraper mit ordentlichem Error-Handling."""

    BASE_URL = "https://www.bundesanzeiger.de"

    HEADERS = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "de-DE,de;q=0.9,en-US;q=0.8,en;q=0.7",
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "DNT": "1",
        "Host": "www.bundesanzeiger.de",
        "Pragma": "no-cache",
        "Referer": "https://www.bundesanzeiger.de/",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    }

    def __init__(self):
        self.session = None  # Wird pro Suche neu erstellt

    def _create_session(self) -> requests.Session:
        """Erstelle eine frische Session pro Suche."""
        session = requests.Session()
        session.cookies["cc"] = "1628606977-805e172265bfdbde-10"
        session.headers.update(self.HEADERS)
        return session

    def _init_session(self, session: requests.Session):
        """Initialisiere Session mit JSESSIONID Cookie."""
        try:
            session.get(f"{self.BASE_URL}", timeout=15)
            session.get(f"{self.BASE_URL}/pub/de/start?0", timeout=15)
        except requests.RequestException as e:
            raise ConnectionError(f"Bundesanzeiger nicht erreichbar: {e}")

    def _search(self, session: requests.Session, company_name: str) -> str:
        """Fuehre Suche durch und gib HTML zurueck."""
        search_url = (
            f"{self.BASE_URL}/pub/de/start?0-2."
            f"-top%7Econtent%7Epanel-left%7Ecard-form="
            f"&fulltext={requests.utils.quote(company_name)}"
            f"&area_select=&search_button=Suchen"
        )
        response = session.get(search_url, timeout=30)
        response.raise_for_status()
        return response.text

    def _parse_search_results(self, html: str):
        """Parse Suchergebnisse. Gibt Liste von Dicts zurueck."""
        soup = BeautifulSoup(html, "html.parser")

        # Pruefen ob "keine Ergebnisse"
        error_alert = soup.find("div", {"class": "alert-form-error"})
        if error_alert:
            error_text = error_alert.get_text(strip=True)
            if "keine passenden Daten" in error_text.lower() or "keine" in error_text.lower():
                logger.info(f"Bundesanzeiger: Keine Ergebnisse gefunden")
                return []

        # result_container suchen
        wrapper = soup.find("div", {"class": "result_container"})
        if wrapper is None:
            logger.warning("Bundesanzeiger: Kein result_container gefunden")
            return []

        results = []
        rows = wrapper.find_all("div", {"class": "row"})

        for row in rows:
            info_element = row.find("div", {"class": "info"})
            if not info_element:
                continue

            link_element = info_element.find("a")
            if not link_element:
                continue

            entry_link = link_element.get("href", "")
            entry_name = ""
            if link_element.contents:
                entry_name = link_element.contents[0].strip()

            date_element = row.find("div", {"class": "date"})
            date_str = ""
            date_parsed = None
            if date_element and date_element.contents:
                date_str = date_element.contents[0].strip()
                date_parsed = dateparser.parse(date_str, languages=["de"])

            company_element = row.find("div", {"class": "first"})
            company_name = ""
            if company_element and company_element.contents:
                company_name = company_element.contents[0].strip()

            # Absolute URL bauen
            if entry_link and not entry_link.startswith("http"):
                entry_link = f"{self.BASE_URL}{entry_link}"

            results.append({
                "name": entry_name,
                "date": date_parsed,
                "date_str": date_str,
                "company": company_name,
                "content_url": entry_link,
            })

        logger.info(f"Bundesanzeiger: {len(results)} Eintraege gefunden")
        return results

    def _fetch_report_content(self, session: requests.Session, content_url: str) -> Optional[str]:
        """Lade den Inhalt eines einzelnen Reports."""
        try:
            response = session.get(content_url, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.warning(f"Fehler beim Laden von {content_url}: {e}")
            return None

        soup = BeautifulSoup(response.text, "html.parser")

        # Captcha-Check
        captcha_wrapper = soup.find("div", {"class": "captcha_wrapper"})
        if captcha_wrapper:
            # Captcha loesen mit dem ML-Modell aus dem deutschland-Package
            try:
                solved_text = self._solve_captcha(session, soup)
                if solved_text:
                    soup = BeautifulSoup(solved_text, "html.parser")
            except Exception as e:
                logger.warning(f"Captcha-Loesung fehlgeschlagen: {e}")
                return None

        publication = soup.find("div", {"class": "publication_container"})
        if publication:
            return publication.get_text(separator=" ", strip=True)

        return None

    def _solve_captcha(self, session: requests.Session, soup: BeautifulSoup) -> Optional[str]:
        """Versuche Captcha zu loesen mit dem ML-Modell."""
        try:
            from deutschland.bundesanzeiger.model import load_model, load_image_arr, prediction_to_str
            from io import BytesIO
            import numpy as np

            captcha_img = soup.find("div", {"class": "captcha_wrapper"})
            if not captcha_img:
                return None

            img_tag = captcha_img.find("img")
            if not img_tag:
                return None

            img_src = img_tag.get("src", "")
            if not img_src.startswith("http"):
                img_src = f"{self.BASE_URL}{img_src}"

            img_response = session.get(img_src, timeout=15)
            image = BytesIO(img_response.content)
            image_arr = load_image_arr(image)
            image_arr = image_arr.reshape((1, 50, 250, 1)).astype(np.float32)

            model = load_model()
            prediction = model.run(None, {"captcha": image_arr})[0][0]
            captcha_result = prediction_to_str(prediction)

            # Captcha-Formular absenden
            forms = soup.find_all("form")
            if len(forms) >= 2:
                captcha_endpoint = forms[1].get("action", "")
                if not captcha_endpoint.startswith("http"):
                    captcha_endpoint = f"{self.BASE_URL}{captcha_endpoint}"

                response = session.post(
                    captcha_endpoint,
                    data={"solution": captcha_result, "confirm-button": "OK"},
                    timeout=15,
                )
                return response.text

        except ImportError:
            logger.warning("deutschland.bundesanzeiger.model nicht verfuegbar fuer Captcha-Loesung")
        except Exception as e:
            logger.warning(f"Captcha-Fehler: {e}")

        return None

    def get_reports(self, company_name: str) -> dict:
        """
        Hauptmethode: Suche nach Firma und lade Reports.
        Gibt Dict zurueck: {hash: {name, date, company, report}} oder leeres Dict.
        """
        logger.info(f"Suche nach: {company_name}")

        session = self._create_session()
        self._init_session(session)
        html = self._search(session, company_name)
        entries = self._parse_search_results(html)

        if not entries:
            return {}

        results = {}
        for entry in entries:
            content_url = entry.get("content_url", "")
            if not content_url:
                continue

            report_text = self._fetch_report_content(session, content_url)
            if not report_text:
                continue

            import hashlib
            import json

            report_dict = {
                "date": entry["date"],
                "name": entry["name"],
                "company": entry["company"],
                "report": report_text,
            }

            # Hash als Key
            hash_data = {
                "date": entry["date"].isoformat() if entry["date"] else "",
                "name": entry["name"],
                "company": entry["company"],
                "report": report_text,
            }
            dhash = hashlib.md5(
                json.dumps(hash_data, sort_keys=True).encode("utf-8")
            ).hexdigest()

            results[dhash] = report_dict

        logger.info(f"Reports geladen: {len(results)}")
        return results


# ============================================================
# Financial Text Parser
# ============================================================

class FinancialTextParser:

    # Jedes Feld hat mehrere Regex-Patterns (in Prioritaetsreihenfolge)
    FIELD_PATTERNS = {
        "bilanzsumme": [
            # Exakter Match: "Bilanzsumme 835.729,28" oder "Bilanzsumme: EUR 835.729"
            r"Bilanzsumme\s*[:\.\s]*(?:EUR|€)?\s*([0-9]{2,3}(?:\.[0-9]{3})+(?:,[0-9]{2})?)",
            r"Bilanzsumme\s*[:\.\s]*(?:EUR|€)?\s*([0-9]{4,}(?:,[0-9]{2})?)",
            r"(?:Summe\s+(?:der\s+)?Aktiva|Summe\s+(?:der\s+)?Passiva)\s*[:\.\s]*(?:EUR|€)?\s*([0-9]{2,3}(?:\.[0-9]{3})+(?:,[0-9]{2})?)",
            # Fliesstext: "Bilanzsumme betraegt EUR 4.567.890"
            r"Bilanzsumme\s+(?:betr[aä]gt|von|in\s+Höhe\s+von)\s*(?:EUR|€|T€|TEUR)?\s*([0-9]{2,3}(?:\.[0-9]{3})+(?:,[0-9]{2})?)",
        ],
        "eigenkapital": [
            r"(?:Summe\s+)?Eigenkapital\s*[:\.\s]*(?:EUR|€)?\s*([0-9][0-9.,]+)",
            r"Eigenkapital\s+(?:betr[aä]gt|von|i\.?\s*H\.?\s*v\.?)\s*(?:EUR|€|T€|TEUR)?\s*([0-9][0-9.,]+)",
        ],
        "umsatz": [
            r"Umsatzerlöse?\s*[:\.\s]*(?:EUR|€)?\s*([0-9][0-9.,]+)",
            r"Umsatz\s*[:\.\s]*(?:EUR|€)?\s*([0-9][0-9.,]+)",
            r"Gesamtleistung\s*[:\.\s]*(?:EUR|€)?\s*([0-9][0-9.,]+)",
        ],
        "jahresergebnis": [
            r"Jahresfehlbetrag\s*[:\.\s]*(?:EUR|€)?\s*(-?[0-9][0-9.,]+)",
            r"Jahresüberschuss\s*[:\.\s]*(?:EUR|€)?\s*(-?[0-9][0-9.,]+)",
            r"Jahresergebnis\s*[:\.\s]*(?:EUR|€)?\s*(-?[0-9][0-9.,]+)",
            r"Ergebnis\s+nach\s+Steuern\s*[:\.\s]*(?:EUR|€)?\s*(-?[0-9][0-9.,]+)",
            r"Bilanzgewinn\s*[:\.\s]*(?:EUR|€)?\s*(-?[0-9][0-9.,]+)",
            r"Bilanzverlust\s*[:\.\s]*(?:EUR|€)?\s*(-?[0-9][0-9.,]+)",
        ],
        "mitarbeiter": [
            r"(?:Anzahl\s+(?:der\s+)?)?Mitarbeiter(?:innen|zahl|/-?innen)?\s*[:\.\s]*(\d[\d.,]*)",
            r"Beschäftigte(?:n)?\s*[:\.\s]*(\d[\d.,]*)",
            r"Arbeitnehmer(?:innen)?\s*[:\.\s]*(\d[\d.,]*)",
            r"(\d[\d.]*)\s+Mitarbeiter",
        ],
    }

    def parse(self, text: str) -> FinancialData:
        financial = FinancialData()
        financial.geschaeftsjahr = self._extract_year(text)

        # TEUR-Erkennung: Sind Werte in Tausend Euro?
        is_teur = bool(re.search(r"\b(?:TEUR|T€|Tsd\.?\s*EUR|in\s+Tausend\s+EUR)\b", text, re.IGNORECASE))

        # Negativer Fehlbetrag (muss VOR normalem EK-Match laufen)
        neg_ek_match = re.search(
            r"nicht\s+(?:durch\s+)?Eigenkapital\s+gedeckte?r?\s+Fehlbetrag\s*[:\.\s]*(?:EUR|€)?\s*([0-9][0-9.,]+)",
            text, re.IGNORECASE
        )
        if neg_ek_match:
            value = self._parse_number(neg_ek_match.group(1))
            if value is not None:
                if is_teur:
                    value *= 1000
                financial.eigenkapital = -abs(value)

        for field, patterns in self.FIELD_PATTERNS.items():
            # Eigenkapital ueberspringen wenn schon durch Fehlbetrag gesetzt
            if field == "eigenkapital" and financial.eigenkapital is not None:
                continue

            for pattern in patterns:
                match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if match:
                    value = self._parse_number(match.group(1))
                    if value is not None:
                        # Fehlbetrag/Bilanzverlust = negativ
                        if field == "jahresergebnis":
                            matched_text = match.group(0).lower()
                            if "fehlbetrag" in matched_text or "verlust" in matched_text:
                                value = -abs(value)

                        # TEUR-Multiplikator
                        if is_teur and field != "mitarbeiter":
                            value *= 1000

                        # Mitarbeiter: nur ganzzahlig, max. 5-stellig sinnvoll
                        if field == "mitarbeiter":
                            value = int(value)
                            if value > 999999:
                                continue  # Unplausibel, naechstes Pattern probieren

                        setattr(financial, field, value)
                    break  # Erstes Match gewinnt

        self._calculate_ratios(financial)
        return financial

    def _calculate_ratios(self, f: FinancialData):
        if f.eigenkapital and f.bilanzsumme and f.bilanzsumme > 0:
            f.eigenkapitalquote = round((f.eigenkapital / f.bilanzsumme) * 100, 2)
        if f.umsatz and f.mitarbeiter and f.mitarbeiter > 0:
            f.umsatz_pro_mitarbeiter = round(f.umsatz / f.mitarbeiter, 2)

    def _parse_number(self, value_str: str) -> Optional[float]:
        if not value_str:
            return None
        cleaned = value_str.strip().replace(" ", "")
        # Trailing minus (DATEV-Format: "339.286,24-")
        trailing_minus = cleaned.endswith("-")
        if trailing_minus:
            cleaned = cleaned[:-1]
        if "," in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(".", "")
        try:
            result = float(cleaned)
            if trailing_minus:
                result = -abs(result)
            return result
        except ValueError:
            return None

    def _extract_year(self, text: str) -> Optional[str]:
        match = re.search(r"(?:zum|per)\s+31\.\s*12\.\s*(\d{4})", text, re.IGNORECASE)
        if match:
            return match.group(1)
        match = re.search(r"31\.\s*Dezember\s*(\d{4})", text, re.IGNORECASE)
        if match:
            return match.group(1)
        years = re.findall(r"\b(20\d{2})\b", text)
        if years:
            from collections import Counter
            return Counter(years).most_common(1)[0][0]
        return None


parser = FinancialTextParser()
scraper = BundesanzeigerScraper()


# ============================================================
# API Endpoints
# ============================================================

@app.get("/")
async def root():
    return {"status": "ok", "service": "OpenRisk AI Backend", "version": "1.1.0"}

@app.post("/api/company/lookup", response_model=ScoringInput)
async def lookup_company(request: CompanyRequest):
    company_name = request.name.strip()
    if not company_name:
        raise HTTPException(status_code=400, detail="Bitte einen Firmennamen eingeben.")

    # Bundesanzeiger abfragen (eigener Scraper statt deutschland-Package)
    try:
        reports = scraper.get_reports(company_name)
    except ConnectionError as e:
        raise HTTPException(
            status_code=503,
            detail=f"Bundesanzeiger nicht erreichbar. Bitte versuchen Sie es in einigen Minuten erneut."
        )
    except Exception as e:
        logger.error(f"Unerwarteter Fehler bei Bundesanzeiger-Abfrage: {e}")
        raise HTTPException(
            status_code=503,
            detail=f"Fehler bei der Bundesanzeiger-Abfrage. Bitte versuchen Sie es erneut."
        )

    if not reports:
        raise HTTPException(
            status_code=404,
            detail=f"Keine Jahresabschlüsse für '{company_name}' im Bundesanzeiger gefunden. "
                   f"Tipp: Versuchen Sie den exakten Firmennamen (z.B. 'SAP SE' statt 'SAP')."
        )

    # Neuesten Jahresabschluss finden (priorisierte Suche)
    jahresabschluss = None

    # Prioritaet 1: Explizite Jahresabschluesse
    ja_keywords = ["jahresabschluss", "jahresabschluß"]
    for key, report in reports.items():
        name = report.get("name", "").lower()
        if any(kw in name for kw in ja_keywords):
            if jahresabschluss is None:
                jahresabschluss = report
            else:
                if report.get("date") and jahresabschluss.get("date"):
                    if report["date"] > jahresabschluss["date"]:
                        jahresabschluss = report

    # Prioritaet 2: Rechnungslegung / Finanzberichte
    if not jahresabschluss:
        fallback_keywords = ["rechnungslegung", "konzernabschluss", "bilanz", "lagebericht"]
        for key, report in reports.items():
            name = report.get("name", "").lower()
            if any(kw in name for kw in fallback_keywords):
                if jahresabschluss is None:
                    jahresabschluss = report
                else:
                    if report.get("date") and jahresabschluss.get("date"):
                        if report["date"] > jahresabschluss["date"]:
                            jahresabschluss = report

    # Prioritaet 3: Irgendeinen Report mit Finanzdaten nehmen
    if not jahresabschluss:
        for key, report in reports.items():
            text = report.get("report", "").lower()
            if any(kw in text for kw in ["bilanzsumme", "eigenkapital", "umsatzerlöse", "jahresüberschuss"]):
                jahresabschluss = report
                break

    # Letzter Fallback
    if not jahresabschluss:
        jahresabschluss = list(reports.values())[0]

    raw_text = jahresabschluss.get("report", "")
    company_found = jahresabschluss.get("company", request.name)

    if not raw_text:
        raise HTTPException(
            status_code=404,
            detail=f"Jahresabschluss für '{company_found}' gefunden, aber der Inhalt konnte nicht geladen werden."
        )

    financials = parser.parse(raw_text)
    financials.quelle = "Bundesanzeiger"

    logger.info(f"Ergebnis fuer {company_found}: Bilanzsumme={financials.bilanzsumme}, EK={financials.eigenkapital}")

    return ScoringInput(
        company_name=company_found,
        financials=financials,
        raw_text=raw_text[:2000] if raw_text else None
    )

@app.get("/api/health")
async def health():
    return {"status": "ok", "version": "1.1.0"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
