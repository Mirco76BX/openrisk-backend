“””
OpenRisk AI – Automatische Datenpipeline
Bundesanzeiger → PDF Parser → Scoring-ready JSON
“””

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import pdfplumber
import re
import io
import json
from typing import Optional

app = FastAPI(title=“OpenRisk AI Backend”, version=“1.0.0”)

# CORS — erlaubt Lovable-Frontend Zugriff

app.add_middleware(
CORSMiddleware,
allow_origins=[”*”],  # In Produktion: [“https://openrisk-ai.lovable.app”]
allow_methods=[”*”],
allow_headers=[”*”],
)

# ─────────────────────────────────────────────

# DATENMODELLE

# ─────────────────────────────────────────────

class CompanyRequest(BaseModel):
name: str
handelsregister_nr: Optional[str] = None  # z.B. “HRB 716”
bundesland: Optional[str] = None          # z.B. “Bayern”

class FinancialData(BaseModel):
# Bilanz
bilanzsumme: Optional[float] = None
eigenkapital: Optional[float] = None
umlaufvermoegen: Optional[float] = None
kurzfristige_verbindlichkeiten: Optional[float] = None
langfristige_verbindlichkeiten: Optional[float] = None
# GuV
umsatz: Optional[float] = None
jahresergebnis: Optional[float] = None
ebit: Optional[float] = None
# Stammdaten
mitarbeiter: Optional[int] = None
gruendungsjahr: Optional[int] = None
rechtsform: Optional[str] = None
branche: Optional[str] = None
# Berechnete Kennzahlen
eigenkapitalquote: Optional[float] = None
liquiditaet_1: Optional[float] = None
umsatz_pro_mitarbeiter: Optional[float] = None
# Metadaten
quelle: Optional[str] = None
geschaeftsjahr: Optional[str] = None

class ScoringInput(BaseModel):
company_name: str
financials: FinancialData
raw_text: Optional[str] = None

# ─────────────────────────────────────────────

# BUNDESANZEIGER SCRAPER

# ─────────────────────────────────────────────

class BundesanzeigerScraper:
BASE_URL = “https://www.bundesanzeiger.de”
SEARCH_URL = f”{BASE_URL}/pub/de/suchergebnis”

```
async def search(self, company_name: str) -> list[dict]:
    """Sucht Unternehmen im Bundesanzeiger und gibt verfügbare Dokumente zurück."""
    params = {
        "fulltext": company_name,
        "category": "JA",  # Jahresabschlüsse
        "sort": "date",
        "order": "desc",
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; OpenRiskAI/1.0)",
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.get(
                f"{self.BASE_URL}/pub/de/native?func=json_sesuche"
                f"&suchtext={company_name}&kategorie=JA",
                headers=headers
            )
            if resp.status_code == 200:
                return self._parse_search_results(resp.text)
        except Exception as e:
            print(f"Bundesanzeiger Suche Fehler: {e}")
    return []

def _parse_search_results(self, html: str) -> list[dict]:
    """Extrahiert Dokumentlinks aus Suchergebnissen."""
    results = []
    # Suche nach Jahresabschluss-Links
    pattern = r'href="(/pub/de/[^"]*?(?:jahresabschluss|JA)[^"]*?)"[^>]*>([^<]+)</a>'
    matches = re.findall(pattern, html, re.IGNORECASE)
    for path, title in matches[:5]:  # Max 5 neueste
        results.append({
            "url": f"{self.BASE_URL}{path}",
            "title": title.strip(),
        })
    return results

async def download_pdf(self, url: str) -> Optional[bytes]:
    """Lädt ein PDF vom Bundesanzeiger herunter."""
    async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
        try:
            resp = await client.get(url)
            if resp.status_code == 200 and "pdf" in resp.headers.get("content-type", ""):
                return resp.content
        except Exception as e:
            print(f"PDF Download Fehler: {e}")
    return None
```

# ─────────────────────────────────────────────

# PDF PARSER — Finanzkennzahlen Extraktion

# ─────────────────────────────────────────────

class FinancialPDFParser:
“””
Extrahiert Finanzkennzahlen aus deutschen Jahresabschluss-PDFs.
Erkennt gängige Formate: HGB Bilanz, GuV, BWA.
“””

```
# Regex-Muster für deutsche Bilanzkennzahlen
PATTERNS = {
    "bilanzsumme": [
        r"Bilanzsumme\s*[\.\s]*([0-9.,]+)",
        r"Summe\s+(?:der\s+)?Aktiva?\s*[\.\s]*([0-9.,]+)",
        r"BILANZSUMME\s*([0-9.,]+)",
    ],
    "eigenkapital": [
        r"Eigenkapital\s*[\.\s]*([0-9.,]+)",
        r"A\.\s*Eigenkapital\s*[\.\s]*([0-9.,]+)",
        r"Gesamtes\s+Eigenkapital\s*[\.\s]*([0-9.,]+)",
    ],
    "umlaufvermoegen": [
        r"Umlaufverm[öo]gen\s*[\.\s]*([0-9.,]+)",
        r"B\.\s*Umlaufverm[öo]gen\s*[\.\s]*([0-9.,]+)",
    ],
    "kurzfristige_verbindlichkeiten": [
        r"Verbindlichkeiten\s+(?:mit\s+einer\s+)?Restlaufzeit\s+(?:bis\s+zu\s+)?(?:einem|1)\s+Jahr\s*[\.\s]*([0-9.,]+)",
        r"Kurzfristige\s+Verbindlichkeiten\s*[\.\s]*([0-9.,]+)",
    ],
    "umsatz": [
        r"Umsatzerlöse?\s*[\.\s]*([0-9.,]+)",
        r"Umsatz\s*[\.\s]*([0-9.,]+)",
        r"1\.\s*Umsatzerlöse?\s*[\.\s]*([0-9.,]+)",
    ],
    "jahresergebnis": [
        r"Jahres[üu]berschuss\s*[\.\s]*([0-9.,]+)",
        r"Jahresfehlbetrag\s*[\.\s]*(-[0-9.,]+)",
        r"Jahresergebnis\s*[\.\s]*(-?[0-9.,]+)",
        r"Ergebnis\s+(?:des\s+)?Gesch[äa]ftsjahres?\s*[\.\s]*(-?[0-9.,]+)",
    ],
    "mitarbeiter": [
        r"(?:Durchschnittlich\s+)?(?:besch[äa]ftigte?\s+)?Mitarbeiter(?:innen)?\s*[\.\s]*([0-9.,]+)",
        r"Arbeitnehmer\s*[\.\s]*([0-9.,]+)",
        r"Personalbestand\s*[\.\s]*([0-9.,]+)",
    ],
    "gruendungsjahr": [
        r"gegr[üu]ndet\s+(?:im\s+Jahr\s+|am\s+\d+\.\d+\.)?(\d{4})",
        r"Gr[üu]ndung(?:sjahr)?\s*[\.\s:]*(\d{4})",
    ],
}

def parse(self, pdf_bytes: bytes) -> tuple[FinancialData, str]:
    """Hauptmethode: PDF → FinancialData + Rohtext."""
    full_text = ""
    tables_data = {}

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            page_text = page.extract_text() or ""
            full_text += page_text + "\n"

            # Tabellen extrahieren (Bilanztabellen)
            tables = page.extract_tables()
            for table in tables:
                self._process_table(table, tables_data)

    financial = FinancialData(quelle="Bundesanzeiger")
    financial.geschaeftsjahr = self._extract_year(full_text)

    # Regex-Extraktion
    for field, patterns in self.PATTERNS.items():
        for pattern in patterns:
            match = re.search(pattern, full_text, re.IGNORECASE | re.MULTILINE)
            if match:
                value = self._parse_number(match.group(1))
                if value is not None:
                    setattr(financial, field, value)
                    break

    # Tabellenwerte überschreiben wenn präziser
    for field, value in tables_data.items():
        if value is not None:
            setattr(financial, field, value)

    # Abgeleitete Kennzahlen berechnen
    self._calculate_ratios(financial)

    return financial, full_text

def _process_table(self, table: list, data: dict):
    """Verarbeitet eine Tabelle und sucht Kennzahlen."""
    if not table:
        return
    for row in table:
        if not row or len(row) < 2:
            continue
        label = str(row[0] or "").strip().lower()
        # Letzten nicht-leeren Wert nehmen (aktuellstes Jahr)
        values = [str(cell or "").strip() for cell in row[1:] if cell]
        if not values:
            continue
        value_str = values[-1]

        if "bilanzsumme" in label or "summe aktiva" in label:
            data["bilanzsumme"] = self._parse_number(value_str)
        elif "eigenkapital" in label and "gesamt" in label:
            data["eigenkapital"] = self._parse_number(value_str)
        elif "umsatzerlös" in label or "umsatz" in label:
            data["umsatz"] = self._parse_number(value_str)
        elif "jahresüberschuss" in label or "jahresergebnis" in label:
            data["jahresergebnis"] = self._parse_number(value_str)

def _calculate_ratios(self, f: FinancialData):
    """Berechnet abgeleitete Kennzahlen."""
    if f.eigenkapital and f.bilanzsumme and f.bilanzsumme > 0:
        f.eigenkapitalquote = round((f.eigenkapital / f.bilanzsumme) * 100, 2)

    if f.umlaufvermoegen and f.kurzfristige_verbindlichkeiten and f.kurzfristige_verbindlichkeiten > 0:
        f.liquiditaet_1 = round((f.umlaufvermoegen / f.kurzfristige_verbindlichkeiten) * 100, 2)

    if f.umsatz and f.mitarbeiter and f.mitarbeiter > 0:
        f.umsatz_pro_mitarbeiter = round(f.umsatz / f.mitarbeiter, 2)

def _parse_number(self, value_str: str) -> Optional[float]:
    """Konvertiert deutsche Zahlenformate: '1.234.567,89' → 1234567.89"""
    if not value_str:
        return None
    cleaned = value_str.strip().replace(" ", "").replace("\xa0", "")
    # Deutsches Format: Punkt = Tausender, Komma = Dezimal
    if re.match(r'^-?[\d.,]+$', cleaned):
        if ',' in cleaned:
            cleaned = cleaned.replace('.', '').replace(',', '.')
        else:
            cleaned = cleaned.replace('.', '')
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None

def _extract_year(self, text: str) -> Optional[str]:
    """Extrahiert das Geschäftsjahr aus dem Text."""
    patterns = [
        r"Gesch[äa]ftsjahr\s+(\d{4})",
        r"(?:zum|per|am)\s+31\.\s*12\.\s*(\d{4})",
        r"Jahresabschluss\s+(\d{4})",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(1)
    # Fallback: häufigstes 4-stelliges Jahr
    years = re.findall(r'\b(20\d{2}|19\d{2})\b', text)
    if years:
        from collections import Counter
        return Counter(years).most_common(1)[0][0]
    return None
```

# ─────────────────────────────────────────────

# API ENDPOINTS

# ─────────────────────────────────────────────

scraper = BundesanzeigerScraper()
parser = FinancialPDFParser()

@app.get(”/”)
async def root():
return {“status”: “OpenRisk AI Backend läuft”, “version”: “1.0.0”}

@app.post(”/api/company/lookup”, response_model=ScoringInput)
async def lookup_company(request: CompanyRequest):
“””
Hauptendpunkt: Firmenname → automatische Datenbeschaffung → Scoring-ready JSON.
Lovable ruft diesen Endpunkt auf statt manuell PDFs hochzuladen.
“””
# 1. Bundesanzeiger durchsuchen
documents = await scraper.search(request.name)

```
if not documents:
    raise HTTPException(
        status_code=404,
        detail=f"Keine Jahresabschlüsse für '{request.name}' im Bundesanzeiger gefunden. "
               f"Bitte prüfen Sie den Firmennamen oder laden Sie die Dokumente manuell hoch."
    )

# 2. Neuestes Dokument herunterladen
pdf_bytes = None
used_url = None
for doc in documents:
    pdf_bytes = await scraper.download_pdf(doc["url"])
    if pdf_bytes:
        used_url = doc["url"]
        break

if not pdf_bytes:
    raise HTTPException(
        status_code=422,
        detail="Dokument gefunden, aber PDF konnte nicht geladen werden."
    )

# 3. Kennzahlen extrahieren
financials, raw_text = parser.parse(pdf_bytes)
financials.quelle = used_url

return ScoringInput(
    company_name=request.name,
    financials=financials,
    raw_text=raw_text[:2000] if raw_text else None  # Ersten 2000 Zeichen für Debug
)
```

@app.post(”/api/parse/pdf”)
async def parse_pdf_upload(pdf_url: str):
“””
Alternativ: Direkte PDF-URL → Kennzahlen.
Für Fälle wo der User die PDF-URL bereits kennt.
“””
pdf_bytes = await scraper.download_pdf(pdf_url)
if not pdf_bytes:
raise HTTPException(status_code=422, detail=“PDF konnte nicht geladen werden.”)

```
financials, _ = parser.parse(pdf_bytes)
return financials
```

@app.get(”/api/health”)
async def health():
return {“status”: “ok”, “services”: {“bundesanzeiger”: “reachable”}}
