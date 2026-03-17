# OpenRisk AI - Automatische Datenpipeline

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import pdfplumber
import re
import io
import os
from typing import Optional

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

class BundesanzeigerScraper:
    BASE_URL = "https://www.bundesanzeiger.de"

    async def search(self, company_name: str) -> list:
        headers = {"User-Agent": "Mozilla/5.0"}
        async with httpx.AsyncClient(timeout=30) as client:
            try:
                url = self.BASE_URL + "/pub/de/native?func=json_sesuche&suchtext=" + company_name + "&kategorie=JA"
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    return self._parse_search_results(resp.text)
            except Exception as e:
                print(str(e))
        return []

    def _parse_search_results(self, html: str) -> list:
        results = []
        pattern = r'href="(/pub/de/[^"]*?(?:jahresabschluss|JA)[^"]*?)"[^>]*>([^<]+)</a>'
        matches = re.findall(pattern, html, re.IGNORECASE)
        for path, title in matches[:5]:
            results.append({"url": self.BASE_URL + path, "title": title.strip()})
        return results

    async def download_pdf(self, url: str) -> Optional[bytes]:
        async with httpx.AsyncClient(timeout=60, follow_redirects=True) as client:
            try:
                resp = await client.get(url)
                if resp.status_code == 200:
                    return resp.content
            except Exception as e:
                print(str(e))
        return None


class FinancialPDFParser:

    def parse(self, pdf_bytes: bytes):
        full_text = ""
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                full_text += (page.extract_text() or "") + "\n"

        financial = FinancialData(quelle="Bundesanzeiger")
        financial.geschaeftsjahr = self._extract_year(full_text)

        patterns = {
            "bilanzsumme": r"Bilanzsumme\s*[\.\s]*([0-9.,]+)",
            "eigenkapital": r"Eigenkapital\s*[\.\s]*([0-9.,]+)",
            "umsatz": r"Umsatzerlöse?\s*[\.\s]*([0-9.,]+)",
            "jahresergebnis": r"Jahresergebnis\s*[\.\s]*(-?[0-9.,]+)",
            "mitarbeiter": r"Mitarbeiter\w*\s*[\.\s]*([0-9.,]+)",
        }

        for field, pattern in patterns.items():
            match = re.search(pattern, full_text, re.IGNORECASE | re.MULTILINE)
            if match:
                value = self._parse_number(match.group(1))
                if value is not None:
                    setattr(financial, field, value)

        self._calculate_ratios(financial)
        return financial, full_text

    def _calculate_ratios(self, f: FinancialData):
        if f.eigenkapital and f.bilanzsumme and f.bilanzsumme > 0:
            f.eigenkapitalquote = round((f.eigenkapital / f.bilanzsumme) * 100, 2)
        if f.umsatz and f.mitarbeiter and f.mitarbeiter > 0:
            f.umsatz_pro_mitarbeiter = round(f.umsatz / f.mitarbeiter, 2)

    def _parse_number(self, value_str: str) -> Optional[float]:
        if not value_str:
            return None
        cleaned = value_str.strip().replace(" ", "")
        if "," in cleaned:
            cleaned = cleaned.replace(".", "").replace(",", ".")
        else:
            cleaned = cleaned.replace(".", "")
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _extract_year(self, text: str) -> Optional[str]:
        match = re.search(r"(?:zum|per)\s+31\.\s*12\.\s*(\d{4})", text, re.IGNORECASE)
        if match:
            return match.group(1)
        years = re.findall(r"\b(20\d{2})\b", text)
        if years:
            from collections import Counter
            return Counter(years).most_common(1)[0][0]
        return None


scraper = BundesanzeigerScraper()
parser = FinancialPDFParser()


@app.get("/")
async def root():
    return {"status": "ok"}


@app.post("/api/company/lookup", response_model=ScoringInput)
async def lookup_company(request: CompanyRequest):
    documents = await scraper.search(request.name)
    if not documents:
        raise HTTPException(status_code=404, detail="Keine Daten gefunden.")

    pdf_bytes = None
    used_url = None
    for doc in documents:
        pdf_bytes = await scraper.download_pdf(doc["url"])
        if pdf_bytes:
            used_url = doc["url"]
            break

    if not pdf_bytes:
        raise HTTPException(status_code=422, detail="PDF nicht ladbar.")

    financials, raw_text = parser.parse(pdf_bytes)
    financials.quelle = used_url

    return ScoringInput(
        company_name=request.name,
        financials=financials,
        raw_text=raw_text[:2000] if raw_text else None
    )


@app.get("/api/health")
async def health():
    return {"status": "ok"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
