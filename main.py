# OpenRisk AI - Automatische Datenpipeline

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import re
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

class FinancialTextParser:

    def parse(self, text: str) -> FinancialData:
        financial = FinancialData()
        financial.geschaeftsjahr = self._extract_year(text)
        patterns = {
            "bilanzsumme": r"Bilanzsumme\s*[\.\s]*([0-9.,]+)",
            "eigenkapital": r"Eigenkapital\s*[\.\s]*([0-9.,]+)",
            "umsatz": r"Umsatzerlöse?\s*[\.\s]*([0-9.,]+)",
            "jahresergebnis": r"Jahres(?:überschuss|ergebnis|fehlbetrag)\s*[\.\s]*(-?[0-9.,]+)",
            "mitarbeiter": r"Mitarbeiter\w*\s*[\.\s]*([0-9.,]+)",
        }
        for field, pattern in patterns.items():
            match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
            if match:
                value = self._parse_number(match.group(1))
                if value is not None:
                    if field == "jahresergebnis" and "fehlbetrag" in match.group(0).lower():
                        value = -abs(value)
                    setattr(financial, field, value)
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

parser = FinancialTextParser()

@app.get("/")
async def root():
    return {"status": "ok"}

@app.post("/api/company/lookup", response_model=ScoringInput)
async def lookup_company(request: CompanyRequest):
    try:
        from deutschland.bundesanzeiger import Bundesanzeiger
        ba = Bundesanzeiger()
        reports = ba.get_reports(request.name)
    except Exception as e:
        raise HTTPException(status_code=503, detail="Bundesanzeiger nicht erreichbar: " + str(e))

    if not reports:
        raise HTTPException(status_code=404, detail="Keine Jahresabschluesse gefunden.")

    jahresabschluss = None
    for key, report in reports.items():
        name = report.get("name", "")
        if "jahresabschluss" in name.lower():
            if jahresabschluss is None:
                jahresabschluss = report
            else:
                if report.get("date") and jahresabschluss.get("date"):
                    if report["date"] > jahresabschluss["date"]:
                        jahresabschluss = report

    if not jahresabschluss:
        jahresabschluss = list(reports.values())[0]

    raw_text = jahresabschluss.get("report", "")
    company_found = jahresabschluss.get("company", request.name)

    financials = parser.parse(raw_text)
    financials.quelle = "Bundesanzeiger"

    return ScoringInput(
        company_name=company_found,
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
