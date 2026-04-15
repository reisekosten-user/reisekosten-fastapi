from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, List, Literal, Optional

import requests
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from pypdf import PdfReader


APP_VERSION = "6.5a"
DEFAULT_MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_BASE = os.getenv("MISTRAL_API_BASE", "https://api.mistral.ai/v1")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "180"))

app = FastAPI(
    title="Reisekosten Mistral API",
    version=APP_VERSION,
    description="Extrahiert strukturierte Reisekosten-Daten aus PDFs, E-Mails und Texten via Mistral.",
)


DocumentType = Literal["Zug", "Flug", "Hotel", "Taxi", "Unbekannt"]


class Segment(BaseModel):
    index: int = Field(..., description="Laufende Nummer des Segments ab 1")
    departure_datetime: str = Field(default="nicht vorhanden")
    arrival_datetime: str = Field(default="nicht vorhanden")
    departure_location: str = Field(default="nicht vorhanden")
    arrival_location: str = Field(default="nicht vorhanden")
    transport_company_and_number: str = Field(default="nicht vorhanden")


class ExtractionResult(BaseModel):
    belegdatum: str = "nicht vorhanden"
    art_des_dokuments: DocumentType = "Unbekannt"
    buchungsnummer_code: str = "nicht vorhanden"
    name_des_reisenden: str = "nicht vorhanden"
    wie_viele_reisesegmente: int = 0
    ticketnummer: str = "nicht vorhanden"
    kosten_mit_steuern: str = "nicht vorhanden"
    waehrung_der_kosten: str = "nicht vorhanden"
    reisesegmente: List[Segment] = Field(default_factory=list)
    confidence_score: Optional[float] = None
    warnungen: List[str] = Field(default_factory=list)
    fehler: List[str] = Field(default_factory=list)
    raw_model_output: Optional[str] = None
    source_filename: Optional[str] = None
    generated_at_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    version: str = APP_VERSION


class AnalyzeTextRequest(BaseModel):
    text: str
    filename: Optional[str] = None
    model: Optional[str] = None
    include_raw_output: bool = False


class HealthResponse(BaseModel):
    status: str
    version: str
    mistral_configured: bool


SYSTEM_PROMPT = """Du bist ein hochpräziser Dokumenten-Parser für Reisekostenbelege.

Deine Aufgabe:
Analysiere das bereitgestellte Dokument (PDF, E-Mail oder Text) und extrahiere ALLE relevanten Informationen.

WICHTIGE REGELN:
- Jede der folgenden Zeilen MUSS IMMER ausgefüllt werden
- Wenn ein Wert nicht vorhanden ist: schreibe \"nicht vorhanden\"
- Keine Interpretation ohne Grundlage im Dokument
- Keine Werte erfinden
- Struktur IMMER exakt einhalten
- Datum und Zeit IMMER im Format: DD.MM.YYYY HH:MM
- Wenn keine Uhrzeit vorhanden: nur Datum schreiben
- Zeitzonen wenn erkennbar ergänzen (z.B. MEZ, IST, GMT)
- Währungen exakt übernehmen (EUR, USD, INR etc.)

KLASSIFIKATION:
Bestimme den Dokumenttyp strikt anhand des Inhalts:
- Flug → Airlines, Flugnummern, Flughäfen
- Hotel → Check-in / Check-out, Nächte
- Taxi → Uber, Fahrt, Fahrer, Strecke
- Zug → Bahn, Rail, ICE, etc.

SPEZIALREGELN:
1. TAXI (z.B. Uber):
- Genau 1 Segment (außer Storno)
- Fahrername als Unternehmen zulässig
- Start und Ziel immer aus Adresse extrahieren

2. HOTEL:
- 1 Segment = gesamter Aufenthalt
- Abreise = Check-in
- Ankunft = Check-out
- Ort = Hotelstandort

3. FLUG:
- Jedes Flugsegment einzeln aufführen
- (+1) Tage korrekt berücksichtigen
- Flughafencodes (z.B. FRA, ZRH) hinzufügen wenn vorhanden

4. STORNIERTE FAHRT:
- Wie viele Reisesegmente: 0
- Tabelle trotzdem mit Hinweis auf Storno als leerer Datensatz oder Warnung

5. DUPLIKATE:
- Trotzdem normal analysieren (keine Entscheidung treffen)

6. KOSTEN:
- Kosten mit Steuern nur angeben, wenn Gesamtbetrag im Dokument eindeutig vorhanden ist
- Wenn nur Steuern oder Teilbeträge vorhanden sind, nichts erfinden

7. QUALITÄTSKONTROLLE:
- Sind alle Felder ausgefüllt?
- Stimmen Datum + Zeiten logisch?
- Anzahl Segmente korrekt?
- Währung vorhanden?

Gib das Ergebnis AUSSCHLIESSLICH als valides JSON zurück.
Keine Markdown-Blöcke. Kein erläuternder Text. Kein Vorwort.
"""


USER_PROMPT_TEMPLATE = """Extrahiere die Daten aus folgendem Dokumentinhalt.

Erwartetes JSON-Schema:
{{
  "belegdatum": "string",
  "art_des_dokuments": "Zug|Flug|Hotel|Taxi|Unbekannt",
  "buchungsnummer_code": "string",
  "name_des_reisenden": "string",
  "wie_viele_reisesegmente": 0,
  "ticketnummer": "string",
  "kosten_mit_steuern": "string",
  "waehrung_der_kosten": "string",
  "reisesegmente": [
    {{
      "index": 1,
      "departure_datetime": "string",
      "arrival_datetime": "string",
      "departure_location": "string",
      "arrival_location": "string",
      "transport_company_and_number": "string"
    }}
  ],
  "confidence_score": 0.0,
  "warnungen": ["string"],
  "fehler": ["string"]
}}

Zusätzliche Anforderungen:
- wie_viele_reisesegmente muss zur Länge von reisesegmente passen, außer bei stornierten Fahrten. Dann 0 und reisesegmente darf leer sein.
- confidence_score zwischen 0.0 und 1.0
- Falls etwas fehlt: "nicht vorhanden"
- Sprache der Felder im JSON exakt wie vorgegeben

Dateiname: {filename}

Dokumentinhalt:
{document_text}
"""


def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        pages: List[str] = []
        for i, page in enumerate(reader.pages, start=1):
            text = page.extract_text() or ""
            pages.append(f"\n--- Seite {i} ---\n{text}\n")
        return "\n".join(pages).strip()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"PDF konnte nicht gelesen werden: {exc}") from exc


def normalize_input_text(text: str) -> str:
    cleaned = text.replace("\x00", " ").strip()
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    return cleaned



def build_user_prompt(document_text: str, filename: str) -> str:
    return USER_PROMPT_TEMPLATE.format(
        filename=filename or "nicht vorhanden",
        document_text=document_text[:120000],
    )



def call_mistral(messages: List[Dict[str, str]], model: Optional[str] = None) -> str:
    if not MISTRAL_API_KEY:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY ist nicht gesetzt.")

    payload = {
        "model": model or DEFAULT_MISTRAL_MODEL,
        "messages": messages,
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }

    try:
        response = requests.post(
            f"{MISTRAL_API_BASE.rstrip('/')}/chat/completions",
            headers=headers,
            json=payload,
            timeout=REQUEST_TIMEOUT,
        )
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Mistral API nicht erreichbar: {exc}") from exc

    if response.status_code >= 400:
        raise HTTPException(
            status_code=502,
            detail=f"Mistral API Fehler {response.status_code}: {response.text[:1000]}",
        )

    data = response.json()
    try:
        return data["choices"][0]["message"]["content"]
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Unerwartete Mistral Antwort: {json.dumps(data)[:1000]}") from exc


JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)


def parse_model_json(raw_output: str) -> Dict[str, Any]:
    text = raw_output.strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        match = JSON_BLOCK_RE.search(text)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
        raise HTTPException(status_code=502, detail="Modellantwort war kein valides JSON.")


KNOWN_CURRENCIES = {"EUR", "USD", "INR", "CHF", "GBP", "JPY", "CNY"}


def normalize_currency(value: str) -> str:
    if not value:
        return "nicht vorhanden"
    value = value.strip().upper()
    if value in KNOWN_CURRENCIES:
        return value
    symbols = {
        "€": "EUR",
        "$": "USD",
        "₹": "INR",
        "£": "GBP",
    }
    return symbols.get(value, value)



def ensure_string(value: Any) -> str:
    if value is None:
        return "nicht vorhanden"
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else "nicht vorhanden"
    return str(value)



def postprocess_result(parsed: Dict[str, Any], source_filename: Optional[str], raw_output: Optional[str]) -> ExtractionResult:
    segments_input = parsed.get("reisesegmente") or []
    segments: List[Segment] = []

    for idx, seg in enumerate(segments_input, start=1):
        if not isinstance(seg, dict):
            continue
        segments.append(
            Segment(
                index=int(seg.get("index", idx) or idx),
                departure_datetime=ensure_string(seg.get("departure_datetime")),
                arrival_datetime=ensure_string(seg.get("arrival_datetime")),
                departure_location=ensure_string(seg.get("departure_location")),
                arrival_location=ensure_string(seg.get("arrival_location")),
                transport_company_and_number=ensure_string(seg.get("transport_company_and_number")),
            )
        )

    art = ensure_string(parsed.get("art_des_dokuments"))
    allowed_art = {"Zug", "Flug", "Hotel", "Taxi", "Unbekannt"}
    if art not in allowed_art:
        art = "Unbekannt"

    warnungen = [ensure_string(x) for x in (parsed.get("warnungen") or []) if ensure_string(x) != "nicht vorhanden"]
    fehler = [ensure_string(x) for x in (parsed.get("fehler") or []) if ensure_string(x) != "nicht vorhanden"]

    try:
        confidence = float(parsed.get("confidence_score")) if parsed.get("confidence_score") is not None else None
        if confidence is not None:
            confidence = max(0.0, min(1.0, confidence))
    except Exception:
        confidence = None
        warnungen.append("confidence_score war ungültig und wurde verworfen")

    result = ExtractionResult(
        belegdatum=ensure_string(parsed.get("belegdatum")),
        art_des_dokuments=art,  # type: ignore[arg-type]
        buchungsnummer_code=ensure_string(parsed.get("buchungsnummer_code")),
        name_des_reisenden=ensure_string(parsed.get("name_des_reisenden")),
        wie_viele_reisesegmente=int(parsed.get("wie_viele_reisesegmente") or 0),
        ticketnummer=ensure_string(parsed.get("ticketnummer")),
        kosten_mit_steuern=ensure_string(parsed.get("kosten_mit_steuern")),
        waehrung_der_kosten=normalize_currency(ensure_string(parsed.get("waehrung_der_kosten"))),
        reisesegmente=segments,
        confidence_score=confidence,
        warnungen=warnungen,
        fehler=fehler,
        raw_model_output=raw_output,
        source_filename=source_filename,
    )

    if result.art_des_dokuments == "Taxi" and "storniert" in " ".join(w.lower() for w in result.warnungen + result.fehler):
        result.wie_viele_reisesegmente = 0

    if result.wie_viele_reisesegmente != len(result.reisesegmente):
        if result.wie_viele_reisesegmente == 0 and len(result.reisesegmente) == 0:
            pass
        else:
            result.warnungen.append(
                f"Segmentanzahl korrigiert: Feld={result.wie_viele_reisesegmente}, Liste={len(result.reisesegmente)}"
            )
            result.wie_viele_reisesegmente = len(result.reisesegmente)

    return result



def analyze_document_text(document_text: str, filename: str, model: Optional[str], include_raw_output: bool) -> ExtractionResult:
    cleaned = normalize_input_text(document_text)
    if not cleaned:
        raise HTTPException(status_code=400, detail="Kein verwertbarer Dokumentinhalt vorhanden.")

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_prompt(cleaned, filename)},
    ]

    raw_output = call_mistral(messages=messages, model=model)
    parsed = parse_model_json(raw_output)
    result = postprocess_result(parsed, source_filename=filename, raw_output=raw_output if include_raw_output else None)
    return result


@app.get("/", response_class=HTMLResponse)
def root() -> str:
    return f"""
    <html>
      <head><title>Reisekosten Mistral API</title></head>
      <body style=\"font-family: Arial, sans-serif; margin: 40px;\">
        <h1>Reisekosten Mistral API</h1>
        <p>Status: läuft 🚀</p>
        <p>Version: {APP_VERSION}</p>
        <ul>
          <li>GET /health</li>
          <li>POST /analyze/text</li>
          <li>POST /analyze/file</li>
          <li>GET /prompt</li>
        </ul>
      </body>
    </html>
    """


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        version=APP_VERSION,
        mistral_configured=bool(MISTRAL_API_KEY),
    )


@app.post("/analyze/text", response_model=ExtractionResult)
def analyze_text(request: AnalyzeTextRequest) -> ExtractionResult:
    return analyze_document_text(
        document_text=request.text,
        filename=request.filename or "text-input.txt",
        model=request.model,
        include_raw_output=request.include_raw_output,
    )


@app.post("/analyze/file", response_model=ExtractionResult)
async def analyze_file(
    file: UploadFile = File(...),
    model: Optional[str] = Form(default=None),
    include_raw_output: bool = Form(default=False),
) -> ExtractionResult:
    filename = file.filename or "upload"
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Datei ist leer.")

    lowered = filename.lower()
    if lowered.endswith(".pdf"):
        document_text = extract_text_from_pdf_bytes(content)
    elif lowered.endswith(".txt") or lowered.endswith(".eml") or lowered.endswith(".md"):
        document_text = content.decode("utf-8", errors="replace")
    else:
        document_text = content.decode("utf-8", errors="replace")

    return analyze_document_text(
        document_text=document_text,
        filename=filename,
        model=model,
        include_raw_output=include_raw_output,
    )


@app.get("/prompt")
def get_prompt() -> JSONResponse:
    return JSONResponse(
        {
            "version": APP_VERSION,
            "system_prompt": SYSTEM_PROMPT,
            "user_prompt_template": USER_PROMPT_TEMPLATE,
        }
    )


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)
