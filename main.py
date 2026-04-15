from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from io import BytesIO
from typing import Any, Dict, List, Literal, Optional

import requests
from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
from pypdf import PdfReader

from database import check_duplicate, init_db, insert_beleg, list_belege

APP_VERSION = "7.0"
DEFAULT_MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_BASE = os.getenv("MISTRAL_API_BASE", "https://api.mistral.ai/v1")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "180"))
DEFAULT_BASE_CURRENCY = os.getenv("BASE_CURRENCY", "EUR").upper()
ENABLE_EXCHANGE_RATES = os.getenv("ENABLE_EXCHANGE_RATES", "true").lower() == "true"
EXCHANGE_RATE_API_URL = os.getenv("EXCHANGE_RATE_API_URL", "https://api.frankfurter.app")

app = FastAPI(
    title="Reisekosten Mistral API",
    version=APP_VERSION,
    description="Extrahiert strukturierte Reisekosten-Daten aus PDFs, E-Mails und Texten via Mistral.",
)

init_db()

DocumentType = Literal["Zug", "Flug", "Hotel", "Taxi", "Unbekannt"]
ReviewStatus = Literal["ok", "pruefen", "fehler"]


class Segment(BaseModel):
    index: int = Field(..., description="Laufende Nummer des Segments ab 1")
    departure_datetime: str = Field(default="nicht vorhanden")
    arrival_datetime: str = Field(default="nicht vorhanden")
    departure_location: str = Field(default="nicht vorhanden")
    arrival_location: str = Field(default="nicht vorhanden")
    transport_company_and_number: str = Field(default="nicht vorhanden")


class DuplicateInfo(BaseModel):
    fingerprint: str
    duplicate_candidate_key: str
    is_duplicate: bool = False


class ExchangeRateInfo(BaseModel):
    source_currency: str = "nicht vorhanden"
    target_currency: str = DEFAULT_BASE_CURRENCY
    rate: Optional[float] = None
    original_amount: Optional[float] = None
    converted_amount: Optional[float] = None
    rate_date: Optional[str] = None
    provider: Optional[str] = None
    success: bool = False
    message: Optional[str] = None


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
    review_status: ReviewStatus = "pruefen"
    is_storno: bool = False
    duplicate_info: Optional[DuplicateInfo] = None
    exchange_rate_info: Optional[ExchangeRateInfo] = None
    raw_model_output: Optional[str] = None
    source_filename: Optional[str] = None
    generated_at_utc: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    version: str = APP_VERSION


class AnalyzeTextRequest(BaseModel):
    text: str
    filename: Optional[str] = None
    model: Optional[str] = None
    include_raw_output: bool = False
    convert_to_base_currency: bool = True


class HealthResponse(BaseModel):
    status: str
    version: str
    mistral_configured: bool
    exchange_rates_enabled: bool
    base_currency: str


SYSTEM_PROMPT = """Du bist ein hochpräziser Dokumenten-Parser für Reisekostenbelege.

Deine Aufgabe:
Analysiere das bereitgestellte Dokument (PDF, E-Mail oder Text) und extrahiere ALLE relevanten Informationen.

WICHTIGE REGELN:
- Jede der folgenden Felder MUSS IMMER ausgefüllt werden.
- Wenn ein Wert nicht vorhanden ist: schreibe "nicht vorhanden".
- Keine Werte erfinden.
- Keine Interpretation ohne Grundlage im Dokument.
- Datum und Zeit IMMER im Format DD.MM.YYYY HH:MM.
- Wenn keine Uhrzeit vorhanden: nur Datum.
- Wenn im Dokument keine Uhrzeit steht, darf keine Standarduhrzeit erfunden werden.
- Wenn kein Belegdatum explizit vorhanden ist, dann "nicht vorhanden".
- Zeitzonen, wenn erkennbar, ergänzen.
- Währungen exakt übernehmen.

BELEGDATUM-PRIORITÄT:
1. Ausstellungsdatum
2. E-Mail-Datum
3. Buchungsdatum
4. Sonst klar erkennbares Dokumentdatum

KLASSIFIKATION:
- Flug → Airlines, Flugnummern, Flughäfen
- Hotel → Check-in / Check-out, Nächte, Zimmer
- Taxi → Uber, Fahrt, Fahrer, Strecke, Fahrzeug
- Zug → Bahn, Rail, ICE, TGV, SNCF, DB, etc.

SPEZIALREGELN:
1. TAXI:
- Normalerweise genau 1 Segment
- Bei Storno: 0 Segmente
- Start und Ziel aus Adressen oder Haltestellen extrahieren
- transport_company_and_number darf z.B. "Uber / Fahrername / Kennzeichen" enthalten

2. HOTEL:
- 1 Segment = gesamter Aufenthalt
- departure_datetime = Check-in
- arrival_datetime = Check-out
- departure_location und arrival_location = Hotelname oder Hotelort
- transport_company_and_number = Hotelname
- Keine Standardzeiten annehmen

3. FLUG:
- Jedes Flugsegment einzeln aufführen
- (+1) Tage korrekt berücksichtigen
- Flughafencodes hinzufügen, wenn vorhanden
- transport_company_and_number = Airline + Flugnummer

4. ZUG:
- Jedes Zugsegment einzeln aufführen
- transport_company_and_number = Bahnunternehmen + Zugnummer

5. KOSTEN:
- kosten_mit_steuern nur angeben, wenn Gesamtbetrag eindeutig vorhanden ist
- Betrag und Währung trennen:
  - kosten_mit_steuern = nur Betrag oder Betrag mit Symbol, ohne Zusatztext
  - waehrung_der_kosten = ISO-Code oder klare Währung

6. CONFIDENCE:
- Realistisch zwischen 0.55 und 0.98
- Niemals pauschal 1.0

7. WARNUNGEN / FEHLER:
- Warnungen bei Unsicherheit, Sonderfällen, widersprüchlichen Daten oder unklaren Kosten
- Fehler nur bei klaren Problemen

Gib das Ergebnis AUSSCHLIESSLICH als valides JSON zurück. Keine Markdown-Blöcke. Kein Vorwort. Kein Nachwort.
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
- wie_viele_reisesegmente muss zur Länge von reisesegmente passen, außer bei Storno. Dann 0 und reisesegmente darf leer sein.
- Falls etwas fehlt: "nicht vorhanden"
- Sprache der Felder im JSON exakt wie vorgegeben

Dateiname: {filename}

Dokumentinhalt:
{document_text}
"""

JSON_BLOCK_RE = re.compile(r"\{.*\}", re.DOTALL)
AMOUNT_RE = re.compile(r"([-+]?\d{1,3}(?:[.,]\d{3})*(?:[.,]\d{2})|[-+]?\d+(?:[.,]\d{2}))")
DATE_RE = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\b")
KNOWN_CURRENCIES = {"EUR", "USD", "INR", "CHF", "GBP", "JPY", "CNY", "AED", "CAD", "AUD"}


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
        raise HTTPException(status_code=502, detail=f"Mistral API Fehler {response.status_code}: {response.text[:1000]}")

    data = response.json()
    try:
        return data["choices"][0]["message"]["content"]
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Unerwartete Mistral Antwort: {json.dumps(data)[:1000]}") from exc


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


def ensure_string(value: Any) -> str:
    if value is None:
        return "nicht vorhanden"
    if isinstance(value, str):
        stripped = value.strip()
        return stripped if stripped else "nicht vorhanden"
    return str(value)


def normalize_currency(value: str) -> str:
    value = ensure_string(value)
    if value == "nicht vorhanden":
        return value
    upper = value.upper().strip()
    if upper in KNOWN_CURRENCIES:
        return upper
    symbol_map = {"€": "EUR", "$": "USD", "₹": "INR", "£": "GBP", "CHF": "CHF"}
    return symbol_map.get(value.strip(), upper)


def parse_decimal_maybe(text: str) -> Optional[Decimal]:
    if not text or text == "nicht vorhanden":
        return None
    match = AMOUNT_RE.search(text.replace(" ", ""))
    if not match:
        return None
    number = match.group(1)
    if number.count(",") > 0 and number.count(".") > 0:
        if number.rfind(",") > number.rfind("."):
            normalized = number.replace(".", "").replace(",", ".")
        else:
            normalized = number.replace(",", "")
    elif number.count(",") > 0:
        normalized = number.replace(".", "").replace(",", ".")
    else:
        normalized = number.replace(",", "")
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def format_decimal(amount: Decimal) -> str:
    return f"{amount.quantize(Decimal('0.01'))}"


def detect_storno(text: str, warnungen: List[str], fehler: List[str]) -> bool:
    joined = " ".join([text.lower()] + [w.lower() for w in warnungen] + [f.lower() for f in fehler])
    keywords = ["storniert", "cancelled", "canceled", "cancelled ride", "storno"]
    return any(keyword in joined for keyword in keywords)


def build_duplicate_info(result: Dict[str, Any], source_filename: Optional[str], original_text: str) -> DuplicateInfo:
    belegdatum = ensure_string(result.get("belegdatum"))
    art = ensure_string(result.get("art_des_dokuments"))
    kosten = ensure_string(result.get("kosten_mit_steuern"))
    waehrung = ensure_string(result.get("waehrung_der_kosten"))
    buchung = ensure_string(result.get("buchungsnummer_code"))
    ticket = ensure_string(result.get("ticketnummer"))
    name = ensure_string(result.get("name_des_reisenden"))

    key = "|".join([
        art.lower(),
        belegdatum.lower(),
        kosten.lower(),
        waehrung.lower(),
        buchung.lower(),
        ticket.lower(),
        name.lower(),
    ])

    fingerprint = hashlib.sha256((original_text + "|" + (source_filename or "")).encode("utf-8", errors="ignore")).hexdigest()[:20]
    duplicate_candidate_key = hashlib.sha256(key.encode("utf-8", errors="ignore")).hexdigest()[:20]
    is_duplicate = check_duplicate(duplicate_candidate_key)

    return DuplicateInfo(
        fingerprint=fingerprint,
        duplicate_candidate_key=duplicate_candidate_key,
        is_duplicate=is_duplicate,
    )


def maybe_fetch_exchange_rate(currency: str, belegdatum: str, amount: Optional[Decimal]) -> ExchangeRateInfo:
    info = ExchangeRateInfo(
        source_currency=currency,
        target_currency=DEFAULT_BASE_CURRENCY,
        original_amount=float(amount) if amount is not None else None,
        success=False,
    )

    if not ENABLE_EXCHANGE_RATES:
        info.message = "Wechselkurse deaktiviert"
        return info

    currency = normalize_currency(currency)
    if currency in {"nicht vorhanden", DEFAULT_BASE_CURRENCY}:
        info.success = currency == DEFAULT_BASE_CURRENCY
        info.rate = 1.0 if currency == DEFAULT_BASE_CURRENCY else None
        info.converted_amount = float(amount) if (amount is not None and currency == DEFAULT_BASE_CURRENCY) else None
        info.rate_date = belegdatum if belegdatum != "nicht vorhanden" else None
        info.provider = "internal/no-conversion"
        info.message = "Keine Umrechnung nötig" if currency == DEFAULT_BASE_CURRENCY else "Währung nicht vorhanden"
        return info

    rate_date = None
    match = DATE_RE.search(belegdatum)
    if match:
        try:
            rate_date = datetime.strptime(match.group(1), "%d.%m.%Y").date().isoformat()
        except ValueError:
            rate_date = None
    if rate_date is None:
        rate_date = date.today().isoformat()

    try:
        response = requests.get(
            f"{EXCHANGE_RATE_API_URL.rstrip('/')}/{rate_date}",
            params={"from": currency, "to": DEFAULT_BASE_CURRENCY},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        rates = data.get("rates", {})
        raw_rate = rates.get(DEFAULT_BASE_CURRENCY)
        if raw_rate is None:
            info.message = f"Kein Wechselkurs für {currency}->{DEFAULT_BASE_CURRENCY}"
            info.provider = EXCHANGE_RATE_API_URL
            info.rate_date = data.get("date", rate_date)
            return info
        rate = float(raw_rate)
        info.rate = rate
        info.rate_date = data.get("date", rate_date)
        info.provider = EXCHANGE_RATE_API_URL
        info.success = True
        if amount is not None:
            info.converted_amount = round(float(amount) * rate, 2)
        info.message = "ok"
        return info
    except requests.RequestException as exc:
        info.message = f"Wechselkursfehler: {exc}"
        info.provider = EXCHANGE_RATE_API_URL
        info.rate_date = rate_date
        return info


def compute_review_status(warnungen: List[str], fehler: List[str], confidence_score: Optional[float], is_storno: bool) -> ReviewStatus:
    if fehler:
        return "fehler"
    if is_storno:
        return "pruefen"
    if confidence_score is None:
        return "pruefen"
    if confidence_score >= 0.85 and not warnungen:
        return "ok"
    return "pruefen"


def save_result_to_db(result: ExtractionResult) -> None:
    if result.duplicate_info is None:
        return
    insert_beleg(
        {
            "belegdatum": result.belegdatum,
            "art": result.art_des_dokuments,
            "kosten": result.kosten_mit_steuern,
            "waehrung": result.waehrung_der_kosten,
            "fingerprint": result.duplicate_info.fingerprint,
            "duplicate_key": result.duplicate_info.duplicate_candidate_key,
        }
    )


def postprocess_result(
    parsed: Dict[str, Any],
    source_filename: Optional[str],
    raw_output: Optional[str],
    original_text: str,
    convert_to_base_currency: bool,
) -> ExtractionResult:
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
            if confidence >= 1.0:
                confidence = 0.93
                warnungen.append("confidence_score vom Modell wurde auf realistischen Wert begrenzt")
            confidence = max(0.0, min(0.98, confidence))
    except Exception:
        confidence = None
        warnungen.append("confidence_score war ungültig und wurde verworfen")

    kosten_value = ensure_string(parsed.get("kosten_mit_steuern"))
    waehrung_value = normalize_currency(ensure_string(parsed.get("waehrung_der_kosten")))

    numeric_amount = parse_decimal_maybe(kosten_value)
    if numeric_amount is not None:
        kosten_value = format_decimal(numeric_amount)
    if kosten_value != "nicht vorhanden" and waehrung_value != "nicht vorhanden" and waehrung_value not in kosten_value:
        kosten_value = f"{kosten_value} {waehrung_value}"

    result = ExtractionResult(
        belegdatum=ensure_string(parsed.get("belegdatum")),
        art_des_dokuments=art,
        buchungsnummer_code=ensure_string(parsed.get("buchungsnummer_code")),
        name_des_reisenden=ensure_string(parsed.get("name_des_reisenden")),
        wie_viele_reisesegmente=int(parsed.get("wie_viele_reisesegmente") or 0),
        ticketnummer=ensure_string(parsed.get("ticketnummer")),
        kosten_mit_steuern=kosten_value,
        waehrung_der_kosten=waehrung_value,
        reisesegmente=segments,
        confidence_score=confidence,
        warnungen=warnungen,
        fehler=fehler,
        raw_model_output=raw_output,
        source_filename=source_filename,
    )

    result.is_storno = detect_storno(original_text, result.warnungen, result.fehler)
    if result.is_storno:
        result.wie_viele_reisesegmente = 0
        result.reisesegmente = []
        if "Storno erkannt" not in result.warnungen:
            result.warnungen.append("Storno erkannt")

    if result.wie_viele_reisesegmente != len(result.reisesegmente):
        if not (result.is_storno and len(result.reisesegmente) == 0):
            result.warnungen.append(
                f"Segmentanzahl korrigiert: Feld={result.wie_viele_reisesegmente}, Liste={len(result.reisesegmente)}"
            )
            result.wie_viele_reisesegmente = len(result.reisesegmente)

    result.duplicate_info = build_duplicate_info(parsed, source_filename, original_text)

    if convert_to_base_currency:
        result.exchange_rate_info = maybe_fetch_exchange_rate(
            result.waehrung_der_kosten,
            result.belegdatum,
            parse_decimal_maybe(result.kosten_mit_steuern),
        )
        if (
            result.exchange_rate_info
            and not result.exchange_rate_info.success
            and result.waehrung_der_kosten not in {"nicht vorhanden", DEFAULT_BASE_CURRENCY}
        ):
            result.warnungen.append("Wechselkurs konnte nicht geladen werden")

    result.review_status = compute_review_status(result.warnungen, result.fehler, result.confidence_score, result.is_storno)
    return result


def analyze_document_text(document_text: str, filename: str, model: Optional[str], include_raw_output: bool, convert_to_base_currency: bool) -> ExtractionResult:
    cleaned = normalize_input_text(document_text)
    if not cleaned:
        raise HTTPException(status_code=400, detail="Kein verwertbarer Dokumentinhalt vorhanden.")

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": build_user_prompt(cleaned, filename)},
    ]

    raw_output = call_mistral(messages=messages, model=model)
    parsed = parse_model_json(raw_output)
    result = postprocess_result(
        parsed=parsed,
        source_filename=filename,
        raw_output=raw_output if include_raw_output else None,
        original_text=cleaned,
        convert_to_base_currency=convert_to_base_currency,
    )
    save_result_to_db(result)
    return result


@app.get("/", response_class=HTMLResponse)
def root() -> str:
    return f"""
    <html>
      <head><title>Reisekosten Mistral API</title></head>
      <body style="font-family: Arial, sans-serif; margin: 40px;">
        <h1>Reisekosten Mistral API</h1>
        <p>Status: läuft 🚀</p>
        <p>Version: {APP_VERSION}</p>
        <ul>
          <li>GET /health</li>
          <li>POST /analyze/text</li>
          <li>POST /analyze/file</li>
          <li>GET /prompt</li>
          <li>GET /belege</li>
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
        exchange_rates_enabled=ENABLE_EXCHANGE_RATES,
        base_currency=DEFAULT_BASE_CURRENCY,
    )


@app.post("/analyze/text", response_model=ExtractionResult)
def analyze_text(request: AnalyzeTextRequest) -> ExtractionResult:
    return analyze_document_text(
        document_text=request.text,
        filename=request.filename or "text-input.txt",
        model=request.model,
        include_raw_output=request.include_raw_output,
        convert_to_base_currency=request.convert_to_base_currency,
    )


@app.post("/analyze/file", response_model=ExtractionResult)
async def analyze_file(
    file: UploadFile = File(...),
    model: Optional[str] = Form(default=None),
    include_raw_output: bool = Form(default=False),
    convert_to_base_currency: bool = Form(default=True),
) -> ExtractionResult:
    filename = file.filename or "upload"
    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Datei ist leer.")

    lowered = filename.lower()
    if lowered.endswith(".pdf"):
        document_text = extract_text_from_pdf_bytes(content)
    elif lowered.endswith((".txt", ".eml", ".md")):
        document_text = content.decode("utf-8", errors="replace")
    else:
        document_text = content.decode("utf-8", errors="replace")

    return analyze_document_text(
        document_text=document_text,
        filename=filename,
        model=model,
        include_raw_output=include_raw_output,
        convert_to_base_currency=convert_to_base_currency,
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


@app.get("/belege")
def get_belege() -> JSONResponse:
    belege = list_belege()
    return JSONResponse({"count": len(belege), "belege": belege})


if __name__ == "__main__":
    import uvicorn

    uvicorn.run("main:app", host="0.0.0.0", port=10000, reload=True)