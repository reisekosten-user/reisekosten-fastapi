from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pypdf import PdfReader

from database import (
    check_duplicate,
    create_mitarbeiter,
    create_reise,
    db_ping,
    get_next_reise_code,
    init_db,
    insert_beleg,
    list_belege,
    list_mitarbeiter,
    list_reisen,
    search_mitarbeiter,
    update_mitarbeiter,
)

APP_VERSION = "7.2b"
DEFAULT_MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_BASE = "https://api.mistral.ai/v1"

app = FastAPI(title="Reisekosten API", version=APP_VERSION)
init_db()


class AnalyzeTextRequest(BaseModel):
    text: str
    filename: Optional[str] = None


class MitarbeiterCreateRequest(BaseModel):
    kuerzel: str
    vorname: str
    nachname: str
    klarname: Optional[str] = None
    geburtsdatum: Optional[str] = None
    email: Optional[str] = None
    aktiv: bool = True


class ReiseCreateRequest(BaseModel):
    reise_jahr: int
    reise_name: str
    startdatum: Optional[str] = None
    enddatum: Optional[str] = None
    anzahl_reisende: int = 1
    mitarbeiter_ids: List[int] = []


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text_parts = []
        for page in reader.pages:
            text_parts.append(page.extract_text() or "")
        return "\n".join(text_parts).strip()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"PDF konnte nicht gelesen werden: {exc}") from exc


def normalize_variants(name: str) -> List[str]:
    if not name:
        return []
    base = name.strip().lower()
    variants = {base}
    variants.add(base.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss"))
    variants.add(base.replace("ä", "a").replace("ö", "o").replace("ü", "u").replace("ß", "ss"))
    variants.add(base.replace("ae", "a").replace("oe", "o").replace("ue", "u"))
    return [v for v in variants if v]


def mask_field_values(text: str, patterns: List[re.Pattern]) -> str:
    out = text
    for pattern in patterns:
        out = pattern.sub(lambda m: f"{m.group(1)}XXXX", out)
    return out


def anonymize_document_text(text: str) -> str:
    anonymized = text

    # Namen aus Mitarbeiterdatenbank durch Max Mustermann ersetzen
    try:
        mitarbeiter = list_mitarbeiter(limit=1000)
    except Exception:
        mitarbeiter = []

    name_candidates = set()
    for m in mitarbeiter:
        for candidate in [
            m.get("klarname"),
            f"{m.get('vorname', '')} {m.get('nachname', '')}".strip(),
            f"{m.get('nachname', '')} {m.get('vorname', '')}".strip(),
            m.get("vorname"),
            m.get("nachname"),
        ]:
            if candidate and candidate.strip():
                name_candidates.add(candidate.strip())

    sorted_names = sorted(name_candidates, key=len, reverse=True)
    for name in sorted_names:
        for variant in normalize_variants(name):
            pattern = re.compile(re.escape(variant), re.IGNORECASE)
            anonymized = pattern.sub("Max Mustermann", anonymized)

    # Geburtsdaten / Birth / DOB
    anonymized = re.sub(
        r"(?i)\b(geburtsdatum|birth date|date of birth|dob)\b\s*[:\-]?\s*([0-9]{1,2}[./\-][0-9]{1,2}[./\-][0-9]{2,4})",
        r"\1: XXXX",
        anonymized,
    )

    # E-Mail
    anonymized = re.sub(
        r"(?i)\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b",
        "XXXX",
        anonymized,
        flags=re.IGNORECASE,
    )

    # Telefon
    anonymized = re.sub(
        r"(?i)\b(\+?\d[\d\s\-()/]{6,}\d)\b",
        "XXXX",
        anonymized,
    )

    # Feste Label-Felder auf XXXX
    patterns = [
        re.compile(r"(?i)\b(booking code\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(booking reference\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(buchungsnummer\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(buchungsreferenz\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(confirmation number\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(reservation number\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(ticket number\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(ticketnummer\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(ticketnummer\s*[:\-]?\s*)[\d\- ]+"),
        re.compile(r"(?i)\b(pnr\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(record locator\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(confirmation no\.?\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(reservation no\.?\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(frequent flyer number\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
        re.compile(r"(?i)\b(vielfliegernummer\s*[:\-]?\s*)[A-Z0-9\-\/]+"),
    ]
    anonymized = mask_field_values(anonymized, patterns)

    return anonymized


def build_prompt(document_text: str, filename: str = "nicht vorhanden") -> str:
    return f"""
Du bist ein hochpräziser Parser für Reisekostenbelege.

Lies den folgenden Dokumentinhalt und gib AUSSCHLIESSLICH valides JSON zurück.

Regeln:
- Keine Werte erfinden
- Wenn nicht vorhanden: "nicht vorhanden"
- Dokumenttypen nur: Zug, Flug, Hotel, Taxi, Unbekannt
- Bei Hotel keine Standarduhrzeiten erfinden
- Bei Flug alle Segmente einzeln
- Bei Taxi normalerweise 1 Segment, bei Storno 0
- Extrahiere:
  belegdatum
  art_des_dokuments
  buchungsnummer_code
  name_des_reisenden
  wie_viele_reisesegmente
  ticketnummer
  kosten_mit_steuern
  waehrung_der_kosten
  reisesegmente[] mit:
    index
    departure_datetime
    arrival_datetime
    departure_location
    arrival_location
    transport_company_and_number
  warnungen[]
  fehler[]

Dateiname: {filename}

Dokumentinhalt:
{document_text[:120000]}
""".strip()


def call_mistral(prompt: str) -> dict:
    if not MISTRAL_API_KEY:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY ist nicht gesetzt.")

    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": DEFAULT_MISTRAL_MODEL,
        "messages": [
            {"role": "system", "content": "Gib ausschließlich valides JSON zurück. Keine Erklärungen."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
        "response_format": {"type": "json_object"},
    }

    try:
        response = requests.post(
            f"{MISTRAL_API_BASE}/chat/completions",
            json=payload,
            headers=headers,
            timeout=180,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        raise HTTPException(status_code=502, detail=f"Mistral API Fehler: {exc}") from exc

    data = response.json()

    try:
        content = data["choices"][0]["message"]["content"]
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Ungültige Mistral Antwort: {data}") from exc

    try:
        return json.loads(content)
    except Exception:
        match = re.search(r"\{.*\}", content, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except Exception:
                pass
        raise HTTPException(status_code=502, detail=f"Modell lieferte kein valides JSON: {content[:500]}")


def ensure_defaults(data: Dict[str, Any]) -> Dict[str, Any]:
    result = {
        "belegdatum": data.get("belegdatum", "nicht vorhanden"),
        "art_des_dokuments": data.get("art_des_dokuments", "Unbekannt"),
        "buchungsnummer_code": data.get("buchungsnummer_code", "nicht vorhanden"),
        "name_des_reisenden": data.get("name_des_reisenden", "nicht vorhanden"),
        "wie_viele_reisesegmente": data.get("wie_viele_reisesegmente", 0),
        "ticketnummer": data.get("ticketnummer", "nicht vorhanden"),
        "kosten_mit_steuern": data.get("kosten_mit_steuern", "nicht vorhanden"),
        "waehrung_der_kosten": data.get("waehrung_der_kosten", "nicht vorhanden"),
        "reisesegmente": data.get("reisesegmente", []),
        "warnungen": data.get("warnungen", []),
        "fehler": data.get("fehler", []),
    }

    if not isinstance(result["reisesegmente"], list):
        result["reisesegmente"] = []

    clean_segments = []
    for i, seg in enumerate(result["reisesegmente"], start=1):
        if not isinstance(seg, dict):
            continue
        clean_segments.append({
            "index": seg.get("index", i),
            "departure_datetime": seg.get("departure_datetime", "nicht vorhanden"),
            "arrival_datetime": seg.get("arrival_datetime", "nicht vorhanden"),
            "departure_location": seg.get("departure_location", "nicht vorhanden"),
            "arrival_location": seg.get("arrival_location", "nicht vorhanden"),
            "transport_company_and_number": seg.get("transport_company_and_number", "nicht vorhanden"),
        })

    result["reisesegmente"] = clean_segments

    try:
        result["wie_viele_reisesegmente"] = int(result["wie_viele_reisesegmente"])
    except Exception:
        result["wie_viele_reisesegmente"] = len(clean_segments)

    if result["wie_viele_reisesegmente"] != len(clean_segments):
        result["wie_viele_reisesegmente"] = len(clean_segments)

    return result


def build_duplicate_info(data: Dict[str, Any], original_text: str) -> dict:
    duplicate_key_source = "|".join([
        str(data.get("belegdatum", "")),
        str(data.get("art_des_dokuments", "")),
        str(data.get("kosten_mit_steuern", "")),
        str(data.get("waehrung_der_kosten", "")),
        str(data.get("buchungsnummer_code", "")),
        str(data.get("ticketnummer", "")),
        str(data.get("name_des_reisenden", "")),
    ])

    duplicate_candidate_key = hashlib.sha256(duplicate_key_source.encode("utf-8")).hexdigest()[:20]
    fingerprint = hashlib.sha256(original_text.encode("utf-8")).hexdigest()[:20]

    return {
        "fingerprint": fingerprint,
        "duplicate_candidate_key": duplicate_candidate_key,
        "is_duplicate": check_duplicate(duplicate_candidate_key),
    }


def analyze_text_internal(text: str, filename: str = "nicht vorhanden") -> dict:
    anonymized_text = anonymize_document_text(text)
    prompt = build_prompt(anonymized_text, filename=filename)
    raw = call_mistral(prompt)
    data = ensure_defaults(raw)

    dup = build_duplicate_info(data, anonymized_text)
    data["duplicate_info"] = dup
    data["version"] = APP_VERSION
    data["generated_at_utc"] = datetime.utcnow().isoformat()
    data["anonymized_preview"] = anonymized_text[:4000]

    insert_beleg({
        "belegdatum": data.get("belegdatum"),
        "art": data.get("art_des_dokuments"),
        "kosten": data.get("kosten_mit_steuern"),
        "waehrung": data.get("waehrung_der_kosten"),
        "fingerprint": dup["fingerprint"],
        "duplicate_key": dup["duplicate_candidate_key"],
    })

    return data


@app.get("/")
def root():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "routes": [
            "/health",
            "/db-test",
            "/dashboard",
            "/belege",
            "/mitarbeiter",
            "/mitarbeiter/suche?q=...",
            "/reisen",
            "/reisen/next-code?jahr=2027",
            "/anonymize/text",
            "/anonymize/file",
            "/analyze/text",
            "/analyze/file",
        ],
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "mistral_configured": bool(MISTRAL_API_KEY),
    }


@app.get("/db-test")
def db_test():
    try:
        return db_ping()
    except Exception as exc:
        return {
            "status": "error",
            "detail": str(exc),
            "database_url_present": bool(os.getenv("DATABASE_URL", "").strip()),
        }


@app.get("/dashboard")
def dashboard():
    return FileResponse("templates/dashboard.html")


@app.get("/belege")
def belege():
    try:
        items = list_belege()
        return {"count": len(items), "belege": items}
    except Exception as exc:
        return {"status": "error", "detail": f"/belege Fehler: {exc}"}


@app.get("/mitarbeiter")
def mitarbeiter_list():
    try:
        items = list_mitarbeiter()
        return {"count": len(items), "mitarbeiter": items}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.get("/mitarbeiter/suche")
def mitarbeiter_suche(q: str = Query(default="")):
    try:
        items = search_mitarbeiter(q)
        return {"count": len(items), "mitarbeiter": items}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.post("/mitarbeiter")
def mitarbeiter_create(payload: MitarbeiterCreateRequest):
    try:
        new_id = create_mitarbeiter(payload.model_dump())
        return {"status": "ok", "id": new_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.put("/mitarbeiter/{mitarbeiter_id}")
def mitarbeiter_update(mitarbeiter_id: int, payload: MitarbeiterCreateRequest):
    try:
        update_mitarbeiter(mitarbeiter_id, payload.model_dump())
        return {"status": "ok", "id": mitarbeiter_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/reisen")
def reisen_list():
    try:
        items = list_reisen()
        return {"count": len(items), "reisen": items}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.get("/reisen/next-code")
def reisen_next_code(jahr: int = Query(...)):
    try:
        return get_next_reise_code(jahr)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/reisen")
def reisen_create(payload: ReiseCreateRequest):
    try:
        result = create_reise(payload.model_dump())
        return {"status": "ok", **result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/anonymize/text")
def anonymize_text(payload: AnalyzeTextRequest):
    try:
        anonymized = anonymize_document_text(payload.text)
        return {
            "status": "ok",
            "filename": payload.filename or "text-input.txt",
            "anonymized_text": anonymized,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/anonymize/file")
async def anonymize_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        if (file.filename or "").lower().endswith(".pdf"):
            text = extract_text_from_pdf(content)
        else:
            text = content.decode("utf-8", errors="replace")

        anonymized = anonymize_document_text(text)
        return {
            "status": "ok",
            "filename": file.filename or "upload",
            "anonymized_text": anonymized,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/analyze/text")
def analyze_text(payload: AnalyzeTextRequest):
    try:
        return analyze_text_internal(payload.text, payload.filename or "text-input.txt")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/analyze/file")
async def analyze_file(file: UploadFile = File(...)):
    try:
        content = await file.read()

        if (file.filename or "").lower().endswith(".pdf"):
            text = extract_text_from_pdf(content)
        else:
            text = content.decode("utf-8", errors="replace")

        return analyze_text_internal(text, file.filename or "upload")
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc