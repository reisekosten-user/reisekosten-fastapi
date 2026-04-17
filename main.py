from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pypdf import PdfReader

from database import check_duplicate, db_ping, init_db, insert_beleg, list_belege

APP_VERSION = "7.1"
DEFAULT_MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_BASE = "https://api.mistral.ai/v1"

app = FastAPI(title="Reisekosten API", version=APP_VERSION)

init_db()


class AnalyzeTextRequest(BaseModel):
    text: str
    filename: Optional[str] = None


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        text_parts = []
        for page in reader.pages:
            text_parts.append(page.extract_text() or "")
        return "\n".join(text_parts).strip()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"PDF konnte nicht gelesen werden: {exc}") from exc


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
- Extrahiere wenn möglich:
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
            {
                "role": "system",
                "content": "Gib ausschließlich valides JSON zurück. Keine Erklärungen."
            },
            {
                "role": "user",
                "content": prompt
            }
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
    prompt = build_prompt(text, filename=filename)
    raw = call_mistral(prompt)
    data = ensure_defaults(raw)

    dup = build_duplicate_info(data, text)
    data["duplicate_info"] = dup
    data["version"] = APP_VERSION
    data["generated_at_utc"] = datetime.utcnow().isoformat()

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
        return {
            "count": len(list_belege()),
            "belege": list_belege(),
        }
    except Exception as exc:
        return {
            "status": "error",
            "detail": f"/belege Fehler: {exc}",
            "database_url_present": bool(os.getenv("DATABASE_URL", "").strip()),
        }


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