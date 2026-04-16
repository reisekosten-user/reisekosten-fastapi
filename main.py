# --- main.py ---
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
from fastapi.responses import HTMLResponse, FileResponse
from pydantic import BaseModel, Field
from pypdf import PdfReader

from database import check_duplicate, db_ping, init_db, insert_beleg, list_belege

APP_VERSION = "7.0b"

DEFAULT_MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_BASE = "https://api.mistral.ai/v1"

app = FastAPI()
init_db()

# ---------- MODELS ----------
class Segment(BaseModel):
    index: int
    departure_datetime: str = "nicht vorhanden"
    arrival_datetime: str = "nicht vorhanden"
    departure_location: str = "nicht vorhanden"
    arrival_location: str = "nicht vorhanden"
    transport_company_and_number: str = "nicht vorhanden"


class ExtractionResult(BaseModel):
    belegdatum: str = "nicht vorhanden"
    art_des_dokuments: str = "Unbekannt"
    buchungsnummer_code: str = "nicht vorhanden"
    name_des_reisenden: str = "nicht vorhanden"
    wie_viele_reisesegmente: int = 0
    ticketnummer: str = "nicht vorhanden"
    kosten_mit_steuern: str = "nicht vorhanden"
    waehrung_der_kosten: str = "nicht vorhanden"
    reisesegmente: List[Segment] = []
    warnungen: List[str] = []
    fehler: List[str] = []
    duplicate_info: Optional[dict] = None


# ---------- HELPERS ----------
def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    reader = PdfReader(BytesIO(pdf_bytes))
    text = ""
    for p in reader.pages:
        text += p.extract_text() or ""
    return text


def call_mistral(text: str) -> dict:
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": DEFAULT_MISTRAL_MODEL,
        "messages": [
            {"role": "system", "content": "Extrahiere strukturierte Reisekosten-Daten als JSON."},
            {"role": "user", "content": text[:8000]},
        ],
        "temperature": 0,
    }

    r = requests.post(f"{MISTRAL_API_BASE}/chat/completions", json=payload, headers=headers)

    data = r.json()
    content = data["choices"][0]["message"]["content"]

    try:
        return json.loads(content)
    except:
        return {"fehler": ["kein valides JSON vom Modell"]}


def build_duplicate(data: dict, text: str):
    key = f"{data.get('belegdatum')}|{data.get('kosten_mit_steuern')}"
    dup = hashlib.sha256(key.encode()).hexdigest()[:20]
    fingerprint = hashlib.sha256(text.encode()).hexdigest()[:20]

    return {
        "fingerprint": fingerprint,
        "duplicate_candidate_key": dup,
        "is_duplicate": check_duplicate(dup),
    }


# ---------- ROUTES ----------
@app.get("/")
def root():
    return {"status": "ok", "version": APP_VERSION}


@app.get("/dashboard")
def dashboard():
    return FileResponse("templates/dashboard.html")


@app.get("/db-test")
def db_test():
    try:
        row = db_ping()
        return row
    except Exception as e:
        return {"error": str(e)}


@app.get("/belege")
def belege():
    try:
        return {"belege": list_belege()}
    except Exception as e:
        return {"error": str(e)}


@app.post("/analyze/text")
def analyze_text(payload: dict):
    text = payload.get("text", "")

    data = call_mistral(text)
    dup = build_duplicate(data, text)

    insert_beleg({
        "belegdatum": data.get("belegdatum"),
        "art": data.get("art_des_dokuments"),
        "kosten": data.get("kosten_mit_steuern"),
        "waehrung": data.get("waehrung_der_kosten"),
        "fingerprint": dup["fingerprint"],
        "duplicate_key": dup["duplicate_candidate_key"],
    })

    data["duplicate_info"] = dup
    return data


@app.post("/analyze/file")
async def analyze_file(file: UploadFile = File(...)):
    content = await file.read()

    if file.filename.endswith(".pdf"):
        text = extract_text_from_pdf(content)
    else:
        text = content.decode()

    data = call_mistral(text)
    dup = build_duplicate(data, text)

    insert_beleg({
        "belegdatum": data.get("belegdatum"),
        "art": data.get("art_des_dokuments"),
        "kosten": data.get("kosten_mit_steuern"),
        "waehrung": data.get("waehrung_der_kosten"),
        "fingerprint": dup["fingerprint"],
        "duplicate_key": dup["duplicate_candidate_key"],
    })

    data["duplicate_info"] = dup
    return data