# VERSION 6.5b - Improved Extraction Quality

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

APP_VERSION = "6.5b"
DEFAULT_MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_BASE = os.getenv("MISTRAL_API_BASE", "https://api.mistral.ai/v1")
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT_SECONDS", "180"))

app = FastAPI(title="Reisekosten Mistral API", version=APP_VERSION)

DocumentType = Literal["Zug", "Flug", "Hotel", "Taxi", "Unbekannt"]

class Segment(BaseModel):
    index: int
    departure_datetime: str = "nicht vorhanden"
    arrival_datetime: str = "nicht vorhanden"
    departure_location: str = "nicht vorhanden"
    arrival_location: str = "nicht vorhanden"
    transport_company_and_number: str = "nicht vorhanden"

class ExtractionResult(BaseModel):
    belegdatum: str = "nicht vorhanden"
    art_des_dokuments: DocumentType = "Unbekannt"
    buchungsnummer_code: str = "nicht vorhanden"
    name_des_reisenden: str = "nicht vorhanden"
    wie_viele_reisesegmente: int = 0
    ticketnummer: str = "nicht vorhanden"
    kosten_mit_steuern: str = "nicht vorhanden"
    waehrung_der_kosten: str = "nicht vorhanden"
    reisesegmente: List[Segment] = []
    confidence_score: Optional[float] = None
    warnungen: List[str] = []
    fehler: List[str] = []
    version: str = APP_VERSION

# =========================
# IMPROVED PROMPT
# =========================

SYSTEM_PROMPT = """
Du bist ein präziser Parser für Reisekostenbelege.

NEUE REGELN (WICHTIG):
- Belegdatum PRIORITÄT:
  1. Ausstellungsdatum
  2. E-Mail Datum
  3. Buchungsdatum

- Kosten:
  IMMER Betrag + Währung kombinieren (z.B. 385.00 EUR)

- Confidence Score:
  REALISTISCH (0.6–0.95), NIEMALS pauschal 1.0

- Hotel:
  departure_location = Stadt oder Hotel
  arrival_location = gleich

- Taxi:
  immer Start/Ziel unterschiedlich

- Wenn unsicher → warnungen hinzufügen

Gib ausschließlich JSON zurück.
"""

USER_PROMPT_TEMPLATE = """
Extrahiere strukturierte Daten aus:

{document_text}
"""

# =========================
# HELPERS
# =========================

def extract_text_from_pdf_bytes(pdf_bytes: bytes) -> str:
    reader = PdfReader(BytesIO(pdf_bytes))
    return "\n".join([p.extract_text() or "" for p in reader.pages])


def call_mistral(messages: List[Dict[str, str]]) -> str:
    response = requests.post(
        f"{MISTRAL_API_BASE}/chat/completions",
        headers={"Authorization": f"Bearer {MISTRAL_API_KEY}"},
        json={
            "model": DEFAULT_MISTRAL_MODEL,
            "messages": messages,
            "temperature": 0,
            "response_format": {"type": "json_object"},
        },
        timeout=REQUEST_TIMEOUT,
    )
    return response.json()["choices"][0]["message"]["content"]


def postprocess(data: Dict[str, Any]) -> ExtractionResult:

    # Kosten + Währung kombinieren
    kosten = data.get("kosten_mit_steuern", "nicht vorhanden")
    waehrung = data.get("waehrung_der_kosten", "")

    if kosten and waehrung and waehrung not in kosten:
        kosten = f"{kosten} {waehrung}"

    # Confidence fix
    confidence = data.get("confidence_score", 0.8)
    if confidence == 1:
        confidence = 0.9

    return ExtractionResult(
        belegdatum=data.get("belegdatum", "nicht vorhanden"),
        art_des_dokuments=data.get("art_des_dokuments", "Unbekannt"),
        buchungsnummer_code=data.get("buchungsnummer_code", "nicht vorhanden"),
        name_des_reisenden=data.get("name_des_reisenden", "nicht vorhanden"),
        wie_viele_reisesegmente=data.get("wie_viele_reisesegmente", 0),
        ticketnummer=data.get("ticketnummer", "nicht vorhanden"),
        kosten_mit_steuern=kosten,
        waehrung_der_kosten=waehrung,
        reisesegmente=data.get("reisesegmente", []),
        confidence_score=confidence,
        warnungen=data.get("warnungen", []),
        fehler=data.get("fehler", []),
    )

# =========================
# ROUTES
# =========================

@app.get("/")
def root():
    return {"status": "läuft", "version": APP_VERSION}

@app.post("/analyze/text")
def analyze_text(text: str):
    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": USER_PROMPT_TEMPLATE.format(document_text=text)},
    ]
    raw = call_mistral(messages)
    parsed = json.loads(raw)
    return postprocess(parsed)

@app.post("/analyze/file")
async def analyze_file(file: UploadFile = File(...)):
    content = await file.read()
    text = extract_text_from_pdf_bytes(content)
    return analyze_text(text)
