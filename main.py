from __future__ import annotations

import json
import os
import re
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

import requests
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from pypdf import PdfReader

from database import (
    attach_beleg_to_event,
    create_event,
    create_mitarbeiter,
    create_reise,
    db_ping,
    get_conn,
    get_event_detail,
    get_next_reise_code,
    get_reise_detail,
    init_db,
    insert_beleg,
    list_belege,
    list_mitarbeiter,
    list_reisen,
    search_mitarbeiter,
    update_event_status,
    update_mitarbeiter,
)

APP_VERSION = "7.8"
DEFAULT_MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_API_BASE = "https://api.mistral.ai/v1"

app = FastAPI(title="Reisekosten API", version=APP_VERSION)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def startup():
    init_db()


class AnalyzeTextRequest(BaseModel):
    text: str
    filename: Optional[str] = None
    reise_id: Optional[int] = None
    event_id: Optional[int] = None


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


class EventCreateRequest(BaseModel):
    reise_id: int
    typ: str
    titel: str
    status: str = "planung"


class EventStatusRequest(BaseModel):
    status: str


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
            anonymized = re.sub(re.escape(variant), "Max Mustermann", anonymized, flags=re.IGNORECASE)

    anonymized = re.sub(
        r"(?i)\b(geburtsdatum|birth date|date of birth|dob)\b\s*[:\-]?\s*([0-9]{1,2}[./\-][0-9]{1,2}[./\-][0-9]{2,4})",
        r"\1: XXXX",
        anonymized,
    )

    anonymized = re.sub(
        r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b",
        "XXXX",
        anonymized,
        flags=re.IGNORECASE,
    )

    anonymized = re.sub(r"(?i)\b(\+?\d[\d\s\-()/]{6,}\d)\b", "XXXX", anonymized)

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
        return {
            "belegdatum": "nicht vorhanden",
            "art_des_dokuments": "Unbekannt",
            "buchungsnummer_code": "nicht vorhanden",
            "name_des_reisenden": "nicht vorhanden",
            "wie_viele_reisesegmente": 0,
            "ticketnummer": "nicht vorhanden",
            "kosten_mit_steuern": "nicht vorhanden",
            "waehrung_der_kosten": "nicht vorhanden",
            "reisesegmente": [],
            "warnungen": ["MISTRAL_API_KEY ist nicht gesetzt"],
            "fehler": [],
        }

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

    response = requests.post(
        f"{MISTRAL_API_BASE}/chat/completions",
        json=payload,
        headers=headers,
        timeout=180,
    )
    response.raise_for_status()
    data = response.json()
    content = data["choices"][0]["message"]["content"]
    return json.loads(content)


def ensure_defaults(data: Dict[str, Any]) -> Dict[str, Any]:
    return {
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


def compute_reise_status(detail: dict) -> dict:
    events = detail.get("events", [])
    reisende = detail.get("reisende", [])

    typen = [e.get("typ") for e in events]
    warnungen = []
    status = "ok"

    if len(reisende) == 0:
        warnungen.append("Keine Reisenden zugeordnet")
        status = "fehler"

    if "Hotel" not in typen:
        warnungen.append("Hotel fehlt")
        if status != "fehler":
            status = "pruefen"

    if len(events) == 0:
        warnungen.append("Keine Events vorhanden")
        status = "fehler"

    taxi_count = sum(1 for e in events if e.get("typ") == "Taxi")
    if taxi_count > 1:
        warnungen.append("Mehrere Taxi-Events prüfen")
        if status == "ok":
            status = "pruefen"

    return {
        "status": status,
        "warnungen": warnungen,
        "flug": "Flug" in typen,
        "hotel": "Hotel" in typen,
        "taxi": "Taxi" in typen,
    }


def auto_create_event_for_analysis(reise_id: int, beleg_id: int, data: dict):
    art = data.get("art_des_dokuments", "Unbekannt")
    titel = art if art != "Unbekannt" else "Beleg"
    event_id = create_event({
        "reise_id": reise_id,
        "typ": art,
        "titel": titel,
        "status": "abgeschlossen",
    })
    attach_beleg_to_event(event_id, beleg_id)
    return event_id


def analyze_text_internal(
    text: str,
    filename: str = "nicht vorhanden",
    reise_id: Optional[int] = None,
    event_id: Optional[int] = None
) -> dict:
    anonymized_text = anonymize_document_text(text)
    prompt = build_prompt(anonymized_text, filename=filename)
    raw = call_mistral(prompt)
    data = ensure_defaults(raw)

    data["version"] = APP_VERSION
    data["generated_at_utc"] = datetime.utcnow().isoformat()
    data["anonymized_preview"] = anonymized_text[:4000]

    beleg_id = insert_beleg({
        "belegdatum": data.get("belegdatum"),
        "art": data.get("art_des_dokuments"),
        "kosten": data.get("kosten_mit_steuern"),
        "waehrung": data.get("waehrung_der_kosten"),
    })

    data["beleg_id"] = beleg_id

    if event_id:
        attach_beleg_to_event(event_id, beleg_id)
        update_event_status(event_id, "abgeschlossen")
        data["attached_event_id"] = event_id
    elif reise_id:
        new_event_id = auto_create_event_for_analysis(reise_id, beleg_id, data)
        data["created_event_id"] = new_event_id

    return data


@app.get("/")
def root():
    return {"status": "ok", "version": APP_VERSION}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "mistral_configured": bool(MISTRAL_API_KEY),
    }


@app.post("/admin/reset-db")
def reset_db():
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("DROP TABLE IF EXISTS event_belege CASCADE")
                cur.execute("DROP TABLE IF EXISTS events CASCADE")
                cur.execute("DROP TABLE IF EXISTS reise_reisende CASCADE")
                cur.execute("DROP TABLE IF EXISTS belege CASCADE")
                cur.execute("DROP TABLE IF EXISTS mitarbeiter CASCADE")
                cur.execute("DROP TABLE IF EXISTS reisen CASCADE")
            conn.commit()

        init_db()
        return {"status": "ok", "message": "DB reset done"}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.get("/db-test")
def db_test():
    try:
        return db_ping()
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.get("/dashboard")
def dashboard():
    return FileResponse("templates/dashboard.html")


@app.get("/belege")
def belege():
    try:
        items = list_belege()
        return {"count": len(items), "belege": items}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


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
        data = payload.model_dump()
        if not data.get("klarname"):
            data["klarname"] = f'{data.get("vorname", "")} {data.get("nachname", "")}'.strip()
        new_id = create_mitarbeiter(data)
        return {"status": "ok", "id": new_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.put("/mitarbeiter/{mitarbeiter_id}")
def mitarbeiter_update(mitarbeiter_id: int, payload: MitarbeiterCreateRequest):
    try:
        data = payload.model_dump()
        if not data.get("klarname"):
            data["klarname"] = f'{data.get("vorname", "")} {data.get("nachname", "")}'.strip()
        update_mitarbeiter(mitarbeiter_id, data)
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


@app.get("/reisen/overview")
def reisen_overview():
    try:
        items = list_reisen()
        enriched = []
        for r in items:
            detail = get_reise_detail(r["id"])
            meta = compute_reise_status(detail)
            enriched.append({**r, **meta})
        return {"count": len(enriched), "reisen": enriched}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.get("/reisen/{reise_id}")
def reisen_detail(reise_id: int):
    try:
        detail = get_reise_detail(reise_id)
        meta = compute_reise_status(detail)
        return {**detail, **meta}
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


@app.post("/events")
def events_create(payload: EventCreateRequest):
    try:
        event_id = create_event(payload.model_dump())
        return {"status": "ok", "event_id": event_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/events/{event_id}")
def events_detail(event_id: int):
    try:
        return get_event_detail(event_id)
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.put("/events/{event_id}/status")
def events_update_status(event_id: int, payload: EventStatusRequest):
    try:
        update_event_status(event_id, payload.status)
        return {"status": "ok", "event_id": event_id, "new_status": payload.status}
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
        return analyze_text_internal(
            payload.text,
            payload.filename or "text-input.txt",
            payload.reise_id,
            payload.event_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/analyze/file")
async def analyze_file(
    file: UploadFile = File(...),
    reise_id: Optional[int] = Query(default=None),
    event_id: Optional[int] = Query(default=None),
):
    try:
        content = await file.read()

        if (file.filename or "").lower().endswith(".pdf"):
            text = extract_text_from_pdf(content)
        else:
            text = content.decode("utf-8", errors="replace")

        return analyze_text_internal(
            text,
            file.filename or "upload",
            reise_id,
            event_id,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc