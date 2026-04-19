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
from openai import OpenAI
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

APP_VERSION = "7.9e"

AI_PROVIDER = os.getenv("AI_PROVIDER", "mistral").strip().lower()

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
MISTRAL_API_BASE = "https://api.mistral.ai/v1"

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4")

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
    ai_provider: Optional[str] = None
    ai_model: Optional[str] = None


class MitarbeiterCreateRequest(BaseModel):
    kuerzel: str
    vorname: str
    nachname: str
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


def normalize_variants(value: str) -> List[str]:
    if not value:
        return []
    base = value.strip().lower()
    variants = {base}
    variants.add(base.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss"))
    variants.add(base.replace("ä", "a").replace("ö", "o").replace("ü", "u").replace("ß", "ss"))
    variants.add(base.replace("ae", "a").replace("oe", "o").replace("ue", "u"))
    return [v for v in variants if v]


def anonymize_emails(text: str) -> str:
    return re.sub(
        r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b",
        "ff@bb.com",
        text,
        flags=re.IGNORECASE,
    )


def build_name_candidates_for_reise(reise_id: Optional[int]) -> List[str]:
    if not reise_id:
        return []

    try:
        detail = get_reise_detail(reise_id)
    except Exception:
        return []

    candidates: set[str] = set()
    for reisender in detail.get("reisende", []):
        vollname = (reisender.get("vollname") or "").strip()
        if vollname:
            candidates.add(vollname)

            parts = [p for p in vollname.split() if p.strip()]
            if len(parts) >= 2:
                first = parts[0]
                last = parts[-1]
                candidates.add(f"{first} {last}".strip())
                candidates.add(f"{last} {first}".strip())
                candidates.add(first)
                candidates.add(last)

    return sorted(candidates, key=len, reverse=True)


def replace_reise_names(text: str, reise_id: Optional[int]) -> str:
    anonymized = text
    candidates = build_name_candidates_for_reise(reise_id)

    if not candidates:
        return anonymized

    full_names = [c for c in candidates if " " in c.strip()]
    single_names = [c for c in candidates if " " not in c.strip()]

    # 1. Ganze Namen zuerst
    for candidate in full_names:
        for variant in normalize_variants(candidate):
            anonymized = re.sub(re.escape(variant), "Max Mustermann", anonymized, flags=re.IGNORECASE)

    # 2. Doppelte Ersetzungen glätten
    anonymized = re.sub(r"(?i)\bMax Mustermann(?:\s+Max Mustermann)+", "Max Mustermann", anonymized)

    # 3. Einzelne Namen nur vorsichtig ersetzen
    for candidate in single_names:
        for variant in normalize_variants(candidate):
            anonymized = re.sub(
                rf"(?i)(?<!Max\s)(?<!Mustermann\s)\b{re.escape(variant)}\b(?!\s+Mustermann)",
                "Max Mustermann",
                anonymized,
            )

    # 4. Nochmals glätten
    anonymized = re.sub(r"(?i)\bMax Mustermann(?:\s+Max Mustermann)+", "Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+Max Mustermann(?:\s+Max Mustermann)+", r"\1 Max Mustermann", anonymized)

    return anonymized


def resolve_reise_id(reise_id: Optional[int], event_id: Optional[int]) -> Optional[int]:
    if reise_id:
        return reise_id
    if not event_id:
        return None
    try:
        event_detail = get_event_detail(event_id)
        event = event_detail.get("event") or {}
        return event.get("reise_id")
    except Exception:
        return None


def anonymize_document_text(text: str, reise_id: Optional[int] = None, event_id: Optional[int] = None) -> str:
    effective_reise_id = resolve_reise_id(reise_id, event_id)
    anonymized = text
    anonymized = replace_reise_names(anonymized, effective_reise_id)
    anonymized = anonymize_emails(anonymized)
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


def call_mistral(prompt: str, model: Optional[str] = None) -> dict:
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

    use_model = model or MISTRAL_MODEL

    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }

    payload = {
        "model": use_model,
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


def call_openai(prompt: str, model: Optional[str] = None) -> dict:
    if not OPENAI_API_KEY:
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
            "warnungen": ["OPENAI_API_KEY ist nicht gesetzt"],
            "fehler": [],
        }

    use_model = model or OPENAI_MODEL
    client = OpenAI(api_key=OPENAI_API_KEY)

    response = client.responses.create(
        model=use_model,
        input=prompt,
    )

    output_text = response.output_text
    return json.loads(output_text)


def call_ai_provider(prompt: str, provider_override: Optional[str] = None, model_override: Optional[str] = None) -> tuple[dict, str, str]:
    provider = (provider_override or AI_PROVIDER).strip().lower()

    if provider == "openai":
        model = model_override or OPENAI_MODEL
        return call_openai(prompt, model), provider, model

    if provider == "mistral":
        model = model_override or MISTRAL_MODEL
        return call_mistral(prompt, model), provider, model

    return ({
        "belegdatum": "nicht vorhanden",
        "art_des_dokuments": "Unbekannt",
        "buchungsnummer_code": "nicht vorhanden",
        "name_des_reisenden": "nicht vorhanden",
        "wie_viele_reisesegmente": 0,
        "ticketnummer": "nicht vorhanden",
        "kosten_mit_steuern": "nicht vorhanden",
        "waehrung_der_kosten": "nicht vorhanden",
        "reisesegmente": [],
        "warnungen": [f"Unbekannter AI Provider: {provider}"],
        "fehler": [],
    }, provider, model_override or "")


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
    event_id: Optional[int] = None,
    ai_provider_override: Optional[str] = None,
    ai_model_override: Optional[str] = None,
) -> dict:
    anonymized_text = anonymize_document_text(text, reise_id=reise_id, event_id=event_id)
    prompt = build_prompt(anonymized_text, filename=filename)
    raw, used_provider, used_model = call_ai_provider(prompt, ai_provider_override, ai_model_override)
    data = ensure_defaults(raw)

    data["version"] = APP_VERSION
    data["generated_at_utc"] = datetime.utcnow().isoformat()
    data["ai_provider"] = used_provider
    data["ai_model"] = used_model
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
        "ai_provider": AI_PROVIDER,
        "mistral_configured": bool(MISTRAL_API_KEY),
        "openai_configured": bool(OPENAI_API_KEY),
        "openai_model": OPENAI_MODEL,
        "mistral_model": MISTRAL_MODEL,
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
        anonymized = anonymize_document_text(payload.text, reise_id=payload.reise_id, event_id=payload.event_id)
        return {
            "status": "ok",
            "filename": payload.filename or "text-input.txt",
            "anonymized_text": anonymized,
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/anonymize/file")
async def anonymize_file(
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

        anonymized = anonymize_document_text(text, reise_id=reise_id, event_id=event_id)
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
            payload.ai_provider,
            payload.ai_model,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/analyze/file")
async def analyze_file(
    file: UploadFile = File(...),
    reise_id: Optional[int] = Query(default=None),
    event_id: Optional[int] = Query(default=None),
    ai_provider: Optional[str] = Query(default=None),
    ai_model: Optional[str] = Query(default=None),
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
            ai_provider,
            ai_model,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc