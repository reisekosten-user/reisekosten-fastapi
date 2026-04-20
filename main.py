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

APP_VERSION = "7.11"

AI_PROVIDER = os.getenv("AI_PROVIDER", "openai").strip().lower()

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
        "abc@123.com",
        text,
        flags=re.IGNORECASE,
    )


def anonymize_employee_names(text: str) -> str:
    anonymized = text
    mitarbeiter = list_mitarbeiter(limit=5000)

    all_names: List[str] = []

    for m in mitarbeiter:
        vorname = (m.get("vorname") or "").strip()
        nachname = (m.get("nachname") or "").strip()
        if vorname and nachname:
            all_names.append(f"{vorname} {nachname}")
            all_names.append(f"{nachname} {vorname}")
        if vorname:
            all_names.append(vorname)
        if nachname:
            all_names.append(nachname)

    for candidate in sorted(set(all_names), key=len, reverse=True):
        for variant in normalize_variants(candidate):
            anonymized = re.sub(re.escape(variant), "Max Mustermann", anonymized, flags=re.IGNORECASE)

    anonymized = re.sub(r"(?i)\bMax Mustermann(?:\s+Max Mustermann)+", "Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+Max Mustermann(?:\s+Max Mustermann)+", r"\1 Max Mustermann", anonymized)

    return anonymized


def anonymize_document_text(text: str) -> str:
    anonymized = text
    anonymized = anonymize_employee_names(anonymized)
    anonymized = anonymize_emails(anonymized)
    return anonymized


def build_json_prompt(document_text: str, filename: str = "nicht vorhanden") -> str:
    schema = {
        "belegdatum": "",
        "art_des_dokuments": "",
        "buchungsnummer_code": "",
        "name_des_reisenden": "",
        "wie_viele_reisesegmente": 0,
        "reisesegmente": [
            {
                "index": 1,
                "abreise_datum_und_zeit": "",
                "ankunft_datum_und_zeit": "",
                "abreise_ort": "",
                "ankunft_ort": "",
                "transportunternehmen_und_nummer": "",
            }
        ],
        "ticketnummer": "",
        "kosten_mit_steuern": "",
        "kosten_ohne_steuern": "",
        "waehrung_der_kosten": "",
        "warnungen": [],
    }

    return f"""
Bitte analysiere mir das folgende PDF/EMAIL/BELEG.

Gib AUSSCHLIESSLICH gültiges JSON zurück.
Keine Erklärung. Kein Markdown. Kein Zusatztext.

Nutze genau diese Felder:
{json.dumps(schema, ensure_ascii=False, indent=2)}

Regeln:
- "art_des_dokuments" nur: "Zug", "Flug", "Hotel", "Taxi", "Unbekannt"
- Wenn ein Feld nicht vorhanden ist: "nicht vorhanden"
- "wie_viele_reisesegmente" ist eine Zahl
- Für jedes Reisesegment einen Eintrag in "reisesegmente"
- Zeiten möglichst inklusive Zeitzonenhinweis, falls vorhanden
- "kosten_mit_steuern" und "kosten_ohne_steuern" getrennt angeben
- Währung separat angeben

Dateiname: {filename}

TEXT:
{document_text[:120000]}
""".strip()


def call_mistral_json(prompt: str, model: Optional[str] = None) -> dict:
    if not MISTRAL_API_KEY:
        return {"status": "error", "detail": "MISTRAL_API_KEY ist nicht gesetzt"}

    use_model = model or MISTRAL_MODEL
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": use_model,
        "messages": [
            {"role": "system", "content": "Gib ausschließlich valides JSON zurück."},
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


def call_openai_json(prompt: str, model: Optional[str] = None) -> dict:
    if not OPENAI_API_KEY:
        return {"status": "error", "detail": "OPENAI_API_KEY ist nicht gesetzt"}

    use_model = model or OPENAI_MODEL
    client = OpenAI(api_key=OPENAI_API_KEY)

    response = client.chat.completions.create(
        model=use_model,
        messages=[
            {"role": "system", "content": "Gib ausschließlich valides JSON zurück."},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )

    content = response.choices[0].message.content or "{}"
    return json.loads(content)


def call_ai_provider_json(
    prompt: str,
    provider_override: Optional[str] = None,
    model_override: Optional[str] = None,
) -> tuple[dict, str, str]:
    provider = (provider_override or AI_PROVIDER).strip().lower()

    if provider == "openai":
        model = model_override or OPENAI_MODEL
        return call_openai_json(prompt, model), provider, model

    if provider == "mistral":
        model = model_override or MISTRAL_MODEL
        return call_mistral_json(prompt, model), provider, model

    return {"status": "error", "detail": f"Unbekannter AI Provider: {provider}"}, provider, model_override or ""


def ensure_defaults(data: Dict[str, Any]) -> Dict[str, Any]:
    base = {
        "belegdatum": "nicht vorhanden",
        "art_des_dokuments": "Unbekannt",
        "buchungsnummer_code": "nicht vorhanden",
        "name_des_reisenden": "nicht vorhanden",
        "wie_viele_reisesegmente": 0,
        "reisesegmente": [],
        "ticketnummer": "nicht vorhanden",
        "kosten_mit_steuern": "nicht vorhanden",
        "kosten_ohne_steuern": "nicht vorhanden",
        "waehrung_der_kosten": "nicht vorhanden",
        "warnungen": [],
    }
    if not isinstance(data, dict):
        return base
    base.update(data)
    if not isinstance(base.get("reisesegmente"), list):
        base["reisesegmente"] = []
    if not isinstance(base.get("warnungen"), list):
        base["warnungen"] = []
    return base


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
    anonymized_text = anonymize_document_text(text)
    prompt = build_json_prompt(anonymized_text, filename=filename)
    raw, used_provider, used_model = call_ai_provider_json(prompt, ai_provider_override, ai_model_override)

    if isinstance(raw, dict) and raw.get("status") == "error":
        return {
            "status": "error",
            "detail": raw.get("detail", "Analysefehler"),
            "version": APP_VERSION,
            "ai_provider": used_provider,
            "ai_model": used_model,
            "anonymized_preview": anonymized_text[:4000],
        }

    data = ensure_defaults(raw)
    data["status"] = "ok"
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


@app.get("/ai/test")
def ai_test(
    ai_provider: Optional[str] = Query(default=None),
    ai_model: Optional[str] = Query(default=None),
):
    prompt = build_json_prompt("Testbeleg: Hotel in Berlin, Check-in 01.05.2026, Check-out 03.05.2026, Total 300 EUR.")
    raw, used_provider, used_model = call_ai_provider_json(prompt, ai_provider, ai_model)
    return {
        "status": "ok" if not (isinstance(raw, dict) and raw.get("status") == "error") else "error",
        "ai_provider": used_provider,
        "ai_model": used_model,
        "result": raw,
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