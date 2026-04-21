from __future__ import annotations

import json
import os
import re
import imaplib
import email
from datetime import datetime
from io import BytesIO
from typing import Any, Dict, List, Optional

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

APP_VERSION = "7.12e"
AI_PROVIDER = os.getenv("AI_PROVIDER", "openai").strip().lower()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4")

IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")

app = FastAPI(title="Reisekosten API", version=APP_VERSION)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def startup() -> None:
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


# ----------------------------
# Text / PDF extraction
# ----------------------------

def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    reader = PdfReader(BytesIO(pdf_bytes))
    return "\n".join([page.extract_text() or "" for page in reader.pages]).strip()


def normalize_variants(value: str) -> List[str]:
    if not value:
        return []
    base = value.strip().lower()
    return list(
        {
            base,
            base.replace("ä", "ae").replace("ö", "oe").replace("ü", "ue").replace("ß", "ss"),
            base.replace("ä", "a").replace("ö", "o").replace("ü", "u").replace("ß", "ss"),
        }
    )


# ----------------------------
# Anonymization
# ----------------------------

def anonymize_emails(text: str) -> str:
    return re.sub(
        r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b",
        "abc@123.com",
        text,
        flags=re.IGNORECASE,
    )


def anonymize_employee_names(text: str) -> str:
    anonymized = text

    for m in list_mitarbeiter(limit=5000):
        vorname = (m.get("vorname") or "").strip()
        nachname = (m.get("nachname") or "").strip()
        if not vorname or not nachname:
            continue

        forward = f"{vorname} {nachname}"
        reverse = f"{nachname} {vorname}"

        # 1. Exakte 2-teilige Namen zuerst
        for candidate in [forward, reverse]:
            for variant in normalize_variants(candidate):
                anonymized = re.sub(re.escape(variant), "Max Mustermann", anonymized, flags=re.IGNORECASE)

        # 2. Mit Titel und optionalem Mittelteil: Vorname ... Nachname
        for vv in normalize_variants(vorname):
            for nv in normalize_variants(nachname):
                anonymized = re.sub(
                    rf"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+{re.escape(vv)}(?:\s+[A-Za-zÄÖÜäöüß.\-]+)*\s+{re.escape(nv)}",
                    lambda match: f"{match.group(1)} Max Mustermann",
                    anonymized,
                )
                anonymized = re.sub(
                    rf"(?i)\b{re.escape(vv)}(?:\s+[A-Za-zÄÖÜäöüß.\-]+)*\s+{re.escape(nv)}",
                    "Max Mustermann",
                    anonymized,
                )

        # 3. Häufige Hotel-/Mail-Formate mit Nachname Vorname
        for variant in normalize_variants(reverse):
            anonymized = re.sub(re.escape(variant), "Max Mustermann", anonymized, flags=re.IGNORECASE)

        # 4. Anrede + nur Nachname, z. B. Herr Diesslin / Mr Diesslin
        for nv in normalize_variants(nachname):
            anonymized = re.sub(
                rf"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+{re.escape(nv)}\b",
                lambda match: f"{match.group(1)} Max Mustermann",
                anonymized,
            )

    # 5. Typische Felder glätten
    anonymized = re.sub(r"(?i)(Guest\s*name\s*:\s*)[^\r\n]+", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)(This Marriott\.com reservation email has been forwarded to you by\s+)[^\r\n]+", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)(An\s*:\s*)[^<\r\n]+", r"\1Max Mustermann ", anonymized)
    anonymized = re.sub(r"(?i)(Betreff\s*:\s*)Max Mustermann\s*\([^\)]*\)", r"\1Max Mustermann", anonymized)

    # 6. Cleanup
    anonymized = re.sub(r"(?i)\bMax Mustermann(?:\s+Max Mustermann)+", "Max Mustermann", anonymized)
    anonymized = re.sub(
        r"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+Max Mustermann(?:\s+Max Mustermann)+",
        r"\1 Max Mustermann",
        anonymized,
    )
    return anonymized


def anonymize_document_text(text: str) -> str:
    return anonymize_emails(anonymize_employee_names(text))


# ----------------------------
# Prompt / OpenAI
# ----------------------------

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


def call_openai_json(prompt: str, model: Optional[str] = None) -> dict:
    if not OPENAI_API_KEY:
        return {"status": "error", "detail": "OPENAI_API_KEY ist nicht gesetzt"}

    client = OpenAI(api_key=OPENAI_API_KEY)
    use_model = model or OPENAI_MODEL
    response = client.chat.completions.create(
        model=use_model,
        messages=[
            {"role": "system", "content": "Gib ausschließlich valides JSON zurück."},
            {"role": "user", "content": prompt},
        ],
        response_format={"type": "json_object"},
        temperature=0,
    )
    return json.loads(response.choices[0].message.content or "{}")


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
    if isinstance(data, dict):
        base.update(data)
    if not isinstance(base.get("reisesegmente"), list):
        base["reisesegmente"] = []
    if not isinstance(base.get("warnungen"), list):
        base["warnungen"] = []
    return base


# ----------------------------
# Mail helpers
# ----------------------------

def extract_plain_text_from_email_message(msg) -> str:
    bodies: List[str] = []

    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition") or "")
            if "attachment" in disposition.lower():
                continue
            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    bodies.append(payload.decode(errors="ignore"))
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            bodies.append(payload.decode(errors="ignore"))

    return "\n\n".join([b for b in bodies if b]).strip()


def read_latest_mails(limit: int = 3, subject_contains: Optional[str] = None) -> List[Dict[str, str]]:
    if not IMAP_HOST or not IMAP_USER or not IMAP_PASS:
        raise RuntimeError("IMAP config fehlt")

    mail = imaplib.IMAP4_SSL(IMAP_HOST)
    mail.login(IMAP_USER, IMAP_PASS)
    mail.select("inbox")

    result, data = mail.search(None, "ALL")
    if result != "OK":
        raise RuntimeError("IMAP Suche fehlgeschlagen")

    ids = data[0].split()
    mails: List[Dict[str, str]] = []

    for msg_id in reversed(ids):
        res, msg_data = mail.fetch(msg_id, "(RFC822)")
        if res != "OK":
            continue

        raw_email = msg_data[0][1]
        msg = email.message_from_bytes(raw_email)
        subject = msg.get("Subject", "")
        body = extract_plain_text_from_email_message(msg)

        if subject_contains and subject_contains.lower() not in subject.lower():
            continue

        mails.append({
            "subject": subject,
            "body": body,
            "preview": body[:500],
        })

        if len(mails) >= limit:
            break

    try:
        mail.logout()
    except Exception:
        pass

    return mails


# ----------------------------
# Analyze core
# ----------------------------

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

    return {
        "status": status,
        "warnungen": warnungen,
        "flug": "Flug" in typen,
        "hotel": "Hotel" in typen,
        "taxi": "Taxi" in typen,
    }


def auto_create_event_for_analysis(reise_id: int, beleg_id: int, data: dict) -> int:
    art = data.get("art_des_dokuments", "Unbekannt")
    titel = art if art != "Unbekannt" else "Beleg"
    event_id = create_event(
        {
            "reise_id": reise_id,
            "typ": art,
            "titel": titel,
            "status": "abgeschlossen",
        }
    )
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
    raw = call_openai_json(prompt, ai_model_override)

    if isinstance(raw, dict) and raw.get("status") == "error":
        return {
            "status": "error",
            "detail": raw.get("detail", "Analysefehler"),
            "version": APP_VERSION,
            "ai_provider": "openai",
            "ai_model": ai_model_override or OPENAI_MODEL,
            "anonymized_preview": anonymized_text[:4000],
        }

    data = ensure_defaults(raw)
    data["status"] = "ok"
    data["version"] = APP_VERSION
    data["generated_at_utc"] = datetime.utcnow().isoformat()
    data["ai_provider"] = "openai"
    data["ai_model"] = ai_model_override or OPENAI_MODEL
    data["anonymized_preview"] = anonymized_text[:4000]

    beleg_id = insert_beleg(
        {
            "belegdatum": data.get("belegdatum"),
            "art": data.get("art_des_dokuments"),
            "kosten": data.get("kosten_mit_steuern"),
            "waehrung": data.get("waehrung_der_kosten"),
        }
    )
    data["beleg_id"] = beleg_id

    if event_id:
        attach_beleg_to_event(event_id, beleg_id)
        update_event_status(event_id, "abgeschlossen")
        data["attached_event_id"] = event_id
    elif reise_id:
        data["created_event_id"] = auto_create_event_for_analysis(reise_id, beleg_id, data)

    return data


# ----------------------------
# API routes
# ----------------------------

@app.get("/")
def root():
    return {"status": "ok", "version": APP_VERSION}


@app.get("/health")
def health():
    return {
        "status": "ok",
        "version": APP_VERSION,
        "ai_provider": AI_PROVIDER,
        "openai_configured": bool(OPENAI_API_KEY),
        "openai_model": OPENAI_MODEL,
        "imap_host_configured": bool(IMAP_HOST),
        "imap_user_configured": bool(IMAP_USER),
        "imap_pass_configured": bool(IMAP_PASS),
    }


@app.get("/dashboard")
def dashboard():
    return FileResponse("templates/dashboard.html")


@app.get("/ai/test")
def ai_test(ai_model: Optional[str] = Query(default=None)):
    prompt = build_json_prompt("Testbeleg: Hotel in Berlin, Check-in 01.05.2026, Check-out 03.05.2026, Total 300 EUR.")
    raw = call_openai_json(prompt, ai_model)
    return {
        "status": "ok" if not (isinstance(raw, dict) and raw.get("status") == "error") else "error",
        "ai_provider": "openai",
        "ai_model": ai_model or OPENAI_MODEL,
        "result": raw,
    }


@app.get("/mail/test")
def mail_test(
    limit: int = Query(default=3, ge=1, le=20),
    subject_contains: Optional[str] = Query(default=None),
):
    try:
        mails = read_latest_mails(limit=limit, subject_contains=subject_contains)
        return {
            "status": "ok",
            "count": len(mails),
            "subject_filter": subject_contains,
            "mails": [{"subject": m["subject"], "preview": m["preview"]} for m in mails],
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.get("/mail/analyze-latest")
def mail_analyze_latest(
    limit: int = Query(default=3, ge=1, le=10),
    subject_contains: Optional[str] = Query(default=None),
):
    try:
        mails = read_latest_mails(limit=limit, subject_contains=subject_contains)
        analyzed = []
        for m in mails:
            result = analyze_text_internal(
                m["body"],
                filename=f"mail:{m['subject']}",
                reise_id=None,
                event_id=None,
                ai_provider_override="openai",
                ai_model_override=None,
            )
            analyzed.append({
                "subject": m["subject"],
                "preview": m["preview"],
                "analysis": result,
            })
        return {
            "status": "ok",
            "count": len(analyzed),
            "subject_filter": subject_contains,
            "results": analyzed,
        }
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


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
            enriched.append({**r, **compute_reise_status(detail)})
        return {"count": len(enriched), "reisen": enriched}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.get("/reisen/{reise_id}")
def reisen_detail(reise_id: int):
    try:
        detail = get_reise_detail(reise_id)
        return {**detail, **compute_reise_status(detail)}
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
        return {
            "status": "ok",
            "filename": payload.filename or "text-input.txt",
            "anonymized_text": anonymize_document_text(payload.text),
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/anonymize/file")
async def anonymize_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        text = extract_text_from_pdf(content) if (file.filename or "").lower().endswith(".pdf") else content.decode("utf-8", errors="replace")
        return {"status": "ok", "filename": file.filename or "upload", "anonymized_text": anonymize_document_text(text)}
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
        text = extract_text_from_pdf(content) if (file.filename or "").lower().endswith(".pdf") else content.decode("utf-8", errors="replace")
        return analyze_text_internal(text, file.filename or "upload", reise_id, event_id, ai_provider, ai_model)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
