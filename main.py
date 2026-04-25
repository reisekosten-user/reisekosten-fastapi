from __future__ import annotations

import hashlib
import json
import os
import re
import imaplib
import email
import mimetypes
import uuid
from datetime import datetime
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, Response
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

APP_VERSION = "7.22"
AI_PROVIDER = os.getenv("AI_PROVIDER", "openai").strip().lower()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4")

IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")

ORIGINAL_UPLOAD_DIR = Path(os.getenv("ORIGINAL_UPLOAD_DIR", "uploads/original_belege"))
ORIGINAL_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Reisekosten API", version=APP_VERSION)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def startup() -> None:
    init_db()
    ensure_belege_extra_columns()


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


def normalize_for_fingerprint(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def safe_filename(filename: str) -> str:
    name = filename or "beleg"
    name = name.replace("\\", "_").replace("/", "_").replace(":", "_")
    name = re.sub(r"[^A-Za-z0-9ÄÖÜäöüß._ -]", "_", name)
    return name[:160] or "beleg"


def save_original_file(filename: str, content: bytes, content_type: Optional[str] = None) -> Dict[str, str]:
    safe = safe_filename(filename)
    stored_name = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:12]}_{safe}"
    path = ORIGINAL_UPLOAD_DIR / stored_name
    path.write_bytes(content or b"")
    return {
        "original_file_path": str(path),
        "original_filename": safe,
        "original_content_type": content_type or mimetypes.guess_type(safe)[0] or "application/octet-stream",
    }


def save_original_text_as_file(filename: str, text: str, content_type: str = "text/plain") -> Dict[str, str]:
    return save_original_file(filename, (text or "").encode("utf-8", errors="replace"), content_type)


def extract_reise_code_from_text(text: str) -> Optional[str]:
    if not text:
        return None
    match = re.search(r"\b(\d{2}-\d{3})\b", text)
    if match:
        return match.group(1)
    return None


def find_reise_by_code(reise_code: str) -> Optional[Dict[str, Any]]:
    if not reise_code:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, reise_code, reise_name
                    FROM reisen
                    WHERE reise_code = %s
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (reise_code,),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {"id": row[0], "reise_code": row[1], "reise_name": row[2]}
    except Exception:
        return None


def ensure_belege_extra_columns() -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS fingerprint TEXT")
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS source_filename TEXT")
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS original_text TEXT")
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS anonymized_text TEXT")
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS analysis_json TEXT")
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS original_file_path TEXT")
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS original_filename TEXT")
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS original_content_type TEXT")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_belege_fingerprint ON belege(fingerprint)")
            conn.commit()
    except Exception:
        pass


def build_beleg_fingerprint(filename: str, data: Dict[str, Any], reise_id: Optional[int]) -> str:
    parts = [
        normalize_for_fingerprint(reise_id or "no-reise"),
        normalize_for_fingerprint(filename),
        normalize_for_fingerprint(data.get("art_des_dokuments")),
        normalize_for_fingerprint(data.get("belegdatum")),
        normalize_for_fingerprint(data.get("buchungsnummer_code")),
        normalize_for_fingerprint(data.get("ticketnummer")),
        normalize_for_fingerprint(data.get("kosten_mit_steuern")),
        normalize_for_fingerprint(data.get("waehrung_der_kosten")),
    ]
    raw = "|".join(parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def find_beleg_by_fingerprint(fingerprint: str) -> Optional[int]:
    if not fingerprint:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM belege WHERE fingerprint = %s ORDER BY id DESC LIMIT 1", (fingerprint,))
                row = cur.fetchone()
                return int(row[0]) if row else None
    except Exception:
        return None


def update_beleg_extra_data(
    beleg_id: int,
    fingerprint: str,
    filename: str,
    original_text: str,
    anonymized_text: str,
    analysis_data: Dict[str, Any],
    original_file_path: Optional[str] = None,
    original_filename: Optional[str] = None,
    original_content_type: Optional[str] = None,
) -> None:
    if not beleg_id:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE belege
                       SET fingerprint = %s,
                           source_filename = %s,
                           original_text = %s,
                           anonymized_text = %s,
                           analysis_json = %s,
                           original_file_path = COALESCE(%s, original_file_path),
                           original_filename = COALESCE(%s, original_filename),
                           original_content_type = COALESCE(%s, original_content_type)
                     WHERE id = %s
                    """,
                    (
                        fingerprint,
                        filename,
                        original_text[:300000],
                        anonymized_text[:300000],
                        json.dumps(analysis_data, ensure_ascii=False)[:300000],
                        original_file_path,
                        original_filename,
                        original_content_type,
                        beleg_id,
                    ),
                )
            conn.commit()
    except Exception:
        pass


def get_beleg_record(beleg_id: int) -> Optional[Dict[str, Any]]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM belege WHERE id = %s", (beleg_id,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))
    except Exception:
        return None


def list_belege_for_event(event_id: int) -> List[Dict[str, Any]]:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT b.*
                    FROM belege b
                    JOIN event_belege eb ON eb.beleg_id = b.id
                    WHERE eb.event_id = %s
                    ORDER BY b.id DESC
                    """,
                    (event_id,),
                )
                rows = cur.fetchall()
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in rows]
    except Exception:
        return []


def delete_mitarbeiter_by_id(mitarbeiter_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM reise_reisende WHERE mitarbeiter_id = %s", (mitarbeiter_id,))
            cur.execute("DELETE FROM mitarbeiter WHERE id = %s", (mitarbeiter_id,))
        conn.commit()


def delete_reise_by_id(reise_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM event_belege WHERE event_id IN (SELECT id FROM events WHERE reise_id = %s)", (reise_id,))
            cur.execute("DELETE FROM events WHERE reise_id = %s", (reise_id,))
            cur.execute("DELETE FROM reise_reisende WHERE reise_id = %s", (reise_id,))
            cur.execute("DELETE FROM reisen WHERE id = %s", (reise_id,))
        conn.commit()


def delete_event_by_id(event_id: int) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM event_belege WHERE event_id = %s", (event_id,))
            cur.execute("DELETE FROM events WHERE id = %s", (event_id,))
        conn.commit()


def update_reise_basic(reise_id: int, payload: Dict[str, Any]) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reisen
                   SET reise_name = %s,
                       startdatum = %s,
                       enddatum = %s,
                       anzahl_reisende = %s
                 WHERE id = %s
                """,
                (
                    payload.get("reise_name"),
                    payload.get("startdatum"),
                    payload.get("enddatum"),
                    payload.get("anzahl_reisende") or 1,
                    reise_id,
                ),
            )
            cur.execute("DELETE FROM reise_reisende WHERE reise_id = %s", (reise_id,))
            for mitarbeiter_id in payload.get("mitarbeiter_ids") or []:
                cur.execute(
                    "INSERT INTO reise_reisende (reise_id, mitarbeiter_id, alias_name) VALUES (%s, %s, %s)",
                    (reise_id, mitarbeiter_id, f"REISENDER_{mitarbeiter_id}"),
                )
        conn.commit()
    return {"reise_id": reise_id}


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
        for candidate in [forward, reverse]:
            for variant in normalize_variants(candidate):
                anonymized = re.sub(re.escape(variant), "Max Mustermann", anonymized, flags=re.IGNORECASE)
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
        for nv in normalize_variants(nachname):
            anonymized = re.sub(
                rf"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+{re.escape(nv)}\b",
                lambda match: f"{match.group(1)} Max Mustermann",
                anonymized,
            )
    anonymized = re.sub(r"(?i)(Guest\s*name\s*:\s*)[^\r\n]+", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)(This Marriott\.com reservation email has been forwarded to you by\s+)[^\r\n]+", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)(An\s*:\s*)[^<\r\n]+", r"\1Max Mustermann ", anonymized)
    anonymized = re.sub(r"(?i)(Betreff\s*:\s*)Max Mustermann\s*\([^\)]*\)", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)\bMax Mustermann(?:\s+Max Mustermann)+", "Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+Max Mustermann(?:\s+Max Mustermann)+", r"\1 Max Mustermann", anonymized)
    return anonymized


def anonymize_document_text(text: str) -> str:
    return anonymize_emails(anonymize_employee_names(text))


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
            elif content_type == "text/html" and not bodies:
                payload = part.get_payload(decode=True)
                if payload:
                    html = payload.decode(errors="ignore")
                    bodies.append(re.sub(r"<[^>]+>", " ", html))
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
        subject = email.header.decode_header(msg.get("Subject", ""))[0][0]
        if isinstance(subject, bytes):
            subject = subject.decode(errors="ignore")
        body = extract_plain_text_from_email_message(msg)
        if subject_contains and subject_contains.lower() not in str(subject).lower():
            continue
        mails.append({"subject": str(subject), "body": body, "preview": body[:500], "raw_email": raw_email.decode("utf-8", errors="replace")})
        if len(mails) >= limit:
            break
    try:
        mail.logout()
    except Exception:
        pass
    return mails


def find_existing_event_for_reise(reise_id: int, beleg_typ: str) -> Optional[Dict[str, Any]]:
    if not reise_id or not beleg_typ:
        return None
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, typ, titel, status, created_at
                    FROM events
                    WHERE reise_id = %s
                      AND LOWER(COALESCE(typ, '')) = LOWER(%s)
                    ORDER BY id DESC
                    LIMIT 1
                    """,
                    (reise_id, beleg_typ),
                )
                row = cur.fetchone()
                if not row:
                    return None
                return {"id": row[0], "typ": row[1], "titel": row[2], "status": row[3], "created_at": row[4].isoformat() if hasattr(row[4], "isoformat") else row[4]}
    except Exception:
        return None


def attach_or_create_event_for_analysis(reise_id: int, beleg_id: int, data: dict) -> Dict[str, Any]:
    art = data.get("art_des_dokuments", "Unbekannt")
    titel = art if art != "Unbekannt" else "Beleg"
    existing_event = find_existing_event_for_reise(reise_id, art)
    if existing_event:
        attach_beleg_to_event(existing_event["id"], beleg_id)
        update_event_status(existing_event["id"], "abgeschlossen")
        return {"matched_event_id": existing_event["id"], "matched_event_typ": existing_event.get("typ"), "matched_existing_event": True}
    new_event_id = create_event({"reise_id": reise_id, "typ": art, "titel": titel, "status": "abgeschlossen"})
    attach_beleg_to_event(new_event_id, beleg_id)
    return {"created_event_id": new_event_id, "matched_event_typ": art, "matched_existing_event": False}


def _pdf_escape(value: str) -> str:
    value = (value or "").replace("\r", "")
    value = value.encode("latin-1", errors="replace").decode("latin-1")
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def _wrap_text(text: str, width: int = 92, max_lines: int = 220) -> List[str]:
    lines: List[str] = []
    for raw in (text or "").split("\n"):
        raw = raw.rstrip()
        while len(raw) > width:
            lines.append(raw[:width])
            raw = raw[width:]
            if len(lines) >= max_lines:
                return lines + ["... gekuerzt ..."]
        lines.append(raw)
        if len(lines) >= max_lines:
            return lines + ["... gekuerzt ..."]
    return lines


def make_simple_pdf(title: str, body: str) -> bytes:
    lines = [title, "", f"Erzeugt: {datetime.utcnow().isoformat()} UTC", ""] + _wrap_text(body)
    pages: List[List[str]] = []
    page_size = 52
    for i in range(0, len(lines), page_size):
        pages.append(lines[i:i + page_size])
    if not pages:
        pages = [[title]]
    objects: List[bytes] = []
    page_refs: List[int] = []

    def add_obj(content: bytes) -> int:
        objects.append(content)
        return len(objects)

    catalog_id = add_obj(b"<< /Type /Catalog /Pages 2 0 R >>")
    pages_id = add_obj(b"PLACEHOLDER")
    font_id = add_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    for page_lines in pages:
        content_parts = ["BT", "/F1 10 Tf", "50 800 Td", "14 TL"]
        for line in page_lines:
            content_parts.append(f"({_pdf_escape(line)}) Tj")
            content_parts.append("T*")
        content_parts.append("ET")
        stream = "\n".join(content_parts).encode("latin-1", errors="replace")
        stream_id = add_obj(b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream")
        page_id = add_obj(f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {stream_id} 0 R >>".encode())
        page_refs.append(page_id)
    kids = " ".join(f"{pid} 0 R" for pid in page_refs)
    objects[pages_id - 1] = f"<< /Type /Pages /Kids [{kids}] /Count {len(page_refs)} >>".encode()
    out = BytesIO()
    out.write(b"%PDF-1.4\n")
    offsets = [0]
    for idx, obj in enumerate(objects, start=1):
        offsets.append(out.tell())
        out.write(f"{idx} 0 obj\n".encode())
        out.write(obj)
        out.write(b"\nendobj\n")
    xref_pos = out.tell()
    out.write(f"xref\n0 {len(objects) + 1}\n".encode())
    out.write(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        out.write(f"{off:010d} 00000 n \n".encode())
    out.write(f"trailer\n<< /Size {len(objects) + 1} /Root {catalog_id} 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode())
    return out.getvalue()


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
    return {"status": status, "warnungen": warnungen, "flug": "Flug" in typen, "hotel": "Hotel" in typen, "taxi": "Taxi" in typen}


def analyze_text_internal(
    text: str,
    filename: str = "nicht vorhanden",
    reise_id: Optional[int] = None,
    event_id: Optional[int] = None,
    ai_provider_override: Optional[str] = None,
    ai_model_override: Optional[str] = None,
    original_file_info: Optional[Dict[str, str]] = None,
) -> dict:
    anonymized_text = anonymize_document_text(text)
    prompt = build_json_prompt(anonymized_text, filename=filename)
    raw = call_openai_json(prompt, ai_model_override)
    if isinstance(raw, dict) and raw.get("status") == "error":
        return {"status": "error", "detail": raw.get("detail", "Analysefehler"), "version": APP_VERSION, "ai_provider": "openai", "ai_model": ai_model_override or OPENAI_MODEL, "anonymized_preview": anonymized_text[:4000]}
    data = ensure_defaults(raw)
    data["status"] = "ok"
    data["version"] = APP_VERSION
    data["generated_at_utc"] = datetime.utcnow().isoformat()
    data["ai_provider"] = "openai"
    data["ai_model"] = ai_model_override or OPENAI_MODEL
    data["anonymized_preview"] = anonymized_text[:4000]
    fingerprint = build_beleg_fingerprint(filename, data, reise_id)
    existing_beleg_id = find_beleg_by_fingerprint(fingerprint)
    data["fingerprint"] = fingerprint
    data["duplicate_detected"] = bool(existing_beleg_id)
    original_file_info = original_file_info or {}
    if existing_beleg_id:
        data["existing_beleg_id"] = existing_beleg_id
        data["beleg_id"] = existing_beleg_id
        data["duplicate_action"] = "skipped_insert_existing_beleg_used"
        update_beleg_extra_data(existing_beleg_id, fingerprint, filename, text, anonymized_text, data, original_file_info.get("original_file_path"), original_file_info.get("original_filename"), original_file_info.get("original_content_type"))
        if event_id:
            attach_beleg_to_event(event_id, existing_beleg_id)
            update_event_status(event_id, "abgeschlossen")
            data["attached_event_id"] = event_id
        elif reise_id:
            data.update(attach_or_create_event_for_analysis(reise_id, existing_beleg_id, data))
        return data
    beleg_id = insert_beleg({"belegdatum": data.get("belegdatum"), "art": data.get("art_des_dokuments"), "kosten": data.get("kosten_mit_steuern"), "waehrung": data.get("waehrung_der_kosten")})
    update_beleg_extra_data(beleg_id, fingerprint, filename, text, anonymized_text, data, original_file_info.get("original_file_path"), original_file_info.get("original_filename"), original_file_info.get("original_content_type"))
    data["beleg_id"] = beleg_id
    data["existing_beleg_id"] = None
    data["duplicate_action"] = "inserted_new_beleg"
    if event_id:
        attach_beleg_to_event(event_id, beleg_id)
        update_event_status(event_id, "abgeschlossen")
        data["attached_event_id"] = event_id
    elif reise_id:
        data.update(attach_or_create_event_for_analysis(reise_id, beleg_id, data))
    return data


@app.get("/")
def root():
    return {"status": "ok", "version": APP_VERSION}


@app.get("/health")
def health():
    return {"status": "ok", "version": APP_VERSION, "ai_provider": AI_PROVIDER, "openai_configured": bool(OPENAI_API_KEY), "openai_model": OPENAI_MODEL, "imap_host_configured": bool(IMAP_HOST), "imap_user_configured": bool(IMAP_USER), "imap_pass_configured": bool(IMAP_PASS), "original_upload_dir": str(ORIGINAL_UPLOAD_DIR)}


@app.get("/dashboard")
def dashboard():
    return FileResponse("templates/dashboard.html")


@app.get("/ai/test")
def ai_test(ai_model: Optional[str] = Query(default=None)):
    prompt = build_json_prompt("Testbeleg: Hotel in Berlin, Check-in 01.05.2026, Check-out 03.05.2026, Total 300 EUR.")
    raw = call_openai_json(prompt, ai_model)
    return {"status": "ok" if not (isinstance(raw, dict) and raw.get("status") == "error") else "error", "ai_provider": "openai", "ai_model": ai_model or OPENAI_MODEL, "result": raw}


@app.get("/mail/test")
def mail_test(limit: int = Query(default=3, ge=1, le=20), subject_contains: Optional[str] = Query(default=None)):
    try:
        mails = read_latest_mails(limit=limit, subject_contains=subject_contains)
        return {"status": "ok", "count": len(mails), "subject_filter": subject_contains, "mails": [{"subject": m["subject"], "preview": m["preview"]} for m in mails]}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.get("/mail/analyze-latest")
def mail_analyze_latest(limit: int = Query(default=3, ge=1, le=10), subject_contains: Optional[str] = Query(default=None)):
    try:
        mails = read_latest_mails(limit=limit, subject_contains=subject_contains)
        analyzed = []
        for m in mails:
            detected_reise_code = extract_reise_code_from_text(m["subject"])
            detected_reise = find_reise_by_code(detected_reise_code) if detected_reise_code else None
            detected_reise_id = detected_reise["id"] if detected_reise else None
            original_info = save_original_text_as_file(f"mail_{safe_filename(m['subject'])}.eml", m.get("raw_email") or m.get("body") or "", "message/rfc822")
            result = analyze_text_internal(m["body"], filename=f"mail:{m['subject']}", reise_id=detected_reise_id, event_id=None, ai_provider_override="openai", ai_model_override=None, original_file_info=original_info)
            analyzed.append({"subject": m["subject"], "preview": m["preview"], "detected_reise_code": detected_reise_code, "assigned_reise_id": detected_reise_id, "assigned_reise_name": detected_reise["reise_name"] if detected_reise else None, "analysis": result})
        return {"status": "ok", "count": len(analyzed), "subject_filter": subject_contains, "results": analyzed}
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
        ensure_belege_extra_columns()
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


@app.get("/belege/{beleg_id}")
def beleg_detail(beleg_id: int):
    rec = get_beleg_record(beleg_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Beleg nicht gefunden")
    return {"status": "ok", "beleg": rec, "pdf_url": f"/belege/{beleg_id}/pdf", "original_url": f"/belege/{beleg_id}/original"}


@app.get("/belege/{beleg_id}/original")
def beleg_original(beleg_id: int):
    rec = get_beleg_record(beleg_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Beleg nicht gefunden")
    path = rec.get("original_file_path")
    if not path or not os.path.exists(path):
        raise HTTPException(status_code=404, detail="Originaldatei wurde für diesen Beleg noch nicht gespeichert. Bitte Beleg ab Version 7.22 erneut einlesen.")
    filename = rec.get("original_filename") or rec.get("source_filename") or f"beleg_{beleg_id}"
    media_type = rec.get("original_content_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return FileResponse(path, media_type=media_type, filename=filename)


@app.get("/belege/{beleg_id}/pdf")
def beleg_pdf(beleg_id: int):
    rec = get_beleg_record(beleg_id)
    if not rec:
        raise HTTPException(status_code=404, detail="Beleg nicht gefunden")
    path = rec.get("original_file_path")
    filename = rec.get("original_filename") or rec.get("source_filename") or f"beleg_{beleg_id}"
    content_type = rec.get("original_content_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream"
    if path and os.path.exists(path):
        lower = filename.lower()
        if lower.endswith(".pdf") or content_type == "application/pdf":
            return FileResponse(path, media_type="application/pdf", filename=filename)
        if lower.endswith((".jpg", ".jpeg", ".png", ".webp")) or content_type.startswith("image/"):
            return FileResponse(path, media_type=content_type, filename=filename)
    title = f"Beleg {beleg_id} - {rec.get('art') or ''}"
    body_parts = [
        f"Beleg-ID: {beleg_id}",
        f"Datei/Quelle: {rec.get('source_filename') or filename or 'nicht gespeichert'}",
        f"Belegdatum: {rec.get('belegdatum') or 'nicht vorhanden'}",
        f"Art: {rec.get('art') or 'nicht vorhanden'}",
        f"Kosten: {rec.get('kosten') or 'nicht vorhanden'} {rec.get('waehrung') or ''}",
        f"Fingerprint: {rec.get('fingerprint') or 'nicht vorhanden'}",
        "",
        "--- Original / Mail-Text / OCR-Text ---",
        rec.get("original_text") or "Für diesen Beleg wurde kein Originaltext gespeichert. Bitte Beleg ab Version 7.22 erneut einlesen.",
    ]
    pdf_bytes = make_simple_pdf(title, "\n".join(body_parts))
    return Response(content=pdf_bytes, media_type="application/pdf", headers={"Content-Disposition": f"inline; filename=beleg_{beleg_id}.pdf"})


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


@app.delete("/mitarbeiter/{mitarbeiter_id}")
def mitarbeiter_delete(mitarbeiter_id: int):
    try:
        delete_mitarbeiter_by_id(mitarbeiter_id)
        return {"status": "ok", "deleted_mitarbeiter_id": mitarbeiter_id}
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


@app.put("/reisen/{reise_id}")
def reisen_update(reise_id: int, payload: ReiseCreateRequest):
    try:
        result = update_reise_basic(reise_id, payload.model_dump())
        return {"status": "ok", **result}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.delete("/reisen/{reise_id}")
def reisen_delete(reise_id: int):
    try:
        delete_reise_by_id(reise_id)
        return {"status": "ok", "deleted_reise_id": reise_id}
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
        detail = get_event_detail(event_id)
        belege_items = list_belege_for_event(event_id)
        return {**detail, "belege": belege_items, "beleg_pdf_urls": [{"beleg_id": b.get("id"), "pdf_url": f"/belege/{b.get('id')}/pdf", "original_url": f"/belege/{b.get('id')}/original"} for b in belege_items if b.get("id")]}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.delete("/events/{event_id}")
def events_delete(event_id: int):
    try:
        delete_event_by_id(event_id)
        return {"status": "ok", "deleted_event_id": event_id}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.put("/events/{event_id}/status")
def events_update_status(event_id: int, payload: EventStatusRequest):
    try:
        update_event_status(event_id, payload.status)
        return {"status": "ok", "event_id": event_id, "new_status": payload.status}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.get("/reisen/{reise_id}")
def reisen_detail(reise_id: int):
    try:
        detail = get_reise_detail(reise_id)
        return {**detail, **compute_reise_status(detail)}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.post("/anonymize/text")
def anonymize_text(payload: AnalyzeTextRequest):
    try:
        return {"status": "ok", "filename": payload.filename or "text-input.txt", "anonymized_text": anonymize_document_text(payload.text)}
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
        original_info = save_original_text_as_file(payload.filename or "text-input.txt", payload.text, "text/plain")
        return analyze_text_internal(payload.text, payload.filename or "text-input.txt", payload.reise_id, payload.event_id, payload.ai_provider, payload.ai_model, original_file_info=original_info)
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
        original_info = save_original_file(file.filename or "upload", content, file.content_type)
        text = extract_text_from_pdf(content) if (file.filename or "").lower().endswith(".pdf") else content.decode("utf-8", errors="replace")
        return analyze_text_internal(text, file.filename or "upload", reise_id, event_id, ai_provider, ai_model, original_file_info=original_info)
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
