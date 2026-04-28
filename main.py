from __future__ import annotations

import email
import hashlib
import imaplib
import json
import mimetypes
import os
import re
import uuid
from datetime import datetime, date, timedelta
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

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

APP_VERSION = "8.1"

AI_PROVIDER = os.getenv("AI_PROVIDER", "openai").strip().lower()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-5.4")

IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")

ORIGINAL_UPLOAD_DIR = Path(os.getenv("ORIGINAL_UPLOAD_DIR", "uploads/original_belege"))
ORIGINAL_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)

GENERATED_PDF_DIR = Path(os.getenv("GENERATED_PDF_DIR", "uploads/generated_pdfs"))
GENERATED_PDF_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="Reisekosten API", version=APP_VERSION)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.on_event("startup")
def startup() -> None:
    init_db()
    ensure_db_extensions()


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


# ============================================================
# DB EXTENSIONS
# ============================================================

def ensure_db_extensions() -> None:
    """Erweitert bestehende Tabellen ohne Reset.
    Wichtig: Kein DROP, damit Testdaten erhalten bleiben.
    """
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
                cur.execute("ALTER TABLE belege ADD COLUMN IF NOT EXISTS generated_pdf_path TEXT")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_belege_fingerprint ON belege(fingerprint)")
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS mail_import_log (
                        id SERIAL PRIMARY KEY,
                        mail_key TEXT UNIQUE,
                        subject TEXT,
                        imported_at TEXT,
                        status TEXT,
                        detail TEXT
                    )
                """)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS vma_tage (
                        id SERIAL PRIMARY KEY,
                        reise_id INTEGER NOT NULL,
                        tag DATE NOT NULL,
                        land TEXT DEFAULT 'Deutschland',
                        ort TEXT DEFAULT '',
                        fruehstueck BOOLEAN DEFAULT FALSE,
                        mittag BOOLEAN DEFAULT FALSE,
                        abend BOOLEAN DEFAULT FALSE,
                        betrag NUMERIC DEFAULT 0,
                        notiz TEXT DEFAULT '',
                        updated_at TEXT,
                        UNIQUE(reise_id, tag)
                    )
                """)
            conn.commit()
    except Exception:
        # App soll auch starten, wenn DB noch nicht bereit ist.
        pass


# ============================================================
# FILE / TEXT HELPERS
# ============================================================

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


def extract_text_from_pdf(pdf_bytes: bytes) -> str:
    try:
        reader = PdfReader(BytesIO(pdf_bytes))
        return "\n".join([page.extract_text() or "" for page in reader.pages]).strip()
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"PDF konnte nicht gelesen werden: {exc}") from exc


def extract_text_from_upload(filename: str, content: bytes) -> str:
    lower = (filename or "").lower()
    if lower.endswith(".pdf"):
        return extract_text_from_pdf(content)
    return content.decode("utf-8", errors="replace")


# ============================================================
# DATE NORMALIZATION / SORT SUPPORT
# ============================================================

def normalize_analysis_date(value: Any) -> Any:
    """Normalisiert typische Beleg-Datumswerte auf ISO YYYY-MM-DD.
    Wichtig fuer richtige chronologische Sortierung im Dashboard.
    Beispiele:
    - 20-04-26 -> 2026-04-20
    - 21.04.2026 -> 2026-04-21
    - 25MAY2026 06:35 -> 2026-05-25 06:35
    - 25 Mai 2026 06:35 -> 2026-05-25 06:35
    """
    if value is None:
        return value
    text = str(value).strip()
    if not text or text.lower() == "nicht vorhanden":
        return value

    # Already ISO date at beginning.
    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})(.*)$", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}{m.group(4)}"

    # DD-MM-YY / DD.MM.YYYY / DD/MM/YY
    m = re.search(r"\b(\d{1,2})[-./](\d{1,2})[-./](\d{2}|\d{4})(.*)$", text)
    if m:
        day = int(m.group(1))
        month = int(m.group(2))
        year = int(m.group(3))
        if year < 100:
            year += 2000
        rest = m.group(4) or ""
        return f"{year:04d}-{month:02d}-{day:02d}{rest}"

    months = {
        "JAN": "01", "FEB": "02", "MAR": "03", "MÄR": "03", "MAER": "03", "APR": "04",
        "MAY": "05", "MAI": "05", "JUN": "06", "JUL": "07", "AUG": "08", "SEP": "09",
        "OCT": "10", "OKT": "10", "NOV": "11", "DEC": "12", "DEZ": "12",
        "JANUAR": "01", "FEBRUAR": "02", "MÄRZ": "03", "MAERZ": "03", "APRIL": "04",
        "JUNI": "06", "JULI": "07", "AUGUST": "08", "SEPTEMBER": "09", "OKTOBER": "10",
        "NOVEMBER": "11", "DEZEMBER": "12",
    }

    # 25MAY2026 06:35
    m = re.search(r"\b(\d{1,2})([A-Za-zÄÖÜäöü]{3,9})(20\d{2})(.*)$", text)
    if m:
        mon = months.get(m.group(2).upper())
        if mon:
            return f"{int(m.group(3)):04d}-{mon}-{int(m.group(1)):02d}{m.group(4) or ''}"

    # 25 Mai 2026 06:35 / 25 May 2026
    m = re.search(r"\b(\d{1,2})\s+([A-Za-zÄÖÜäöü]{3,9})\s+(20\d{2})(.*)$", text)
    if m:
        mon = months.get(m.group(2).upper())
        if mon:
            return f"{int(m.group(3)):04d}-{mon}-{int(m.group(1)):02d}{m.group(4) or ''}"

    return value


def normalize_analysis_dates(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        return data
    data["belegdatum"] = normalize_analysis_date(data.get("belegdatum"))
    for segment in data.get("reisesegmente") or []:
        if isinstance(segment, dict):
            segment["abreise_datum_und_zeit"] = normalize_analysis_date(segment.get("abreise_datum_und_zeit"))
            segment["ankunft_datum_und_zeit"] = normalize_analysis_date(segment.get("ankunft_datum_und_zeit"))
    return data


def _month_to_number(mon: str) -> Optional[str]:
    months = {
        "JAN": "01", "JANUAR": "01", "FEB": "02", "FEBRUAR": "02", "MAR": "03", "MÄR": "03", "MÄRZ": "03", "MAER": "03", "MAERZ": "03",
        "APR": "04", "APRIL": "04", "MAY": "05", "MAI": "05", "JUN": "06", "JUNI": "06", "JUL": "07", "JULI": "07",
        "AUG": "08", "AUGUST": "08", "SEP": "09", "SEPTEMBER": "09", "OCT": "10", "OKT": "10", "OKTOBER": "10",
        "NOV": "11", "NOVEMBER": "11", "DEC": "12", "DEZ": "12", "DEZEMBER": "12",
    }
    return months.get((mon or "").strip().upper())


def parse_hotel_date_to_iso(value: Any, default_time: str) -> Optional[str]:
    """Parst typische Hotel-Check-in/Check-out-Daten robust nach YYYY-MM-DD HH:MM."""
    if not value:
        return None
    text = str(value).strip()
    if not text or text.lower() == "nicht vorhanden":
        return None

    m = re.search(r"\b(20\d{2})-(\d{2})-(\d{2})(?:[ T]+(\d{1,2}:\d{2}))?", text)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4) or default_time}"

    m = re.search(r"\b(\d{1,2})[-./](\d{1,2})[-./](\d{2}|\d{4})(?:\D+(\d{1,2}:\d{2}))?", text)
    if m:
        y = int(m.group(3))
        if y < 100:
            y += 2000
        return f"{y:04d}-{int(m.group(2)):02d}-{int(m.group(1)):02d} {m.group(4) or default_time}"

    m = re.search(r"\b(\d{1,2})\s*([A-Za-zÄÖÜäöü]{3,9})\s*,?\s*(20\d{2})(?:\D+(\d{1,2}:\d{2}))?", text)
    if m:
        mon = _month_to_number(m.group(2))
        if mon:
            return f"{int(m.group(3)):04d}-{mon}-{int(m.group(1)):02d} {m.group(4) or default_time}"

    m = re.search(r"\b([A-Za-zÄÖÜäöü]{3,9})\s+(\d{1,2}),?\s*(20\d{2})(?:\D+(\d{1,2}:\d{2}))?", text)
    if m:
        mon = _month_to_number(m.group(1))
        if mon:
            return f"{int(m.group(3)):04d}-{mon}-{int(m.group(2)):02d} {m.group(4) or default_time}"

    m = re.search(r"\b(\d{1,2})([A-Za-z]{3})(20\d{2})(?:\D+(\d{1,2}:\d{2}))?", text)
    if m:
        mon = _month_to_number(m.group(2))
        if mon:
            return f"{int(m.group(3)):04d}-{mon}-{int(m.group(1)):02d} {m.group(4) or default_time}"

    return None


def _find_date_after_keywords(text: str, keywords: List[str], default_time: str) -> Optional[str]:
    if not text:
        return None
    date_pattern = r"((?:20\d{2}-\d{2}-\d{2})|(?:\d{1,2}[-./]\d{1,2}[-./](?:\d{2}|\d{4}))|(?:\d{1,2}\s*[A-Za-zÄÖÜäöü]{3,9}\s*,?\s*20\d{2})|(?:[A-Za-zÄÖÜäöü]{3,9}\s+\d{1,2},?\s*20\d{2})|(?:\d{1,2}[A-Za-z]{3}20\d{2}))"
    for kw in keywords:
        pattern = rf"(?is){kw}[^\n\r]{{0,90}}?{date_pattern}[^\n\r]{{0,30}}?(\d{{1,2}}:\d{{2}})?"
        m = re.search(pattern, text)
        if m:
            candidate = m.group(1)
            if m.lastindex and m.group(m.lastindex) and ':' in m.group(m.lastindex):
                candidate += " " + m.group(m.lastindex)
            parsed = parse_hotel_date_to_iso(candidate, default_time)
            if parsed:
                return parsed
    return None


def extract_hotel_dates_from_text(text: str) -> Tuple[Optional[str], Optional[str]]:
    checkin = _find_date_after_keywords(
        text,
        [r"check\s*[- ]?in", r"arrival", r"ankunft", r"anreise", r"von", r"from"],
        "23:59",
    )
    checkout = _find_date_after_keywords(
        text,
        [r"check\s*[- ]?out", r"departure", r"abreise", r"bis", r"to"],
        "00:00",
    )
    return checkin, checkout


def enhance_hotel_analysis(data: Dict[str, Any], original_text: str) -> Dict[str, Any]:
    """Korrigiert Hotelbelege: Check-in/Check-out statt Rechnungsdatum für die Timeline verwenden."""
    if not isinstance(data, dict):
        return data
    art = str(data.get("art_des_dokuments") or "").strip().lower()
    if art != "hotel":
        return data

    segs = data.get("reisesegmente") or []
    first = segs[0] if segs and isinstance(segs[0], dict) else {}

    checkin = (
        parse_hotel_date_to_iso(first.get("abreise_datum_und_zeit"), "23:59")
        or parse_hotel_date_to_iso(data.get("check_in"), "23:59")
        or parse_hotel_date_to_iso(data.get("checkin"), "23:59")
        or parse_hotel_date_to_iso(data.get("check-in"), "23:59")
    )
    checkout = (
        parse_hotel_date_to_iso(first.get("ankunft_datum_und_zeit"), "00:00")
        or parse_hotel_date_to_iso(data.get("check_out"), "00:00")
        or parse_hotel_date_to_iso(data.get("checkout"), "00:00")
        or parse_hotel_date_to_iso(data.get("check-out"), "00:00")
    )

    text_checkin, text_checkout = extract_hotel_dates_from_text(original_text or "")
    checkin = text_checkin or checkin
    checkout = text_checkout or checkout

    if not checkin and not checkout:
        checkin = parse_hotel_date_to_iso(data.get("belegdatum"), "23:59")
        checkout = checkin

    hotel_name = first.get("ankunft_ort") or data.get("hotel") or data.get("anbieter") or data.get("buchungsnummer_code") or "Hotel"
    data["check_in"] = checkin or "nicht vorhanden"
    data["check_out"] = checkout or "nicht vorhanden"
    data["reisesegmente"] = [{
        "index": 1,
        "abreise_datum_und_zeit": checkin or "nicht vorhanden",
        "ankunft_datum_und_zeit": checkout or "nicht vorhanden",
        "abreise_ort": hotel_name,
        "ankunft_ort": hotel_name,
        "transportunternehmen_und_nummer": "Hotelaufenthalt",
    }]
    data["wie_viele_reisesegmente"] = 1
    if not (checkin and checkout):
        data.setdefault("warnungen", []).append("Hotel Check-in/Check-out bitte prüfen")
    return data


# ============================================================
# ANONYMIZATION
# ============================================================

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


def anonymize_emails(text: str) -> str:
    return re.sub(
        r"\b[A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,}\b",
        "abc@123.com",
        text,
        flags=re.IGNORECASE,
    )


def anonymize_employee_names(text: str) -> str:
    anonymized = text
    try:
        mitarbeiter = list_mitarbeiter(limit=5000)
    except TypeError:
        mitarbeiter = list_mitarbeiter()
    except Exception:
        mitarbeiter = []

    for m in mitarbeiter:
        vorname = (m.get("vorname") or "").strip()
        nachname = (m.get("nachname") or "").strip()
        if not vorname or not nachname:
            continue

        for vv in normalize_variants(vorname):
            for nv in normalize_variants(nachname):
                # Titel + Vorname + optional Mittelname + Nachname
                anonymized = re.sub(
                    rf"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+{re.escape(vv)}(?:\s+[A-Za-zÄÖÜäöüß.\-]+)*\s+{re.escape(nv)}",
                    lambda match: f"{match.group(1)} Max Mustermann",
                    anonymized,
                )
                # Vorname + optional Mittelname + Nachname
                anonymized = re.sub(
                    rf"(?i)\b{re.escape(vv)}(?:\s+[A-Za-zÄÖÜäöüß.\-]+)*\s+{re.escape(nv)}",
                    "Max Mustermann",
                    anonymized,
                )
                # Nachname, Vorname
                anonymized = re.sub(
                    rf"(?i)\b{re.escape(nv)}\s*,\s*{re.escape(vv)}(?:\s+[A-Za-zÄÖÜäöüß.\-]+)*",
                    "Max Mustermann",
                    anonymized,
                )
        for nv in normalize_variants(nachname):
            anonymized = re.sub(
                rf"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+{re.escape(nv)}\b",
                lambda match: f"{match.group(1)} Max Mustermann",
                anonymized,
            )

    # Häufige Hotel-/Mail-Formulierungen
    anonymized = re.sub(r"(?i)(Guest\s*name\s*:\s*)[^\r\n]+", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)(Name\s*:\s*)[^\r\n]+", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)(This Marriott\.com reservation email has been forwarded to you by\s+)[^\r\n]+", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)(An\s*:\s*)[^<\r\n]+", r"\1Max Mustermann ", anonymized)
    anonymized = re.sub(r"(?i)(Betreff\s*:\s*)Max Mustermann\s*\([^\)]*\)", r"\1Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)\bMax Mustermann(?:\s+Max Mustermann)+", "Max Mustermann", anonymized)
    anonymized = re.sub(r"(?i)\b(Mr|Mrs|Ms|Herr|Frau)\s+Max Mustermann(?:\s+Max Mustermann)+", r"\1 Max Mustermann", anonymized)
    return anonymized


def anonymize_document_text(text: str) -> str:
    return anonymize_emails(anonymize_employee_names(text or ""))


# ============================================================
# AI ANALYSIS
# ============================================================

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
- Für Flüge/Züge/Taxi: jedes echte Teilsegment separat ausgeben.
- Für Hotel: IMMER genau ein Segment ausgeben: abreise_datum_und_zeit = Check-in-Datum/Uhrzeit, ankunft_datum_und_zeit = Check-out-Datum/Uhrzeit, ankunft_ort = Hotelname + Ort. Das Beleg-/Rechnungsdatum darf bei Hotel NICHT als Check-in verwendet werden, wenn Check-in/Check-out im Dokument stehen.
- Datumswerte bevorzugt als YYYY-MM-DD HH:MM ausgeben. Wenn im Dokument nur TT-MM-JJ steht, trotzdem korrekt als 20JJ-MM-TT interpretieren.
- Zeiten möglichst inklusive Zeitzonenhinweis, falls vorhanden.
- "kosten_mit_steuern" und "kosten_ohne_steuern" getrennt angeben.
- Währung separat angeben.
- Wenn Ticketnummer und Rechnungsnummer vorhanden sind: Ticketnummer in ticketnummer; Rechnungsnummer/Bestätigung in buchungsnummer_code.

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
    return normalize_analysis_dates(base)


# ============================================================
# BELEG STORAGE / DUPLICATES
# ============================================================

def normalize_for_fingerprint(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower())


def build_beleg_fingerprint(filename: str, data: Dict[str, Any], reise_id: Optional[int]) -> str:
    parts = [
        normalize_for_fingerprint(reise_id or "no-reise"),
        normalize_for_fingerprint(data.get("art_des_dokuments")),
        normalize_for_fingerprint(data.get("belegdatum")),
        normalize_for_fingerprint(data.get("buchungsnummer_code")),
        normalize_for_fingerprint(data.get("ticketnummer")),
        normalize_for_fingerprint(data.get("kosten_mit_steuern")),
        normalize_for_fingerprint(data.get("waehrung_der_kosten")),
    ]
    # filename bewusst nur schwach: gleicher Beleg als Mail/PDF soll eher als Duplikat erkannt werden.
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


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
    generated_pdf_path: Optional[str] = None,
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
                           original_content_type = COALESCE(%s, original_content_type),
                           generated_pdf_path = COALESCE(%s, generated_pdf_path)
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
                        generated_pdf_path,
                        beleg_id,
                    ),
                )
            conn.commit()
    except Exception:
        pass


def update_beleg_generated_pdf_path(beleg_id: int, generated_pdf_path: str) -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("UPDATE belege SET generated_pdf_path = %s WHERE id = %s", (generated_pdf_path, beleg_id))
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
                    SELECT DISTINCT b.*
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


def safe_attach_beleg_to_event(event_id: int, beleg_id: int) -> None:
    """Verknüpft Beleg und Event nur einmal.
    Wichtig für Version 8.0: erkannte Duplikate dürfen nicht mehrfach in der Reiseseite erscheinen.
    """
    if not event_id or not beleg_id:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM event_belege WHERE event_id = %s AND beleg_id = %s LIMIT 1",
                    (event_id, beleg_id),
                )
                if cur.fetchone():
                    return
        safe_attach_beleg_to_event(event_id, beleg_id)
    except Exception:
        # Fallback: bestehende Datenbankfunktion verwenden, falls die Prüfung wegen alter Struktur scheitert.
        try:
            safe_attach_beleg_to_event(event_id, beleg_id)
        except Exception:
            pass


def cleanup_unused_original_file(original_file_info: Optional[Dict[str, str]]) -> None:
    """Löscht frisch gespeicherte Originaldateien, wenn nach der Analyse ein Duplikat erkannt wurde.
    Der zuerst gespeicherte Originalbeleg bleibt erhalten.
    """
    try:
        path = (original_file_info or {}).get("original_file_path")
        if path and os.path.exists(path):
            os.remove(path)
    except Exception:
        pass


# ============================================================
# EVENT MATCHING: mehrere Belege pro Event
# ============================================================

def event_key_from_analysis(data: Dict[str, Any]) -> str:
    art = normalize_for_fingerprint(data.get("art_des_dokuments"))
    code = normalize_for_fingerprint(data.get("buchungsnummer_code"))
    ticket = normalize_for_fingerprint(data.get("ticketnummer"))
    segs = data.get("reisesegmente") or []
    start = ""
    end = ""
    place = ""
    if segs and isinstance(segs[0], dict):
        start = normalize_for_fingerprint(segs[0].get("abreise_datum_und_zeit"))
        end = normalize_for_fingerprint(segs[-1].get("ankunft_datum_und_zeit"))
        place = normalize_for_fingerprint(segs[0].get("ankunft_ort") or segs[0].get("abreise_ort"))
    return "|".join([art, code, ticket, start, end, place])


def find_matching_event_for_reise(reise_id: int, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Sucht vorhandenes Event zur Reise.
    7.27: nicht mehr nur nach Typ, sondern nach Analyse-Key. Dadurch können mehrere Hotels/Flüge in einer Reise sauber getrennt werden,
    aber derselbe Belegtyp mit gleichem Code landet wieder beim vorhandenen Event.
    """
    art = data.get("art_des_dokuments") or "Unbekannt"
    wanted_key = event_key_from_analysis(data)
    try:
        detail = get_reise_detail(reise_id)
        for ev in detail.get("events", []):
            if (ev.get("typ") or "").lower() != art.lower():
                continue
            belege = list_belege_for_event(ev.get("id"))
            for b in belege:
                raw = b.get("analysis_json")
                if not raw:
                    continue
                try:
                    old_data = json.loads(raw) if isinstance(raw, str) else raw
                except Exception:
                    continue
                if event_key_from_analysis(old_data) == wanted_key:
                    return ev
    except Exception:
        return None
    return None


def make_event_title(data: Dict[str, Any]) -> str:
    art = data.get("art_des_dokuments") or "Beleg"
    code = data.get("buchungsnummer_code")
    segs = data.get("reisesegmente") or []
    if art == "Hotel" and segs and isinstance(segs[0], dict):
        hotel = segs[0].get("ankunft_ort") or "Hotel"
        return f"Hotel · {hotel[:90]}"
    if art in {"Flug", "Zug", "Taxi"} and segs:
        first = segs[0] if isinstance(segs[0], dict) else {}
        carrier = first.get("transportunternehmen_und_nummer") or code or art
        return f"{art} · {carrier[:90]}"
    return f"{art} · {code}" if code and code != "nicht vorhanden" else art


def attach_or_create_event_for_analysis(reise_id: int, beleg_id: int, data: dict) -> Dict[str, Any]:
    existing_event = find_matching_event_for_reise(reise_id, data)
    if existing_event:
        safe_attach_beleg_to_event(existing_event["id"], beleg_id)
        update_event_status(existing_event["id"], "abgeschlossen")
        return {
            "matched_event_id": existing_event["id"],
            "matched_event_typ": existing_event.get("typ"),
            "matched_existing_event": True,
        }

    art = data.get("art_des_dokuments", "Unbekannt")
    titel = make_event_title(data)
    new_event_id = create_event({"reise_id": reise_id, "typ": art, "titel": titel, "status": "abgeschlossen"})
    safe_attach_beleg_to_event(new_event_id, beleg_id)
    return {"created_event_id": new_event_id, "matched_event_typ": art, "matched_existing_event": False}


# ============================================================
# PDF FALLBACK GENERATION
# ============================================================

def pdf_escape(value: str) -> str:
    value = (value or "").replace("\r", "")
    value = value.encode("latin-1", errors="replace").decode("latin-1")
    return value.replace("\\", "\\\\").replace("(", "\\(").replace(")", "\\)")


def wrap_text(text: str, width: int = 92, max_lines: int = 300) -> List[str]:
    lines: List[str] = []
    for raw in (text or "").split("\n"):
        raw = raw.rstrip()
        if not raw:
            lines.append("")
            continue
        while len(raw) > width:
            lines.append(raw[:width])
            raw = raw[width:]
            if len(lines) >= max_lines:
                return lines + ["... gekuerzt ..."]
        lines.append(raw)
        if len(lines) >= max_lines:
            return lines + ["... gekuerzt ..."]
    return lines


def make_simple_pdf_bytes(title: str, body: str) -> bytes:
    lines = [title, "", f"Erzeugt: {datetime.utcnow().isoformat()} UTC", ""] + wrap_text(body)
    page_size = 52
    pages = [lines[i : i + page_size] for i in range(0, len(lines), page_size)] or [[title]]
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
            content_parts.append(f"({pdf_escape(line)}) Tj")
            content_parts.append("T*")
        content_parts.append("ET")
        stream = "\n".join(content_parts).encode("latin-1", errors="replace")
        stream_id = add_obj(b"<< /Length " + str(len(stream)).encode() + b" >>\nstream\n" + stream + b"\nendstream")
        page_id = add_obj(
            f"<< /Type /Page /Parent {pages_id} 0 R /MediaBox [0 0 595 842] /Resources << /Font << /F1 {font_id} 0 R >> >> /Contents {stream_id} 0 R >>".encode()
        )
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


def generate_beleg_pdf_file(beleg_id: int, rec: Dict[str, Any]) -> Path:
    cached = rec.get("generated_pdf_path")
    if cached and os.path.exists(cached):
        return Path(cached)

    pdf_path = GENERATED_PDF_DIR / f"beleg_{beleg_id}.pdf"
    title = f"Reisekosten Beleg BE-{beleg_id:04d}"
    body_parts = [
        f"Beleg-ID: BE-{beleg_id:04d}",
        f"Datei/Quelle: {rec.get('source_filename') or rec.get('original_filename') or 'nicht gespeichert'}",
        f"Belegdatum: {rec.get('belegdatum') or 'nicht vorhanden'}",
        f"Art: {rec.get('art') or 'nicht vorhanden'}",
        f"Kosten: {rec.get('kosten') or 'nicht vorhanden'} {rec.get('waehrung') or ''}",
        f"Fingerprint: {rec.get('fingerprint') or 'nicht vorhanden'}",
        "",
        "--- Original / Mail-Text / OCR-Text ---",
        rec.get("original_text") or "Für diesen Beleg wurde kein Originaltext gespeichert. Bitte Beleg erneut einlesen.",
    ]
    pdf_path.write_bytes(make_simple_pdf_bytes(title, "\n".join(body_parts)))
    update_beleg_generated_pdf_path(beleg_id, str(pdf_path))
    return pdf_path


# ============================================================
# REISE STATUS
# ============================================================

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


# ============================================================
# MAIN ANALYZE PIPELINE
# ============================================================

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

    # Aktuell OpenAI als Standard. Mistral kann später wieder ergänzt werden.
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
    data = enhance_hotel_analysis(data, text)
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
        cleanup_unused_original_file(original_file_info)
        update_beleg_extra_data(
            existing_beleg_id,
            fingerprint,
            filename,
            text,
            anonymized_text,
            data,
            None,
            None,
            None,
        )
        if event_id:
            safe_attach_beleg_to_event(event_id, existing_beleg_id)
            update_event_status(event_id, "abgeschlossen")
            data["attached_event_id"] = event_id
        elif reise_id:
            data.update(attach_or_create_event_for_analysis(reise_id, existing_beleg_id, data))
        return data

    beleg_id = insert_beleg(
        {
            "belegdatum": data.get("belegdatum"),
            "art": data.get("art_des_dokuments"),
            "kosten": data.get("kosten_mit_steuern"),
            "waehrung": data.get("waehrung_der_kosten"),
        }
    )
    update_beleg_extra_data(
        beleg_id,
        fingerprint,
        filename,
        text,
        anonymized_text,
        data,
        original_file_info.get("original_file_path"),
        original_file_info.get("original_filename"),
        original_file_info.get("original_content_type"),
    )

    data["beleg_id"] = beleg_id
    data["existing_beleg_id"] = None
    data["duplicate_action"] = "inserted_new_beleg"

    if event_id:
        safe_attach_beleg_to_event(event_id, beleg_id)
        update_event_status(event_id, "abgeschlossen")
        data["attached_event_id"] = event_id
    elif reise_id:
        data.update(attach_or_create_event_for_analysis(reise_id, beleg_id, data))

    return data


# ============================================================
# MAIL
# ============================================================

def extract_plain_text_from_email_message(msg) -> str:
    bodies: List[str] = []
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition") or "")
            if "attachment" in disposition.lower():
                continue
            payload = part.get_payload(decode=True)
            if not payload:
                continue
            decoded = payload.decode(errors="ignore")
            if content_type == "text/plain":
                bodies.append(decoded)
            elif content_type == "text/html" and not bodies:
                text = re.sub(r"<br\s*/?>", "\n", decoded, flags=re.IGNORECASE)
                text = re.sub(r"</p\s*>", "\n", text, flags=re.IGNORECASE)
                text = re.sub(r"<[^>]+>", " ", text)
                bodies.append(text)
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
        raw_email_bytes = msg_data[0][1]
        msg = email.message_from_bytes(raw_email_bytes)
        subject_raw = email.header.decode_header(msg.get("Subject", ""))[0][0]
        subject = subject_raw.decode(errors="ignore") if isinstance(subject_raw, bytes) else str(subject_raw)
        body = extract_plain_text_from_email_message(msg)
        attachments: List[Dict[str, Any]] = []
        try:
            for part in msg.walk() if msg.is_multipart() else []:
                disposition = str(part.get("Content-Disposition") or "").lower()
                filename = part.get_filename()
                if not filename and "attachment" not in disposition:
                    continue
                payload = part.get_payload(decode=True)
                if not payload:
                    continue
                if filename:
                    decoded_name = email.header.decode_header(filename)[0][0]
                    filename = decoded_name.decode(errors="ignore") if isinstance(decoded_name, bytes) else str(decoded_name)
                else:
                    filename = "mail_anhang"
                attachments.append({
                    "filename": safe_filename(filename),
                    "content": payload,
                    "content_type": part.get_content_type() or mimetypes.guess_type(filename)[0] or "application/octet-stream",
                })
        except Exception:
            attachments = []
        if subject_contains and subject_contains.lower() not in subject.lower():
            continue
        mails.append(
            {
                "imap_id": msg_id.decode("ascii", errors="ignore") if isinstance(msg_id, bytes) else str(msg_id),
                "message_id": str(msg.get("Message-ID") or ""),
                "date": str(msg.get("Date") or ""),
                "from": str(msg.get("From") or ""),
                "subject": subject,
                "body": body,
                "preview": body[:500],
                "raw_email": raw_email_bytes.decode("utf-8", errors="replace"),
                "attachments": attachments,
            }
        )
        if len(mails) >= limit:
            break
    try:
        mail.logout()
    except Exception:
        pass
    return mails


def extract_reise_code_from_text(text: str) -> Optional[str]:
    match = re.search(r"\b(\d{2}-\d{3})\b", text or "")
    return match.group(1) if match else None


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


# ============================================================
# DELETE / UPDATE HELPERS
# ============================================================

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



# ============================================================
# MAIL CHECK + VMA HELPERS 8.1
# ============================================================

def mail_key_for_message(m: Dict[str, str]) -> str:
    base = m.get("message_id") or "|".join([m.get("subject", ""), m.get("date", ""), m.get("from", "")])
    return hashlib.sha256(base.encode("utf-8", errors="ignore")).hexdigest()


def mail_already_imported(mail_key: str) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM mail_import_log WHERE mail_key = %s LIMIT 1", (mail_key,))
                return cur.fetchone() is not None
    except Exception:
        return False


def mark_mail_imported(mail_key: str, subject: str, status: str, detail: str = "") -> None:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO mail_import_log (mail_key, subject, imported_at, status, detail)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (mail_key) DO NOTHING
                    """,
                    (mail_key, subject[:500], datetime.utcnow().isoformat(), status, detail[:2000]),
                )
            conn.commit()
    except Exception:
        pass


def check_and_import_mails(limit: int = 20, only_new: bool = True, subject_contains: Optional[str] = None) -> Dict[str, Any]:
    ensure_db_extensions()
    mails = read_latest_mails(limit=limit, subject_contains=subject_contains)
    results = []
    skipped_known = 0
    duplicates = 0
    imported = 0
    errors = 0
    for m in mails:
        key = mail_key_for_message(m)
        if only_new and mail_already_imported(key):
            skipped_known += 1
            continue
        try:
            detected_reise_code = extract_reise_code_from_text(m.get("subject") or "")
            detected_reise = find_reise_by_code(detected_reise_code) if detected_reise_code else None
            detected_reise_id = detected_reise["id"] if detected_reise else None
            mail_results = []
            attachments = m.get("attachments") or []
            if attachments:
                for att in attachments:
                    original_info = save_original_file(att.get("filename") or "mail_anhang", att.get("content") or b"", att.get("content_type"))
                    text = extract_text_from_upload(att.get("filename") or "mail_anhang", att.get("content") or b"")
                    result = analyze_text_internal(
                        text,
                        filename=f"mail:{m.get('subject') or ''}:{att.get('filename') or 'Anhang'}",
                        reise_id=detected_reise_id,
                        event_id=None,
                        ai_provider_override="openai",
                        ai_model_override=None,
                        original_file_info=original_info,
                    )
                    mail_results.append(result)
                    if result.get("duplicate_detected"):
                        duplicates += 1
                    else:
                        imported += 1
            else:
                original_info = save_original_text_as_file(
                    f"mail_{safe_filename(m.get('subject') or 'ohne_betreff')}.eml",
                    m.get("raw_email") or m.get("body") or "",
                    "message/rfc822",
                )
                result = analyze_text_internal(
                    m.get("body") or "",
                    filename=f"mail:{m.get('subject') or ''}",
                    reise_id=detected_reise_id,
                    event_id=None,
                    ai_provider_override="openai",
                    ai_model_override=None,
                    original_file_info=original_info,
                )
                mail_results.append(result)
                if result.get("duplicate_detected"):
                    duplicates += 1
                else:
                    imported += 1
            mark_mail_imported(key, m.get("subject") or "", "ok", json.dumps({"reise_code": detected_reise_code, "results": len(mail_results)}, ensure_ascii=False))
            results.append({
                "subject": m.get("subject"),
                "detected_reise_code": detected_reise_code,
                "assigned_reise_id": detected_reise_id,
                "attachments": len(attachments),
                "items": [
                    {
                        "duplicate_detected": bool(r.get("duplicate_detected")),
                        "beleg_id": r.get("beleg_id"),
                        "existing_beleg_id": r.get("existing_beleg_id"),
                    }
                    for r in mail_results
                ],
            })
        except Exception as exc:
            errors += 1
            mark_mail_imported(key, m.get("subject") or "", "error", str(exc))
            results.append({"subject": m.get("subject"), "status": "error", "detail": str(exc)})
    return {
        "status": "ok",
        "version": APP_VERSION,
        "checked": len(mails),
        "imported": imported,
        "duplicates": duplicates,
        "skipped_known": skipped_known,
        "errors": errors,
        "results": results,
    }


def vma_amount_for_day(tag: date, start: Optional[date], end: Optional[date], fruehstueck: bool, mittag: bool, abend: bool) -> float:
    base = 28.0
    if start and end and (tag == start or tag == end):
        base = 14.0
    deduction = 0.0
    if fruehstueck:
        deduction += 28.0 * 0.20
    if mittag:
        deduction += 28.0 * 0.40
    if abend:
        deduction += 28.0 * 0.40
    return max(0.0, round(base - deduction, 2))


def ensure_vma_days(reise_id: int) -> None:
    detail = get_reise_detail(reise_id)
    r = detail.get("reise") or {}
    if not r.get("startdatum") or not r.get("enddatum"):
        return
    try:
        start = datetime.fromisoformat(str(r["startdatum"])).date()
        end = datetime.fromisoformat(str(r["enddatum"])).date()
    except Exception:
        return
    if end < start:
        return
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                current = start
                while current <= end:
                    betrag = vma_amount_for_day(current, start, end, False, False, False)
                    cur.execute(
                        """
                        INSERT INTO vma_tage (reise_id, tag, land, ort, fruehstueck, mittag, abend, betrag, notiz, updated_at)
                        VALUES (%s, %s, 'Deutschland', '', FALSE, FALSE, FALSE, %s, '', %s)
                        ON CONFLICT (reise_id, tag) DO NOTHING
                        """,
                        (reise_id, current.isoformat(), betrag, datetime.utcnow().isoformat()),
                    )
                    current += timedelta(days=1)
            conn.commit()
    except Exception:
        pass


def list_vma_days(reise_id: int) -> List[Dict[str, Any]]:
    ensure_vma_days(reise_id)
    detail = get_reise_detail(reise_id)
    r = detail.get("reise") or {}
    try:
        start = datetime.fromisoformat(str(r.get("startdatum"))).date() if r.get("startdatum") else None
        end = datetime.fromisoformat(str(r.get("enddatum"))).date() if r.get("enddatum") else None
    except Exception:
        start = end = None
    rows: List[Dict[str, Any]] = []
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, reise_id, tag, land, ort, fruehstueck, mittag, abend, betrag, notiz FROM vma_tage WHERE reise_id = %s ORDER BY tag", (reise_id,))
                dbrows = cur.fetchall()
                cols = [d[0] for d in cur.description]
        for raw in dbrows:
            row = dict(zip(cols, raw))
            tag = row.get("tag")
            if not isinstance(tag, date):
                try:
                    tag = datetime.fromisoformat(str(tag)).date()
                except Exception:
                    tag = None
            amount = vma_amount_for_day(tag, start, end, bool(row.get("fruehstueck")), bool(row.get("mittag")), bool(row.get("abend"))) if tag else float(row.get("betrag") or 0)
            row["betrag"] = amount
            row["tag"] = tag.isoformat() if tag else str(row.get("tag") or "")
            row["tag_display"] = tag.strftime("%d.%m.%Y") if tag else str(row.get("tag") or "")
            rows.append(row)
    except Exception:
        return []
    return rows


# ============================================================
# ROUTES
# ============================================================

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
        "original_upload_dir": str(ORIGINAL_UPLOAD_DIR),
        "generated_pdf_dir": str(GENERATED_PDF_DIR),
        "duplicate_detection": "active",
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



@app.get("/mail/check")
def mail_check_get(limit: int = Query(default=20, ge=1, le=50), only_new: bool = Query(default=True), subject_contains: Optional[str] = Query(default=None)):
    try:
        return check_and_import_mails(limit=limit, only_new=only_new, subject_contains=subject_contains)
    except Exception as exc:
        return {"status": "error", "detail": str(exc), "version": APP_VERSION}


@app.post("/mail/check")
def mail_check_post(limit: int = Query(default=20, ge=1, le=50), only_new: bool = Query(default=True), subject_contains: Optional[str] = Query(default=None)):
    try:
        return check_and_import_mails(limit=limit, only_new=only_new, subject_contains=subject_contains)
    except Exception as exc:
        return {"status": "error", "detail": str(exc), "version": APP_VERSION}


@app.post("/mail/fetch")
def mail_fetch_alias(limit: int = Query(default=20, ge=1, le=50)):
    return mail_check_post(limit=limit, only_new=True)


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
            result = analyze_text_internal(
                m["body"],
                filename=f"mail:{m['subject']}",
                reise_id=detected_reise_id,
                event_id=None,
                ai_provider_override="openai",
                ai_model_override=None,
                original_file_info=original_info,
            )
            analyzed.append(
                {
                    "subject": m["subject"],
                    "preview": m["preview"],
                    "detected_reise_code": detected_reise_code,
                    "assigned_reise_id": detected_reise_id,
                    "assigned_reise_name": detected_reise["reise_name"] if detected_reise else None,
                    "analysis": result,
                }
            )
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
        ensure_db_extensions()
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
        raise HTTPException(status_code=404, detail="Originaldatei wurde für diesen Beleg noch nicht gespeichert. Bitte Beleg erneut einlesen.")
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

    pdf_path = generate_beleg_pdf_file(beleg_id, rec)
    return FileResponse(str(pdf_path), media_type="application/pdf", filename=f"beleg_{beleg_id}.pdf")


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


@app.get("/reisen/{reise_id}")
def reisen_detail(reise_id: int):
    try:
        detail = get_reise_detail(reise_id)
        return {**detail, **compute_reise_status(detail)}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}



@app.get("/reisen/{reise_id}/vma")
def reisen_vma(reise_id: int):
    try:
        return {"status": "ok", "reise_id": reise_id, "tage": list_vma_days(reise_id)}
    except Exception as exc:
        return {"status": "error", "detail": str(exc)}


@app.post("/reisen/{reise_id}/vma/{vma_id}")
def reisen_vma_update(reise_id: int, vma_id: int, payload: Dict[str, Any]):
    try:
        fr = bool(payload.get("fruehstueck"))
        mi = bool(payload.get("mittag"))
        ab = bool(payload.get("abend"))
        land = str(payload.get("land") or "Deutschland")
        ort = str(payload.get("ort") or "")
        notiz = str(payload.get("notiz") or "")
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT tag FROM vma_tage WHERE id = %s AND reise_id = %s", (vma_id, reise_id))
                row = cur.fetchone()
                if not row:
                    raise HTTPException(status_code=404, detail="VMA-Tag nicht gefunden")
                cur.execute(
                    """
                    UPDATE vma_tage
                       SET land=%s, ort=%s, fruehstueck=%s, mittag=%s, abend=%s, notiz=%s, updated_at=%s
                     WHERE id=%s AND reise_id=%s
                    """,
                    (land, ort, fr, mi, ab, notiz, datetime.utcnow().isoformat(), vma_id, reise_id),
                )
            conn.commit()
        return {"status": "ok", "tage": list_vma_days(reise_id)}
    except HTTPException:
        raise
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
        return {
            **detail,
            "belege": belege_items,
            "beleg_pdf_urls": [
                {"beleg_id": b.get("id"), "pdf_url": f"/belege/{b.get('id')}/pdf", "original_url": f"/belege/{b.get('id')}/original"}
                for b in belege_items
                if b.get("id")
            ],
        }
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
        text = extract_text_from_upload(file.filename or "upload", content)
        return {"status": "ok", "filename": file.filename or "upload", "anonymized_text": anonymize_document_text(text)}
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@app.post("/analyze/text")
def analyze_text(payload: AnalyzeTextRequest):
    try:
        original_info = save_original_text_as_file(payload.filename or "text-input.txt", payload.text, "text/plain")
        return analyze_text_internal(
            payload.text,
            payload.filename or "text-input.txt",
            payload.reise_id,
            payload.event_id,
            payload.ai_provider,
            payload.ai_model,
            original_file_info=original_info,
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
        original_info = save_original_file(file.filename or "upload", content, file.content_type)
        text = extract_text_from_upload(file.filename or "upload", content)
        return analyze_text_internal(
            text,
            file.filename or "upload",
            reise_id,
            event_id,
            ai_provider,
            ai_model,
            original_file_info=original_info,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
