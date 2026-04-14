from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
import os
import psycopg2
import imaplib
import email
from email.header import decode_header
import re
import json
import requests
import boto3
import pdfplumber
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas

APP_VERSION = "6.5"

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

DATABASE_URL = os.getenv("DATABASE_URL")
IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_REGION = os.getenv("S3_REGION")

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_CHAT_URL = "https://api.mistral.ai/v1/chat/completions"
MISTRAL_MODEL = "mistral-small-latest"


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION,
    )


def ensure_schema():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mail_messages (
            id SERIAL PRIMARY KEY,
            mail_uid TEXT UNIQUE
        )
    """)
    for sql in [
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS sender TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS subject TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS body TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS trip_code TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS document_group TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS detected_type TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS detected_role TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS detected_destination TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS ai_json TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS confidence TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS review_flag TEXT",
        "ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now()",
    ]:
        cur.execute(sql)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trip_events (
            id SERIAL PRIMARY KEY,
            trip_code TEXT,
            event_code TEXT,
            event_type TEXT,
            event_status TEXT,
            event_key TEXT,
            created_at TIMESTAMP DEFAULT now()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mail_attachments (
            id SERIAL PRIMARY KEY,
            mail_uid TEXT
        )
    """)
    for sql in [
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS trip_code TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS event_code TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS original_filename TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS saved_filename TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS content_type TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS file_path TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS document_group TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_type TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_role TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS extracted_text TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS original_amount TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS original_currency TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS eur_amount_display TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS eur_amount_final TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS fx_status TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_date TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_vendor TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS analysis_status TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS storage_key TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS confidence TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS review_flag TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS duplicate_flag TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS ai_json TEXT",
        "ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now()",
    ]:
        cur.execute(sql)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trip_meta (
            trip_code TEXT PRIMARY KEY,
            hotel_mode TEXT
        )
    """)

    conn.commit()
    cur.close()
    conn.close()


def extract_trip_code(text: str):
    match = re.search(r"\b\d{2}-\d{3}\b", text or "")
    return match.group(0) if match else None


def decode_mime_header(value):
    if not value:
        return ""
    decoded_parts = decode_header(value)
    result = []
    for part, encoding in decoded_parts:
        if isinstance(part, bytes):
            result.append(part.decode(encoding or "utf-8", errors="ignore"))
        else:
            result.append(part)
    return "".join(result)


def sanitize_filename(name: str):
    name = (name or "").replace("\\", "_").replace("/", "_").strip()
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name)
    return name[:180] if name else "attachment.bin"


def is_supported_analysis_file(filename: str):
    return (filename or "").lower().endswith(".pdf")


def extract_text_from_s3_object(storage_key: str, filename: str):
    try:
        s3 = get_s3()
        response = s3.get_object(Bucket=S3_BUCKET, Key=storage_key)
        file_bytes = response["Body"].read()

        if filename.lower().endswith(".pdf"):
            text = ""
            with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                for page in pdf.pages:
                    text += (page.extract_text() or "") + "\n"
            text = text.strip()
            return text[:30000] if text else "KEIN_TEXT_GEFUNDEN"

        return "NICHT_ANALYSIERBAR"
    except Exception as e:
        return f"ERROR: {e}"


def handle_currency(amount_str: str, currency: str):
    if not amount_str:
        return "", "", "", "manuelle_korrektur_offen"
    normalized_currency = (currency or "").upper().replace("₹", "INR")
    if normalized_currency == "EUR":
        return amount_str, amount_str, amount_str, "direkt_eur"
    return "", "", "", "manuelle_korrektur_offen"


def maybe_mark_duplicate(cur, trip_code, detected_type, original_amount, detected_date, detected_vendor, attachment_id):
    if not original_amount or not detected_date or not detected_vendor:
        return ""
    cur.execute(
        """
        SELECT COUNT(*)
        FROM mail_attachments
        WHERE COALESCE(trip_code, '') = COALESCE(%s, '')
          AND COALESCE(detected_type, '') = COALESCE(%s, '')
          AND COALESCE(original_amount, '') = COALESCE(%s, '')
          AND COALESCE(detected_date, '') = COALESCE(%s, '')
          AND COALESCE(detected_vendor, '') = COALESCE(%s, '')
          AND id <> %s
        """,
        (trip_code, detected_type, original_amount, detected_date, detected_vendor, attachment_id),
    )
    return "ja" if cur.fetchone()[0] > 0 else ""


def assign_event(cur, trip_code, document_group, detected_type, booking_code, person_name, vendor_name):
    if not trip_code:
        return None

    event_anchor = booking_code or person_name or vendor_name or "generic"
    event_key = f"{trip_code}_{document_group}_{detected_type}_{event_anchor}"

    cur.execute(
        "SELECT event_code FROM trip_events WHERE trip_code=%s AND event_type=%s AND event_key=%s",
        (trip_code, detected_type, event_key),
    )
    row = cur.fetchone()
    if row:
        return row[0]

    cur.execute("SELECT COUNT(*) FROM trip_events WHERE trip_code=%s", (trip_code,))
    event_code = f"{cur.fetchone()[0] + 1:02d}"
    cur.execute(
        "INSERT INTO trip_events (trip_code, event_code, event_type, event_status, event_key) VALUES (%s,%s,%s,%s,%s)",
        (trip_code, event_code, detected_type, "in_planung", event_key),
    )
    return event_code


def schema_classification():
    return {
        "name": "document_classification",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "document_group": {
                    "type": "string",
                    "enum": ["transport", "accommodation", "restaurant", "supporting_document", "unknown"],
                },
                "document_type": {"type": "string"},
                "document_role": {
                    "type": "string",
                    "enum": ["booking_confirmation", "itinerary", "invoice", "receipt", "calendar_entry", "information_only", "unknown"],
                },
                "confidence": {"type": "string", "enum": ["hoch", "mittel", "niedrig"]},
            },
            "required": ["document_group", "document_type", "document_role", "confidence"],
        },
    }


def schema_transport():
    return {
        "name": "transport_details",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "person_name": {"type": "string"},
                "booking_code": {"type": "string"},
                "ticket_number": {"type": "string"},
                "document_date": {"type": "string"},
                "currency": {"type": "string"},
                "total_amount": {"type": "string"},
                "segments": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "departure_datetime": {"type": "string"},
                            "arrival_datetime": {"type": "string"},
                            "departure_location": {"type": "string"},
                            "arrival_location": {"type": "string"},
                            "provider": {"type": "string"},
                            "transport_number": {"type": "string"},
                            "transport_mode": {"type": "string", "enum": ["Flug", "Zug", "Sonstiger Transport", "unknown"]},
                        },
                        "required": [
                            "departure_datetime",
                            "arrival_datetime",
                            "departure_location",
                            "arrival_location",
                            "provider",
                            "transport_number",
                            "transport_mode",
                        ],
                    },
                },
                "line_items": {"type": "array", "items": {"type": "string"}},
                "extras": {"type": "object", "additionalProperties": True},
                "confidence": {"type": "string", "enum": ["hoch", "mittel", "niedrig"]},
                "review_flag": {"type": "string", "enum": ["ok", "pruefen"]},
            },
            "required": [
                "person_name",
                "booking_code",
                "ticket_number",
                "document_date",
                "currency",
                "total_amount",
                "segments",
                "line_items",
                "extras",
                "confidence",
                "review_flag",
            ],
        },
    }


def schema_accommodation():
    return {
        "name": "accommodation_details",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "person_name": {"type": "string"},
                "booking_code": {"type": "string"},
                "hotel_name": {"type": "string"},
                "location": {"type": "string"},
                "checkin_date": {"type": "string"},
                "checkout_date": {"type": "string"},
                "nights": {"type": "integer"},
                "currency": {"type": "string"},
                "total_amount": {"type": "string"},
                "taxes": {"type": "string"},
                "line_items": {"type": "array", "items": {"type": "string"}},
                "extras": {"type": "object", "additionalProperties": True},
                "confidence": {"type": "string", "enum": ["hoch", "mittel", "niedrig"]},
                "review_flag": {"type": "string", "enum": ["ok", "pruefen"]},
            },
            "required": [
                "person_name",
                "booking_code",
                "hotel_name",
                "location",
                "checkin_date",
                "checkout_date",
                "nights",
                "currency",
                "total_amount",
                "taxes",
                "line_items",
                "extras",
                "confidence",
                "review_flag",
            ],
        },
    }


def schema_restaurant():
    return {
        "name": "restaurant_details",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "vendor": {"type": "string"},
                "location": {"type": "string"},
                "document_date": {"type": "string"},
                "currency": {"type": "string"},
                "total_amount": {"type": "string"},
                "tax_amount": {"type": "string"},
                "line_items": {"type": "array", "items": {"type": "string"}},
                "extras": {"type": "object", "additionalProperties": True},
                "confidence": {"type": "string", "enum": ["hoch", "mittel", "niedrig"]},
                "review_flag": {"type": "string", "enum": ["ok", "pruefen"]},
            },
            "required": [
                "vendor",
                "location",
                "document_date",
                "currency",
                "total_amount",
                "tax_amount",
                "line_items",
                "extras",
                "confidence",
                "review_flag",
            ],
        },
    }


def schema_supporting():
    return {
        "name": "supporting_document_details",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "description": {"type": "string"},
                "dates": {"type": "string"},
                "locations": {"type": "string"},
                "line_items": {"type": "array", "items": {"type": "string"}},
                "extras": {"type": "object", "additionalProperties": True},
                "confidence": {"type": "string", "enum": ["hoch", "mittel", "niedrig"]},
                "review_flag": {"type": "string", "enum": ["ok", "pruefen"]},
            },
            "required": ["description", "dates", "locations", "line_items", "extras", "confidence", "review_flag"],
        },
    }


def mistral_request(messages, schema):
    if not MISTRAL_API_KEY:
        return {"error": "MISTRAL_API_KEY fehlt"}

    payload = {
        "model": MISTRAL_MODEL,
        "messages": messages,
        "temperature": 0,
        "response_format": {"type": "json_schema", "json_schema": schema},
    }
    headers = {"Authorization": f"Bearer {MISTRAL_API_KEY}", "Content-Type": "application/json"}

    try:
        response = requests.post(MISTRAL_CHAT_URL, headers=headers, json=payload, timeout=90)
        response.raise_for_status()
        data = response.json()
        content = data["choices"][0]["message"]["content"]
        if isinstance(content, list):
            content = "".join(c.get("text", "") if isinstance(c, dict) else str(c) for c in content)
        return json.loads(content)
    except Exception as e:
        return {"error": str(e)}


def mistral_classify(document_text: str, source_type: str):
    system_prompt = (
        "Du klassifizierst Dokumente für ein Reisekosten-System. "
        "Arbeite vollständig allgemeingültig ohne feste Listen von Städten, Airlines oder Anbietern. "
        "Bestimme document_group, document_type und document_role. "
        "Transport ist jede Fortbewegung von A nach B. "
        "Unter Transport darf das spätere Detailmodell Flug, Zug oder sonstigen Transport unterscheiden. "
        "Wenn ein Dokument von einer Airline stammt, aber ein Abschnitt durch Deutsche Bahn, Bahn, Rail oder Train ausgeführt wird, darf dieser Abschnitt später Zug sein. "
        "Wenn unsicher, verwende unknown."
    )
    user_prompt = f"Quelle: {source_type}\n\nText:\n{document_text[:20000]}"
    return mistral_request(
        [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_prompt}],
        schema_classification(),
    )


def mistral_extract_by_group(document_text: str, source_type: str, document_group: str):
    if document_group == "transport":
        prompt = (
            "Extrahiere Details aus einem Transport-Dokument. "
            "Für jedes Segment MUSS transport_mode gesetzt werden. "
            "Regeln: Flug bei Luftverkehr mit Flugnummer, Flughafen oder Airline. "
            "Zug bei Deutsche Bahn, DB, Bahn, Train, Rail oder ausdrücklich bahnausgeführtem Segment. "
            "Sonstiger Transport für Taxi, Uber, Shuttle, Transfer, Mietwagen oder ähnliche Einzeltransporte. "
            "Ein Dokument kann mehrere Segmente enthalten. Jedes Segment separat klassifizieren. "
            "Keine Währungsumrechnung."
        )
        schema = schema_transport()
    elif document_group == "accommodation":
        prompt = (
            "Extrahiere Details aus einem Unterkunftsdokument. "
            "Arbeite allgemeingültig für Hotel, Airbnb und sonstige Unterkunft. "
            "Check-in, Check-out, Nächte, Gesamtbetrag, Steuern und Zusatzpositionen erfassen."
        )
        schema = schema_accommodation()
    elif document_group == "restaurant":
        prompt = (
            "Extrahiere Details aus einem Restaurant- oder Verpflegungsbeleg. "
            "Arbeite allgemeingültig. Gesamtbetrag, Steuer oder MwSt und optionale Positionen erfassen."
        )
        schema = schema_restaurant()
    else:
        prompt = "Extrahiere nur Zeitdaten, Orte und Kontext aus einem unterstützenden Dokument. Keine Annahmen."
        schema = schema_supporting()

    return mistral_request(
        [{"role": "system", "content": prompt}, {"role": "user", "content": f"Quelle: {source_type}\n\nText:\n{document_text[:20000]}"}],
        schema,
    )


def heuristic_classify(text: str, filename: str = ""):
    combined = f"{filename} {text or ''}".lower()
    if filename.lower().endswith(".ics"):
        return {"document_group": "supporting_document", "document_type": "Kalendereintrag", "document_role": "calendar_entry", "confidence": "mittel"}
    if any(x in combined for x in ["hotel", "airbnb", "reservation", "check-in", "check-out"]):
        return {"document_group": "accommodation", "document_type": "Unterkunft", "document_role": "booking_confirmation", "confidence": "niedrig"}
    if any(x in combined for x in ["restaurant", "meal", "essen", "verpflegung", "cafe", "bar"]):
        return {"document_group": "restaurant", "document_type": "Restaurant", "document_role": "receipt", "confidence": "niedrig"}
    if any(x in combined for x in ["flight", "flug", "train", "bahn", "rail", "db", "uber", "taxi", "shuttle", "transfer", "rental"]):
        return {"document_group": "transport", "document_type": "Transport", "document_role": "unknown", "confidence": "niedrig"}
    return {"document_group": "unknown", "document_type": "Unbekannt", "document_role": "unknown", "confidence": "niedrig"}


def heuristic_extract(text: str, document_group: str):
    date = ""
    for p in [r"\b\d{2}[./]\d{2}[./]\d{4}\b", r"\b\d{1,2}\s+[A-Za-zäöüÄÖÜ]+\s+\d{4}\b"]:
        m = re.search(p, text or "")
        if m:
            date = m.group(0)
            break

    amounts = re.findall(r"\b\d{1,3}(?:\.\d{3})*,\d{2}\b", text or "")
    amount = amounts[-1] if amounts else ""

    currency = "EUR"
    lower = (text or "").lower()
    if " inr" in lower or "₹" in lower:
        currency = "INR"
    elif " usd" in lower or "$" in lower:
        currency = "USD"
    elif " gbp" in lower or "£" in lower:
        currency = "GBP"

    if document_group == "transport":
        mode = "Sonstiger Transport"
        if any(x in lower for x in ["train", "bahn", "db", "rail"]):
            mode = "Zug"
        elif any(x in lower for x in ["flight", "flug", "airport", "airline"]):
            mode = "Flug"
        return {
            "person_name": "",
            "booking_code": "",
            "ticket_number": "",
            "document_date": date,
            "currency": currency,
            "total_amount": amount,
            "segments": [{
                "departure_datetime": "",
                "arrival_datetime": "",
                "departure_location": "",
                "arrival_location": "",
                "provider": "",
                "transport_number": "",
                "transport_mode": mode,
            }],
            "line_items": [],
            "extras": {},
            "confidence": "niedrig",
            "review_flag": "pruefen",
        }
    if document_group == "accommodation":
        return {
            "person_name": "",
            "booking_code": "",
            "hotel_name": "",
            "location": "",
            "checkin_date": "",
            "checkout_date": "",
            "nights": 0,
            "currency": currency,
            "total_amount": amount,
            "taxes": "",
            "line_items": [],
            "extras": {},
            "confidence": "niedrig",
            "review_flag": "pruefen",
        }
    if document_group == "restaurant":
        return {
            "vendor": "",
            "location": "",
            "document_date": date,
            "currency": currency,
            "total_amount": amount,
            "tax_amount": "",
            "line_items": [],
            "extras": {},
            "confidence": "niedrig",
            "review_flag": "pruefen",
        }
    return {
        "description": "",
        "dates": date,
        "locations": "",
        "line_items": [],
        "extras": {},
        "confidence": "niedrig",
        "review_flag": "pruefen",
    }


def normalize_extraction(document_group: str, classification: dict, details: dict):
    detected_type = classification.get("document_type", "Unbekannt") or "Unbekannt"
    detected_role = classification.get("document_role", "unknown") or "unknown"
    confidence = details.get("confidence") or classification.get("confidence", "niedrig") or "niedrig"
    review_flag = details.get("review_flag", "pruefen") or "pruefen"

    booking_code = ""
    person_name = ""
    detected_date = ""
    original_amount = ""
    original_currency = ""
    detected_vendor = ""

    if document_group == "transport":
        booking_code = details.get("booking_code", "")
        person_name = details.get("person_name", "")
        detected_date = details.get("document_date", "")
        original_amount = details.get("total_amount", "")
        original_currency = details.get("currency", "")
        segments = details.get("segments", []) or []
        modes = [s.get("transport_mode") for s in segments if isinstance(s, dict)]
        if "Flug" in modes:
            detected_type = "Flug"
        elif "Zug" in modes:
            detected_type = "Zug"
        else:
            detected_type = "Transport"
        if segments:
            detected_vendor = segments[0].get("provider", "") or ""
    elif document_group == "accommodation":
        booking_code = details.get("booking_code", "")
        person_name = details.get("person_name", "")
        detected_date = details.get("checkin_date", "") or details.get("checkout_date", "")
        original_amount = details.get("total_amount", "")
        original_currency = details.get("currency", "")
        detected_vendor = details.get("hotel_name", "")
        detected_type = "Unterkunft"
    elif document_group == "restaurant":
        detected_date = details.get("document_date", "")
        original_amount = details.get("total_amount", "")
        original_currency = details.get("currency", "")
        detected_vendor = details.get("vendor", "")
        detected_type = "Restaurant"
    else:
        detected_date = details.get("dates", "")
        detected_vendor = details.get("locations", "")

    eur_amount_display, eur_amount_final, _, fx_status = handle_currency(original_amount, original_currency)

    return {
        "document_group": document_group,
        "detected_type": detected_type,
        "detected_role": detected_role,
        "booking_code": booking_code,
        "person_name": person_name,
        "detected_date": detected_date,
        "original_amount": original_amount,
        "original_currency": original_currency,
        "eur_amount_display": eur_amount_display,
        "eur_amount_final": eur_amount_final,
        "fx_status": fx_status,
        "detected_vendor": detected_vendor,
        "confidence": confidence,
        "review_flag": review_flag,
    }


def get_trip_events(cur, trip_code: str):
    cur.execute("SELECT event_code, event_type, event_status FROM trip_events WHERE trip_code=%s ORDER BY event_code", (trip_code,))
    return cur.fetchall()


def classify_document_side(role: str, eur_amount_final: str):
    if (role or "") in ["invoice", "receipt"] or eur_amount_final:
        return "completed"
    return "planning"



@app.get("/")
def dashboard():
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(trip_code, '') AS trip_code, detected_type, COALESCE(eur_amount_final, ''), review_flag, duplicate_flag FROM mail_attachments ORDER BY COALESCE(trip_code, '')")
        rows = cur.fetchall()
        cur.execute("SELECT trip_code, hotel_mode FROM trip_meta")
        hotel_meta = dict(cur.fetchall())

        trips = {}
        for trip_code, detected_type, eur_final, review_flag, duplicate_flag in rows:
            code = trip_code or "(ohne Code)"
            trips.setdefault(code, {"flight": False, "hotel": False, "transport": False, "restaurant": False, "sum_eur": 0.0, "review_count": 0, "duplicate_count": 0})
            if detected_type == "Flug":
                trips[code]["flight"] = True
            elif detected_type in ["Hotel", "Unterkunft"]:
                trips[code]["hotel"] = True
            elif detected_type in ["Zug", "Transport"]:
                trips[code]["transport"] = True
            elif detected_type == "Restaurant":
                trips[code]["restaurant"] = True
            if review_flag == "pruefen":
                trips[code]["review_count"] += 1
            if duplicate_flag == "ja":
                trips[code]["duplicate_count"] += 1
            if eur_final:
                try:
                    trips[code]["sum_eur"] += float(eur_final.replace(".", "").replace(",", "."))
                except Exception:
                    pass

        rows_html = ""
        for code, data in trips.items():
            has_hotel = data["hotel"]
            warnings, errors = [], []
            hotel_note = ""
            if code == "(ohne Code)":
                errors.append("Einträge ohne Reisecode")
            else:
                hotel_mode = hotel_meta.get(code, "")
                if hotel_mode == "customer":
                    has_hotel = True
                    hotel_note = "Kundenhotel"
                elif hotel_mode == "own":
                    hotel_note = "Hotel selbst"
                if data["flight"] and not has_hotel:
                    warnings.append("Hotel fehlt")
            if data["duplicate_count"] > 0:
                warnings.append(f"{data['duplicate_count']} mögliche Dublette(n)")
            status = '<span class="badge-bad">Fehler</span>' if errors else ('<span class="badge-warn">prüfen</span>' if warnings or data["review_count"] > 0 else '<span class="badge-ok">vollständig</span>')
            actions = ""
            if code != "(ohne Code)":
                actions = f'<a class="btn-light" href="/set-hotel?code={code}&mode=customer">Hotel Kunde</a> <a class="btn-light" href="/set-hotel?code={code}&mode=own">Hotel selbst</a> <a class="btn-light" href="/trip/{code}">Ereignisse</a>'
            rows_html += f"<tr><td class='code'>{code}</td><td>{'ja' if data['flight'] else 'nein'}</td><td>{'ja' if has_hotel else 'nein'} {hotel_note}</td><td>{'ja' if data['transport'] else 'nein'}</td><td>{'ja' if data['restaurant'] else 'nein'}</td><td>{data['review_count']}</td><td>{format(data['sum_eur'], '.2f').replace('.', ',')} €</td><td>{', '.join(warnings)}</td><td>{', '.join(errors)}</td><td>{status}</td><td>{actions}</td></tr>"

        cur.close()
        conn.close()
        return HTMLResponse(page_shell("Dashboard", f"<div class='card'><h2>Dashboard 6.5</h2><div class='sub'>Allgemeine Mistral-Klassifikation mit sauberer Trennung: Flug, Zug oder sonstiger Transport.</div><p><a class='btn' href='/fetch-mails'>Mails abrufen</a> <a class='btn' href='/analyze-attachments'>Anhänge analysieren</a> <a class='btn-light' href='/attachment-log'>Anhang Log</a> <a class='btn-light' href='/mail-log'>Mail Log</a> <a class='btn-light' href='/trip-review'>Reisebewertung</a></p><table><tr><th>Code</th><th>Flug</th><th>Hotel</th><th>Transport</th><th>Restaurant</th><th>Offen</th><th>Summe EUR</th><th>Warnungen</th><th>Fehler</th><th>Status</th><th>Aktion</th></tr>{rows_html}</table></div>"))
    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f"<div class='card'><h2>Dashboard-Fehler</h2><p>{str(e)}</p></div>"), status_code=500)


@app.get("/trip/{trip_code}", response_class=HTMLResponse)
def trip_detail(trip_code: str):
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        events = get_trip_events(cur, trip_code)
        rows = "".join([f"<tr><td class='code'>{trip_code}</td><td>{event_code}</td><td>{event_type}</td><td>{event_status}</td><td><a class='btn-light' href='/event/{trip_code}/{event_code}'>Öffnen</a></td></tr>" for event_code, event_type, event_status in events])
        cur.close()
        conn.close()
        return page_shell("Reise", f"<div class='card'><h2>Reise {trip_code}</h2><table><tr><th>Reise</th><th>Ereignis</th><th>Typ</th><th>Status</th><th>Aktion</th></tr>{rows}</table></div>")
    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2>Reise-Fehler</h2><p>{str(e)}</p></div>")


@app.get("/event/{trip_code}/{event_code}", response_class=HTMLResponse)
def event_detail(trip_code: str, event_code: str):
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT id, original_filename, document_group, detected_type, detected_role,
                   original_amount, original_currency, eur_amount_display, eur_amount_final,
                   fx_status, detected_date, detected_vendor, analysis_status
            FROM mail_attachments WHERE trip_code=%s AND event_code=%s ORDER BY id
        """, (trip_code, event_code))
        rows = cur.fetchall()
        planning_blocks, completed_blocks = [], []
        for row in rows:
            attachment_id, filename, document_group, dtype, drole, original_amount, original_currency, eur_display, eur_final, fx_status, detected_date, detected_vendor, analysis_status = row
            side = classify_document_side(drole, eur_final)
            block = f"""
            <div class='card'>
                <b>{dtype}</b><br>
                Gruppe: {document_group or ''}<br>
                Rolle: {drole or ''}<br>
                Datei: {filename}<br>
                Datum: {detected_date or ''}<br>
                Anbieter: {detected_vendor or ''}<br>
                Betrag Original: {original_amount or ''} {original_currency or ''}<br>
                EUR: {eur_final or eur_display or ''}<br>
                FX Status: {fx_status or ''}<br>
                Analyse: {analysis_status or ''}<br><br>
                <a class='btn-light' href='/download-attachment/{attachment_id}'>Original herunterladen</a>
                <a class='btn-light' href='/attachment-ai/{attachment_id}'>AI JSON</a>
            </div>
            """
            if side == "completed":
                completed_blocks.append(block)
            else:
                planning_blocks.append(block)
        cur.close()
        conn.close()
        return page_shell("Ereignis", f"<div class='card'><h2>Reise {trip_code} / Ereignis {event_code}</h2><p><a class='btn' href='/event-pdf/{trip_code}/{event_code}'>Ereignis-PDF herunterladen</a> <a class='btn-light' href='/trip/{trip_code}'>Zur Reise</a></p></div><div class='columns'><div class='col'><div class='card'><h3>Planung</h3>{''.join(planning_blocks) or 'Keine Daten'}</div></div><div class='col'><div class='card'><h3>Abgeschlossen</h3>{''.join(completed_blocks) or 'Keine Daten'}</div></div></div>")
    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2>Ereignis-Fehler</h2><p>{str(e)}</p></div>")


@app.get("/event-pdf/{trip_code}/{event_code}")
def event_pdf(trip_code: str, event_code: str):
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT original_filename, document_group, detected_type, detected_role,
                   original_amount, original_currency, eur_amount_display, eur_amount_final,
                   fx_status, detected_date, detected_vendor, analysis_status
            FROM mail_attachments WHERE trip_code=%s AND event_code=%s ORDER BY id
        """, (trip_code, event_code))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        buffer = BytesIO()
        pdf = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4
        y = height - 50

        def line(text, step=15):
            nonlocal y
            if y < 60:
                pdf.showPage()
                y = height - 50
            pdf.drawString(40, y, text[:115])
            y -= step

        pdf.setTitle(f"Reise_{trip_code}_Ereignis_{event_code}")
        pdf.setFont("Helvetica-Bold", 14)
        line(f"Reise {trip_code} / Ereignis {event_code}", 22)
        pdf.setFont("Helvetica", 10)
        line("Zusammenfassung Ereignis", 18)
        line(" ", 8)

        for row in rows:
            filename, document_group, dtype, drole, original_amount, original_currency, eur_display, eur_final, fx_status, detected_date, detected_vendor, analysis_status = row
            line(f"Dokument: {filename}", 14)
            line(f"Gruppe: {document_group} | Typ: {dtype} | Rolle: {drole}", 14)
            line(f"Datum: {detected_date or ''} | Anbieter: {detected_vendor or ''}", 14)
            line(f"Original: {original_amount or ''} {original_currency or ''}", 14)
            line(f"EUR final: {eur_final or eur_display or ''} | FX: {fx_status or ''}", 14)
            line(f"Status: {analysis_status or ''}", 18)

        pdf.save()
        buffer.seek(0)
        return StreamingResponse(buffer, media_type="application/pdf", headers={"Content-Disposition": f'attachment; filename="reise_{trip_code}_ereignis_{event_code}.pdf"'})
    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f"<div class='card'><h2>Ereignis-PDF-Fehler</h2><p>{str(e)}</p></div>"), status_code=500)


@app.get("/download-attachment/{attachment_id}")
def download_attachment(attachment_id: int):
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT original_filename, storage_key, content_type FROM mail_attachments WHERE id=%s", (attachment_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return HTMLResponse("Datei nicht gefunden", status_code=404)
        original_filename, storage_key, content_type = row
        s3 = get_s3()
        response = s3.get_object(Bucket=S3_BUCKET, Key=storage_key)
        data = response["Body"].read()
        return StreamingResponse(BytesIO(data), media_type=content_type or "application/octet-stream", headers={"Content-Disposition": f'attachment; filename="{original_filename or "download.bin"}"'})
    except Exception as e:
        return HTMLResponse(f"Download-Fehler: {str(e)}", status_code=500)


@app.get("/set-hotel")
def set_hotel(code: str, mode: str):
    ensure_schema()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("INSERT INTO trip_meta (trip_code, hotel_mode) VALUES (%s,%s) ON CONFLICT (trip_code) DO UPDATE SET hotel_mode=%s", (code, mode, mode))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "ok", "code": code, "mode": mode, "version": APP_VERSION}


@app.get("/reset-mail-log")
def reset_mail_log():
    ensure_schema()
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE mail_attachments RESTART IDENTITY")
    cur.execute("TRUNCATE TABLE mail_messages RESTART IDENTITY")
    cur.execute("TRUNCATE TABLE trip_events RESTART IDENTITY")
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "mail log, events und anhaenge geloescht", "version": APP_VERSION}


@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    try:
        ensure_schema()
        s3 = get_s3()
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")
        _, data = mail.search(None, "ALL")
        ids = data[0].split()[-20:]
        conn = get_conn()
        cur = conn.cursor()
        imported = skipped = attachment_count = ai_processed_emails = 0

        for i in ids:
            uid = i.decode()
            cur.execute("SELECT id FROM mail_messages WHERE mail_uid=%s", (uid,))
            if cur.fetchone():
                skipped += 1
                continue

            _, msg_data = mail.fetch(i, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])
            subject = decode_mime_header(msg.get("Subject", ""))
            sender = decode_mime_header(msg.get("From", ""))
            body = ""

            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition") or "")
                    if content_type == "text/plain" and "attachment" not in content_disposition.lower():
                        payload = part.get_payload(decode=True)
                        if payload:
                            body = payload.decode(errors="ignore")
                            break
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode(errors="ignore")

            full_text = subject + "\n" + body
            code = extract_trip_code(full_text)

            classification = heuristic_classify(full_text)
            details = heuristic_extract(full_text, classification.get("document_group", "unknown"))

            if body and len(body) > 80:
                c2 = mistral_classify(full_text, "email")
                if "error" not in c2:
                    classification = c2
                d2 = mistral_extract_by_group(full_text, "email", classification.get("document_group", "unknown"))
                if "error" not in d2:
                    details = d2
                    ai_processed_emails += 1

            normalized = normalize_extraction(classification.get("document_group", "unknown"), classification, details)

            cur.execute(
                """INSERT INTO mail_messages
                (mail_uid, sender, subject, body, trip_code, document_group, detected_type, detected_role,
                 detected_destination, ai_json, confidence, review_flag)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (
                    uid, sender, subject, body, code, normalized["document_group"], normalized["detected_type"], normalized["detected_role"],
                    "", json.dumps({"classification": classification, "details": details}, ensure_ascii=False), normalized["confidence"], normalized["review_flag"],
                ),
            )

            if msg.is_multipart():
                for part in msg.walk():
                    filename = part.get_filename()
                    content_disposition = str(part.get("Content-Disposition") or "")
                    if not filename and "attachment" not in content_disposition.lower():
                        continue

                    if filename:
                        decoded_filename = decode_mime_header(filename)
                    else:
                        ext = ".bin"
                        part_type = part.get_content_type()
                        if part_type == "application/pdf":
                            ext = ".pdf"
                        elif part_type.startswith("image/jpeg"):
                            ext = ".jpg"
                        elif part_type.startswith("image/png"):
                            ext = ".png"
                        elif part_type.startswith("image/webp"):
                            ext = ".webp"
                        elif part_type == "text/calendar":
                            ext = ".ics"
                        decoded_filename = f"attachment{ext}"

                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue

                    safe_original = sanitize_filename(decoded_filename)
                    saved_filename = f"{uid}_{safe_original}"
                    storage_key = f"mail_attachments/{saved_filename}"

                    s3.put_object(Bucket=S3_BUCKET, Key=storage_key, Body=payload, ContentType=part.get_content_type() or "application/octet-stream")

                    cur.execute(
                        """INSERT INTO mail_attachments
                        (mail_uid, trip_code, original_filename, saved_filename, content_type, file_path,
                         document_group, detected_type, detected_role, analysis_status, storage_key,
                         confidence, review_flag, duplicate_flag, ai_json)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (uid, code, safe_original, saved_filename, part.get_content_type(), storage_key, "", "", "", "neu", storage_key, "niedrig", "pruefen", "", ""),
                    )
                    attachment_count += 1
            imported += 1

        conn.commit()
        cur.close()
        conn.close()
        mail.logout()

        return page_shell("Mails importiert", f"<div class='card'><h2>Mailabruf erfolgreich</h2><p><b>Neu importierte Mails:</b> {imported}</p><p><b>Übersprungen:</b> {skipped}</p><p><b>Gespeicherte Anhänge im Bucket:</b> {attachment_count}</p><p><b>Mit Mistral analysierte E-Mails:</b> {ai_processed_emails}</p><a class='btn' href='/'>Zum Dashboard</a> <a class='btn-light' href='/attachment-log'>Zum Anhang Log</a></div>")
    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2>Fehler beim Mailabruf</h2><p>{str(e)}</p></div>")


@app.get("/analyze-attachments", response_class=HTMLResponse)
def analyze_attachments():
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT id, trip_code, storage_key, original_filename FROM mail_attachments ORDER BY id")
        rows = cur.fetchall()
        processed = 0
        ai_processed = 0

        for attachment_id, trip_code, storage_key, original_filename in rows:
            storage_key = storage_key or ""
            original_filename = original_filename or ""

            if not storage_key:
                cur.execute("UPDATE mail_attachments SET extracted_text=%s, analysis_status=%s, confidence=%s, review_flag=%s WHERE id=%s", ("KEIN_STORAGE_KEY", "kein storage key", "niedrig", "pruefen", attachment_id))
                processed += 1
                continue

            if not is_supported_analysis_file(original_filename):
                cur.execute("UPDATE mail_attachments SET extracted_text=%s, analysis_status=%s, confidence=%s, review_flag=%s WHERE id=%s", ("NICHT_ANALYSIERBAR", "nicht analysierbar", "niedrig", "pruefen", attachment_id))
                processed += 1
                continue

            text = extract_text_from_s3_object(storage_key, original_filename)
            status = "ok"
            if text == "NICHT_ANALYSIERBAR":
                status = "nicht analysierbar"
            elif text.startswith("ERROR:"):
                status = "analysefehler"
            elif text == "KEIN_TEXT_GEFUNDEN":
                status = "kein text"

            document_group = "unknown"
            detected_type = "Unbekannt"
            detected_role = "unknown"
            original_amount = ""
            original_currency = ""
            eur_amount_display = ""
            eur_amount_final = ""
            detected_date = ""
            detected_vendor = ""
            fx_status = "manuelle_korrektur_offen"
            confidence = "niedrig"
            review_flag = "pruefen"
            duplicate_flag = ""
            event_code = None
            ai_payload = {}

            if status == "ok":
                classification = mistral_classify(text, "pdf")
                if "error" in classification:
                    classification = heuristic_classify(text, original_filename)

                details = mistral_extract_by_group(text, "pdf", classification.get("document_group", "unknown"))
                if "error" in details:
                    details = heuristic_extract(text, classification.get("document_group", "unknown"))

                normalized = normalize_extraction(classification.get("document_group", "unknown"), classification, details)

                document_group = normalized["document_group"]
                detected_type = normalized["detected_type"]
                detected_role = normalized["detected_role"]
                original_amount = normalized["original_amount"]
                original_currency = normalized["original_currency"]
                eur_amount_display = normalized["eur_amount_display"]
                eur_amount_final = normalized["eur_amount_final"]
                fx_status = normalized["fx_status"]
                detected_date = normalized["detected_date"]
                detected_vendor = normalized["detected_vendor"]
                confidence = normalized["confidence"]
                review_flag = normalized["review_flag"]

                event_code = assign_event(cur, trip_code, document_group, detected_type, normalized["booking_code"], normalized["person_name"], detected_vendor)
                duplicate_flag = maybe_mark_duplicate(cur, trip_code, detected_type, original_amount, detected_date, detected_vendor, attachment_id)
                ai_payload = {"classification": classification, "details": details}
                ai_processed += 1

            cur.execute(
                """UPDATE mail_attachments
                SET extracted_text=%s, document_group=%s, detected_type=%s, detected_role=%s, event_code=%s,
                    original_amount=%s, original_currency=%s, eur_amount_display=%s, eur_amount_final=%s,
                    fx_status=%s, detected_date=%s, detected_vendor=%s, analysis_status=%s, confidence=%s,
                    review_flag=%s, duplicate_flag=%s, ai_json=%s
                WHERE id=%s""",
                (text, document_group, detected_type, detected_role, event_code, original_amount, original_currency, eur_amount_display, eur_amount_final, fx_status, detected_date, detected_vendor, status, confidence, review_flag, duplicate_flag, json.dumps(ai_payload, ensure_ascii=False), attachment_id),
            )
            processed += 1

        conn.commit()
        cur.close()
        conn.close()
        return page_shell("Analyse", f"<div class='card'><h2>{processed} Anhänge analysiert</h2><p><b>Mit Mistral analysierte PDFs:</b> {ai_processed}</p><a class='btn' href='/'>Zum Dashboard</a> <a class='btn-light' href='/attachment-log'>Zum Anhang Log</a></div>")
    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2>Analyse-Fehler</h2><p>{str(e)}</p></div>")


@app.get("/trip-review", response_class=HTMLResponse)
def trip_review():
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT COALESCE(trip_code, '') AS trip_code FROM mail_attachments GROUP BY COALESCE(trip_code, '') ORDER BY COALESCE(trip_code, '')")
        trip_codes = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT trip_code, hotel_mode FROM trip_meta")
        hotel_meta = dict(cur.fetchall())
        rows_html = ""
        for trip_code in trip_codes:
            cur.execute("SELECT detected_type, analysis_status, review_flag, duplicate_flag FROM mail_attachments WHERE COALESCE(trip_code, '')=%s", (trip_code,))
            items = cur.fetchall()
            has_flight = any(x[0] == "Flug" for x in items)
            has_hotel = any(x[0] in ["Hotel", "Unterkunft"] for x in items)
            has_transport = any(x[0] in ["Zug", "Transport"] for x in items)
            open_reviews = sum(1 for x in items if x[2] == "pruefen")
            duplicates = sum(1 for x in items if x[3] == "ja")
            warnings, errors = [], []
            if trip_code == "":
                errors.append("Einträge ohne Reisecode")
            else:
                if hotel_meta.get(trip_code, "") == "customer":
                    has_hotel = True
                if has_flight and not has_hotel:
                    warnings.append("Hotel fehlt")
            if duplicates > 0:
                warnings.append(f"{duplicates} mögliche Dublette(n)")
            badge = '<span class="badge-bad">Fehler</span>' if errors else ('<span class="badge-warn">prüfen</span>' if open_reviews > 0 or warnings else '<span class="badge-ok">vollständig</span>')
            rows_html += f"<tr><td class='code'>{trip_code or '(ohne Code)'}</td><td>{'ja' if has_flight else 'nein'}</td><td>{'ja' if has_hotel else 'nein'}</td><td>{'ja' if has_transport else 'nein'}</td><td>{open_reviews}</td><td>{', '.join(warnings)}</td><td>{', '.join(errors)}</td><td>{badge}</td></tr>"
        cur.close()
        conn.close()
        return page_shell("Reisebewertung", f"<div class='card'><h2>Reisebewertung v6.5</h2><table><tr><th>Code</th><th>Flug</th><th>Hotel</th><th>Transport</th><th>Offene Prüfungen</th><th>Warnungen</th><th>Fehler</th><th>Status</th></tr>{rows_html}</table></div>")
    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2>Reisebewertung-Fehler</h2><p>{str(e)}</p></div>")


@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT sender, subject, trip_code, document_group, detected_type, detected_role, confidence, review_flag FROM mail_messages ORDER BY id DESC LIMIT 50")
        rows = cur.fetchall()
        html = "".join([f"<tr><td>{r[0] or ''}</td><td>{r[1] or ''}</td><td class='code'>{r[2] or ''}</td><td>{r[3] or ''}</td><td>{r[4] or ''}</td><td>{r[5] or ''}</td><td>{r[6] or ''}</td><td>{r[7] or ''}</td></tr>" for r in rows])
        cur.close()
        conn.close()
        return page_shell("Mail Log", f"<div class='card'><h2>Mail Log</h2><table><tr><th>Von</th><th>Betreff</th><th>Code</th><th>Gruppe</th><th>Typ</th><th>Rolle</th><th>Confidence</th><th>Review</th></tr>{html}</table></div>")
    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2>Mail-Log-Fehler</h2><p>{str(e)}</p></div>")


@app.get("/attachment-log", response_class=HTMLResponse)
def attachment_log():
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT trip_code, event_code, original_filename, document_group, detected_type, detected_role,
                   original_amount, original_currency, eur_amount_final, fx_status,
                   detected_date, detected_vendor, analysis_status, confidence,
                   review_flag, duplicate_flag, storage_key
            FROM mail_attachments ORDER BY id DESC LIMIT 100
        """)
        rows = cur.fetchall()
        html = "".join([f"<tr><td class='code'>{r[0] or ''}</td><td>{r[1] or ''}</td><td>{r[2] or ''}</td><td>{r[3] or ''}</td><td>{r[4] or ''}</td><td>{r[5] or ''}</td><td>{r[6] or ''}</td><td>{r[7] or ''}</td><td>{r[8] or ''}</td><td>{r[9] or ''}</td><td>{r[10] or ''}</td><td>{r[11] or ''}</td><td>{r[12] or ''}</td><td>{r[13] or ''}</td><td>{r[14] or ''}</td><td>{r[15] or ''}</td><td>{r[16] or ''}</td></tr>" for r in rows])
        cur.close()
        conn.close()
        return page_shell("Anhang Log", f"<div class='card'><h2>Anhang Log mit Analyse v6.5</h2><table><tr><th>Code</th><th>Ereignis</th><th>Datei</th><th>Gruppe</th><th>Typ</th><th>Rolle</th><th>Original Betrag</th><th>Original Währung</th><th>EUR final</th><th>FX Status</th><th>Datum</th><th>Anbieter</th><th>Status</th><th>Confidence</th><th>Review</th><th>Dublette</th><th>Storage Key</th></tr>{html}</table></div>")
    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2>Anhang-Log-Fehler</h2><p>{str(e)}</p></div>")


@app.get("/attachment-ai/{attachment_id}", response_class=HTMLResponse)
def attachment_ai(attachment_id: int):
    try:
        ensure_schema()
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT original_filename, ai_json FROM mail_attachments WHERE id=%s", (attachment_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return page_shell("Nicht gefunden", "<div class='card'><h2>Anhang nicht gefunden</h2></div>")
        filename, ai_json = row
        return page_shell("AI JSON", f"<div class='card'><h2>AI JSON für {filename}</h2><pre>{ai_json or ''}</pre></div>")
    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2>AI-JSON-Fehler</h2><p>{str(e)}</p></div>")
