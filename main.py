from fastapi import FastAPI
from fastapi.responses import HTMLResponse
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

APP_VERSION = "5.5"

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
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS sender TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS subject TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS body TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS trip_code TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS detected_type TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS detected_destination TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS ai_json TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS confidence TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS review_flag TEXT")
    cur.execute("ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now()")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mail_attachments (
            id SERIAL PRIMARY KEY,
            mail_uid TEXT
        )
    """)
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS trip_code TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS original_filename TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS saved_filename TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS content_type TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS file_path TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_type TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS extracted_text TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_amount TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_amount_eur TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_currency TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_date TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_vendor TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS analysis_status TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS storage_key TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS confidence TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS review_flag TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS duplicate_flag TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS ai_json TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now()")

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


def detect_mail_type(text: str):
    t = (text or "").lower()

    if any(x in t for x in [
        "flug", "flight", "boarding", "boardingpass", "boarding pass",
        "pnr", "ticket", "airline", "itinerary", "e-ticket", "eticket"
    ]):
        return "Flug"

    if any(x in t for x in [
        "hotel", "booking.com", "check-in", "check out", "check-out",
        "reservation", "zimmer", "accommodation"
    ]):
        return "Hotel"

    if any(x in t for x in ["taxi", "uber", "cab", "ride"]):
        return "Taxi"

    if any(x in t for x in ["bahn", "zug", "train", "ice", "db "]):
        return "Bahn"

    if any(x in t for x in [
        "meal", "restaurant", "verpflegung", "essen",
        "dinner", "lunch", "breakfast", "food"
    ]):
        return "Essen"

    if any(x in t for x in ["mietwagen", "rental car", "car rental", "hertz", "sixt", "avis"]):
        return "Mietwagen"

    return "Unbekannt"


def detect_destination(text: str):
    t = (text or "").lower()
    places = [
        "delhi", "mumbai", "bangalore", "new york", "london", "paris",
        "dubai", "shanghai", "beijing", "tokyo", "singapore", "mexico city",
        "lyon", "frankfurt", "zaq", "zürich", "zurich", "san josé", "san jose"
    ]
    for place in places:
        if place in t:
            return place.title()
    return ""


def sanitize_filename(name: str):
    name = (name or "").replace("\\", "_").replace("/", "_").strip()
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name)
    return name[:180] if name else "attachment.bin"


def detect_attachment_type(filename: str, subject: str, body: str):
    filename = filename or ""
    text = f"{filename} {subject or ''} {body or ''}".lower()

    if filename.lower().endswith(".ics"):
        return "Kalendereintrag"
    if filename.lower().endswith(".emz"):
        return "Inline-Grafik"

    if any(x in text for x in [
        "boarding", "boardingpass", "boarding pass", "eticket",
        "e-ticket", "flight", "flug", "ticket", "pnr", "itinerary"
    ]):
        return "Flug"

    if any(x in text for x in [
        "hotel", "booking", "reservation", "zimmer", "check-in", "check-out"
    ]):
        return "Hotel"

    if any(x in text for x in ["taxi", "uber", "cab", "receipt_", "ride"]):
        return "Taxi"

    if any(x in text for x in ["bahn", "zug", "train", "ice", "db"]):
        return "Bahn"

    if any(x in text for x in [
        "meal", "restaurant", "essen", "verpflegung",
        "breakfast", "lunch", "dinner", "food"
    ]):
        return "Essen"

    if any(x in text for x in ["mietwagen", "rental", "car rental", "hertz", "sixt", "avis"]):
        return "Mietwagen"

    return "Unbekannt"


def is_supported_analysis_file(filename: str):
    f = (filename or "").lower()
    return f.endswith(".pdf")


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


def convert_to_eur(amount_str: str, currency: str):
    if not amount_str:
        return ""
    try:
        amount = float(str(amount_str).replace(".", "").replace(",", "."))
    except Exception:
        return ""

    # Stabile Fallback-Raten in 5.5.
    rates_to_eur = {
        "EUR": 1.0,
        "USD": 0.93,
        "GBP": 1.17,
        "INR": 0.011
    }

    rate = rates_to_eur.get((currency or "EUR").upper(), 1.0)
    eur = round(amount * rate, 2)
    return f"{eur:.2f}".replace(".", ",")


def compute_confidence(detected_type: str, amount: str, date: str, vendor: str, status: str):
    if status != "ok":
        return "niedrig"

    score = 0
    if detected_type and detected_type != "Unbekannt":
        score += 1
    if amount:
        score += 1
    if date:
        score += 1
    if vendor:
        score += 1

    if score >= 4:
        return "hoch"
    if score >= 2:
        return "mittel"
    return "niedrig"


def compute_review_flag(confidence: str, status: str):
    if status in ["analysefehler", "datei fehlt", "kein text", "nicht analysierbar"]:
        return "pruefen"
    if confidence == "niedrig":
        return "pruefen"
    return "ok"


def maybe_mark_duplicate(cur, trip_code, detected_type, detected_amount, detected_date, detected_vendor, attachment_id):
    if not detected_amount or not detected_date or not detected_vendor:
        return ""

    cur.execute("""
        SELECT COUNT(*)
        FROM mail_attachments
        WHERE COALESCE(trip_code, '') = COALESCE(%s, '')
          AND COALESCE(detected_type, '') = COALESCE(%s, '')
          AND COALESCE(detected_amount, '') = COALESCE(%s, '')
          AND COALESCE(detected_date, '') = COALESCE(%s, '')
          AND COALESCE(detected_vendor, '') = COALESCE(%s, '')
          AND id <> %s
    """, (trip_code, detected_type, detected_amount, detected_date, detected_vendor, attachment_id))
    count = cur.fetchone()[0]
    return "ja" if count > 0 else ""


def mistral_schema():
    return {
        "name": "reisekosten_extraction",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "belegdatum": {"type": "string"},
                "art_des_dokuments": {
                    "type": "string",
                    "enum": ["Flug", "Hotel", "Zug", "Taxi", "Essen", "Bahn", "Mietwagen", "Kalendereintrag", "Unbekannt"]
                },
                "buchungsnummer_code": {"type": "string"},
                "name_des_reisenden": {"type": "string"},
                "anzahl_reisesegmente": {"type": "integer"},
                "ticketnummer": {"type": "string"},
                "kosten_mit_steuern": {"type": "string"},
                "waehrung": {"type": "string"},
                "segmente": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "additionalProperties": False,
                        "properties": {
                            "segment_nr": {"type": "integer"},
                            "abreise_datum_zeit": {"type": "string"},
                            "ankunft_datum_zeit": {"type": "string"},
                            "abreise_zeitzone": {"type": "string"},
                            "ankunft_zeitzone": {"type": "string"},
                            "abreiseort": {"type": "string"},
                            "ankunftsort": {"type": "string"},
                            "abreiseort_code": {"type": "string"},
                            "ankunftsort_code": {"type": "string"},
                            "transportunternehmen_oder_unterkunft": {"type": "string"},
                            "transportnummer": {"type": "string"}
                        },
                        "required": [
                            "segment_nr",
                            "abreise_datum_zeit",
                            "ankunft_datum_zeit",
                            "abreise_zeitzone",
                            "ankunft_zeitzone",
                            "abreiseort",
                            "ankunftsort",
                            "abreiseort_code",
                            "ankunftsort_code",
                            "transportunternehmen_oder_unterkunft",
                            "transportnummer"
                        ]
                    }
                },
                "zusatzinfos": {
                    "type": "object",
                    "additionalProperties": True,
                    "properties": {
                        "anzahl_naechte": {"type": "string"},
                        "anzahl_zimmer": {"type": "string"},
                        "anzahl_gaeste": {"type": "string"},
                        "positionsliste": {"type": "array", "items": {"type": "string"}},
                        "hinweise": {"type": "array", "items": {"type": "string"}},
                        "validierungen": {"type": "array", "items": {"type": "string"}}
                    }
                },
                "confidence": {
                    "type": "string",
                    "enum": ["hoch", "mittel", "niedrig"]
                },
                "review_flag": {
                    "type": "string",
                    "enum": ["ok", "pruefen"]
                }
            },
            "required": [
                "belegdatum",
                "art_des_dokuments",
                "buchungsnummer_code",
                "name_des_reisenden",
                "anzahl_reisesegmente",
                "ticketnummer",
                "kosten_mit_steuern",
                "waehrung",
                "segmente",
                "zusatzinfos",
                "confidence",
                "review_flag"
            ]
        }
    }


def call_mistral_structured(document_text: str, source_type: str):
    if not MISTRAL_API_KEY:
        return {
            "error": "MISTRAL_API_KEY fehlt"
        }

    schema = mistral_schema()

    system_prompt = (
        "Du bist ein Extraktionsmodul für ein deutsches Reisekosten-System. "
        "Extrahiere strukturierte Daten nur aus dem übergebenen Dokumenttext. "
        "Erfinde keine Werte. "
        "Wenn ein Feld nicht vorhanden ist, gib leeren String oder leeres Array zurück. "
        "Hotels sollen als Aufenthalt mit einem Segment modelliert werden. "
        "Bei Flug/Zug sollen echte Segmente erkannt werden. "
        "Kosten_mit_steuern soll der Gesamtbetrag sein, wenn vorhanden. "
        "Gib nur JSON zurück, das exakt dem Schema entspricht."
    )

    user_prompt = (
        f"Quelle: {source_type}\n\n"
        "Analysiere den folgenden Text für das Reisekosten-System.\n"
        "Extrahiere:\n"
        "- belegdatum\n"
        "- art_des_dokuments\n"
        "- buchungsnummer_code\n"
        "- name_des_reisenden\n"
        "- anzahl_reisesegmente\n"
        "- ticketnummer\n"
        "- kosten_mit_steuern\n"
        "- waehrung\n"
        "- segmente[]\n"
        "- zusatzinfos{}\n"
        "- confidence\n"
        "- review_flag\n\n"
        "Text:\n"
        f"{document_text[:24000]}"
    )

    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": MISTRAL_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ],
        "temperature": 0,
        "response_format": {
            "type": "json_schema",
            "json_schema": schema
        }
    }

    try:
        response = requests.post(MISTRAL_CHAT_URL, headers=headers, json=payload, timeout=90)
        response.raise_for_status()
        data = response.json()

        content = data["choices"][0]["message"]["content"]
        if isinstance(content, list):
            # defensive handling
            content = "".join([c.get("text", "") if isinstance(c, dict) else str(c) for c in content])

        parsed = json.loads(content)
        return parsed

    except Exception as e:
        return {"error": str(e)}


def heuristic_extract(text: str, detected_type: str):
    """
    Fallback, falls Mistral nicht verfügbar ist.
    """
    date = ""
    patterns = [
        r"\b\d{2}[./]\d{2}[./]\d{4}\b",
        r"\b\d{1,2}\s+[A-Za-zäöüÄÖÜ]+\s+\d{4}\b"
    ]
    for p in patterns:
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

    vendor = ""
    vendor_words = ["uber", "lufthansa", "marriott", "booking", "hotel", "air france", "swiss"]
    for w in vendor_words:
        if w in lower:
            vendor = w.title()
            break

    return {
        "belegdatum": date,
        "art_des_dokuments": detected_type or "Unbekannt",
        "buchungsnummer_code": "",
        "name_des_reisenden": "",
        "anzahl_reisesegmente": 0,
        "ticketnummer": "",
        "kosten_mit_steuern": amount,
        "waehrung": currency,
        "segmente": [],
        "zusatzinfos": {},
        "confidence": "niedrig",
        "review_flag": "pruefen",
        "_vendor_fallback": vendor
    }


def page_shell(title: str, content: str):
    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
        <style>
            body {{
                font-family: Arial;
                margin: 0;
                background: #eef4fb;
            }}
            .topbar {{
                background: #12365f;
                color: white;
                padding: 20px;
                display: flex;
                align-items: center;
                justify-content: space-between;
                gap: 15px;
            }}
            .topbar-left {{
                display: flex;
                align-items: center;
                gap: 15px;
            }}
            .topbar .logo-wrap {{
                background: rgba(255,255,255,0.55);
                padding: 10px 14px;
                border-radius: 12px;
                display: inline-flex;
                align-items: center;
            }}
            .topbar img {{
                height: 60px;
                display: block;
            }}
            .version {{
                font-size: 13px;
                opacity: 0.95;
            }}
            .wrap {{
                padding: 20px;
            }}
            .card {{
                background: white;
                padding: 20px;
                border-radius: 10px;
                margin-bottom: 20px;
            }}
            .btn {{
                background: #2a6ab1;
                color: white;
                padding: 10px 12px;
                border: none;
                border-radius: 6px;
                text-decoration: none;
                display: inline-block;
            }}
            .btn-light {{
                background: white;
                color: #2a6ab1;
                padding: 10px 12px;
                border: 1px solid #b8cbe0;
                border-radius: 6px;
                text-decoration: none;
                display: inline-block;
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
            }}
            th, td {{
                border: 1px solid #d9e2ec;
                padding: 8px;
                text-align: left;
                vertical-align: top;
            }}
            th {{
                background: #f4f7fb;
            }}
            .code {{
                font-weight: bold;
                color: #12365f;
            }}
            .badge-ok {{
                color: white;
                background: #177245;
                padding: 4px 8px;
                border-radius: 999px;
                font-size: 12px;
            }}
            .badge-warn {{
                color: white;
                background: #b46b00;
                padding: 4px 8px;
                border-radius: 999px;
                font-size: 12px;
            }}
            .badge-bad {{
                color: white;
                background: #b3261e;
                padding: 4px 8px;
                border-radius: 999px;
                font-size: 12px;
            }}
            .sub {{
                color: #567;
                font-size: 14px;
            }}
            pre {{
                white-space: pre-wrap;
                word-break: break-word;
                background: #f7f9fc;
                padding: 12px;
                border-radius: 8px;
                border: 1px solid #d9e2ec;
            }}
        </style>
    </head>
    <body>
        <div class="topbar">
            <div class="topbar-left">
                <div class="logo-wrap">
                    <img src="/static/herrhammer-logo.png" alt="Herrhammer Logo">
                </div>
                <h2>Herrhammer Reisekosten</h2>
            </div>
            <div class="version">Version {APP_VERSION}</div>
        </div>
        <div class="wrap">
            {content}
        </div>
    </body>
    </html>
    """


@app.get("/version")
def version():
    return {
        "version": APP_VERSION,
        "mistral_configured": bool(MISTRAL_API_KEY),
        "mistral_model": MISTRAL_MODEL
    }


@app.get("/init")
def init():
    ensure_schema()
    return {
        "status": "ok",
        "version": APP_VERSION,
        "mistral_configured": bool(MISTRAL_API_KEY)
    }


@app.get("/")
def dashboard():
    try:
        ensure_schema()

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT COALESCE(trip_code, '') AS trip_code,
                   detected_type,
                   COALESCE(detected_amount_eur, ''),
                   review_flag,
                   duplicate_flag
            FROM mail_attachments
            ORDER BY COALESCE(trip_code, '')
        """)
        rows = cur.fetchall()

        cur.execute("SELECT trip_code, hotel_mode FROM trip_meta")
        hotel_meta = dict(cur.fetchall())

        trips = {}

        for trip_code, detected_type, amount_eur, review_flag, duplicate_flag in rows:
            code = trip_code or "(ohne Code)"
            if code not in trips:
                trips[code] = {
                    "flight": False,
                    "hotel": False,
                    "taxi": False,
                    "essen": False,
                    "sum_eur": 0.0,
                    "review_count": 0,
                    "duplicate_count": 0
                }

            if detected_type == "Flug":
                trips[code]["flight"] = True
            elif detected_type == "Hotel":
                trips[code]["hotel"] = True
            elif detected_type == "Taxi":
                trips[code]["taxi"] = True
            elif detected_type == "Essen":
                trips[code]["essen"] = True

            if review_flag == "pruefen":
                trips[code]["review_count"] += 1

            if duplicate_flag == "ja":
                trips[code]["duplicate_count"] += 1

            if amount_eur:
                try:
                    trips[code]["sum_eur"] += float(amount_eur.replace(".", "").replace(",", "."))
                except Exception:
                    pass

        table_rows = ""
        for code, data in trips.items():
            has_hotel = data["hotel"]
            warnings = []
            errors = []
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

            if errors:
                status = '<span class="badge-bad">Fehler</span>'
            elif warnings or data["review_count"] > 0:
                status = '<span class="badge-warn">prüfen</span>'
            else:
                status = '<span class="badge-ok">vollständig</span>'

            actions = ""
            if code != "(ohne Code)":
                actions = (
                    f'<a class="btn-light" href="/set-hotel?code={code}&mode=customer">Hotel Kunde</a> '
                    f'<a class="btn-light" href="/set-hotel?code={code}&mode=own">Hotel selbst</a>'
                )

            table_rows += f"""
            <tr>
                <td class="code">{code}</td>
                <td>{"ja" if data["flight"] else "nein"}</td>
                <td>{"ja" if has_hotel else "nein"} {hotel_note}</td>
                <td>{"ja" if data["taxi"] else "nein"}</td>
                <td>{"ja" if data["essen"] else "nein"}</td>
                <td>{data["review_count"]}</td>
                <td>{format(data["sum_eur"], ".2f").replace(".", ",")} €</td>
                <td>{", ".join(warnings)}</td>
                <td>{", ".join(errors)}</td>
                <td>{status}</td>
                <td>{actions}</td>
            </tr>
            """

        cur.close()
        conn.close()

        return HTMLResponse(page_shell("Dashboard", f"""
        <div class="card">
            <h2>Dashboard 5.5</h2>
            <div class="sub">Mistral-Strukturextraktion für PDFs und E-Mail-Texte vorbereitet und aktiv.</div>
            <p>
                <a class="btn" href="/fetch-mails">Mails abrufen</a>
                <a class="btn" href="/analyze-attachments">Anhänge analysieren</a>
                <a class="btn-light" href="/attachment-log">Anhang Log</a>
                <a class="btn-light" href="/mail-log">Mail Log</a>
                <a class="btn-light" href="/trip-review">Reisebewertung</a>
            </p>
            <table>
                <tr>
                    <th>Code</th>
                    <th>Flug</th>
                    <th>Hotel</th>
                    <th>Taxi</th>
                    <th>Essen</th>
                    <th>Offen</th>
                    <th>Summe EUR</th>
                    <th>Warnungen</th>
                    <th>Fehler</th>
                    <th>Status</th>
                    <th>Aktion</th>
                </tr>
                {table_rows}
            </table>
        </div>
        """))
    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f"""
        <div class="card">
            <h2>Dashboard-Fehler</h2>
            <p>{str(e)}</p>
        </div>
        """), status_code=500)


@app.get("/set-hotel")
def set_hotel(code: str, mode: str):
    ensure_schema()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        INSERT INTO trip_meta (trip_code, hotel_mode)
        VALUES (%s,%s)
        ON CONFLICT (trip_code)
        DO UPDATE SET hotel_mode=%s
    """, (code, mode, mode))

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
    conn.commit()
    cur.close()
    conn.close()

    return {"status": "mail log und anhaenge geloescht", "version": APP_VERSION}


@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    try:
        ensure_schema()
        s3 = get_s3()

        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")

        status, data = mail.search(None, "ALL")
        ids = data[0].split()[-20:]

        conn = get_conn()
        cur = conn.cursor()

        imported = 0
        skipped = 0
        attachment_count = 0
        ai_processed_emails = 0

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
            detected_type = detect_mail_type(full_text)
            detected_destination = detect_destination(full_text)

            ai_result = {}
            confidence = "niedrig"
            review_flag = "pruefen"

            # KI nur auf E-Mail-Text anwenden, wenn genug Inhalt da ist
            if body and len(body) > 80:
                ai_result = call_mistral_structured(
                    document_text=full_text,
                    source_type="email"
                )

                if "error" in ai_result:
                    ai_result = heuristic_extract(full_text, detected_type)
                else:
                    detected_type = ai_result.get("art_des_dokuments", detected_type) or detected_type
                    confidence = ai_result.get("confidence", "mittel")
                    review_flag = ai_result.get("review_flag", "pruefen")
                    ai_processed_emails += 1
            else:
                ai_result = heuristic_extract(full_text, detected_type)

            cur.execute("""
                INSERT INTO mail_messages
                (mail_uid, sender, subject, body, trip_code, detected_type, detected_destination, ai_json, confidence, review_flag)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                uid,
                sender,
                subject,
                body,
                code,
                detected_type,
                detected_destination,
                json.dumps(ai_result, ensure_ascii=False),
                confidence,
                review_flag
            ))

            if msg.is_multipart():
                for part in msg.walk():
                    filename = part.get_filename()
                    content_disposition = str(part.get("Content-Disposition") or "")
                    if not filename and "attachment" not in content_disposition.lower():
                        continue

                    if filename:
                        decoded_filename = decode_mime_header(filename)
                    else:
                        ext = ""
                        content_type = part.get_content_type()
                        if content_type == "application/pdf":
                            ext = ".pdf"
                        elif content_type.startswith("image/jpeg"):
                            ext = ".jpg"
                        elif content_type.startswith("image/png"):
                            ext = ".png"
                        elif content_type.startswith("image/webp"):
                            ext = ".webp"
                        elif content_type == "text/calendar":
                            ext = ".ics"
                        else:
                            ext = ".bin"
                        decoded_filename = f"attachment{ext}"

                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue

                    safe_original = sanitize_filename(decoded_filename)
                    saved_filename = f"{uid}_{safe_original}"
                    storage_key = f"mail_attachments/{saved_filename}"

                    s3.put_object(
                        Bucket=S3_BUCKET,
                        Key=storage_key,
                        Body=payload,
                        ContentType=part.get_content_type() or "application/octet-stream"
                    )

                    attachment_type = detect_attachment_type(
                        safe_original,
                        subject,
                        body
                    )

                    cur.execute("""
                        INSERT INTO mail_attachments
                        (mail_uid, trip_code, original_filename, saved_filename, content_type, file_path,
                         detected_type, analysis_status, storage_key, confidence, review_flag, duplicate_flag, ai_json)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        uid,
                        code,
                        safe_original,
                        saved_filename,
                        part.get_content_type(),
                        storage_key,
                        attachment_type,
                        "neu",
                        storage_key,
                        "niedrig",
                        "pruefen",
                        "",
                        ""
                    ))

                    attachment_count += 1

            imported += 1

        conn.commit()
        cur.close()
        conn.close()
        mail.logout()

        return page_shell("Mails importiert", f"""
        <div class="card">
            <h2>Mailabruf erfolgreich</h2>
            <p><b>Neu importierte Mails:</b> {imported}</p>
            <p><b>Übersprungen:</b> {skipped}</p>
            <p><b>Gespeicherte Anhänge im Bucket:</b> {attachment_count}</p>
            <p><b>Mit Mistral analysierte E-Mails:</b> {ai_processed_emails}</p>
            <a class="btn" href="/">Zum Dashboard</a>
            <a class="btn-light" href="/attachment-log">Zum Anhang Log</a>
        </div>
        """)

    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2>Fehler beim Mailabruf</h2>
            <p>{str(e)}</p>
        </div>
        """)


@app.get("/analyze-attachments", response_class=HTMLResponse)
def analyze_attachments():
    try:
        ensure_schema()

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, trip_code, storage_key, original_filename, detected_type
            FROM mail_attachments
            ORDER BY id
        """)
        rows = cur.fetchall()

        processed = 0
        ai_processed = 0

        for row in rows:
            attachment_id = row[0]
            trip_code = row[1]
            storage_key = row[2] or ""
            original_filename = row[3] or ""
            detected_type = row[4] or "Unbekannt"

            if not storage_key:
                status = "kein storage key"
                confidence = "niedrig"
                review_flag = "pruefen"
                duplicate_flag = ""
                cur.execute("""
                    UPDATE mail_attachments
                    SET extracted_text=%s,
                        detected_amount=%s,
                        detected_amount_eur=%s,
                        detected_currency=%s,
                        detected_date=%s,
                        detected_vendor=%s,
                        analysis_status=%s,
                        confidence=%s,
                        review_flag=%s,
                        duplicate_flag=%s
                    WHERE id=%s
                """, (
                    "KEIN_STORAGE_KEY", "", "", "", "", "", status, confidence, review_flag, duplicate_flag, attachment_id
                ))
                processed += 1
                continue

            if not is_supported_analysis_file(original_filename):
                status = "nicht analysierbar"
                confidence = "niedrig"
                review_flag = "pruefen"
                duplicate_flag = ""
                cur.execute("""
                    UPDATE mail_attachments
                    SET extracted_text=%s,
                        detected_amount=%s,
                        detected_amount_eur=%s,
                        detected_currency=%s,
                        detected_date=%s,
                        detected_vendor=%s,
                        analysis_status=%s,
                        confidence=%s,
                        review_flag=%s,
                        duplicate_flag=%s
                    WHERE id=%s
                """, (
                    "NICHT_ANALYSIERBAR", "", "", "", "", "", status, confidence, review_flag, duplicate_flag, attachment_id
                ))
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

            ai_result = {}
            amount = ""
            currency = ""
            amount_eur = ""
            date = ""
            vendor = ""
            confidence = "niedrig"
            review_flag = "pruefen"

            if status == "ok":
                ai_result = call_mistral_structured(
                    document_text=text,
                    source_type="pdf"
                )

                if "error" in ai_result:
                    fallback = heuristic_extract(text, detected_type)
                    ai_result = fallback
                    detected_type = fallback.get("art_des_dokuments", detected_type)
                    amount = fallback.get("kosten_mit_steuern", "")
                    currency = fallback.get("waehrung", "EUR")
                    amount_eur = convert_to_eur(amount, currency)
                    date = fallback.get("belegdatum", "")
                    vendor = fallback.get("_vendor_fallback", "")
                    confidence = fallback.get("confidence", "niedrig")
                    review_flag = fallback.get("review_flag", "pruefen")
                else:
                    detected_type = ai_result.get("art_des_dokuments", detected_type) or detected_type
                    amount = ai_result.get("kosten_mit_steuern", "") or ""
                    currency = ai_result.get("waehrung", "EUR") or "EUR"
                    amount_eur = convert_to_eur(amount, currency)
                    date = ai_result.get("belegdatum", "") or ""
                    segments = ai_result.get("segmente", []) or []
                    if segments:
                        vendor = segments[0].get("transportunternehmen_oder_unterkunft", "") or ""
                    if not vendor:
                        vendor = ""
                    confidence = ai_result.get("confidence", "mittel") or "mittel"
                    review_flag = ai_result.get("review_flag", "pruefen") or "pruefen"
                    ai_processed += 1
            else:
                confidence = "niedrig"
                review_flag = "pruefen"

            duplicate_flag = maybe_mark_duplicate(
                cur, trip_code, detected_type, amount, date, vendor, attachment_id
            )

            cur.execute("""
                UPDATE mail_attachments
                SET extracted_text=%s,
                    detected_type=%s,
                    detected_amount=%s,
                    detected_amount_eur=%s,
                    detected_currency=%s,
                    detected_date=%s,
                    detected_vendor=%s,
                    analysis_status=%s,
                    confidence=%s,
                    review_flag=%s,
                    duplicate_flag=%s,
                    ai_json=%s
                WHERE id=%s
            """, (
                text,
                detected_type,
                amount,
                amount_eur,
                currency,
                date,
                vendor,
                status,
                confidence,
                review_flag,
                duplicate_flag,
                json.dumps(ai_result, ensure_ascii=False),
                attachment_id
            ))

            processed += 1

        conn.commit()
        cur.close()
        conn.close()

        return page_shell("Analyse", f"""
        <div class="card">
            <h2>{processed} Anhänge analysiert</h2>
            <p><b>Mit Mistral analysierte PDFs:</b> {ai_processed}</p>
            <a class="btn" href="/">Zum Dashboard</a>
            <a class="btn-light" href="/attachment-log">Zum Anhang Log</a>
        </div>
        """)

    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2>Analyse-Fehler</h2>
            <p>{str(e)}</p>
        </div>
        """)


@app.get("/trip-review", response_class=HTMLResponse)
def trip_review():
    try:
        ensure_schema()

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT COALESCE(trip_code, '') AS trip_code
            FROM mail_attachments
            GROUP BY COALESCE(trip_code, '')
            ORDER BY COALESCE(trip_code, '')
        """)
        trip_codes = [r[0] for r in cur.fetchall()]

        cur.execute("SELECT trip_code, hotel_mode FROM trip_meta")
        hotel_meta = dict(cur.fetchall())

        rows_html = ""

        for trip_code in trip_codes:
            cur.execute("""
                SELECT detected_type, analysis_status, review_flag, duplicate_flag
                FROM mail_attachments
                WHERE COALESCE(trip_code, '') = %s
            """, (trip_code,))
            items = cur.fetchall()

            has_flight = any(x[0] == "Flug" for x in items)
            has_hotel = any(x[0] == "Hotel" for x in items)
            has_taxi = any(x[0] == "Taxi" for x in items)
            open_reviews = sum(1 for x in items if x[2] == "pruefen")
            duplicates = sum(1 for x in items if x[3] == "ja")

            warnings = []
            errors = []

            if trip_code == "":
                errors.append("Einträge ohne Reisecode")
            else:
                hotel_mode = hotel_meta.get(trip_code, "")
                if hotel_mode == "customer":
                    has_hotel = True
                if has_flight and not has_hotel:
                    warnings.append("Hotel fehlt")

            if duplicates > 0:
                warnings.append(f"{duplicates} mögliche Dublette(n)")

            if errors:
                badge = '<span class="badge-bad">Fehler</span>'
            elif open_reviews > 0 or warnings:
                badge = '<span class="badge-warn">prüfen</span>'
            else:
                badge = '<span class="badge-ok">vollständig</span>'

            rows_html += f"""
            <tr>
                <td class="code">{trip_code or '(ohne Code)'}</td>
                <td>{"ja" if has_flight else "nein"}</td>
                <td>{"ja" if has_hotel else "nein"}</td>
                <td>{"ja" if has_taxi else "nein"}</td>
                <td>{open_reviews}</td>
                <td>{", ".join(warnings) if warnings else ""}</td>
                <td>{", ".join(errors) if errors else ""}</td>
                <td>{badge}</td>
            </tr>
            """

        cur.close()
        conn.close()

        return page_shell("Reisebewertung", f"""
        <div class="card">
            <h2>Reisebewertung v5.5</h2>
            <table>
                <tr>
                    <th>Code</th>
                    <th>Flug</th>
                    <th>Hotel</th>
                    <th>Taxi</th>
                    <th>Offene Prüfungen</th>
                    <th>Warnungen</th>
                    <th>Fehler</th>
                    <th>Status</th>
                </tr>
                {rows_html}
            </table>
        </div>
        """)

    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2>Reisebewertung-Fehler</h2>
            <p>{str(e)}</p>
        </div>
        """)


@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    try:
        ensure_schema()

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT sender, subject, trip_code, detected_type, detected_destination, confidence, review_flag
            FROM mail_messages
            ORDER BY id DESC
            LIMIT 50
        """)
        rows = cur.fetchall()

        html = ""
        for r in rows:
            html += f"""
            <tr>
                <td>{r[0] or ''}</td>
                <td>{r[1] or ''}</td>
                <td class="code">{r[2] or ''}</td>
                <td>{r[3] or ''}</td>
                <td>{r[4] or ''}</td>
                <td>{r[5] or ''}</td>
                <td>{r[6] or ''}</td>
            </tr>
            """

        cur.close()
        conn.close()

        return page_shell("Mail Log", f"""
        <div class="card">
            <h2>Mail Log</h2>
            <table>
                <tr>
                    <th>Von</th>
                    <th>Betreff</th>
                    <th>Code</th>
                    <th>Typ erkannt</th>
                    <th>Ziel erkannt</th>
                    <th>Confidence</th>
                    <th>Review</th>
                </tr>
                {html}
            </table>
        </div>
        """)
    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2>Mail-Log-Fehler</h2>
            <p>{str(e)}</p>
        </div>
        """)


@app.get("/attachment-log", response_class=HTMLResponse)
def attachment_log():
    try:
        ensure_schema()

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT trip_code, original_filename, detected_type, detected_amount, detected_amount_eur,
                   detected_currency, detected_date, detected_vendor, analysis_status,
                   confidence, review_flag, duplicate_flag, storage_key
            FROM mail_attachments
            ORDER BY id DESC
            LIMIT 100
        """)
        rows = cur.fetchall()

        html = ""
        for r in rows:
            html += f"""
            <tr>
                <td class="code">{r[0] or ''}</td>
                <td>{r[1] or ''}</td>
                <td>{r[2] or ''}</td>
                <td>{r[3] or ''}</td>
                <td>{r[4] or ''}</td>
                <td>{r[5] or ''}</td>
                <td>{r[6] or ''}</td>
                <td>{r[7] or ''}</td>
                <td>{r[8] or ''}</td>
                <td>{r[9] or ''}</td>
                <td>{r[10] or ''}</td>
                <td>{r[11] or ''}</td>
                <td>{r[12] or ''}</td>
            </tr>
            """

        cur.close()
        conn.close()

        return page_shell("Anhang Log", f"""
        <div class="card">
            <h2>Anhang Log mit Analyse v5.5</h2>
            <table>
                <tr>
                    <th>Code</th>
                    <th>Datei</th>
                    <th>Typ erkannt</th>
                    <th>Betrag</th>
                    <th>Betrag EUR</th>
                    <th>Währung</th>
                    <th>Datum</th>
                    <th>Anbieter</th>
                    <th>Status</th>
                    <th>Confidence</th>
                    <th>Review</th>
                    <th>Dublette</th>
                    <th>Storage Key</th>
                </tr>
                {html}
            </table>
        </div>
        """)
    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2>Anhang-Log-Fehler</h2>
            <p>{str(e)}</p>
        </div>
        """)


@app.get("/attachment-ai/{attachment_id}", response_class=HTMLResponse)
def attachment_ai(attachment_id: int):
    try:
        ensure_schema()

        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT original_filename, ai_json
            FROM mail_attachments
            WHERE id = %s
        """, (attachment_id,))
        row = cur.fetchone()

        cur.close()
        conn.close()

        if not row:
            return page_shell("Nicht gefunden", "<div class='card'><h2>Anhang nicht gefunden</h2></div>")

        filename, ai_json = row
        return page_shell("AI JSON", f"""
        <div class="card">
            <h2>AI JSON für {filename}</h2>
            <pre>{ai_json or ''}</pre>
        </div>
        """)
    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2>AI-JSON-Fehler</h2>
            <p>{str(e)}</p>
        </div>
        """)