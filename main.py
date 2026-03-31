from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
import psycopg2
import imaplib
import email
from email.header import decode_header
import re
import boto3
import pdfplumber
from io import BytesIO

APP_VERSION = "5.0a"

app = FastAPI()

# WICHTIG: static-Ordner muss im Repo existieren
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

    if any(x in t for x in ["flug", "flight", "boarding", "boardingpass", "boarding pass", "pnr", "ticket", "airline", "itinerary"]):
        return "Flug"
    if any(x in t for x in ["hotel", "booking.com", "check-in", "check out", "check-out", "reservation", "zimmer"]):
        return "Hotel"
    if any(x in t for x in ["taxi", "uber", "cab"]):
        return "Taxi"
    if any(x in t for x in ["bahn", "zug", "train", "ice", "db "]):
        return "Bahn"
    if any(x in t for x in ["meal", "restaurant", "verpflegung", "essen", "dinner", "lunch", "breakfast"]):
        return "Essen"
    if any(x in t for x in ["mietwagen", "rental car", "car rental", "hertz", "sixt", "avis"]):
        return "Mietwagen"

    return "Unbekannt"


def detect_destination(text: str):
    t = (text or "").lower()
    places = [
        "delhi", "mumbai", "bangalore", "new york", "london", "paris",
        "dubai", "shanghai", "beijing", "tokyo", "singapore", "mexico city",
        "lyon", "frankfurt", "zaq"
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

    if any(x in text for x in ["boarding", "boardingpass", "boarding pass", "eticket", "e-ticket", "flight", "flug", "ticket", "pnr", "itinerary"]):
        return "Flug"
    if any(x in text for x in ["hotel", "booking", "reservation", "zimmer", "check-in", "check-out"]):
        return "Hotel"
    if any(x in text for x in ["taxi", "uber", "cab", "receipt_"]):
        return "Taxi"
    if any(x in text for x in ["bahn", "zug", "train", "ice", "db"]):
        return "Bahn"
    if any(x in text for x in ["meal", "restaurant", "essen", "verpflegung", "breakfast", "lunch", "dinner"]):
        return "Essen"
    if any(x in text for x in ["mietwagen", "rental", "car rental", "hertz", "sixt", "avis"]):
        return "Mietwagen"

    return "Unbekannt"


def is_supported_analysis_file(filename: str):
    f = (filename or "").lower()
    return f.endswith(".pdf")


def detect_currency(text: str):
    t = (text or "").lower()
    if "$" in t or " usd" in t or "usd " in t:
        return "USD"
    if "£" in t or " gbp" in t or "gbp " in t:
        return "GBP"
    if "₹" in t or " inr" in t or "inr " in t:
        return "INR"
    return "EUR"


def extract_amount(text: str, detected_type: str):
    if not text:
        return ""

    candidates = re.findall(r"\b\d{1,3}(?:\.\d{3})*,\d{2}\b", text)

    values = []
    for c in candidates:
        try:
            v = float(c.replace(".", "").replace(",", "."))
            values.append((c, v))
        except Exception:
            pass

    if not values:
        return ""

    if detected_type == "Taxi":
        plausible = [x for x in values if 2 <= x[1] <= 300]
        if plausible:
            return plausible[-1][0]

    if detected_type == "Hotel":
        plausible = [x for x in values if 20 <= x[1] <= 5000]
        if plausible:
            return plausible[-1][0]

    if detected_type == "Flug":
        plausible = [x for x in values if 20 <= x[1] <= 5000]
        if plausible:
            return plausible[-1][0]

    if detected_type == "Essen":
        plausible = [x for x in values if 2 <= x[1] <= 300]
        if plausible:
            return plausible[-1][0]

    return values[-1][0]


def convert_to_eur(amount_str: str, currency: str):
    if not amount_str:
        return ""
    try:
        amount = float(amount_str.replace(".", "").replace(",", "."))
    except Exception:
        return ""

    rates_to_eur = {
        "EUR": 1.0,
        "USD": 0.93,
        "GBP": 1.17,
        "INR": 0.011
    }

    rate = rates_to_eur.get(currency or "EUR", 1.0)
    eur = round(amount * rate, 2)
    return f"{eur:.2f}".replace(".", ",")


def extract_date(text: str):
    if not text:
        return ""

    patterns = [
        r"\b\d{2}[./]\d{2}[./]\d{4}\b",
        r"\b\d{1,2}\s+[A-Za-zäöüÄÖÜ]+\s+\d{4}\b"
    ]

    for p in patterns:
        match = re.search(p, text)
        if match:
            return match.group(0)

    return ""


def extract_vendor(text: str, detected_type: str):
    if not text:
        return ""

    lower = text.lower()

    vendor_groups = {
        "Flug": [
            "lufthansa", "air france", "klm", "ryanair", "austrian",
            "azerbaijan airlines", "azal", "turkish airlines", "emirates",
            "qatar airways", "air india"
        ],
        "Taxi": [
            "uber", "bolt", "taxi", "free now", "lyft"
        ],
        "Hotel": [
            "marriott", "hilton", "booking", "novotel", "ibis",
            "hyatt", "radisson", "holiday inn", "hotel"
        ],
        "Bahn": [
            "deutsche bahn", "db", "sncf", "trenitalia", "rail"
        ],
        "Mietwagen": [
            "hertz", "sixt", "avis", "europcar", "enterprise"
        ],
        "Essen": [
            "restaurant", "mcdonald", "burger king", "starbucks", "cafe"
        ]
    }

    keywords = vendor_groups.get(detected_type, [])
    for k in keywords:
        if k in lower:
            return k.title()

    bad_starts = ["ihre", "booking reference", "buchungsreferenz", "receipt", "invoice number", "datum", "date"]
    for line in text.split("\n")[:15]:
        l = line.strip()
        if len(l) < 4:
            continue
        if any(l.lower().startswith(x) for x in bad_starts):
            continue
        if re.search(r"\d{2}[./]\d{2}[./]\d{4}", l):
            continue
        return l[:100]

    return ""


def extract_text_from_s3_object(storage_key: str, filename: str):
    try:
        s3 = get_s3()
        response = s3.get_object(Bucket=S3_BUCKET, Key=storage_key)
        file_bytes = response["Body"].read()

        if filename.lower().endswith(".pdf"):
            text = ""
            with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                for page in pdf.pages:
                    page_text = page.extract_text() or ""
                    text += page_text + "\n"
            text = text.strip()
            return text[:10000] if text else "KEIN_TEXT_GEFUNDEN"

        return "NICHT_ANALYSIERBAR"

    except Exception as e:
        return f"ERROR: {e}"


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
    if status in ["analysefehler", "datei fehlt", "kein text"]:
        return "pruefen"
    if confidence == "niedrig":
        return "pruefen"
    return "ok"


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
            .ok {{
                color: #177245;
                font-weight: bold;
            }}
            .warn {{
                color: #b46b00;
                font-weight: bold;
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


@app.get("/init")
def init():
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

    return {"status": "ok", "version": APP_VERSION}


@app.get("/trip-review", response_class=HTMLResponse)
def trip_review():
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
            SELECT detected_type, analysis_status, review_flag
            FROM mail_attachments
            WHERE COALESCE(trip_code, '') = %s
        """, (trip_code,))
        items = cur.fetchall()

        has_flight = any(x[0] == "Flug" for x in items)
        has_hotel = any(x[0] == "Hotel" for x in items)
        has_taxi = any(x[0] == "Taxi" for x in items)
        open_reviews = sum(1 for x in items if x[2] == "pruefen")

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
            <h2>Reisebewertung v2</h2>
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


@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT sender, subject, trip_code, detected_type, detected_destination
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
        </tr>
        """

    cur.close()
    conn.close()

    return page_shell("Mail Log", f"""
        <div class="card">
            <h2>Mail Log mit Erkennung</h2>
            <table>
                <tr>
                    <th>Von</th>
                    <th>Betreff</th>
                    <th>Code</th>
                    <th>Typ erkannt</th>
                    <th>Ziel erkannt</th>
                </tr>
                {html}
            </table>
        </div>
        """)


@app.get("/attachment-log", response_class=HTMLResponse)
def attachment_log():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT trip_code, original_filename, detected_type, detected_amount, detected_amount_eur, detected_currency, detected_date, detected_vendor, analysis_status, confidence, review_flag, storage_key
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
        </tr>
        """

    cur.close()
    conn.close()

    return page_shell("Anhang Log", f"""
        <div class="card">
            <h2>Anhang Log mit Analyse v5.0</h2>
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
                    <th>Storage Key</th>
                </tr>
                {html}
            </table>
        </div>
        """)