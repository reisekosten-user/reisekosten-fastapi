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

APP_VERSION = "5.4"

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
    """
    5.4: S3-Zugriff und PDF-Parsing einzeln abgesichert.
    Kein globaler Crash mehr bei einem defekten Anhang.
    """
    try:
        s3 = get_s3()
        response = s3.get_object(Bucket=S3_BUCKET, Key=storage_key)
        file_bytes = response["Body"].read()

        if filename.lower().endswith(".pdf"):
            text = ""
            try:
                with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                    for page in pdf.pages:
                        page_text = page.extract_text() or ""
                        text += page_text + "\n"
            except Exception as pdf_err:
                return f"ERROR: PDF-Parsing fehlgeschlagen: {pdf_err}"

            text = text.strip()
            return text[:15000] if text else "KEIN_TEXT_GEFUNDEN"

        return "NICHT_ANALYSIERBAR"

    except Exception as e:
        return f"ERROR: {e}"


def detect_currency(text: str):
    """
    5.4 Bugfix Taxi-Währung:
    Fremdwährung nur bei explizitem Währungscode/Symbol.
    Ohne explizite Angabe immer EUR – kein INR-Slip mehr.
    """
    t = (text or "")

    if re.search(r"\bINR\b|₹", t):
        return "INR"
    if re.search(r"\bUSD\b|\bUS\$\b", t):
        return "USD"
    if re.search(r"\bGBP\b|£", t):
        return "GBP"

    # Fallback: immer EUR
    return "EUR"


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

    bad_starts = [
        "ihre", "booking reference", "buchungsreferenz",
        "receipt", "invoice number", "datum", "date"
    ]
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


def find_best_amount_and_currency(text: str, detected_type: str):
    """
    5.4: Betragserkennung mit konservativer Währungslogik.
    Taxi ohne explizite Fremdwährung → immer EUR.
    """
    if not text:
        return "", ""

    lines = [line.strip() for line in text.splitlines() if line.strip()]

    priority_markers = [
        "total", "gesamt", "summe", "amount paid", "paid",
        "you paid", "fare", "trip fare", "grand total", "total due"
    ]

    amount_pattern = r"\b\d{1,3}(?:\.\d{3})*,\d{2}\b"

    prioritized_hits = []
    all_hits = []

    for line in lines:
        amounts = re.findall(amount_pattern, line)
        if not amounts:
            continue

        currency = detect_currency(line)
        for amount in amounts:
            try:
                value = float(amount.replace(".", "").replace(",", "."))
            except Exception:
                continue

            hit = {
                "amount": amount,
                "currency": currency,
                "value": value,
                "line": line.lower()
            }
            all_hits.append(hit)

            if any(marker in line.lower() for marker in priority_markers):
                prioritized_hits.append(hit)

    def pick_by_type(hits):
        if not hits:
            return "", ""

        if detected_type == "Taxi":
            hits = [h for h in hits if 2 <= h["value"] <= 300] or hits
            # 5.4: Taxi-Zeilen ohne explizites Fremdwährungs-Symbol → EUR erzwingen
            for h in hits:
                if h["currency"] != "EUR":
                    if not re.search(r"\bINR\b|₹|\bUSD\b|\bUS\$\b|\bGBP\b|£", h["line"]):
                        h["currency"] = "EUR"
        elif detected_type == "Essen":
            hits = [h for h in hits if 2 <= h["value"] <= 300] or hits
        elif detected_type == "Hotel":
            hits = [h for h in hits if 20 <= h["value"] <= 5000] or hits
        elif detected_type == "Flug":
            hits = [h for h in hits if 20 <= h["value"] <= 5000] or hits

        chosen = hits[-1]
        return chosen["amount"], chosen["currency"]

    amount, currency = pick_by_type(prioritized_hits)
    if amount:
        return amount, currency

    amount, currency = pick_by_type(all_hits)
    return amount, currency


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
            .sub {{
                color: #567;
                font-size: 14px;
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
    return {"version": APP_VERSION}


@app.get("/init")
def init():
    try:
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

    except Exception as e:
        return {"status": "fehler", "detail": str(e), "version": APP_VERSION}


@app.get("/", response_class=HTMLResponse)
def home():
    """
    5.4 Bugfix: try/except um den gesamten DB-Block.
    Fehler werden als lesbares HTML angezeigt statt Internal Server Error.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT COALESCE(trip_code, '') AS trip_code,
                   detected_type,
                   COALESCE(detected_amount_eur, ''),
                   review_flag
            FROM mail_attachments
            ORDER BY COALESCE(trip_code, '')
        """)
        rows = cur.fetchall()

        cur.execute("SELECT trip_code, hotel_mode FROM trip_meta")
        hotel_meta = dict(cur.fetchall())

        trips = {}

        for trip_code, detected_type, amount_eur, review_flag in rows:
            code = trip_code or "(ohne Code)"
            if code not in trips:
                trips[code] = {
                    "flight": False,
                    "hotel": False,
                    "taxi": False,
                    "essen": False,
                    "sum_eur": 0.0,
                    "review_count": 0
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

            if code == "(ohne Code)":
                errors.append("Einträge ohne Reisecode")
            else:
                hotel_mode = hotel_meta.get(code, "")
                if hotel_mode == "customer":
                    has_hotel = True
                if data["flight"] and not has_hotel:
                    warnings.append("Hotel fehlt")

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
                <td>{"ja" if has_hotel else "nein"}</td>
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

        empty_hint = ""
        if not trips:
            empty_hint = '<p class="sub">Noch keine Daten. Starte mit <b>/init</b>, dann <b>Mails abrufen</b>.</p>'

        return page_shell("Dashboard", f"""
        <div class="card">
            <h2>Dashboard {APP_VERSION}</h2>
            <div class="sub">Mit Summen, Warnungen und Hotel-Override.</div>
            {empty_hint}
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
        """)

    except Exception as e:
        return HTMLResponse(
            content=page_shell("Dashboard – Fehler", f"""
            <div class="card">
                <h2 class="warn">Dashboard konnte nicht geladen werden (v{APP_VERSION})</h2>
                <p><b>Fehler:</b> {e}</p>
                <p>
                    <a class="btn" href="/init">DB initialisieren</a>
                    <a class="btn-light" href="/version">Version prüfen</a>
                </p>
            </div>
            """),
            status_code=500
        )


@app.get("/set-hotel")
def set_hotel(code: str, mode: str):
    try:
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

    except Exception as e:
        return {"status": "fehler", "detail": str(e), "version": APP_VERSION}


@app.get("/reset-mail-log")
def reset_mail_log():
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("TRUNCATE TABLE mail_attachments RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE mail_messages RESTART IDENTITY")
        conn.commit()
        cur.close()
        conn.close()
        return {"status": "mail log und anhaenge geloescht", "version": APP_VERSION}
    except Exception as e:
        return {"status": "fehler", "detail": str(e), "version": APP_VERSION}


@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    try:
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

            cur.execute("""
                INSERT INTO mail_messages
                (mail_uid, sender, subject, body, trip_code, detected_type, detected_destination)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (uid, sender, subject, body, code, detected_type, detected_destination))

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

                    try:
                        s3.put_object(
                            Bucket=S3_BUCKET,
                            Key=storage_key,
                            Body=payload,
                            ContentType=part.get_content_type() or "application/octet-stream"
                        )
                    except Exception as s3_err:
                        storage_key = f"S3-FEHLER: {s3_err}"

                    attachment_type = detect_attachment_type(
                        safe_original,
                        subject,
                        body
                    )

                    cur.execute("""
                        INSERT INTO mail_attachments
                        (mail_uid, trip_code, original_filename, saved_filename, content_type, file_path, detected_type, analysis_status, storage_key, confidence, review_flag)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
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
                        "pruefen"
                    ))

                    attachment_count += 1

            imported += 1

        conn.commit()
        cur.close()
        conn.close()
        mail.logout()

        return page_shell("Mails importiert", f"""
        <div class="card">
            <h2 class="ok">Mailabruf erfolgreich</h2>
            <p><b>Neu importierte Mails:</b> {imported}</p>
            <p><b>Übersprungen (schon vorhanden):</b> {skipped}</p>
            <p><b>Gespeicherte Anhänge im Bucket:</b> {attachment_count}</p>
            <a class="btn" href="/">Zum Dashboard</a>
            <a class="btn-light" href="/attachment-log">Zum Anhang Log</a>
        </div>
        """)

    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2 class="warn">Fehler beim Mailabruf</h2>
            <p>{e}</p>
            <a class="btn-light" href="/">Zurück</a>
        </div>
        """)


@app.get("/analyze-attachments", response_class=HTMLResponse)
def analyze_attachments():
    """
    5.4 Bugfix: Jeder Anhang einzeln in try/except.
    Ein defekter Anhang bricht nicht mehr die gesamte Analyse ab.
    """
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, storage_key, original_filename, detected_type
            FROM mail_attachments
            ORDER BY id
        """)
        rows = cur.fetchall()

        processed = 0

        for row in rows:
            attachment_id = row[0]
            storage_key = row[1] or ""
            original_filename = row[2] or ""
            detected_type = row[3] or "Unbekannt"

            try:
                if not storage_key or storage_key.startswith("S3-FEHLER"):
                    cur.execute("""
                        UPDATE mail_attachments
                        SET extracted_text=%s, detected_amount=%s, detected_amount_eur=%s,
                            detected_currency=%s, detected_date=%s, detected_vendor=%s,
                            analysis_status=%s, confidence=%s, review_flag=%s
                        WHERE id=%s
                    """, ("KEIN_STORAGE_KEY", "", "", "", "", "",
                          "kein storage key", "niedrig", "pruefen", attachment_id))
                    processed += 1
                    continue

                if not is_supported_analysis_file(original_filename):
                    cur.execute("""
                        UPDATE mail_attachments
                        SET extracted_text=%s, detected_amount=%s, detected_amount_eur=%s,
                            detected_currency=%s, detected_date=%s, detected_vendor=%s,
                            analysis_status=%s, confidence=%s, review_flag=%s
                        WHERE id=%s
                    """, ("NICHT_ANALYSIERBAR", "", "", "", "", "",
                          "nicht analysierbar", "niedrig", "pruefen", attachment_id))
                    processed += 1
                    continue

                text = extract_text_from_s3_object(storage_key, original_filename)

                amount, currency = find_best_amount_and_currency(text, detected_type)
                if not currency:
                    currency = detect_currency(text)
                amount_eur = convert_to_eur(amount, currency)

                date = extract_date(text)
                vendor = extract_vendor(text, detected_type)

                status = "ok"
                if text == "NICHT_ANALYSIERBAR":
                    status = "nicht analysierbar"
                elif text.startswith("ERROR:"):
                    status = "analysefehler"
                elif text == "KEIN_TEXT_GEFUNDEN":
                    status = "kein text"

                confidence = compute_confidence(detected_type, amount, date, vendor, status)
                review_flag = compute_review_flag(confidence, status)

                cur.execute("""
                    UPDATE mail_attachments
                    SET extracted_text=%s, detected_amount=%s, detected_amount_eur=%s,
                        detected_currency=%s, detected_date=%s, detected_vendor=%s,
                        analysis_status=%s, confidence=%s, review_flag=%s
                    WHERE id=%s
                """, (
                    text, amount, amount_eur, currency, date, vendor,
                    status, confidence, review_flag, attachment_id
                ))

            except Exception as row_err:
                try:
                    cur.execute("""
                        UPDATE mail_attachments
                        SET analysis_status=%s, confidence=%s, review_flag=%s
                        WHERE id=%s
                    """, (f"analysefehler: {str(row_err)[:80]}", "niedrig", "pruefen", attachment_id))
                except Exception:
                    pass

            processed += 1

        conn.commit()
        cur.close()
        conn.close()

        return page_shell("Analyse", f"""
        <div class="card">
            <h2 class="ok">{processed} Anhänge analysiert</h2>
            <a class="btn" href="/">Zum Dashboard</a>
            <a class="btn-light" href="/attachment-log">Zum Anhang Log</a>
        </div>
        """)

    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2 class="warn">Fehler bei Analyse</h2>
            <p>{e}</p>
            <a class="btn-light" href="/">Zurück</a>
        </div>
        """)


@app.get("/trip-review", response_class=HTMLResponse)
def trip_review():
    try:
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
            <h2>Reisebewertung v{APP_VERSION}</h2>
            <a class="btn-light" href="/">Zurück</a>
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
        return page_shell("Fehler", f"<div class='card'><h2 class='warn'>Fehler</h2><p>{e}</p></div>")


@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    try:
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
            <h2>Mail Log</h2>
            <a class="btn-light" href="/">Zurück</a>
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

    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2 class='warn'>Fehler</h2><p>{e}</p></div>")


@app.get("/attachment-log", response_class=HTMLResponse)
def attachment_log():
    try:
        conn = get_conn()
        cur = conn.cursor()

        cur.execute("""
            SELECT trip_code, original_filename, detected_type, detected_amount,
                   detected_amount_eur, detected_currency, detected_date, detected_vendor,
                   analysis_status, confidence, review_flag, storage_key
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
            <h2>Anhang Log mit Analyse v{APP_VERSION}</h2>
            <a class="btn-light" href="/">Zurück</a>
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

    except Exception as e:
        return page_shell("Fehler", f"<div class='card'><h2 class='warn'>Fehler</h2><p>{e}</p></div>")
