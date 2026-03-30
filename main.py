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

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")

S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_REGION = os.getenv("S3_REGION")

app.mount("/static", StaticFiles(directory="static"), name="static")


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
    match = re.search(r"\b\d{2}-\d{3}\b", text)
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
    t = text.lower()

    if any(x in t for x in ["flug", "flight", "boarding", "boardingpass", "boarding pass", "pnr", "ticket", "airline"]):
        return "Flug"
    if any(x in t for x in ["hotel", "booking.com", "check-in", "check out", "check-out", "reservation", "zimmer"]):
        return "Hotel"
    if any(x in t for x in ["taxi", "uber", "cab"]):
        return "Taxi"
    if any(x in t for x in ["bahn", "zug", "train", "ice", "db "]):
        return "Bahn"
    if any(x in t for x in ["meal", "restaurant", "verpflegung", "essen", "dinner", "lunch", "breakfast"]):
        return "Verpflegung"
    if any(x in t for x in ["mietwagen", "rental car", "car rental", "hertz", "sixt", "avis"]):
        return "Mietwagen"

    return "Unbekannt"


def detect_destination(text: str):
    t = text.lower()

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
    name = name.replace("\\", "_").replace("/", "_").strip()
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name)
    return name[:180] if name else "attachment.bin"


def detect_attachment_type(filename: str, subject: str, body: str):
    text = f"{filename} {subject} {body}".lower()

    if filename.lower().endswith(".ics"):
        return "Kalendereintrag"
    if filename.lower().endswith(".emz"):
        return "Inline-Grafik"

    if any(x in text for x in ["boarding", "boardingpass", "boarding pass", "eticket", "e-ticket", "flight", "flug", "ticket", "pnr"]):
        return "Flug"
    if any(x in text for x in ["hotel", "booking", "reservation", "zimmer", "check-in", "check-out"]):
        return "Hotel"
    if any(x in text for x in ["taxi", "uber", "cab"]):
        return "Taxi"
    if any(x in text for x in ["bahn", "zug", "train", "ice", "db"]):
        return "Bahn"
    if any(x in text for x in ["meal", "restaurant", "essen", "verpflegung", "breakfast", "lunch", "dinner"]):
        return "Verpflegung"
    if any(x in text for x in ["mietwagen", "rental", "car rental", "hertz", "sixt", "avis"]):
        return "Mietwagen"

    return "Unbekannt"


def is_supported_analysis_file(filename: str):
    f = filename.lower()
    return f.endswith(".pdf") or f.endswith(".png") or f.endswith(".jpg") or f.endswith(".jpeg") or f.endswith(".webp")


def extract_amount(text: str):
    if not text:
        return ""

    matches = re.findall(r"\d{1,3}(?:\.\d{3})*,\d{2}", text)

    if matches:
        return matches[0]

    return ""


def extract_date(text: str):
    if not text:
        return ""
    match = re.search(r"\d{2}[./]\d{2}[./]\d{4}", text)
    return match.group(0) if match else ""


def extract_vendor(text: str):
    if not text:
        return ""

    keywords = [
        "lufthansa", "air france", "klm", "ryanair",
        "uber", "taxi", "bolt",
        "hotel", "marriott", "hilton", "booking"
    ]

    lower = text.lower()

    for k in keywords:
        if k in lower:
            return k.title()

    for line in text.split("\n")[:10]:
        l = line.strip()
        if len(l) > 5 and not l.lower().startswith("ihre"):
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
                    text += (page.extract_text() or "") + "\n"
            text = text.strip()
            return text[:10000] if text else "KEIN_TEXT_GEFUNDEN"

        if filename.lower().endswith((".png", ".jpg", ".jpeg", ".webp")):
            return "BILD_OHNE_OCR"

        return "NICHT_ANALYSIERBAR"

    except Exception as e:
        return f"ERROR: {e}"


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
        </style>
    </head>
    <body>
        <div class="topbar">
            <div class="logo-wrap">
                <img src="/static/herrhammer-logo.png" alt="Herrhammer Logo">
            </div>
            <h2>Herrhammer Reisekosten</h2>
        </div>
        <div class="wrap">
            {content}
        </div>
    </body>
    </html>
    """


@app.get("/", response_class=HTMLResponse)
def home():
    return page_shell("Start", """
    <div class="card">
        <h2>Aktionen</h2>
        <a class="btn" href="/init">Init / Migration</a><br><br>
        <a class="btn" href="/fetch-mails">Mails abrufen</a><br><br>
        <a class="btn" href="/mail-log">Mail Log</a><br><br>
        <a class="btn" href="/attachment-log">Anhang Log</a><br><br>
        <a class="btn" href="/analyze-attachments">Anhänge analysieren</a><br><br>
        <a class="btn-light" href="/reset-mail-log">Mail Log löschen</a>
    </div>
    """)


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
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_date TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_vendor TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS analysis_status TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS storage_key TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT now()")

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "ok"}


@app.get("/reset-mail-log")
def reset_mail_log():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE mail_attachments RESTART IDENTITY")
    cur.execute("TRUNCATE TABLE mail_messages RESTART IDENTITY")
    conn.commit()
    cur.close()
    conn.close()

    return {"status": "mail log und anhaenge geloescht"}


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
                        (mail_uid, trip_code, original_filename, saved_filename, content_type, file_path, detected_type, analysis_status, storage_key)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        uid,
                        code,
                        safe_original,
                        saved_filename,
                        part.get_content_type(),
                        storage_key,
                        attachment_type,
                        "neu",
                        storage_key
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
            <a class="btn" href="/mail-log">Zum Mail Log</a>
            <a class="btn-light" href="/attachment-log">Zum Anhang Log</a>
        </div>
        """)

    except Exception as e:
        return page_shell("Fehler", f"""
        <div class="card">
            <h2 class="warn">Fehler beim Mailabruf</h2>
            <p>{e}</p>
        </div>
        """)


@app.get("/analyze-attachments", response_class=HTMLResponse)
def analyze_attachments():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT id, storage_key, original_filename
        FROM mail_attachments
        ORDER BY id
    """)
    rows = cur.fetchall()

    processed = 0

    for row in rows:
        attachment_id = row[0]
        storage_key = row[1] or ""
        original_filename = row[2] or ""

        if not storage_key:
            cur.execute("""
                UPDATE mail_attachments
                SET extracted_text=%s,
                    detected_amount=%s,
                    detected_date=%s,
                    detected_vendor=%s,
                    analysis_status=%s
                WHERE id=%s
            """, (
                "KEIN_STORAGE_KEY",
                "",
                "",
                "",
                "kein storage key",
                attachment_id
            ))
            processed += 1
            continue

        if not is_supported_analysis_file(original_filename):
            cur.execute("""
                UPDATE mail_attachments
                SET extracted_text=%s,
                    detected_amount=%s,
                    detected_date=%s,
                    detected_vendor=%s,
                    analysis_status=%s
                WHERE id=%s
            """, (
                "NICHT_ANALYSIERBAR",
                "",
                "",
                "",
                "nicht analysierbar",
                attachment_id
            ))
            processed += 1
            continue

        text = extract_text_from_s3_object(storage_key, original_filename)
        amount = extract_amount(text)
        date = extract_date(text)
        vendor = extract_vendor(text)

        status = "ok"
        if text == "NICHT_ANALYSIERBAR":
            status = "nicht analysierbar"
        elif text == "BILD_OHNE_OCR":
            status = "ocr fehlt"
        elif text.startswith("ERROR:"):
            status = "analysefehler"
        elif text == "KEIN_TEXT_GEFUNDEN":
            status = "kein text"

        cur.execute("""
            UPDATE mail_attachments
            SET extracted_text=%s,
                detected_amount=%s,
                detected_date=%s,
                detected_vendor=%s,
                analysis_status=%s
            WHERE id=%s
        """, (
            text,
            amount,
            date,
            vendor,
            status,
            attachment_id
        ))

        processed += 1

    conn.commit()
    cur.close()
    conn.close()

    return page_shell("Analyse", f"""
    <div class="card">
        <h2 class="ok">{processed} Anhänge analysiert</h2>
        <a class="btn" href="/attachment-log">Zum Anhang Log</a>
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
        SELECT trip_code, original_filename, detected_type, detected_amount, detected_date, detected_vendor, analysis_status, storage_key
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
        </tr>
        """

    cur.close()
    conn.close()

    return page_shell("Anhang Log", f"""
    <div class="card">
        <h2>Anhang Log mit Analyse</h2>
        <table>
            <tr>
                <th>Code</th>
                <th>Datei</th>
                <th>Typ erkannt</th>
                <th>Betrag</th>
                <th>Datum</th>
                <th>Anbieter</th>
                <th>Status</th>
                <th>Storage Key</th>
            </tr>
            {html}
        </table>
    </div>
    """)