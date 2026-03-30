from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
import psycopg2
import imaplib
import email
from email.header import decode_header
import re
import shutil
from pathlib import Path

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")

UPLOAD_DIR = "uploads"
MAIL_ATTACHMENT_DIR = os.path.join(UPLOAD_DIR, "mail_attachments")

app.mount("/static", StaticFiles(directory="static"), name="static")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def ensure_dirs():
    os.makedirs(UPLOAD_DIR, exist_ok=True)
    os.makedirs(MAIL_ATTACHMENT_DIR, exist_ok=True)


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
        "dubai", "shanghai", "beijing", "tokyo", "singapore", "mexico city"
    ]

    for place in places:
        if place in t:
            return place.title()

    return ""


def sanitize_filename(name: str):
    name = name.replace("\\", "_").replace("/", "_").strip()
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name)
    return name[:180] if name else "attachment.bin"


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
        <a class="btn-light" href="/reset-mail-log">Mail Log löschen</a>
    </div>
    """)


@app.get("/init")
def init():
    ensure_dirs()

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
            mail_uid TEXT,
            trip_code TEXT,
            original_filename TEXT,
            saved_filename TEXT,
            content_type TEXT,
            file_path TEXT,
            created_at TIMESTAMP DEFAULT now()
        )
    """)

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

    # Dateien optional mit löschen
    ensure_dirs()
    for path in Path(MAIL_ATTACHMENT_DIR).glob("*"):
        if path.is_file():
            path.unlink()

    return {"status": "mail log und anhaenge geloescht"}


@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    try:
        ensure_dirs()

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

            # Anhänge speichern
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
                        decoded_filename = f"attachment{ext}"

                    payload = part.get_payload(decode=True)
                    if not payload:
                        continue

                    safe_original = sanitize_filename(decoded_filename)
                    saved_filename = f"{uid}_{safe_original}"
                    file_path = os.path.join(MAIL_ATTACHMENT_DIR, saved_filename)

                    with open(file_path, "wb") as f:
                        f.write(payload)

                    cur.execute("""
                        INSERT INTO mail_attachments
                        (mail_uid, trip_code, original_filename, saved_filename, content_type, file_path)
                        VALUES (%s,%s,%s,%s,%s,%s)
                    """, (
                        uid,
                        code,
                        safe_original,
                        saved_filename,
                        part.get_content_type(),
                        file_path
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
            <p><b>Gespeicherte Anhänge:</b> {attachment_count}</p>
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
        SELECT trip_code, original_filename, content_type, file_path, created_at
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
        </tr>
        """

    cur.close()
    conn.close()

    return page_shell("Anhang Log", f"""
    <div class="card">
        <h2>Mail-Anhänge</h2>
        <table>
            <tr>
                <th>Code</th>
                <th>Datei</th>
                <th>Typ</th>
                <th>Pfad</th>
                <th>Zeitpunkt</th>
            </tr>
            {html}
        </table>
    </div>
    """)
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