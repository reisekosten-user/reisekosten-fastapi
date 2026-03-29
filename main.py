from fastapi import FastAPI, Form, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
import psycopg2
from datetime import datetime
import shutil
import imaplib
import email
from email.header import decode_header
import re

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")
UPLOAD_DIR = "uploads"

IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")

app.mount("/static", StaticFiles(directory="static"), name="static")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def ensure_upload_dir():
    os.makedirs(UPLOAD_DIR, exist_ok=True)


def generate_trip_code():
    year = datetime.now().strftime("%y")

    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        "SELECT trip_code FROM trips WHERE trip_code LIKE %s ORDER BY id DESC LIMIT 1",
        (f"{year}-%",)
    )
    last = cur.fetchone()

    if last and last[0]:
        last_number = int(last[0].split("-")[1])
        new_number = last_number + 1
    else:
        new_number = 1

    cur.close()
    conn.close()

    return f"{year}-{str(new_number).zfill(3)}"


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


def page_shell(title: str, content: str):
    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
        <style>
            body {{ font-family: Arial; margin:0; background:#eef4fb; }}
            .topbar {{ background:#12365f; color:white; padding:20px; display:flex; align-items:center; gap:15px; }}
            .topbar img {{ height:60px; background:rgba(255,255,255,0.3); padding:10px; border-radius:10px; }}
            .wrap {{ padding:20px; }}
            .card {{ background:white; padding:20px; border-radius:10px; margin-bottom:20px; }}
            .btn {{ background:#2a6ab1; color:white; padding:10px; border:none; border-radius:6px; text-decoration:none; }}
        </style>
    </head>
    <body>
        <div class="topbar">
            <img src="/static/herrhammer-logo.png">
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
        <a class="btn" href="/init">Init</a><br><br>
        <a class="btn" href="/fetch-mails">Mails abrufen</a><br><br>
        <a class="btn" href="/mail-log">Mail Log</a>
    </div>
    """)


@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")

        status, data = mail.search(None, "ALL")
        ids = data[0].split()[-20:]

        conn = get_conn()
        cur = conn.cursor()

        count = 0

        for i in ids:
            uid = i.decode()

            cur.execute("SELECT id FROM mail_messages WHERE mail_uid=%s", (uid,))
            if cur.fetchone():
                continue

            _, msg_data = mail.fetch(i, "(RFC822)")
            msg = email.message_from_bytes(msg_data[0][1])

            subject = decode_mime_header(msg.get("Subject", ""))
            sender = decode_mime_header(msg.get("From", ""))
            body = ""

            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        body = part.get_payload(decode=True).decode(errors="ignore")
                        break

            code = extract_trip_code(subject + body)

            cur.execute("""
                INSERT INTO mail_messages (mail_uid, sender, subject, body, trip_code)
                VALUES (%s,%s,%s,%s,%s)
            """, (uid, sender, subject, body, code))

            count += 1

        conn.commit()
        cur.close()
        conn.close()
        mail.logout()

        return page_shell("OK", f"<div class='card'>Importiert: {count}</div>")

    except Exception as e:
        return page_shell("Fehler", f"<div class='card'>{e}</div>")


@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT sender, subject, trip_code FROM mail_messages ORDER BY id DESC LIMIT 20")
    rows = cur.fetchall()

    html = ""
    for r in rows:
        html += f"<tr><td>{r[0]}</td><td>{r[1]}</td><td>{r[2]}</td></tr>"

    cur.close()
    conn.close()

    return page_shell("Log", f"""
    <div class="card">
        <table border="1" cellpadding="5">
            <tr><th>Von</th><th>Betreff</th><th>Code</th></tr>
            {html}
        </table>
    </div>
    """)


@app.get("/init")
def init():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mail_messages (
            id SERIAL PRIMARY KEY,
            mail_uid TEXT UNIQUE,
            sender TEXT,
            subject TEXT,
            body TEXT,
            trip_code TEXT
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "ok"}