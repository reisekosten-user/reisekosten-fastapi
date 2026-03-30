from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os
import psycopg2
import imaplib
import email
from email.header import decode_header
import re
from pathlib import Path
import pdfplumber
from PIL import Image
import pytesseract

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


def extract_text_from_file(path):
    text = ""

    try:
        if path.lower().endswith(".pdf"):
            with pdfplumber.open(path) as pdf:
                for page in pdf.pages:
                    text += page.extract_text() or ""

        elif path.lower().endswith((".png", ".jpg", ".jpeg")):
            img = Image.open(path)
            text = pytesseract.image_to_string(img)

    except Exception as e:
        text = f"ERROR: {e}"

    return text[:10000]


def extract_amount(text):
    match = re.search(r"(\d+[.,]\d{2})\s?(€|eur)?", text.lower())
    return match.group(1) if match else ""


def extract_date(text):
    match = re.search(r"\d{2}[./]\d{2}[./]\d{4}", text)
    return match.group(0) if match else ""


def extract_vendor(text):
    lines = text.split("\n")
    for l in lines[:5]:
        if len(l.strip()) > 3:
            return l.strip()
    return ""


def page_shell(title, content):
    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <title>{title}</title>
    </head>
    <body style="font-family:Arial;background:#eef4fb">
        <div style="padding:20px;background:#12365f;color:white;">
            Herrhammer Reisekosten
        </div>
        <div style="padding:20px;">
            {content}
        </div>
    </body>
    </html>
    """


@app.get("/init")
def init():
    ensure_dirs()
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mail_attachments (
            id SERIAL PRIMARY KEY
        )
    """)

    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS file_path TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS original_filename TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS extracted_text TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_amount TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_date TEXT")
    cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS detected_vendor TEXT")

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "init ok"}


@app.get("/analyze-attachments", response_class=HTMLResponse)
def analyze():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id, file_path FROM mail_attachments")
    rows = cur.fetchall()

    processed = 0

    for r in rows:
        text = extract_text_from_file(r[1])

        amount = extract_amount(text)
        date = extract_date(text)
        vendor = extract_vendor(text)

        cur.execute("""
            UPDATE mail_attachments
            SET extracted_text=%s,
                detected_amount=%s,
                detected_date=%s,
                detected_vendor=%s
            WHERE id=%s
        """, (text, amount, date, vendor, r[0]))

        processed += 1

    conn.commit()
    cur.close()
    conn.close()

    return page_shell("Analyse", f"<h2>{processed} Anhänge analysiert</h2>")


@app.get("/attachment-log", response_class=HTMLResponse)
def log():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT original_filename, detected_amount, detected_date, detected_vendor
        FROM mail_attachments
        ORDER BY id DESC
        LIMIT 50
    """)

    rows = cur.fetchall()

    html = ""
    for r in rows:
        html += f"""
        <tr>
            <td>{r[0]}</td>
            <td>{r[1]}</td>
            <td>{r[2]}</td>
            <td>{r[3]}</td>
        </tr>
        """

    return page_shell("Log", f"""
    <table border=1>
        <tr><th>Datei</th><th>Betrag</th><th>Datum</th><th>Anbieter</th></tr>
        {html}
    </table>
    """)