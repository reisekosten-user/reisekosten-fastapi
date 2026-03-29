sudo tee main.py > /dev/null <<'EOF'
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


def parse_iso_date(date_str: str):
    return datetime.strptime(date_str, "%Y-%m-%d").date()


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

    code = f"{year}-{str(new_number).zfill(3)}"

    cur.close()
    conn.close()
    return code


def extract_trip_code(text: str):
    match = re.search(r"\b\d{2}-\d{3}\b", text)
    if match:
        return match.group(0)
    return None


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
    <html lang="de">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{title}</title>
        <style>
            :root {{
                --blue-950: #0b2340;
                --blue-900: #143762;
                --blue-800: #1f4e86;
                --blue-700: #2a6ab1;
                --blue-100: #eaf2fb;
                --blue-050: #f6f9fd;
                --text: #16324c;
                --muted: #64788f;
                --line: #d7e2ee;
                --ok: #177245;
                --warn: #b46b00;
                --white: #ffffff;
                --shadow: 0 12px 28px rgba(16, 38, 64, 0.08);
                --radius: 18px;
            }}
            * {{ box-sizing: border-box; }}
            body {{
                margin: 0;
                font-family: Arial, Helvetica, sans-serif;
                background: linear-gradient(180deg, #f7fbff 0%, #eef4fb 100%);
                color: var(--text);
            }}
            .topbar {{
                background: linear-gradient(135deg, var(--blue-950), var(--blue-700));
                color: var(--white);
                padding: 22px 28px;
                box-shadow: var(--shadow);
            }}
            .brand {{
                max-width: 1240px;
                margin: 0 auto;
                display: flex;
                align-items: center;
                gap: 18px;
            }}
            .brand img {{
                height: 88px;
                width: auto;
                display: block;
                background: rgba(255,255,255,0.30);
                border-radius: 16px;
                padding: 10px 14px;
            }}
            .brand-text h1 {{
                margin: 0;
                font-size: 30px;
                line-height: 1.1;
            }}
            .brand-text p {{
                margin: 6px 0 0 0;
                color: rgba(255,255,255,0.92);
                font-size: 14px;
            }}
            .wrap {{
                max-width: 1240px;
                margin: 24px auto;
                padding: 0 18px 40px 18px;
            }}
            .grid {{
                display: grid;
                grid-template-columns: 1.05fr 0.95fr;
                gap: 20px;
            }}
            .card {{
                background: var(--white);
                border: 1px solid var(--line);
                border-radius: var(--radius);
                box-shadow: var(--shadow);
                padding: 22px;
            }}
            .card h2, .card h3 {{
                margin: 0 0 12px 0;
            }}
            .sub {{
                color: var(--muted);
                line-height: 1.5;
                margin-bottom: 18px;
            }}
            .form-grid {{
                display: grid;
                grid-template-columns: 1fr 1fr;
                gap: 14px;
            }}
            .field {{
                display: flex;
                flex-direction: column;
                gap: 6px;
            }}
            .field label {{
                font-size: 14px;
                font-weight: 700;
                color: var(--muted);
            }}
            input, button, select {{
                font: inherit;
            }}
            input, select {{
                width: 100%;
                padding: 12px 14px;
                border-radius: 12px;
                border: 1px solid #cad7e6;
                background: white;
                color: var(--text);
            }}
            input:focus, select:focus {{
                outline: none;
                border-color: var(--blue-700);
                box-shadow: 0 0 0 4px rgba(37,103,173,0.12);
            }}
            .actions {{
                display: flex;
                gap: 12px;
                flex-wrap: wrap;
                margin-top: 8px;
            }}
            .btn {{
                background: linear-gradient(135deg, var(--blue-800), var(--blue-700));
                color: white;
                border: none;
                border-radius: 12px;
                padding: 12px 18px;
                font-weight: 700;
                cursor: pointer;
                text-decoration: none;
                display: inline-block;
            }}
            .btn-light {{
                background: white;
                color: var(--blue-800);
                border: 1px solid #cad7e6;
                border-radius: 12px;
                padding: 12px 18px;
                font-weight: 700;
                text-decoration: none;
                display: inline-block;
            }}
            .hint {{
                background: var(--blue-100);
                border: 1px solid #d2e1f0;
                border-radius: 14px;
                padding: 14px 16px;
                line-height: 1.5;
            }}
            .ok {{ border-left: 5px solid var(--ok); }}
            .warn {{ border-left: 5px solid var(--warn); }}
            .kpis {{
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 12px;
                margin-bottom: 18px;
            }}
            .kpi {{
                background: white;
                border: 1px solid var(--line);
                border-radius: 16px;
                padding: 16px;
                box-shadow: var(--shadow);
            }}
            .kpi .n {{
                font-size: 28px;
                font-weight: 700;
                color: var(--blue-900);
            }}
            .kpi .l {{
                margin-top: 4px;
                font-size: 13px;
                color: var(--muted);
            }}
            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
                border-radius: 14px;
                overflow: hidden;
            }}
            th {{
                text-align: left;
                background: #f6f9fc;
                color: var(--muted);
                font-size: 13px;
                padding: 14px 12px;
                border-bottom: 1px solid var(--line);
            }}
            td {{
                padding: 14px 12px;
                border-bottom: 1px solid #edf2f7;
                vertical-align: top;
            }}
            tr:last-child td {{ border-bottom: none; }}
            .code {{
                font-weight: 700;
                color: var(--blue-900);
                white-space: nowrap;
            }}
            @media (max-width: 920px) {{
                .grid {{ grid-template-columns: 1fr; }}
                .form-grid {{ grid-template-columns: 1fr; }}
                .kpis {{ grid-template-columns: 1fr; }}
                .brand {{
                    flex-direction: column;
                    align-items: flex-start;
                }}
                .brand-text h1 {{ font-size: 24px; }}
                .brand img {{ height: 72px; }}
            }}
        </style>
    </head>
    <body>
        <div class="topbar">
            <div class="brand">
                <img src="/static/herrhammer-logo.png" alt="Herrhammer Logo">
                <div class="brand-text">
                    <h1>Herrhammer Reisekosten</h1>
                    <p>Codegeführt, übersichtlich und bereit für Mail, Belege und Zuordnung</p>
                </div>
            </div>
        </div>
        <div class="wrap">
            {content}
        </div>
    </body>
    </html>
    """


@app.get("/", response_class=HTMLResponse)
def home():
    today = datetime.now().strftime("%Y-%m-%d")
    content = f"""
    <div class="grid">
        <div class="card">
            <h2>Neue Reise anlegen</h2>
            <div class="sub">
                Das Sekretariat legt die Reise zuerst hier an. Danach erzeugt das System automatisch
                einen Reisecode. Alle späteren Mails, Belege und Infos müssen diesen Code tragen.
            </div>
            <form action="/create-trip" method="post">
                <div class="form-grid">
                    <div class="field">
                        <label>Mitarbeiter</label>
                        <input name="employee" placeholder="z. B. Ralf Diesslin" required>
                    </div>
                    <div class="field">
                        <label>Ziel / Stadt</label>
                        <input name="destination" placeholder="z. B. Delhi" required>
                    </div>
                    <div class="field">
                        <label>Startdatum</label>
                        <input type="date" id="start" name="start" min="{today}" required>
                    </div>
                    <div class="field">
                        <label>Enddatum</label>
                        <input type="date" id="end" name="end" min="{today}" required>
                    </div>
                </div>
                <div class="actions">
                    <button class="btn" type="submit">Reise erstellen</button>
                    <a class="btn-light" href="/dashboard">Zum Dashboard</a>
                    <a class="btn-light" href="/receipts">Zu den Belegen</a>
                    <a class="btn-light" href="/fetch-mails">Mails abrufen</a>
                </div>
            </form>
        </div>

        <div class="card warn">
            <h3>Pflichtregel für die Zuordnung</h3>
            <div class="hint">
                Nach der Anlage bitte immer einen Betreff wie diesen verwenden:<br><br>
                <b>[26-001] Reiseunterlagen Delhi</b><br><br>
                Dann kann das System alles derselben Reise zuordnen.
            </div>
        </div>
    </div>

    <script>
        const startInput = document.getElementById("start");
        const endInput = document.getElementById("end");

        function syncEndDate() {{
            if (startInput.value) {{
                endInput.min = startInput.value;
                if (!endInput.value || endInput.value < startInput.value) {{
                    endInput.value = startInput.value;
                }}
            }}
        }}

        startInput.addEventListener("change", syncEndDate);
        syncEndDate();
    </script>
    """
    return page_shell("Herrhammer Reisekosten", content)


@app.post("/create-trip", response_class=HTMLResponse)
def create_trip(
    employee: str = Form(...),
    destination: str = Form(...),
    start: str = Form(...),
    end: str = Form(...)
):
    start_date = parse_iso_date(start)
    end_date = parse_iso_date(end)

    if end_date < start_date:
        content = """
        <div class="card warn">
            <h2>Datum nicht gültig</h2>
            <div class="hint">Das Enddatum darf nicht vor dem Startdatum liegen.</div>
            <div class="actions"><a class="btn-light" href="/">Zurück</a></div>
        </div>
        """
        return page_shell("Datum ungültig", content)

    code = generate_trip_code()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trips (trip_code, employee, name, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s)
    """, (code, employee, destination, start, end))
    conn.commit()
    cur.close()
    conn.close()

    content = f"""
    <div class="card ok">
        <h2>Reise erfolgreich angelegt</h2>
        <p><b>Reisecode:</b> <span class="code">{code}</span></p>
        <p><b>Mitarbeiter:</b> {employee}</p>
        <p><b>Ziel:</b> {destination}</p>
        <p><b>Zeitraum:</b> {start} bis {end}</p>
        <div class="hint">
            Bitte ab jetzt immer diesen Betreff verwenden:<br><br>
            <b>[{code}] Reiseunterlagen {destination}</b>
        </div>
        <div class="actions" style="margin-top:18px;">
            <a class="btn-light" href="/">Neue Reise</a>
            <a class="btn" href="/dashboard">Zum Dashboard</a>
            <a class="btn-light" href="/receipts">Beleg hochladen</a>
        </div>
    </div>
    """
    return page_shell("Reise angelegt", content)


@app.get("/dashboard", response_class=HTMLResponse)
def dashboard():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM trips")
    trip_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM receipts")
    receipt_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM mail_messages")
    mail_count = cur.fetchone()[0]

    cur.execute("""
        SELECT trip_code, employee, name, start_date, end_date
        FROM trips
        ORDER BY id DESC
    """)
    trips = cur.fetchall()

    rows_html = ""
    for t in trips:
        rows_html += f"""
        <tr>
            <td class="code">{t[0] or ''}</td>
            <td>{t[1] or ''}</td>
            <td>{t[2] or ''}</td>
            <td>{t[3] or ''}</td>
            <td>{t[4] or ''}</td>
        </tr>
        """

    cur.close()
    conn.close()

    content = f"""
    <div class="kpis">
        <div class="kpi"><div class="n">{trip_count}</div><div class="l">Reisen gesamt</div></div>
        <div class="kpi"><div class="n">{receipt_count}</div><div class="l">Belege gesamt</div></div>
        <div class="kpi"><div class="n">{mail_count}</div><div class="l">Mails importiert</div></div>
    </div>

    <div class="card">
        <h2>Dashboard</h2>
        <div class="sub">
            Alle Vorgänge laufen über den Reisecode. Dadurch werden Mails, Belege und Informationen robust zugeordnet.
        </div>
        <table>
            <tr>
                <th>Code</th>
                <th>Mitarbeiter</th>
                <th>Ziel</th>
                <th>Start</th>
                <th>Ende</th>
            </tr>
            {rows_html}
        </table>
        <div class="actions" style="margin-top:18px;">
            <a class="btn-light" href="/">Neue Reise</a>
            <a class="btn" href="/receipts">Belege</a>
            <a class="btn-light" href="/mail-log">Mail-Log</a>
        </div>
    </div>
    """
    return page_shell("Dashboard", content)


@app.get("/receipts", response_class=HTMLResponse)
def receipts_page():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT trip_code, employee, name FROM trips ORDER BY id DESC")
    trips = cur.fetchall()

    options = ""
    for t in trips:
        options += f'<option value="{t[0]}">{t[0]} – {t[1]} – {t[2]}</option>'

    cur.execute("""
        SELECT trip_code, category, original_filename, created_at
        FROM receipts
        ORDER BY id DESC
        LIMIT 20
    """)
    rows = cur.fetchall()

    rows_html = ""
    for r in rows:
        rows_html += f"""
        <tr>
            <td class="code">{r[0]}</td>
            <td>{r[1]}</td>
            <td>{r[2]}</td>
            <td>{r[3]}</td>
        </tr>
        """

    cur.close()
    conn.close()

    content = f"""
    <div class="grid">
        <div class="card">
            <h2>Beleg hochladen</h2>
            <div class="sub">
                Jeder Beleg wird direkt einem Reisecode zugeordnet.
            </div>

            <form action="/upload-receipt" method="post" enctype="multipart/form-data">
                <div class="form-grid">
                    <div class="field">
                        <label>Reise</label>
                        <select name="trip_code" required>
                            {options}
                        </select>
                    </div>
                    <div class="field">
                        <label>Kategorie</label>
                        <select name="category" required>
                            <option value="Taxi">Taxi</option>
                            <option value="Hotel">Hotel</option>
                            <option value="Flug">Flug</option>
                            <option value="Bahn">Bahn</option>
                            <option value="Verpflegung">Verpflegung</option>
                            <option value="Mietwagen">Mietwagen</option>
                            <option value="Tanken Mietwagen">Tanken Mietwagen</option>
                            <option value="Boardingpass">Boardingpass</option>
                            <option value="Sonderkosten">Sonderkosten</option>
                        </select>
                    </div>
                    <div class="field" style="grid-column: 1 / -1;">
                        <label>Datei</label>
                        <input type="file" name="receipt_file" required>
                    </div>
                </div>

                <div class="actions">
                    <button class="btn" type="submit">Beleg speichern</button>
                    <a class="btn-light" href="/dashboard">Zum Dashboard</a>
                </div>
            </form>
        </div>

        <div class="card">
            <h3>Letzte Belege</h3>
            <table>
                <tr>
                    <th>Code</th>
                    <th>Kategorie</th>
                    <th>Datei</th>
                    <th>Zeitpunkt</th>
                </tr>
                {rows_html}
            </table>
        </div>
    </div>
    """
    return page_shell("Belege", content)


@app.post("/upload-receipt", response_class=HTMLResponse)
def upload_receipt(
    trip_code: str = Form(...),
    category: str = Form(...),
    receipt_file: UploadFile = File(...)
):
    ensure_upload_dir()

    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM trips WHERE trip_code = %s", (trip_code,))
    trip = cur.fetchone()

    if not trip:
        cur.close()
        conn.close()
        content = """
        <div class="card warn">
            <h2>Reisecode nicht gefunden</h2>
            <div class="hint">Der ausgewählte Reisecode existiert nicht.</div>
            <div class="actions"><a class="btn-light" href="/receipts">Zurück</a></div>
        </div>
        """
        return page_shell("Fehler", content)

    safe_name = f"{trip_code}_{category}_{receipt_file.filename}"
    file_path = os.path.join(UPLOAD_DIR, safe_name)

    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(receipt_file.file, buffer)

    cur.execute("""
        INSERT INTO receipts (trip_code, category, original_filename, file_path)
        VALUES (%s, %s, %s, %s)
    """, (trip_code, category, receipt_file.filename, file_path))
    conn.commit()
    cur.close()
    conn.close()

    content = f"""
    <div class="card ok">
        <h2>Beleg gespeichert</h2>
        <p><b>Reisecode:</b> <span class="code">{trip_code}</span></p>
        <p><b>Kategorie:</b> {category}</p>
        <p><b>Datei:</b> {receipt_file.filename}</p>
        <div class="actions">
            <a class="btn-light" href="/receipts">Weiterer Beleg</a>
            <a class="btn" href="/dashboard">Zum Dashboard</a>
        </div>
    </div>
    """
    return page_shell("Beleg gespeichert", content)


@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    if not IMAP_HOST or not IMAP_USER or not IMAP_PASS:
        content = """
        <div class="card warn">
            <h2>Mail-Zugang fehlt</h2>
            <div class="hint">
                Bitte zuerst IMAP_HOST, IMAP_USER und IMAP_PASS in den Environment Variables setzen.
            </div>
            <div class="actions"><a class="btn-light" href="/dashboard">Zurück</a></div>
        </div>
        """
        return page_shell("Mail-Zugang fehlt", content)

    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")

        status, data = mail.search(None, "ALL")
        message_ids = data[0].split()[-20:]

        conn = get_conn()
        cur = conn.cursor()

        imported = 0
        skipped = 0

        for msg_id in message_ids:
            mail_uid = msg_id.decode()

            cur.execute("SELECT id FROM mail_messages WHERE mail_uid = %s", (mail_uid,))
            if cur.fetchone():
                skipped += 1
                continue

            status, msg_data = mail.fetch(msg_id, "(RFC822)")
            raw_email = msg_data[0][1]
            msg = email.message_from_bytes(raw_email)

            subject = decode_mime_header(msg.get("Subject", ""))
            sender = decode_mime_header(msg.get("From", ""))
            body = ""

            if msg.is_multipart():
                for part in msg.walk():
                    content_type = part.get_content_type()
                    content_disposition = str(part.get("Content-Disposition"))
                    if content_type == "text/plain" and "attachment" not in content_disposition:
                        payload = part.get_payload(decode=True)
                        if payload:
                            body = payload.decode(errors="ignore")
                            break
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    body = payload.decode(errors="ignore")

            full_text = f"{subject}\n{body}"
            trip_code = extract_trip_code(full_text)

            cur.execute("""
                INSERT INTO mail_messages (mail_uid, sender, subject, body, trip_code)
                VALUES (%s, %s, %s, %s, %s)
            """, (mail_uid, sender, subject, body, trip_code))

            imported += 1

        conn.commit()
        cur.close()
        conn.close()
        mail.logout()

        content = f"""
        <div class="card ok">
            <h2>Mails importiert</h2>
            <p><b>Neu importiert:</b> {imported}</p>
            <p><b>Bereits bekannt übersprungen:</b> {skipped}</p>
            <div class="actions">
                <a class="btn" href="/mail-log">Zum Mail-Log</a>
                <a class="btn-light" href="/dashboard">Zum Dashboard</a>
            </div>
        </div>
        """
        return page_shell("Mails importiert", content)

    except Exception as e:
        content = f"""
        <div class="card warn">
            <h2>Mail-Abruf fehlgeschlagen</h2>
            <div class="hint">{str(e)}</div>
            <div class="actions"><a class="btn-light" href="/dashboard">Zurück</a></div>
        </div>
        """
        return page_shell("Mail-Fehler", content)


@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT created_at, sender, subject, trip_code
        FROM mail_messages
        ORDER BY id DESC
        LIMIT 50
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()

    rows_html = ""
    for r in rows:
        rows_html += f"""
        <tr>
            <td>{r[0]}</td>
            <td>{r[1] or ''}</td>
            <td>{r[2] or ''}</td>
            <td class="code">{r[3] or 'nicht erkannt'}</td>
        </tr>
        """

    content = f"""
    <div class="card">
        <h2>Mail-Log</h2>
        <div class="sub">
            Hier siehst du alle importierten Mails und ob ein Reisecode erkannt wurde.
        </div>
        <table>
            <tr>
                <th>Zeitpunkt</th>
                <th>Von</th>
                <th>Betreff</th>
                <th>Reisecode</th>
            </tr>
            {rows_html}
        </table>
        <div class="actions" style="margin-top:18px;">
            <a class="btn-light" href="/fetch-mails">Mails abrufen</a>
            <a class="btn" href="/dashboard">Zum Dashboard</a>
        </div>
    </div>
    """
    return page_shell("Mail-Log", content)


@app.get("/reset-demo")
def reset_demo():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE receipts RESTART IDENTITY")
    cur.execute("TRUNCATE TABLE mail_messages RESTART IDENTITY")
    cur.execute("TRUNCATE TABLE trips RESTART IDENTITY")
    cur.execute("""
        INSERT INTO trips (trip_code, employee, name, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s)
    """, ("26-001", "Ralf Diesslin", "Delhi", "2026-04-10", "2026-04-14"))
    conn.commit()
    cur.close()
    conn.close()
    return {"status": "Alte Daten gelöscht, nur 26-001 bleibt"}


@app.get("/init")
def init_db():
    ensure_upload_dir()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trips (
            id SERIAL PRIMARY KEY
        )
    """)
    cur.execute("ALTER TABLE trips ADD COLUMN IF NOT EXISTS trip_code TEXT")
    cur.execute("ALTER TABLE trips ADD COLUMN IF NOT EXISTS employee TEXT")
    cur.execute("ALTER TABLE trips ADD COLUMN IF NOT EXISTS name TEXT")
    cur.execute("ALTER TABLE trips ADD COLUMN IF NOT EXISTS start_date TEXT")
    cur.execute("ALTER TABLE trips ADD COLUMN IF NOT EXISTS end_date TEXT")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS receipts (
            id SERIAL PRIMARY KEY,
            trip_code TEXT,
            category TEXT,
            original_filename TEXT,
            file_path TEXT,
            created_at TIMESTAMP DEFAULT now()
        )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS mail_messages (
            id SERIAL PRIMARY KEY,
            mail_uid TEXT UNIQUE,
            sender TEXT,
            subject TEXT,
            body TEXT,
            trip_code TEXT,
            created_at TIMESTAMP DEFAULT now()
        )
    """)

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "DB bereit, Belege aktiviert, Mail-Import aktiviert"}
EOF