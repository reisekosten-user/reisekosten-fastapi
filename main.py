from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse, RedirectResponse
import os
import psycopg2
from datetime import datetime
import re

app = FastAPI()

DATABASE_URL = os.getenv("DATABASE_URL")


def get_conn():
    return psycopg2.connect(DATABASE_URL)


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


def page_shell(title: str, content: str):
    return f"""
    <html lang="de">
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{title}</title>
        <style>
            :root {{
                --blue-900: #0f2f57;
                --blue-800: #18457c;
                --blue-700: #1f5ea8;
                --blue-100: #eaf2fb;
                --blue-050: #f6f9fd;
                --text: #17324d;
                --muted: #5b7088;
                --line: #d9e3ef;
                --success: #127a4a;
                --white: #ffffff;
                --shadow: 0 8px 24px rgba(18, 40, 68, 0.08);
                --radius: 16px;
            }}

            * {{
                box-sizing: border-box;
            }}

            body {{
                margin: 0;
                font-family: Arial, Helvetica, sans-serif;
                color: var(--text);
                background: linear-gradient(180deg, var(--blue-050) 0%, #eef4fb 100%);
            }}

            .topbar {{
                background: linear-gradient(135deg, var(--blue-900), var(--blue-700));
                color: var(--white);
                padding: 24px 28px;
                box-shadow: var(--shadow);
            }}

            .brand {{
                display: flex;
                align-items: center;
                gap: 18px;
                max-width: 1200px;
                margin: 0 auto;
            }}

            .logo-box {{
                width: 68px;
                height: 68px;
                border-radius: 18px;
                background: rgba(255,255,255,0.14);
                border: 1px solid rgba(255,255,255,0.18);
                display: flex;
                align-items: center;
                justify-content: center;
                font-weight: 700;
                font-size: 22px;
                letter-spacing: 1px;
            }}

            .brand-text h1 {{
                margin: 0;
                font-size: 28px;
                line-height: 1.1;
            }}

            .brand-text p {{
                margin: 6px 0 0 0;
                color: rgba(255,255,255,0.88);
                font-size: 14px;
            }}

            .wrap {{
                max-width: 1200px;
                margin: 24px auto;
                padding: 0 18px 36px 18px;
            }}

            .grid {{
                display: grid;
                grid-template-columns: 1.1fr 0.9fr;
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
                margin-top: 0;
                margin-bottom: 14px;
            }}

            .sub {{
                color: var(--muted);
                margin-top: -4px;
                margin-bottom: 18px;
                line-height: 1.45;
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
                color: var(--muted);
                font-weight: 600;
            }}

            input, select, button {{
                font: inherit;
            }}

            input, select {{
                width: 100%;
                padding: 12px 14px;
                border: 1px solid #cfd9e6;
                border-radius: 12px;
                background: #fff;
                color: var(--text);
            }}

            input:focus, select:focus {{
                outline: none;
                border-color: var(--blue-700);
                box-shadow: 0 0 0 4px rgba(31, 94, 168, 0.12);
            }}

            .full {{
                grid-column: 1 / -1;
            }}

            .actions {{
                display: flex;
                gap: 12px;
                align-items: center;
                margin-top: 6px;
            }}

            .btn {{
                background: linear-gradient(135deg, var(--blue-800), var(--blue-700));
                color: white;
                border: 0;
                border-radius: 12px;
                padding: 12px 18px;
                font-weight: 700;
                cursor: pointer;
            }}

            .btn-secondary {{
                background: white;
                color: var(--blue-800);
                border: 1px solid #c7d6e8;
                text-decoration: none;
                border-radius: 12px;
                padding: 12px 18px;
                font-weight: 700;
                display: inline-block;
            }}

            .hint {{
                background: var(--blue-100);
                border: 1px solid #cfe0f3;
                border-radius: 12px;
                padding: 14px;
                color: var(--blue-900);
                line-height: 1.45;
            }}

            table {{
                width: 100%;
                border-collapse: collapse;
                background: white;
                overflow: hidden;
                border-radius: 14px;
            }}

            th {{
                text-align: left;
                font-size: 13px;
                letter-spacing: 0.02em;
                color: var(--muted);
                background: #f6f9fc;
                border-bottom: 1px solid var(--line);
                padding: 14px 12px;
            }}

            td {{
                padding: 14px 12px;
                border-bottom: 1px solid #edf2f7;
                vertical-align: top;
            }}

            tr:last-child td {{
                border-bottom: none;
            }}

            .code {{
                font-weight: 700;
                color: var(--blue-900);
            }}

            .mini {{
                font-size: 13px;
                color: var(--muted);
            }}

            .success {{
                border-left: 5px solid var(--success);
            }}

            .kpi {{
                display: grid;
                grid-template-columns: repeat(3, 1fr);
                gap: 12px;
                margin-bottom: 18px;
            }}

            .kpi-box {{
                background: #f8fbff;
                border: 1px solid #dce8f4;
                border-radius: 14px;
                padding: 16px;
            }}

            .kpi-box .n {{
                font-size: 26px;
                font-weight: 700;
                color: var(--blue-900);
            }}

            .kpi-box .l {{
                font-size: 13px;
                color: var(--muted);
                margin-top: 4px;
            }}

            @media (max-width: 900px) {{
                .grid {{
                    grid-template-columns: 1fr;
                }}

                .form-grid {{
                    grid-template-columns: 1fr;
                }}

                .kpi {{
                    grid-template-columns: 1fr;
                }}

                .brand {{
                    align-items: flex-start;
                }}

                .brand-text h1 {{
                    font-size: 22px;
                }}
            }}
        </style>
    </head>
    <body>
        <div class="topbar">
            <div class="brand">
                <div class="logo-box">HH</div>
                <div class="brand-text">
                    <h1>Herrhammer Reisekosten</h1>
                    <p>Travel Workflow · Technology for candles · Reisecode-basierte Zuordnung</p>
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
    content = """
    <div class="grid">
        <div class="card">
            <h2>Neue Reise anlegen</h2>
            <div class="sub">
                Das Sekretariat legt die Reise zuerst hier an. Das System erzeugt dann automatisch
                einen eindeutigen Reisecode wie <b>26-001</b>. Dieser Code muss danach in jeden
                Betreff und auf jede spätere Information.
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
                        <input type="date" name="start" required>
                    </div>

                    <div class="field">
                        <label>Enddatum</label>
                        <input type="date" name="end" required>
                    </div>
                </div>

                <div class="actions">
                    <button class="btn" type="submit">Reise erstellen</button>
                    <a class="btn-secondary" href="/dashboard">Zum Dashboard</a>
                </div>
            </form>
        </div>

        <div class="card">
            <h3>So arbeitet das System</h3>
            <div class="hint">
                1. Reise hier anlegen<br>
                2. Reisecode erzeugen lassen<br>
                3. Code immer im Betreff verwenden, z. B. <b>[26-001]</b><br>
                4. Belege und Nachträge später exakt dieser Reise zuordnen
            </div>
        </div>
    </div>
    """
    return page_shell("Herrhammer Reisekosten", content)


@app.post("/create-trip", response_class=HTMLResponse)
def create_trip(
    employee: str = Form(...),
    destination: str = Form(...),
    start: str = Form(...),
    end: str = Form(...)
):
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
    <div class="card success">
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
            <a class="btn-secondary" href="/">Neue Reise</a>
            <a class="btn" href="/dashboard">Zum Dashboard</a>
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

    cur.execute("""
        SELECT trip_code, employee, name, start_date, end_date
        FROM trips
        ORDER BY id DESC
    """)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    rows_html = ""
    for r in rows:
        rows_html += f"""
        <tr>
            <td class="code">{r[0] or ''}</td>
            <td>{r[1] or ''}</td>
            <td>{r[2] or ''}</td>
            <td>{r[3] or ''}</td>
            <td>{r[4] or ''}</td>
        </tr>
        """

    content = f"""
    <div class="kpi">
        <div class="kpi-box">
            <div class="n">{trip_count}</div>
            <div class="l">Reisen gesamt</div>
        </div>
        <div class="kpi-box">
            <div class="n">{datetime.now().strftime("%y")}</div>
            <div class="l">Aktuelles Reisecode-Jahr</div>
        </div>
        <div class="kpi-box">
            <div class="n">[{datetime.now().strftime("%y")}-001]</div>
            <div class="l">Beispielformat</div>
        </div>
    </div>

    <div class="card">
        <h2>Dashboard</h2>
        <div class="sub">
            Alle Reisen werden über den Reisecode geführt. Dadurch können spätere Belege, Mails
            und Hinweise sauber zugeordnet werden.
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
    </div>

    <div class="actions">
        <a class="btn-secondary" href="/">Neue Reise anlegen</a>
    </div>
    """
    return page_shell("Dashboard", content)


@app.post("/email")
def email_input(text: str = Form(...)):
    code = extract_trip_code(text)

    if not code:
        return {"error": "Kein Reisecode gefunden"}

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("SELECT id FROM trips WHERE trip_code = %s", (code,))
    trip = cur.fetchone()

    cur.close()
    conn.close()

    if not trip:
        return {"error": "Reisecode existiert nicht"}

    return {
        "status": "Mail korrekt zugeordnet",
        "trip_code": code
    }


@app.get("/init")
def init_db():
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

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "DB bereit und Spalten geprüft"}