from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
import os, psycopg2, re, requests, datetime

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

DATABASE_URL = os.getenv("DATABASE_URL")

def get_conn():
    return psycopg2.connect(DATABASE_URL)

# =========================
# 💰 LIVE WÄHRUNG (EZB)
# =========================

def get_live_rates():
    try:
        url = "https://api.exchangerate.host/latest?base=EUR"
        data = requests.get(url, timeout=5).json()
        return data.get("rates", {})
    except:
        return {}

def convert_to_eur(amount, currency, rates):
    try:
        value = float(amount.replace(".", "").replace(",", "."))
        if currency == "EUR":
            return value
        if currency in rates:
            return round(value / rates[currency], 2)
        return None
    except:
        return None

def detect_currency(text):
    t = text.lower()
    if "$" in t or "usd" in t:
        return "USD"
    if "£" in t or "gbp" in t:
        return "GBP"
    if "₹" in t or "inr" in t:
        return "INR"
    return "EUR"

# =========================
# 🧠 ERKENNUNG
# =========================

def detect_type(text):
    t = text.lower()

    if any(x in t for x in ["boarding","flight","flug","pnr","ticket"]):
        return "Flug"
    if any(x in t for x in ["hotel","booking","reservation","zimmer"]):
        return "Hotel"
    if any(x in t for x in ["uber","taxi","cab"]):
        return "Taxi"
    if any(x in t for x in ["restaurant","essen","meal"]):
        return "Essen"
    if any(x in t for x in ["bahn","train","ice"]):
        return "Bahn"

    return "Unbekannt"

def extract_amount(text):
    matches = re.findall(r"\d{1,3}(?:\.\d{3})*,\d{2}", text)
    return matches[-1] if matches else ""

def extract_date(text):
    m = re.search(r"\d{2}[./]\d{2}[./]\d{4}", text)
    return m.group(0) if m else ""

# =========================
# 🏨 HOTEL OVERRIDE
# =========================

@app.get("/set-hotel")
def set_hotel(code: str, mode: str):

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS trip_meta (
            trip_code TEXT PRIMARY KEY,
            hotel_mode TEXT
        )
    """)

    cur.execute("""
        INSERT INTO trip_meta (trip_code, hotel_mode)
        VALUES (%s,%s)
        ON CONFLICT (trip_code)
        DO UPDATE SET hotel_mode=%s
    """, (code, mode, mode))

    conn.commit()
    cur.close()
    conn.close()

    return {"status": "ok"}

# =========================
# 📊 DASHBOARD 4.1
# =========================

@app.get("/", response_class=HTMLResponse)
def dashboard():

    rates = get_live_rates()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT trip_code, detected_type, detected_amount, detected_vendor, review_flag
        FROM mail_attachments
    """)
    data = cur.fetchall()

    cur.execute("SELECT trip_code, hotel_mode FROM trip_meta")
    meta = dict(cur.fetchall())

    trips = {}

    for code, t, amount, vendor, review in data:
        code = code or "OHNE CODE"

        if code not in trips:
            trips[code] = {
                "flight": False,
                "hotel": False,
                "taxi": False,
                "sum_eur": 0,
                "review": 0
            }

        if t == "Flug":
            trips[code]["flight"] = True
        if t == "Hotel":
            trips[code]["hotel"] = True
        if t == "Taxi":
            trips[code]["taxi"] = True

        if review == "pruefen":
            trips[code]["review"] += 1

        if amount:
            currency = detect_currency(amount)
            eur = convert_to_eur(amount, currency, rates)
            if eur:
                trips[code]["sum_eur"] += eur

    html = ""

    for code, t in trips.items():

        hotel_override = meta.get(code)

        has_hotel = t["hotel"]

        if hotel_override == "customer":
            has_hotel = True

        warnings = []
        errors = []

        if code == "OHNE CODE":
            errors.append("Kein Reisecode")

        if t["flight"] and not has_hotel:
            warnings.append("Hotel fehlt")

        if errors:
            status = "🔴 Fehler"
        elif warnings or t["review"] > 0:
            status = "🟡 prüfen"
        else:
            status = "🟢 OK"

        html += f"""
        <tr>
            <td>{code}</td>
            <td>{'✓' if t['flight'] else ''}</td>
            <td>{'✓' if has_hotel else ''}</td>
            <td>{'✓' if t['taxi'] else ''}</td>
            <td>{t['review']}</td>
            <td>{round(t['sum_eur'],2)} €</td>
            <td>{", ".join(warnings)}</td>
            <td>{", ".join(errors)}</td>
            <td>{status}</td>
            <td>
                <a href="/set-hotel?code={code}&mode=customer">Hotel Kunde</a> |
                <a href="/set-hotel?code={code}&mode=own">Hotel selbst</a>
            </td>
        </tr>
        """

    cur.close()
    conn.close()

    return f"""
    <html>
    <body style="font-family:Arial;background:#eef4fb">

    <h2>🚀 Reisekosten Dashboard 4.1</h2>

    <table border=1 cellpadding=8 style="background:white">
    <tr>
        <th>Code</th>
        <th>Flug</th>
        <th>Hotel</th>
        <th>Taxi</th>
        <th>Offen</th>
        <th>Summe €</th>
        <th>Warnungen</th>
        <th>Fehler</th>
        <th>Status</th>
        <th>Aktion</th>
    </tr>
    {html}
    </table>

    </body>
    </html>
    """