from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse
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
from datetime import date, datetime
import httpx

APP_VERSION = "6.0"

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

DATABASE_URL = os.getenv("DATABASE_URL")
IMAP_HOST    = os.getenv("IMAP_HOST")
IMAP_USER    = os.getenv("IMAP_USER")
IMAP_PASS    = os.getenv("IMAP_PASS")
S3_ENDPOINT  = os.getenv("S3_ENDPOINT")
S3_BUCKET    = os.getenv("S3_BUCKET")
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_REGION    = os.getenv("S3_REGION")

# Amadeus API (optional)
AMADEUS_CLIENT_ID     = os.getenv("AMADEUS_CLIENT_ID", "")
AMADEUS_CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET", "")

# AeroDataBox / RapidAPI (optional)
AERODATABOX_KEY = os.getenv("AERODATABOX_KEY", "")

# ──────────────────────────────────────────────
# BMF-Verpflegungsmehraufwand §9 EStG
# Tagespauschalen in EUR (Stand 2024)
# ──────────────────────────────────────────────
VMA_INLAND = {"full": 28.0, "partial_arrival": 14.0, "partial_departure": 14.0}

VMA_AUSLAND = {
    "DE": {"full": 28.0,  "partial": 14.0},
    "FR": {"full": 40.0,  "partial": 20.0},
    "GB": {"full": 54.0,  "partial": 27.0},
    "US": {"full": 56.0,  "partial": 28.0},
    "IN": {"full": 32.0,  "partial": 16.0},
    "AE": {"full": 53.0,  "partial": 26.5},
    "AZ": {"full": 37.0,  "partial": 18.5},
    "CN": {"full": 44.0,  "partial": 22.0},
    "JP": {"full": 48.0,  "partial": 24.0},
    "SG": {"full": 45.0,  "partial": 22.5},
    "TR": {"full": 35.0,  "partial": 17.5},
    "CH": {"full": 55.0,  "partial": 27.5},
    "AT": {"full": 35.0,  "partial": 17.5},
    "IT": {"full": 37.0,  "partial": 18.5},
    "ES": {"full": 35.0,  "partial": 17.5},
    "NL": {"full": 39.0,  "partial": 19.5},
    "PL": {"full": 24.0,  "partial": 12.0},
}

MEAL_DEDUCTION = {"breakfast": 5.60, "lunch": 11.20, "dinner": 11.20}


def get_vma(country_code: str, day_type: str, meals: list) -> float:
    cc = (country_code or "DE").upper()
    rates = VMA_AUSLAND.get(cc, {"full": 28.0, "partial": 14.0})
    base = rates["full"] if day_type == "full" else rates["partial"]
    deduction = sum(MEAL_DEDUCTION.get(m, 0) for m in (meals or []))
    return max(0.0, round(base - deduction, 2))


# ──────────────────────────────────────────────
# DB / S3
# ──────────────────────────────────────────────

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


# ──────────────────────────────────────────────
# Trip-Status automatisch anhand Datum
# ──────────────────────────────────────────────

def compute_trip_status(departure_date, return_date) -> str:
    today = date.today()
    if not departure_date:
        return "planned"
    if isinstance(departure_date, str):
        try:
            departure_date = date.fromisoformat(departure_date)
        except Exception:
            return "planned"
    if isinstance(return_date, str):
        try:
            return_date = date.fromisoformat(return_date)
        except Exception:
            return_date = None
    if today < departure_date:
        return "planned"
    if return_date and today > return_date:
        return "done"
    return "active"


# ──────────────────────────────────────────────
# Amadeus Flugstatus
# ──────────────────────────────────────────────

_amadeus_token = None
_amadeus_token_expiry = 0

async def get_amadeus_token() -> str:
    global _amadeus_token, _amadeus_token_expiry
    if _amadeus_token and datetime.now().timestamp() < _amadeus_token_expiry - 60:
        return _amadeus_token
    if not AMADEUS_CLIENT_ID or not AMADEUS_CLIENT_SECRET:
        return ""
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "https://test.api.amadeus.com/v1/security/oauth2/token",
            data={
                "grant_type": "client_credentials",
                "client_id": AMADEUS_CLIENT_ID,
                "client_secret": AMADEUS_CLIENT_SECRET,
            }
        )
        if resp.status_code == 200:
            data = resp.json()
            _amadeus_token = data.get("access_token", "")
            _amadeus_token_expiry = datetime.now().timestamp() + data.get("expires_in", 1799)
            return _amadeus_token
    return ""


async def get_flight_status_amadeus(flight_number: str, flight_date: str) -> dict:
    """
    Prüft Flugstatus via Amadeus Flight Status API.
    flight_number z.B. 'AZ770', flight_date z.B. '2026-03-22'
    """
    if not flight_number or not AMADEUS_CLIENT_ID:
        return {"source": "none", "status": "unbekannt", "delay_min": None}
    try:
        token = await get_amadeus_token()
        if not token:
            return {"source": "amadeus", "status": "kein Token", "delay_min": None}
        carrier = flight_number[:2].upper()
        number  = re.sub(r"[^0-9]", "", flight_number)
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://test.api.amadeus.com/v2/schedule/flights",
                headers={"Authorization": f"Bearer {token}"},
                params={
                    "carrierCode": carrier,
                    "flightNumber": number,
                    "scheduledDepartureDate": flight_date,
                },
                timeout=8.0
            )
        if resp.status_code == 200:
            data = resp.json()
            flights = data.get("data", [])
            if flights:
                f = flights[0]
                dep = f.get("flightPoints", [{}])[0]
                delay = dep.get("departure", {}).get("timings", [{}])[0].get("delays", [{}])
                delay_min = delay[0].get("duration", "PT0M") if delay else "PT0M"
                mins = int(re.sub(r"[^0-9]", "", delay_min) or 0)
                status_raw = f.get("flightDesignator", {}).get("carrierCode", "")
                return {
                    "source": "amadeus",
                    "status": "verspätet" if mins > 15 else "pünktlich",
                    "delay_min": mins,
                    "raw": f.get("flightDesignator", {}),
                }
        return {"source": "amadeus", "status": f"HTTP {resp.status_code}", "delay_min": None}
    except Exception as e:
        return {"source": "amadeus", "status": f"Fehler: {e}", "delay_min": None}


async def get_flight_status_aerodatabox(flight_number: str, flight_date: str) -> dict:
    """Fallback: AeroDataBox via RapidAPI."""
    if not AERODATABOX_KEY or not flight_number:
        return {"source": "aerodatabox", "status": "kein Key", "delay_min": None}
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(
                f"https://aerodatabox.p.rapidapi.com/flights/number/{flight_number}/{flight_date}",
                headers={
                    "X-RapidAPI-Key": AERODATABOX_KEY,
                    "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com",
                },
                timeout=8.0
            )
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and data:
                f = data[0]
                status = f.get("status", "unknown")
                dep = f.get("departure", {})
                delay_min = dep.get("delay", 0) or 0
                return {
                    "source": "aerodatabox",
                    "status": "verspätet" if delay_min > 15 else status,
                    "delay_min": delay_min,
                }
        return {"source": "aerodatabox", "status": f"HTTP {resp.status_code}", "delay_min": None}
    except Exception as e:
        return {"source": "aerodatabox", "status": f"Fehler: {e}", "delay_min": None}


# ──────────────────────────────────────────────
# Mail-Hilfsfunktionen (aus 5.4 übernommen)
# ──────────────────────────────────────────────

def extract_trip_code(text: str):
    match = re.search(r"\b\d{2}-\d{3}\b", text or "")
    return match.group(0) if match else None


def decode_mime_header(value):
    if not value:
        return ""
    parts = decode_header(value)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="ignore"))
        else:
            result.append(part)
    return "".join(result)


def detect_mail_type(text: str):
    t = (text or "").lower()
    if any(x in t for x in ["flug","flight","boarding","boardingpass","pnr","ticket","airline","itinerary","e-ticket","eticket"]):
        return "Flug"
    if any(x in t for x in ["hotel","booking.com","check-in","check out","check-out","reservation","zimmer","accommodation"]):
        return "Hotel"
    if any(x in t for x in ["taxi","uber","cab","ride"]):
        return "Taxi"
    if any(x in t for x in ["bahn","zug","train","ice","db "]):
        return "Bahn"
    if any(x in t for x in ["meal","restaurant","verpflegung","essen","dinner","lunch","breakfast","food"]):
        return "Essen"
    if any(x in t for x in ["mietwagen","rental car","car rental","hertz","sixt","avis"]):
        return "Mietwagen"
    return "Unbekannt"


def detect_destination(text: str):
    t = (text or "").lower()
    places = ["delhi","mumbai","bangalore","new york","london","paris","dubai","shanghai",
              "beijing","tokyo","singapore","mexico city","lyon","frankfurt","zaq","baku","istanbul"]
    for p in places:
        if p in t:
            return p.title()
    return ""


def sanitize_filename(name: str):
    name = (name or "").replace("\\","_").replace("/","_").strip()
    name = re.sub(r"[^A-Za-z0-9._ -]", "_", name)
    return name[:180] if name else "attachment.bin"


def detect_attachment_type(filename: str, subject: str, body: str):
    filename = filename or ""
    text = f"{filename} {subject or ''} {body or ''}".lower()
    if filename.lower().endswith(".ics"):  return "Kalendereintrag"
    if filename.lower().endswith(".emz"):  return "Inline-Grafik"
    if any(x in text for x in ["boarding","boardingpass","eticket","e-ticket","flight","flug","ticket","pnr","itinerary"]): return "Flug"
    if any(x in text for x in ["hotel","booking","reservation","zimmer","check-in","check-out"]): return "Hotel"
    if any(x in text for x in ["taxi","uber","cab","receipt_","ride"]): return "Taxi"
    if any(x in text for x in ["bahn","zug","train","ice","db"]): return "Bahn"
    if any(x in text for x in ["meal","restaurant","essen","verpflegung","breakfast","lunch","dinner","food"]): return "Essen"
    if any(x in text for x in ["mietwagen","rental","car rental","hertz","sixt","avis"]): return "Mietwagen"
    return "Unbekannt"


def is_supported_analysis_file(filename: str):
    return (filename or "").lower().endswith(".pdf")


def detect_currency(text: str):
    t = text or ""
    if re.search(r"\bINR\b|₹", t): return "INR"
    if re.search(r"\bUSD\b|\bUS\$\b", t): return "USD"
    if re.search(r"\bGBP\b|£", t): return "GBP"
    return "EUR"


def convert_to_eur(amount_str: str, currency: str):
    if not amount_str: return ""
    try:
        amount = float(amount_str.replace(".","").replace(",","."))
    except Exception:
        return ""
    rates = {"EUR":1.0,"USD":0.93,"GBP":1.17,"INR":0.011}
    eur = round(amount * rates.get(currency or "EUR", 1.0), 2)
    return f"{eur:.2f}".replace(".", ",")


def extract_date(text: str):
    if not text: return ""
    for p in [r"\b\d{2}[./]\d{2}[./]\d{4}\b", r"\b\d{1,2}\s+[A-Za-zäöüÄÖÜ]+\s+\d{4}\b"]:
        m = re.search(p, text)
        if m: return m.group(0)
    return ""


def extract_vendor(text: str, detected_type: str):
    if not text: return ""
    lower = text.lower()
    vendors = {
        "Flug":  ["lufthansa","air france","klm","ryanair","austrian","azal","turkish airlines","emirates","qatar airways","air india"],
        "Taxi":  ["uber","bolt","taxi","free now","lyft"],
        "Hotel": ["marriott","hilton","booking","novotel","ibis","hyatt","radisson","holiday inn","hotel"],
        "Bahn":  ["deutsche bahn","db","sncf","trenitalia","rail"],
        "Mietwagen": ["hertz","sixt","avis","europcar","enterprise"],
        "Essen": ["restaurant","mcdonald","burger king","starbucks","cafe"],
    }
    for k in vendors.get(detected_type, []):
        if k in lower: return k.title()
    bad = ["ihre","booking reference","buchungsreferenz","receipt","invoice number","datum","date"]
    for line in text.split("\n")[:15]:
        l = line.strip()
        if len(l) < 4: continue
        if any(l.lower().startswith(x) for x in bad): continue
        if re.search(r"\d{2}[./]\d{2}[./]\d{4}", l): continue
        return l[:100]
    return ""


def find_best_amount_and_currency(text: str, detected_type: str):
    if not text: return "", ""
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    priority_markers = ["total","gesamt","summe","amount paid","paid","you paid","fare","trip fare","grand total","total due"]
    amount_pattern = r"\b\d{1,3}(?:\.\d{3})*,\d{2}\b"
    prioritized, all_hits = [], []
    for line in lines:
        amounts = re.findall(amount_pattern, line)
        if not amounts: continue
        currency = detect_currency(line)
        for amount in amounts:
            try: value = float(amount.replace(".","").replace(",","."))
            except Exception: continue
            hit = {"amount": amount, "currency": currency, "value": value, "line": line.lower()}
            all_hits.append(hit)
            if any(m in line.lower() for m in priority_markers):
                prioritized.append(hit)

    def pick(hits):
        if not hits: return "", ""
        if detected_type == "Taxi":
            hits = [h for h in hits if 2 <= h["value"] <= 300] or hits
            for h in hits:
                if h["currency"] != "EUR" and not re.search(r"\bINR\b|₹|\bUSD\b|\bUS\$\b|\bGBP\b|£", h["line"]):
                    h["currency"] = "EUR"
        elif detected_type == "Essen":
            hits = [h for h in hits if 2 <= h["value"] <= 300] or hits
        elif detected_type in ("Hotel","Flug"):
            hits = [h for h in hits if 20 <= h["value"] <= 5000] or hits
        return hits[-1]["amount"], hits[-1]["currency"]

    a, c = pick(prioritized)
    if a: return a, c
    return pick(all_hits)


def compute_confidence(detected_type, amount, date_val, vendor, status):
    if status != "ok": return "niedrig"
    score = sum([
        bool(detected_type and detected_type != "Unbekannt"),
        bool(amount), bool(date_val), bool(vendor)
    ])
    return "hoch" if score >= 4 else ("mittel" if score >= 2 else "niedrig")


def compute_review_flag(confidence, status):
    if status in ["analysefehler","datei fehlt","kein text"]: return "pruefen"
    if confidence == "niedrig": return "pruefen"
    return "ok"


def extract_text_from_s3_object(storage_key: str, filename: str):
    try:
        s3 = get_s3()
        response = s3.get_object(Bucket=S3_BUCKET, Key=storage_key)
        file_bytes = response["Body"].read()
        if filename.lower().endswith(".pdf"):
            text = ""
            try:
                with pdfplumber.open(BytesIO(file_bytes)) as pdf:
                    for page in pdf.pages:
                        text += (page.extract_text() or "") + "\n"
            except Exception as e:
                return f"ERROR: PDF-Parsing: {e}"
            text = text.strip()
            return text[:15000] if text else "KEIN_TEXT_GEFUNDEN"
        return "NICHT_ANALYSIERBAR"
    except Exception as e:
        return f"ERROR: {e}"


# ──────────────────────────────────────────────
# HTML-Shell (Herrhammer Branding)
# ──────────────────────────────────────────────

def page_shell(title: str, content: str, active_tab: str = ""):
    tabs = [
        ("planned", "✈️ Vorplanung",     "/"),
        ("active",  "🔴 Laufende Reisen", "/active"),
        ("done",    "✅ Abgeschlossen",   "/done"),
    ]
    tab_html = ""
    for key, label, href in tabs:
        cls = "tab tab-active" if active_tab == key else "tab"
        tab_html += f'<a href="{href}" class="{cls}">{label}</a>'

    return f"""
    <html>
    <head>
        <meta charset="utf-8">
        <meta name="viewport" content="width=device-width, initial-scale=1">
        <title>{title} – Herrhammer Reisekosten</title>
        <style>
            *{{box-sizing:border-box;margin:0;padding:0}}
            body{{font-family:Arial,sans-serif;background:#eef4fb;color:#1a1a1a}}
            .topbar{{background:#12365f;color:white;padding:16px 24px;display:flex;align-items:center;justify-content:space-between}}
            .topbar-left{{display:flex;align-items:center;gap:14px}}
            .logo-wrap{{background:rgba(255,255,255,.55);padding:8px 12px;border-radius:10px;display:inline-flex;align-items:center}}
            .topbar img{{height:52px;display:block}}
            .topbar h2{{font-size:1.1rem;font-weight:600;letter-spacing:.3px}}
            .version{{font-size:12px;opacity:.85}}
            .tabs{{background:#0e2d50;display:flex;gap:2px;padding:0 24px}}
            .tab{{color:rgba(255,255,255,.7);text-decoration:none;padding:12px 20px;font-size:.9rem;border-bottom:3px solid transparent;transition:all .15s}}
            .tab:hover{{color:white;background:rgba(255,255,255,.07)}}
            .tab-active{{color:white;border-bottom:3px solid #4da6ff;background:rgba(255,255,255,.1)}}
            .wrap{{padding:24px;max-width:1400px;margin:0 auto}}
            .actions{{display:flex;gap:10px;margin-bottom:20px;flex-wrap:wrap;align-items:center}}
            .card{{background:white;padding:20px;border-radius:10px;margin-bottom:20px;box-shadow:0 1px 4px rgba(0,0,0,.06)}}
            .btn{{background:#2a6ab1;color:white;padding:9px 16px;border:none;border-radius:6px;text-decoration:none;display:inline-block;font-size:.88rem;cursor:pointer}}
            .btn:hover{{background:#245a9a}}
            .btn-light{{background:white;color:#2a6ab1;padding:9px 16px;border:1px solid #b8cbe0;border-radius:6px;text-decoration:none;display:inline-block;font-size:.88rem}}
            .btn-light:hover{{background:#f0f6ff}}
            .btn-danger{{background:#c0392b;color:white;padding:9px 16px;border:none;border-radius:6px;text-decoration:none;display:inline-block;font-size:.88rem}}
            table{{width:100%;border-collapse:collapse}}
            th,td{{border:1px solid #d9e2ec;padding:9px 10px;text-align:left;vertical-align:top;font-size:.88rem}}
            th{{background:#f4f7fb;font-weight:600}}
            tr:hover td{{background:#fafcff}}
            .ok{{color:#177245;font-weight:600}}
            .warn{{color:#b46b00;font-weight:600}}
            .err{{color:#b3261e;font-weight:600}}
            .code{{font-weight:700;color:#12365f}}
            .badge{{padding:3px 10px;border-radius:999px;font-size:.78rem;font-weight:600;white-space:nowrap}}
            .badge-ok{{background:#177245;color:white}}
            .badge-warn{{background:#b46b00;color:white}}
            .badge-bad{{background:#b3261e;color:white}}
            .badge-info{{background:#2a6ab1;color:white}}
            .badge-active{{background:#e65c00;color:white}}
            .badge-planned{{background:#2a6ab1;color:white}}
            .badge-done{{background:#177245;color:white}}
            .alert-box{{background:#fff3cd;border:1px solid #ffc107;border-radius:8px;padding:12px 16px;margin-bottom:14px;font-size:.88rem}}
            .alert-box.red{{background:#fde8e8;border-color:#e74c3c}}
            .sub{{color:#567;font-size:.85rem;margin-top:4px}}
            select{{padding:7px 10px;border-radius:5px;border:1px solid #ccc;font-size:.88rem;margin-right:6px}}
            input[type=text],input[type=date]{{padding:7px 10px;border-radius:5px;border:1px solid #ccc;font-size:.88rem}}
            h2{{font-size:1.15rem;font-weight:600;margin-bottom:6px}}
            h3{{font-size:1rem;font-weight:600;margin-bottom:10px;color:#12365f}}
        </style>
    </head>
    <body>
        <div class="topbar">
            <div class="topbar-left">
                <div class="logo-wrap">
                    <img src="/static/herrhammer-logo.png" alt="Herrhammer">
                </div>
                <h2>Herrhammer Reisekosten</h2>
            </div>
            <div class="version">Version {APP_VERSION}</div>
        </div>
        <div class="tabs">{tab_html}</div>
        <div class="wrap">{content}</div>
    </body>
    </html>
    """


# ──────────────────────────────────────────────
# /init – DB-Migration (idempotent)
# ──────────────────────────────────────────────

@app.get("/init")
def init():
    try:
        conn = get_conn()
        cur  = conn.cursor()

        # mail_messages
        cur.execute("""CREATE TABLE IF NOT EXISTS mail_messages (id SERIAL PRIMARY KEY, mail_uid TEXT UNIQUE)""")
        for col in ["sender TEXT","subject TEXT","body TEXT","trip_code TEXT","detected_type TEXT","detected_destination TEXT","created_at TIMESTAMP DEFAULT now()"]:
            cur.execute(f"ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS {col}")

        # mail_attachments
        cur.execute("""CREATE TABLE IF NOT EXISTS mail_attachments (id SERIAL PRIMARY KEY, mail_uid TEXT)""")
        for col in ["trip_code TEXT","original_filename TEXT","saved_filename TEXT","content_type TEXT",
                    "file_path TEXT","detected_type TEXT","extracted_text TEXT","detected_amount TEXT",
                    "detected_amount_eur TEXT","detected_currency TEXT","detected_date TEXT","detected_vendor TEXT",
                    "analysis_status TEXT","storage_key TEXT","confidence TEXT","review_flag TEXT",
                    "created_at TIMESTAMP DEFAULT now()"]:
            cur.execute(f"ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS {col}")

        # trip_meta – erweitert für 6.0
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trip_meta (
                trip_code TEXT PRIMARY KEY,
                hotel_mode TEXT,
                departure_date DATE,
                return_date DATE,
                country_code TEXT DEFAULT 'DE',
                traveler_name TEXT,
                colleagues TEXT,
                flight_numbers TEXT,
                car_rental_info TEXT,
                manual_status TEXT,
                nights_planned INTEGER DEFAULT 0,
                meals_reimbursed TEXT DEFAULT '',
                notes TEXT,
                created_at TIMESTAMP DEFAULT now()
            )
        """)
        # Neue Felder nachrüsten falls Tabelle schon existiert
        new_cols = [
            "departure_date DATE","return_date DATE","country_code TEXT DEFAULT 'DE'",
            "traveler_name TEXT","colleagues TEXT","flight_numbers TEXT",
            "car_rental_info TEXT","manual_status TEXT","nights_planned INTEGER DEFAULT 0",
            "meals_reimbursed TEXT DEFAULT ''","notes TEXT","created_at TIMESTAMP DEFAULT now()"
        ]
        for col in new_cols:
            cur.execute(f"ALTER TABLE trip_meta ADD COLUMN IF NOT EXISTS {col}")

        # flight_alerts
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flight_alerts (
                id SERIAL PRIMARY KEY,
                trip_code TEXT,
                flight_number TEXT,
                flight_date TEXT,
                alert_type TEXT,
                message TEXT,
                source TEXT,
                delay_min INTEGER,
                checked_at TIMESTAMP DEFAULT now()
            )
        """)

        conn.commit()
        cur.close()
        conn.close()
        return {"status": "ok", "version": APP_VERSION}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}


@app.get("/version")
def version():
    return {"version": APP_VERSION}


# ──────────────────────────────────────────────
# Hilfsfunktion: Trip-Daten laden
# ──────────────────────────────────────────────

def load_trips_with_status(conn, filter_status: str = None):
    cur = conn.cursor()
    cur.execute("""
        SELECT trip_code, hotel_mode, departure_date, return_date,
               country_code, traveler_name, colleagues, flight_numbers,
               car_rental_info, nights_planned, meals_reimbursed, notes
        FROM trip_meta ORDER BY trip_code
    """)
    raw = cur.fetchall()

    cur.execute("""
        SELECT COALESCE(trip_code,'') AS tc, detected_type,
               COALESCE(detected_amount_eur,'') AS eur, review_flag
        FROM mail_attachments
    """)
    att_rows = cur.fetchall()
    cur.close()

    att_by_code = {}
    for tc, dt, eur, rf in att_rows:
        if tc not in att_by_code:
            att_by_code[tc] = {"types": [], "sum_eur": 0.0, "review_count": 0}
        att_by_code[tc]["types"].append(dt)
        if rf == "pruefen":
            att_by_code[tc]["review_count"] += 1
        if eur:
            try:
                att_by_code[tc]["sum_eur"] += float(eur.replace(".","").replace(",","."))
            except Exception:
                pass

    trips = []
    for row in raw:
        (tc, hotel_mode, dep, ret, cc, traveler, colleagues,
         flight_nums, car_rental, nights_planned, meals_reimb, notes) = row

        status = compute_trip_status(dep, ret)

        if filter_status and status != filter_status:
            continue

        att = att_by_code.get(tc, {"types": [], "sum_eur": 0.0, "review_count": 0})
        types = att["types"]

        has_flight  = "Flug" in types
        has_hotel   = "Hotel" in types or (hotel_mode in ("customer","own"))
        has_taxi    = "Taxi" in types
        has_essen   = "Essen" in types
        has_car     = "Mietwagen" in types or bool(car_rental)

        warnings = []
        if not has_flight:
            warnings.append("Kein Flugbeleg")
        if not has_hotel and status in ("planned","active"):
            warnings.append("Hotel fehlt")
        if nights_planned and nights_planned > 0:
            hotel_count = types.count("Hotel")
            if hotel_count < nights_planned:
                warnings.append(f"Nur {hotel_count}/{nights_planned} Nächte belegt")

        trips.append({
            "trip_code":     tc,
            "status":        status,
            "hotel_mode":    hotel_mode,
            "dep":           dep,
            "ret":           ret,
            "country_code":  cc or "DE",
            "traveler":      traveler or "",
            "colleagues":    colleagues or "",
            "flight_nums":   flight_nums or "",
            "car_rental":    car_rental or "",
            "nights_planned": nights_planned or 0,
            "meals_reimb":   meals_reimb or "",
            "notes":         notes or "",
            "has_flight":    has_flight,
            "has_hotel":     has_hotel,
            "has_taxi":      has_taxi,
            "has_essen":     has_essen,
            "has_car":       has_car,
            "sum_eur":       round(att["sum_eur"], 2),
            "review_count":  att["review_count"],
            "warnings":      warnings,
        })

    return trips


# ──────────────────────────────────────────────
# / – Vorplanung (planned)
# ──────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
def home():
    try:
        conn = get_conn()
        trips = load_trips_with_status(conn, filter_status="planned")
        conn.close()

        rows = ""
        for t in trips:
            dep = str(t["dep"]) if t["dep"] else "–"
            ret = str(t["ret"]) if t["ret"] else "–"

            checks = []
            checks.append(f'<span class="badge badge-{"ok" if t["has_flight"] else "bad"}">Flug {"✓" if t["has_flight"] else "✗"}</span>')
            checks.append(f'<span class="badge badge-{"ok" if t["has_hotel"] else "bad"}">Hotel {"✓" if t["has_hotel"] else "✗"}</span>')
            checks.append(f'<span class="badge badge-{"ok" if t["has_car"] else "warn"}">Mietwagen {"✓" if t["has_car"] else "–"}</span>')

            night_badge = ""
            if t["nights_planned"] > 0:
                hotel_ok = "ok" if t["has_hotel"] else "bad"
                night_badge = f'<span class="badge badge-{hotel_ok}">{t["nights_planned"]} Nächte</span>'

            warn_html = " ".join(f'<span class="warn">⚠ {w}</span>' for w in t["warnings"])

            flight_info = t["flight_nums"] or "–"
            car_info    = t["car_rental"] or "–"

            rows += f"""
            <tr>
                <td class="code">{t["trip_code"]}</td>
                <td>{t["traveler"] or "–"}</td>
                <td>{dep}</td>
                <td>{ret}</td>
                <td>{t["country_code"]}</td>
                <td style="font-size:.8rem">{flight_info}</td>
                <td>{car_info}</td>
                <td>{" ".join(checks)} {night_badge}</td>
                <td>{warn_html or '<span class="ok">OK</span>'}</td>
                <td>{format(t["sum_eur"],".2f").replace(".",",")} €</td>
                <td>
                    <a class="btn-light" href="/trip/{t["trip_code"]}">Detail</a>
                    <a class="btn-light" href="/edit-trip/{t["trip_code"]}">Bearbeiten</a>
                </td>
            </tr>"""

        empty = '<p class="sub">Keine geplanten Reisen. <a href="/new-trip">Neue Reise anlegen</a>.</p>' if not trips else ""

        return page_shell("Vorplanung", f"""
        <div class="card">
            <h2>Vorplanung – geplante Reisen</h2>
            <div class="sub">Status wird automatisch anhand Abflug-/Rückkehrdatum berechnet.</div>
            <div class="actions" style="margin-top:14px">
                <a class="btn" href="/new-trip">+ Neue Reise</a>
                <a class="btn" href="/fetch-mails">Mails abrufen</a>
                <a class="btn" href="/analyze-attachments">Anhänge analysieren</a>
                <a class="btn-light" href="/attachment-log">Anhang Log</a>
                <a class="btn-light" href="/mail-log">Mail Log</a>
            </div>
            {empty}
            <table>
                <tr>
                    <th>Code</th><th>Reisender</th><th>Abflug</th><th>Rückkehr</th>
                    <th>Land</th><th>Flüge</th><th>Mietwagen</th>
                    <th>Vollständigkeit</th><th>Warnungen</th><th>Summe</th><th>Aktion</th>
                </tr>
                {rows}
            </table>
        </div>
        """, active_tab="planned")

    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p><a class="btn" href="/init">DB init</a></div>'), status_code=500)


# ──────────────────────────────────────────────
# /active – Laufende Reisen mit Alerts
# ──────────────────────────────────────────────

@app.get("/active", response_class=HTMLResponse)
def active_trips():
    try:
        conn = get_conn()
        trips = load_trips_with_status(conn, filter_status="active")

        # Aktuelle Alerts laden
        cur = conn.cursor()
        cur.execute("""
            SELECT trip_code, flight_number, alert_type, message, delay_min, checked_at
            FROM flight_alerts ORDER BY checked_at DESC LIMIT 100
        """)
        alert_rows = cur.fetchall()
        cur.close()
        conn.close()

        alerts_by_code = {}
        for tc, fn, at, msg, delay, ts in alert_rows:
            if tc not in alerts_by_code:
                alerts_by_code[tc] = []
            alerts_by_code[tc].append({"fn": fn, "type": at, "msg": msg, "delay": delay, "ts": str(ts)[:16]})

        rows = ""
        for t in trips:
            dep = str(t["dep"]) if t["dep"] else "–"
            ret = str(t["ret"]) if t["ret"] else "–"
            alerts = alerts_by_code.get(t["trip_code"], [])

            alert_html = ""
            for a in alerts:
                cls = "red" if a["type"] in ("delay","rebooking") else ""
                delay_txt = f" ({a['delay']} Min.)" if a["delay"] else ""
                alert_html += f'<div class="alert-box {cls}">⚠ {a["fn"]} – {a["msg"]}{delay_txt} <span class="sub">{a["ts"]}</span></div>'

            flight_badge = ""
            for fn in (t["flight_nums"] or "").split(","):
                fn = fn.strip()
                if fn:
                    flight_badge += f'<span class="badge badge-info">{fn}</span> '

            rows += f"""
            <tr>
                <td class="code">{t["trip_code"]}</td>
                <td>{t["traveler"] or "–"}</td>
                <td>{dep} → {ret}</td>
                <td>{t["country_code"]}</td>
                <td>{flight_badge or "–"}</td>
                <td>{"ja" if t["has_hotel"] else '<span class="warn">nein</span>'}</td>
                <td>{"ja" if t["has_car"] else "–"}</td>
                <td>{alert_html or '<span class="ok">Keine Alerts</span>'}</td>
                <td>
                    <a class="btn" href="/check-flights/{t["trip_code"]}">Flüge prüfen</a>
                    <a class="btn-light" href="/trip/{t["trip_code"]}">Detail</a>
                </td>
            </tr>"""

        empty = '<p class="sub">Keine laufenden Reisen.</p>' if not trips else ""

        return page_shell("Laufende Reisen", f"""
        <div class="card">
            <h2>Laufende Reisen – Live-Überwachung</h2>
            <div class="sub">Flugstatus via Amadeus + AeroDataBox. Alerts werden bei Verspätung &gt; 15 Min. oder Umbuchung gesetzt.</div>
            {empty}
            <table style="margin-top:14px">
                <tr>
                    <th>Code</th><th>Reisender</th><th>Zeitraum</th><th>Land</th>
                    <th>Flüge</th><th>Hotel</th><th>Mietwagen</th><th>Alerts</th><th>Aktion</th>
                </tr>
                {rows}
            </table>
        </div>
        """, active_tab="active")

    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p></div>'), status_code=500)


# ──────────────────────────────────────────────
# /done – Abgeschlossene Reisen + VMA
# ──────────────────────────────────────────────

@app.get("/done", response_class=HTMLResponse)
def done_trips():
    try:
        conn = get_conn()
        trips = load_trips_with_status(conn, filter_status="done")
        conn.close()

        rows = ""
        for t in trips:
            dep = t["dep"]
            ret = t["ret"]
            dep_str = str(dep) if dep else "–"
            ret_str = str(ret) if ret else "–"

            # Reisezeit berechnen
            travel_days = 0
            if dep and ret:
                try:
                    if isinstance(dep, str): dep = date.fromisoformat(dep)
                    if isinstance(ret, str): ret = date.fromisoformat(ret)
                    travel_days = (ret - dep).days + 1
                except Exception:
                    pass

            # VMA berechnen
            vma_total = 0.0
            meals = [m.strip() for m in (t["meals_reimb"] or "").split(",") if m.strip()]
            if travel_days > 0:
                # Anreisetag + Abreisetag = partial, Zwischentage = full
                if travel_days == 1:
                    vma_total = get_vma(t["country_code"], "partial", meals)
                else:
                    vma_total += get_vma(t["country_code"], "partial", [])          # Anreisetag
                    vma_total += get_vma(t["country_code"], "full", []) * max(0, travel_days - 2)  # Volle Tage
                    vma_total += get_vma(t["country_code"], "partial", meals)        # Abreisetag (Mahlzeitenabzug)

            meal_badges = " ".join(f'<span class="badge badge-info">{m}</span>' for m in meals) if meals else "–"

            rows += f"""
            <tr>
                <td class="code">{t["trip_code"]}</td>
                <td>{t["traveler"] or "–"}</td>
                <td>{dep_str} → {ret_str}</td>
                <td>{travel_days} Tage</td>
                <td>{t["country_code"]}</td>
                <td>{meal_badges}</td>
                <td><strong>{vma_total:.2f} €</strong></td>
                <td>{format(t["sum_eur"],".2f").replace(".",",")} €</td>
                <td><strong>{(t["sum_eur"] + vma_total):.2f} €</strong></td>
                <td>
                    <a class="btn" href="/report/{t["trip_code"]}">Abrechnung</a>
                    <a class="btn-light" href="/trip/{t["trip_code"]}">Detail</a>
                </td>
            </tr>"""

        empty = '<p class="sub">Noch keine abgeschlossenen Reisen.</p>' if not trips else ""

        # Gesamtsumme
        total_belege = sum(t["sum_eur"] for t in trips)
        total_vma = 0.0
        for t in trips:
            dep, ret = t["dep"], t["ret"]
            travel_days = 0
            if dep and ret:
                try:
                    if isinstance(dep, str): dep = date.fromisoformat(dep)
                    if isinstance(ret, str): ret = date.fromisoformat(ret)
                    travel_days = (ret - dep).days + 1
                except Exception:
                    pass
            meals = [m.strip() for m in (t["meals_reimb"] or "").split(",") if m.strip()]
            if travel_days > 0:
                if travel_days == 1:
                    total_vma += get_vma(t["country_code"], "partial", meals)
                else:
                    total_vma += get_vma(t["country_code"], "partial", [])
                    total_vma += get_vma(t["country_code"], "full", []) * max(0, travel_days - 2)
                    total_vma += get_vma(t["country_code"], "partial", meals)

        summary = f"""
        <div class="card">
            <h3>Gesamtübersicht abgeschlossene Reisen</h3>
            <table style="width:auto">
                <tr><th>Belegkosten gesamt</th><td><strong>{total_belege:.2f} €</strong></td></tr>
                <tr><th>VMA gesamt (§9 EStG)</th><td><strong>{total_vma:.2f} €</strong></td></tr>
                <tr><th>Summe gesamt</th><td><strong>{(total_belege+total_vma):.2f} €</strong></td></tr>
            </table>
        </div>
        """ if trips else ""

        return page_shell("Abgeschlossen", f"""
        {summary}
        <div class="card">
            <h2>Abgeschlossene Reisen – Abrechnung</h2>
            <div class="sub">VMA nach §9 EStG BMF-Tabelle, länderspezifisch. Mahlzeitenabzug wird berücksichtigt.</div>
            {empty}
            <table style="margin-top:14px">
                <tr>
                    <th>Code</th><th>Reisender</th><th>Zeitraum</th><th>Dauer</th><th>Land</th>
                    <th>Erstattete Mahlzeiten</th><th>VMA</th><th>Belege</th><th>Gesamt</th><th>Aktion</th>
                </tr>
                {rows}
            </table>
        </div>
        """, active_tab="done")

    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p></div>'), status_code=500)


# ──────────────────────────────────────────────
# /new-trip – Neue Reise anlegen
# ──────────────────────────────────────────────

@app.get("/new-trip", response_class=HTMLResponse)
def new_trip_form():
    return page_shell("Neue Reise", f"""
    <div class="card">
        <h2>Neue Reise anlegen</h2>
        <form method="post" action="/new-trip">
            <table style="width:100%;max-width:700px;border:none">
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Reisecode (z.B. 26-003)</label></td>
                    <td style="border:none;padding:6px 0"><input type="text" name="trip_code" required style="width:100%"></td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Reisender</label></td>
                    <td style="border:none;padding:6px 0"><input type="text" name="traveler_name" style="width:100%"></td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Kollegen (kommagetrennt)</label></td>
                    <td style="border:none;padding:6px 0"><input type="text" name="colleagues" style="width:100%"></td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Abflugdatum</label></td>
                    <td style="border:none;padding:6px 0"><input type="date" name="departure_date" style="width:100%"></td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Rückkehrdatum</label></td>
                    <td style="border:none;padding:6px 0"><input type="date" name="return_date" style="width:100%"></td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Land (ISO-Code, z.B. IN)</label></td>
                    <td style="border:none;padding:6px 0">
                        <select name="country_code" style="width:100%">
                            <option value="DE">DE – Deutschland (28 €)</option>
                            <option value="FR">FR – Frankreich (40 €)</option>
                            <option value="GB">GB – Großbritannien (54 €)</option>
                            <option value="US">US – USA (56 €)</option>
                            <option value="IN">IN – Indien (32 €)</option>
                            <option value="AE">AE – VAE/Dubai (53 €)</option>
                            <option value="AZ">AZ – Aserbaidschan (37 €)</option>
                            <option value="CH">CH – Schweiz (55 €)</option>
                            <option value="AT">AT – Österreich (35 €)</option>
                            <option value="IT">IT – Italien (37 €)</option>
                            <option value="ES">ES – Spanien (35 €)</option>
                            <option value="NL">NL – Niederlande (39 €)</option>
                            <option value="TR">TR – Türkei (35 €)</option>
                            <option value="JP">JP – Japan (48 €)</option>
                            <option value="SG">SG – Singapur (45 €)</option>
                            <option value="PL">PL – Polen (24 €)</option>
                        </select>
                    </td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Flugnummern (z.B. AZ770,AZ281)</label></td>
                    <td style="border:none;padding:6px 0"><input type="text" name="flight_numbers" style="width:100%"></td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Geplante Übernachtungen</label></td>
                    <td style="border:none;padding:6px 0"><input type="text" name="nights_planned" value="0" style="width:100%"></td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Mietwagen-Info</label></td>
                    <td style="border:none;padding:6px 0"><input type="text" name="car_rental_info" style="width:100%"></td>
                </tr>
                <tr style="border:none">
                    <td style="border:none;padding:6px 0"><label>Notizen</label></td>
                    <td style="border:none;padding:6px 0"><input type="text" name="notes" style="width:100%"></td>
                </tr>
            </table>
            <br>
            <button type="submit" class="btn">Reise anlegen</button>
            <a class="btn-light" href="/">Abbrechen</a>
        </form>
    </div>
    """, active_tab="planned")


@app.post("/new-trip", response_class=HTMLResponse)
async def new_trip_save(request: Request):
    try:
        form = await request.form()
        tc = (form.get("trip_code") or "").strip()
        if not tc:
            return HTMLResponse("Kein Reisecode angegeben", status_code=400)
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            INSERT INTO trip_meta
              (trip_code, traveler_name, colleagues, departure_date, return_date,
               country_code, flight_numbers, nights_planned, car_rental_info, notes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (trip_code) DO UPDATE SET
              traveler_name=EXCLUDED.traveler_name,
              colleagues=EXCLUDED.colleagues,
              departure_date=EXCLUDED.departure_date,
              return_date=EXCLUDED.return_date,
              country_code=EXCLUDED.country_code,
              flight_numbers=EXCLUDED.flight_numbers,
              nights_planned=EXCLUDED.nights_planned,
              car_rental_info=EXCLUDED.car_rental_info,
              notes=EXCLUDED.notes
        """, (
            tc,
            form.get("traveler_name") or None,
            form.get("colleagues") or None,
            form.get("departure_date") or None,
            form.get("return_date") or None,
            form.get("country_code") or "DE",
            form.get("flight_numbers") or None,
            int(form.get("nights_planned") or 0),
            form.get("car_rental_info") or None,
            form.get("notes") or None,
        ))
        conn.commit()
        cur.close()
        conn.close()
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p></div>'))


# ──────────────────────────────────────────────
# /edit-trip/<code> – Reise bearbeiten
# ──────────────────────────────────────────────

@app.get("/edit-trip/{trip_code}", response_class=HTMLResponse)
def edit_trip_form(trip_code: str):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT traveler_name, colleagues, departure_date, return_date, country_code,
                   flight_numbers, nights_planned, car_rental_info, meals_reimbursed, notes, hotel_mode
            FROM trip_meta WHERE trip_code=%s
        """, (trip_code,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return HTMLResponse("Reise nicht gefunden", status_code=404)

        (traveler, colleagues, dep, ret, cc, fns, nights, car, meals, notes, hotel_mode) = row
        dep_val  = str(dep)  if dep  else ""
        ret_val  = str(ret)  if ret  else ""

        cc_options = ""
        for code, label in [("DE","DE – Deutschland"),("FR","FR – Frankreich"),("GB","GB – Großbritannien"),
                             ("US","US – USA"),("IN","IN – Indien"),("AE","AE – VAE/Dubai"),
                             ("AZ","AZ – Aserbaidschan"),("CH","CH – Schweiz"),("AT","AT – Österreich"),
                             ("IT","IT – Italien"),("ES","ES – Spanien"),("TR","TR – Türkei"),
                             ("JP","JP – Japan"),("SG","SG – Singapur"),("PL","PL – Polen")]:
            sel = "selected" if cc == code else ""
            cc_options += f'<option value="{code}" {sel}>{label}</option>'

        hotel_sel_none = "selected" if not hotel_mode else ""
        hotel_sel_cust = "selected" if hotel_mode == "customer" else ""
        hotel_sel_own  = "selected" if hotel_mode == "own" else ""

        meal_options = ""
        for m in ["breakfast","lunch","dinner"]:
            checked = "checked" if m in (meals or "") else ""
            meal_options += f'<label style="margin-right:14px"><input type="checkbox" name="meals_reimbursed" value="{m}" {checked}> {m}</label>'

        return page_shell(f"Bearbeiten {trip_code}", f"""
        <div class="card">
            <h2>Reise bearbeiten – {trip_code}</h2>
            <form method="post" action="/edit-trip/{trip_code}">
                <table style="width:100%;max-width:700px;border:none">
                    <tr style="border:none"><td style="border:none;padding:6px 0">Reisender</td>
                        <td style="border:none;padding:6px 0"><input type="text" name="traveler_name" value="{traveler or ''}" style="width:100%"></td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Kollegen</td>
                        <td style="border:none;padding:6px 0"><input type="text" name="colleagues" value="{colleagues or ''}" style="width:100%"></td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Abflug</td>
                        <td style="border:none;padding:6px 0"><input type="date" name="departure_date" value="{dep_val}" style="width:100%"></td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Rückkehr</td>
                        <td style="border:none;padding:6px 0"><input type="date" name="return_date" value="{ret_val}" style="width:100%"></td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Land</td>
                        <td style="border:none;padding:6px 0"><select name="country_code" style="width:100%">{cc_options}</select></td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Flugnummern</td>
                        <td style="border:none;padding:6px 0"><input type="text" name="flight_numbers" value="{fns or ''}" style="width:100%"></td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Geplante Nächte</td>
                        <td style="border:none;padding:6px 0"><input type="text" name="nights_planned" value="{nights or 0}" style="width:100%"></td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Mietwagen</td>
                        <td style="border:none;padding:6px 0"><input type="text" name="car_rental_info" value="{car or ''}" style="width:100%"></td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Hotel-Modus</td>
                        <td style="border:none;padding:6px 0">
                            <select name="hotel_mode" style="width:100%">
                                <option value="" {hotel_sel_none}>– kein Override –</option>
                                <option value="customer" {hotel_sel_cust}>Kunde stellt Hotel</option>
                                <option value="own" {hotel_sel_own}>Eigenes Hotel</option>
                            </select>
                        </td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Erstattete Mahlzeiten</td>
                        <td style="border:none;padding:6px 0">{meal_options}</td></tr>
                    <tr style="border:none"><td style="border:none;padding:6px 0">Notizen</td>
                        <td style="border:none;padding:6px 0"><input type="text" name="notes" value="{notes or ''}" style="width:100%"></td></tr>
                </table>
                <br>
                <button type="submit" class="btn">Speichern</button>
                <a class="btn-light" href="/">Abbrechen</a>
            </form>
        </div>
        """)
    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p></div>'))


@app.post("/edit-trip/{trip_code}", response_class=HTMLResponse)
async def edit_trip_save(trip_code: str, request: Request):
    try:
        form  = await request.form()
        meals = ",".join(form.getlist("meals_reimbursed"))
        conn  = get_conn()
        cur   = conn.cursor()
        cur.execute("""
            UPDATE trip_meta SET
                traveler_name=%s, colleagues=%s, departure_date=%s, return_date=%s,
                country_code=%s, flight_numbers=%s, nights_planned=%s,
                car_rental_info=%s, hotel_mode=%s, meals_reimbursed=%s, notes=%s
            WHERE trip_code=%s
        """, (
            form.get("traveler_name") or None,
            form.get("colleagues") or None,
            form.get("departure_date") or None,
            form.get("return_date") or None,
            form.get("country_code") or "DE",
            form.get("flight_numbers") or None,
            int(form.get("nights_planned") or 0),
            form.get("car_rental_info") or None,
            form.get("hotel_mode") or None,
            meals or None,
            form.get("notes") or None,
            trip_code
        ))
        conn.commit()
        cur.close()
        conn.close()
        from fastapi.responses import RedirectResponse
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p></div>'))


# ──────────────────────────────────────────────
# /check-flights/<code> – Flugstatus prüfen
# ──────────────────────────────────────────────

@app.get("/check-flights/{trip_code}", response_class=HTMLResponse)
async def check_flights(trip_code: str):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT flight_numbers, departure_date FROM trip_meta WHERE trip_code=%s", (trip_code,))
        row = cur.fetchone()
        if not row or not row[0]:
            cur.close(); conn.close()
            return page_shell("Flugprüfung", f'<div class="card"><h2>Keine Flugnummern für {trip_code} hinterlegt.</h2><a class="btn-light" href="/edit-trip/{trip_code}">Bearbeiten</a></div>')

        flight_numbers = [f.strip() for f in (row[0] or "").split(",") if f.strip()]
        dep_date = str(row[1]) if row[1] else str(date.today())

        results = []
        for fn in flight_numbers:
            status = await get_flight_status_amadeus(fn, dep_date)
            if status.get("status") in ("kein Token","none",""):
                status = await get_flight_status_aerodatabox(fn, dep_date)

            alert_type = "delay" if (status.get("delay_min") or 0) > 15 else "ok"
            msg = f"Verspätung {status.get('delay_min')} Min." if alert_type == "delay" else status.get("status","–")

            # Alert speichern
            cur.execute("""
                INSERT INTO flight_alerts (trip_code, flight_number, flight_date, alert_type, message, source, delay_min)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (trip_code, fn, dep_date, alert_type, msg, status.get("source",""), status.get("delay_min")))

            results.append({"fn": fn, "status": status, "alert": alert_type, "msg": msg})

        conn.commit()
        cur.close()
        conn.close()

        rows = ""
        for r in results:
            badge_cls = "badge-bad" if r["alert"] == "delay" else "badge-ok"
            rows += f"""
            <tr>
                <td class="code">{r["fn"]}</td>
                <td>{dep_date}</td>
                <td><span class="badge {badge_cls}">{r["msg"]}</span></td>
                <td>{r["status"].get("source","–")}</td>
                <td>{r["status"].get("delay_min","–")}</td>
            </tr>"""

        return page_shell("Flugprüfung", f"""
        <div class="card">
            <h2>Flugstatus – {trip_code}</h2>
            <table>
                <tr><th>Flug</th><th>Datum</th><th>Status</th><th>Quelle</th><th>Verspätung (Min.)</th></tr>
                {rows}
            </table>
            <br>
            <a class="btn-light" href="/active">Zurück</a>
        </div>
        """, active_tab="active")

    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p></div>'))


# ──────────────────────────────────────────────
# /report/<code> – Abrechnungs-HTML (später PDF)
# ──────────────────────────────────────────────

@app.get("/report/{trip_code}", response_class=HTMLResponse)
def report(trip_code: str):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            SELECT traveler_name, departure_date, return_date, country_code,
                   meals_reimbursed, flight_numbers, colleagues, notes
            FROM trip_meta WHERE trip_code=%s
        """, (trip_code,))
        meta = cur.fetchone()
        if not meta:
            cur.close(); conn.close()
            return HTMLResponse("Reise nicht gefunden", status_code=404)

        (traveler, dep, ret, cc, meals_reimb, fns, colleagues, notes) = meta

        cur.execute("""
            SELECT original_filename, detected_type, detected_amount, detected_amount_eur,
                   detected_currency, detected_date, detected_vendor, analysis_status, confidence
            FROM mail_attachments WHERE trip_code=%s ORDER BY id
        """, (trip_code,))
        atts = cur.fetchall()
        cur.close()
        conn.close()

        # Reisezeit
        travel_days = 0
        dep_str = str(dep) if dep else "–"
        ret_str = str(ret) if ret else "–"
        if dep and ret:
            try:
                d = dep if isinstance(dep, date) else date.fromisoformat(str(dep))
                r = ret if isinstance(ret, date) else date.fromisoformat(str(ret))
                travel_days = (r - d).days + 1
            except Exception:
                pass

        meals = [m.strip() for m in (meals_reimb or "").split(",") if m.strip()]
        vma_total = 0.0
        vma_rows  = ""
        if travel_days > 0:
            tag_types = (
                [("Anreisetag", "partial")] +
                [("Reisetag", "full")] * max(0, travel_days - 2) +
                [("Abreisetag", "partial")]
            ) if travel_days > 1 else [("Reisetag (eintägig)", "partial")]

            for i, (tag_label, day_type) in enumerate(tag_types):
                meal_abz = meals if (day_type == "partial" and i == len(tag_types)-1) else []
                vma = get_vma(cc or "DE", day_type, meal_abz)
                vma_total += vma
                meal_txt = ", ".join(meal_abz) if meal_abz else "–"
                vma_rows += f"<tr><td>{tag_label}</td><td>{cc or 'DE'}</td><td>{meal_txt}</td><td>{vma:.2f} €</td></tr>"

        # Belege
        beleg_rows = ""
        beleg_sum  = 0.0
        for att in atts:
            fn, dt, amt, amt_eur, curr, d, vendor, stat, conf = att
            if stat not in ("ok","mittel"): continue
            if not amt_eur: continue
            try:
                beleg_sum += float(amt_eur.replace(".","").replace(",","."))
            except Exception:
                pass
            beleg_rows += f"<tr><td>{fn}</td><td>{dt or '–'}</td><td>{vendor or '–'}</td><td>{d or '–'}</td><td>{amt or '–'} {curr or ''}</td><td>{amt_eur or '–'} €</td></tr>"

        gesamt = beleg_sum + vma_total

        return page_shell(f"Abrechnung {trip_code}", f"""
        <div class="card" style="max-width:900px">
            <h2>Reisekostenabrechnung – {trip_code}</h2>
            <table style="width:auto;margin-bottom:20px;border:none">
                <tr style="border:none"><td style="border:none;padding:3px 10px 3px 0"><strong>Reisender:</strong></td><td style="border:none">{traveler or "–"}</td></tr>
                <tr style="border:none"><td style="border:none;padding:3px 10px 3px 0"><strong>Zeitraum:</strong></td><td style="border:none">{dep_str} – {ret_str} ({travel_days} Tage)</td></tr>
                <tr style="border:none"><td style="border:none;padding:3px 10px 3px 0"><strong>Reiseland:</strong></td><td style="border:none">{cc or "DE"}</td></tr>
                <tr style="border:none"><td style="border:none;padding:3px 10px 3px 0"><strong>Flüge:</strong></td><td style="border:none">{fns or "–"}</td></tr>
                <tr style="border:none"><td style="border:none;padding:3px 10px 3px 0"><strong>Kollegen:</strong></td><td style="border:none">{colleagues or "–"}</td></tr>
            </table>

            <h3>Belege</h3>
            <table>
                <tr><th>Datei</th><th>Typ</th><th>Anbieter</th><th>Datum</th><th>Betrag orig.</th><th>Betrag EUR</th></tr>
                {beleg_rows or '<tr><td colspan="6">Keine analysierten Belege</td></tr>'}
                <tr><td colspan="5"><strong>Summe Belege</strong></td><td><strong>{beleg_sum:.2f} €</strong></td></tr>
            </table>

            <h3 style="margin-top:20px">Verpflegungsmehraufwand §9 EStG</h3>
            <table>
                <tr><th>Tag</th><th>Land</th><th>Abzug Mahlzeiten</th><th>VMA</th></tr>
                {vma_rows or '<tr><td colspan="4">Keine Reisezeit erfasst</td></tr>'}
                <tr><td colspan="3"><strong>Summe VMA</strong></td><td><strong>{vma_total:.2f} €</strong></td></tr>
            </table>

            <div style="margin-top:20px;padding:16px;background:#f4f7fb;border-radius:8px">
                <strong style="font-size:1.1rem">Gesamtbetrag: {gesamt:.2f} €</strong>
                <span class="sub"> (Belege {beleg_sum:.2f} € + VMA {vma_total:.2f} €)</span>
            </div>

            <div style="margin-top:16px">
                <a class="btn-light" href="/done">Zurück</a>
                <span class="sub" style="margin-left:12px">PDF-Export folgt in Version 6.1</span>
            </div>
        </div>
        """, active_tab="done")

    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p></div>'))


# ──────────────────────────────────────────────
# /trip/<code> – Trip-Detail
# ──────────────────────────────────────────────

@app.get("/trip/{trip_code}", response_class=HTMLResponse)
def trip_detail(trip_code: str):
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("SELECT * FROM trip_meta WHERE trip_code=%s", (trip_code,))
        meta = cur.fetchone()
        cur.execute("""
            SELECT original_filename, detected_type, detected_amount_eur, detected_currency,
                   detected_date, detected_vendor, analysis_status, confidence, review_flag
            FROM mail_attachments WHERE trip_code=%s ORDER BY id DESC
        """, (trip_code,))
        atts = cur.fetchall()
        cur.close()
        conn.close()

        rows = "".join(f"""
        <tr>
            <td>{a[0] or ''}</td><td>{a[1] or ''}</td><td>{a[2] or ''} {a[3] or ''}</td>
            <td>{a[4] or ''}</td><td>{a[5] or ''}</td>
            <td><span class="badge badge-{'ok' if a[6]=='ok' else 'warn'}">{a[6] or ''}</span></td>
            <td>{a[7] or ''}</td>
        </tr>""" for a in atts)

        return page_shell(f"Detail {trip_code}", f"""
        <div class="card">
            <h2>Reise {trip_code}</h2>
            <a class="btn-light" href="/edit-trip/{trip_code}">Bearbeiten</a>
            <a class="btn-light" href="/report/{trip_code}" style="margin-left:8px">Abrechnung</a>
            <table style="margin-top:16px">
                <tr><th>Datei</th><th>Typ</th><th>Betrag EUR</th><th>Datum</th><th>Anbieter</th><th>Status</th><th>Konfidenz</th></tr>
                {rows or '<tr><td colspan="7">Keine Anhänge</td></tr>'}
            </table>
        </div>
        """)
    except Exception as e:
        return HTMLResponse(page_shell("Fehler", f'<div class="card"><h2 class="err">Fehler</h2><p>{e}</p></div>'))


# ──────────────────────────────────────────────
# Mail-Routen (aus 5.4, unverändert stabil)
# ──────────────────────────────────────────────

@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    try:
        s3   = get_s3()
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")
        status, data = mail.search(None, "ALL")
        ids = data[0].split()[-20:]
        conn = get_conn()
        cur  = conn.cursor()
        imported = skipped = attachment_count = 0
        for i in ids:
            uid = i.decode()
            cur.execute("SELECT id FROM mail_messages WHERE mail_uid=%s", (uid,))
            if cur.fetchone():
                skipped += 1; continue
            _, msg_data = mail.fetch(i, "(RFC822)")
            msg     = email.message_from_bytes(msg_data[0][1])
            subject = decode_mime_header(msg.get("Subject",""))
            sender  = decode_mime_header(msg.get("From",""))
            body    = ""
            if msg.is_multipart():
                for part in msg.walk():
                    ct = part.get_content_type()
                    cd = str(part.get("Content-Disposition") or "")
                    if ct == "text/plain" and "attachment" not in cd.lower():
                        payload = part.get_payload(decode=True)
                        if payload: body = payload.decode(errors="ignore"); break
            else:
                payload = msg.get_payload(decode=True)
                if payload: body = payload.decode(errors="ignore")
            full_text = subject + "\n" + body
            code      = extract_trip_code(full_text)
            det_type  = detect_mail_type(full_text)
            det_dest  = detect_destination(full_text)
            cur.execute("""
                INSERT INTO mail_messages (mail_uid,sender,subject,body,trip_code,detected_type,detected_destination)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (uid, sender, subject, body, code, det_type, det_dest))
            if msg.is_multipart():
                for part in msg.walk():
                    filename = part.get_filename()
                    cd = str(part.get("Content-Disposition") or "")
                    if not filename and "attachment" not in cd.lower(): continue
                    if filename:
                        decoded_filename = decode_mime_header(filename)
                    else:
                        ext_map = {"application/pdf":".pdf","image/jpeg":".jpg","image/png":".png","image/webp":".webp","text/calendar":".ics"}
                        decoded_filename = "attachment" + ext_map.get(part.get_content_type(), ".bin")
                    payload = part.get_payload(decode=True)
                    if not payload: continue
                    safe_fn    = sanitize_filename(decoded_filename)
                    saved_fn   = f"{uid}_{safe_fn}"
                    storage_key = f"mail_attachments/{saved_fn}"
                    try:
                        s3.put_object(Bucket=S3_BUCKET, Key=storage_key, Body=payload,
                                      ContentType=part.get_content_type() or "application/octet-stream")
                    except Exception as s3e:
                        storage_key = f"S3-FEHLER: {s3e}"
                    att_type = detect_attachment_type(safe_fn, subject, body)
                    cur.execute("""
                        INSERT INTO mail_attachments
                        (mail_uid,trip_code,original_filename,saved_filename,content_type,file_path,
                         detected_type,analysis_status,storage_key,confidence,review_flag)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """, (uid, code, safe_fn, saved_fn, part.get_content_type(), storage_key,
                          att_type, "neu", storage_key, "niedrig", "pruefen"))
                    attachment_count += 1
            imported += 1
        conn.commit(); cur.close(); conn.close(); mail.logout()
        return page_shell("Mails", f"""
        <div class="card">
            <h2 class="ok">Mailabruf erfolgreich</h2>
            <p><b>Importiert:</b> {imported} &nbsp;|&nbsp; <b>Übersprungen:</b> {skipped} &nbsp;|&nbsp; <b>Anhänge:</b> {attachment_count}</p>
            <a class="btn" href="/">Dashboard</a>
            <a class="btn-light" href="/attachment-log" style="margin-left:8px">Anhang Log</a>
        </div>""")
    except Exception as e:
        return page_shell("Fehler", f'<div class="card"><h2 class="warn">Fehler beim Mailabruf</h2><p>{e}</p></div>')


@app.get("/analyze-attachments", response_class=HTMLResponse)
def analyze_attachments():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT id,storage_key,original_filename,detected_type FROM mail_attachments ORDER BY id")
        rows = cur.fetchall()
        processed = 0
        for row in rows:
            att_id, storage_key, original_filename, detected_type = row
            storage_key      = storage_key or ""
            original_filename = original_filename or ""
            detected_type    = detected_type or "Unbekannt"
            try:
                if not storage_key or storage_key.startswith("S3-FEHLER"):
                    cur.execute("UPDATE mail_attachments SET extracted_text=%s,analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                                ("KEIN_STORAGE_KEY","kein storage key","niedrig","pruefen",att_id)); processed+=1; continue
                if not is_supported_analysis_file(original_filename):
                    cur.execute("UPDATE mail_attachments SET extracted_text=%s,analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                                ("NICHT_ANALYSIERBAR","nicht analysierbar","niedrig","pruefen",att_id)); processed+=1; continue
                text = extract_text_from_s3_object(storage_key, original_filename)
                amount, currency = find_best_amount_and_currency(text, detected_type)
                if not currency: currency = detect_currency(text)
                amount_eur = convert_to_eur(amount, currency)
                date_val   = extract_date(text)
                vendor     = extract_vendor(text, detected_type)
                status     = "ok" if text not in ("NICHT_ANALYSIERBAR","KEIN_TEXT_GEFUNDEN") and not text.startswith("ERROR:") else (
                    "nicht analysierbar" if text == "NICHT_ANALYSIERBAR" else ("kein text" if text == "KEIN_TEXT_GEFUNDEN" else "analysefehler"))
                conf = compute_confidence(detected_type, amount, date_val, vendor, status)
                rf   = compute_review_flag(conf, status)
                cur.execute("""UPDATE mail_attachments SET extracted_text=%s,detected_amount=%s,detected_amount_eur=%s,
                    detected_currency=%s,detected_date=%s,detected_vendor=%s,analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s""",
                    (text, amount, amount_eur, currency, date_val, vendor, status, conf, rf, att_id))
            except Exception as re:
                try: cur.execute("UPDATE mail_attachments SET analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                                 (f"analysefehler: {str(re)[:80]}","niedrig","pruefen",att_id))
                except Exception: pass
            processed += 1
        conn.commit(); cur.close(); conn.close()
        return page_shell("Analyse", f"""
        <div class="card">
            <h2 class="ok">{processed} Anhänge analysiert</h2>
            <a class="btn" href="/">Dashboard</a>
            <a class="btn-light" href="/attachment-log" style="margin-left:8px">Anhang Log</a>
        </div>""")
    except Exception as e:
        return page_shell("Fehler", f'<div class="card"><h2 class="warn">Fehler</h2><p>{e}</p></div>')


@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("SELECT sender,subject,trip_code,detected_type,detected_destination FROM mail_messages ORDER BY id DESC LIMIT 50")
        rows = cur.fetchall(); cur.close(); conn.close()
        html = "".join(f"<tr><td>{r[0] or ''}</td><td>{r[1] or ''}</td><td class='code'>{r[2] or ''}</td><td>{r[3] or ''}</td><td>{r[4] or ''}</td></tr>" for r in rows)
        return page_shell("Mail Log", f"""
        <div class="card"><h2>Mail Log</h2>
        <a class="btn-light" href="/">Zurück</a>
        <table style="margin-top:14px">
            <tr><th>Von</th><th>Betreff</th><th>Code</th><th>Typ</th><th>Ziel</th></tr>
            {html}
        </table></div>""")
    except Exception as e:
        return page_shell("Fehler", f'<div class="card"><h2 class="warn">Fehler</h2><p>{e}</p></div>')


@app.get("/attachment-log", response_class=HTMLResponse)
def attachment_log():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("""SELECT trip_code,original_filename,detected_type,detected_amount,detected_amount_eur,
            detected_currency,detected_date,detected_vendor,analysis_status,confidence,review_flag,storage_key
            FROM mail_attachments ORDER BY id DESC LIMIT 100""")
        rows = cur.fetchall(); cur.close(); conn.close()
        html = "".join(f"""<tr>
            <td class='code'>{r[0] or ''}</td><td>{r[1] or ''}</td><td>{r[2] or ''}</td>
            <td>{r[3] or ''}</td><td>{r[4] or ''}</td><td>{r[5] or ''}</td>
            <td>{r[6] or ''}</td><td>{r[7] or ''}</td><td>{r[8] or ''}</td>
            <td>{r[9] or ''}</td><td>{r[10] or ''}</td><td style="font-size:.78rem">{r[11] or ''}</td>
        </tr>""" for r in rows)
        return page_shell("Anhang Log", f"""
        <div class="card"><h2>Anhang Log v{APP_VERSION}</h2>
        <a class="btn-light" href="/">Zurück</a>
        <table style="margin-top:14px">
            <tr><th>Code</th><th>Datei</th><th>Typ</th><th>Betrag</th><th>EUR</th><th>Währung</th>
                <th>Datum</th><th>Anbieter</th><th>Status</th><th>Conf.</th><th>Review</th><th>Storage Key</th></tr>
            {html}
        </table></div>""")
    except Exception as e:
        return page_shell("Fehler", f'<div class="card"><h2 class="warn">Fehler</h2><p>{e}</p></div>')


@app.get("/reset-mail-log")
def reset_mail_log():
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("TRUNCATE TABLE mail_attachments RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE mail_messages RESTART IDENTITY")
        conn.commit(); cur.close(); conn.close()
        return {"status": "ok", "version": APP_VERSION}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}


@app.get("/set-hotel")
def set_hotel(code: str, mode: str):
    try:
        conn = get_conn(); cur = conn.cursor()
        cur.execute("INSERT INTO trip_meta (trip_code,hotel_mode) VALUES (%s,%s) ON CONFLICT (trip_code) DO UPDATE SET hotel_mode=%s", (code, mode, mode))
        conn.commit(); cur.close(); conn.close()
        return {"status": "ok", "code": code, "mode": mode, "version": APP_VERSION}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}
