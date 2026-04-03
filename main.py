"""
Herrhammer Reisekosten – Version 6.3
=====================================
Neu in 6.3:
  - ICS-Parsing: detected_date Spalte fix, ICS korrekt in analyse erkannt
  - Dashboard: Reisetitel + Kundenkürzel neben Reisecode
  - DB Bahn API: echte Verbindungsprüfung mit Client-ID/Secret
  - Analyse: Fehlerursachen klarer, alle Anhänge werden verarbeitet
  - /init: alle fehlenden Spalten zuverlässig nachgerüstet
  - trip_meta: neues Feld trip_title + customer_code
"""

from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
import os, re, base64, json, httpx, imaplib, email, hashlib, threading, time
from email.header import decode_header
from datetime import date, datetime, timedelta
from typing import Optional
import psycopg2
import boto3

APP_VERSION = "7.6"

app = FastAPI(title="Herrhammer Reisekosten", version=APP_VERSION)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── Umgebungsvariablen ─────────────────────────────────────────────────────────
DATABASE_URL          = os.getenv("DATABASE_URL")
IMAP_HOST             = os.getenv("IMAP_HOST")
IMAP_USER             = os.getenv("IMAP_USER")
IMAP_PASS             = os.getenv("IMAP_PASS")
S3_ENDPOINT           = os.getenv("S3_ENDPOINT")
S3_BUCKET             = os.getenv("S3_BUCKET")
S3_ACCESS_KEY         = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY         = os.getenv("S3_SECRET_KEY")
S3_REGION             = os.getenv("S3_REGION")
MISTRAL_API_KEY       = os.getenv("MISTRAL_API_KEY", "")
AMADEUS_CLIENT_ID     = os.getenv("AMADEUS_CLIENT_ID", "")
AMADEUS_CLIENT_SECRET = os.getenv("AMADEUS_CLIENT_SECRET", "")
AVIATIONSTACK_KEY     = os.getenv("AVIATIONSTACK_KEY", "")  # aviationstack.com Free: 100 req/Tag
DB_CLIENT_SECRET      = os.getenv("DB_CLIENT_SECRET", "") # DB Timetables API

MISTRAL_BASE          = "https://api.mistral.ai/v1"
MISTRAL_OCR_MODEL     = "mistral-ocr-2512"
MISTRAL_EXTRACT_MODEL = "mistral-small-latest"

# ── Feiertage Bayern (Herrhammer-Standort) ─────────────────────────────────────
def feiertage_bayern(year: int) -> set:
    """Gesetzliche Feiertage Bayern fuer Trennungspauschale."""
    # Ostern (Gauss)
    a = year % 19; b = year // 100; c = year % 100
    d = b // 4;    e = b % 4;       f = (b + 8) // 25
    g = (b - f + 1) // 3;           h = (19*a + b - d - g + 15) % 30
    i = c // 4;    k = c % 4;       l = (32 + 2*e + 2*i - h - k) % 7
    m = (a + 11*h + 22*l) // 451
    month = (h + l - 7*m + 114) // 31
    day   = ((h + l - 7*m + 114) % 31) + 1
    ostern = date(year, month, day)

    ft = {
        date(year,  1,  1),  # Neujahr
        date(year,  1,  6),  # Heilige Drei Koenige
        date(year,  5,  1),  # Tag der Arbeit
        date(year,  8, 15),  # Maria Himmelfahrt
        date(year, 10,  3),  # Tag der Deutschen Einheit
        date(year, 11,  1),  # Allerheiligen
        date(year, 12, 25),  # 1. Weihnachtstag
        date(year, 12, 26),  # 2. Weihnachtstag
        ostern - timedelta(days=2),   # Karfreitag
        ostern,                        # Ostersonntag
        ostern + timedelta(days=1),   # Ostermontag
        ostern + timedelta(days=39),  # Christi Himmelfahrt
        ostern + timedelta(days=49),  # Pfingstsonntag
        ostern + timedelta(days=50),  # Pfingstmontag
        ostern + timedelta(days=60),  # Fronleichnam
    }
    return ft

def ist_feiertag_oder_wochenende(d: date) -> bool:
    if d.weekday() >= 5: return True
    return d in feiertage_bayern(d.year)

# ── BMF VMA §9 EStG ───────────────────────────────────────────────────────────
VMA = {
    "DE":{"full":28.0,"partial":14.0}, "FR":{"full":40.0,"partial":20.0},
    "GB":{"full":54.0,"partial":27.0}, "US":{"full":56.0,"partial":28.0},
    "IN":{"full":32.0,"partial":16.0}, "AE":{"full":53.0,"partial":26.5},
    "AZ":{"full":37.0,"partial":18.5}, "CN":{"full":44.0,"partial":22.0},
    "JP":{"full":48.0,"partial":24.0}, "SG":{"full":45.0,"partial":22.5},
    "TR":{"full":35.0,"partial":17.5}, "CH":{"full":55.0,"partial":27.5},
    "AT":{"full":35.0,"partial":17.5}, "IT":{"full":37.0,"partial":18.5},
    "ES":{"full":35.0,"partial":17.5}, "NL":{"full":39.0,"partial":19.5},
    "PL":{"full":24.0,"partial":12.0},
}
MEAL_DED = {"breakfast":5.60,"lunch":11.20,"dinner":11.20}

def get_vma(cc, day_type, meals):
    r = VMA.get((cc or "DE").upper().strip(), {"full":28.0,"partial":14.0})
    base  = r["full"] if day_type == "full" else r["partial"]
    abzug = sum(MEAL_DED.get(m,0) for m in (meals or []))
    return max(0.0, round(base - abzug, 2))

def load_daily_meals(trip_code: str) -> dict:
    """Lädt tagesbasierte Mahlzeiten aus DB. Gibt {date: {'breakfast':bool,...}} zurück."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT meal_date,breakfast,lunch,dinner
                       FROM daily_meals WHERE trip_code=%s ORDER BY meal_date""",(trip_code,))
        rows=cur.fetchall();cur.close();conn.close()
        result={}
        for meal_date,b,l,d in rows:
            meals=[]
            if b: meals.append("breakfast")
            if l: meals.append("lunch")
            if d: meals.append("dinner")
            result[meal_date]=meals
        return result
    except Exception:
        return {}

def calc_vma_from_daily(dep_d, ret_d, daily_meals_dict: dict, vma_dest: dict, default_cc: str) -> tuple:
    """
    Berechnet VMA taggenau aus daily_meals Tabelle.
    Für jeden Tag wird geprüft welche Mahlzeiten erstattet wurden → Abzug.
    Gibt (total, rows) zurück.
    """
    if not dep_d or not ret_d:
        return 0.0, []
    days=(ret_d-dep_d).days+1
    total=0.0; rows=[]
    tag_list=(
        [("Anreisetag","partial")]+
        [("Reisetag","full")]*max(0,days-2)+
        [("Abreisetag","partial")]
    ) if days>1 else [("Eintägig","partial")]

    for i,(lbl,dtype) in enumerate(tag_list):
        current_day=dep_d+timedelta(days=i)
        cc=get_country_for_day(current_day,vma_dest,default_cc)
        # Mahlzeiten für diesen Tag – aus daily_meals wenn vorhanden
        ml=daily_meals_dict.get(current_day, [])
        v=get_vma(cc,dtype,ml)
        total+=v
        meal_icons=" ".join(filter(None,[
            "🍳" if "breakfast" in ml else "",
            "🥗" if "lunch" in ml else "",
            "🍽" if "dinner" in ml else "",
        ])) or "–"
        rows.append((str(current_day),lbl,cc,meal_icons,v))
    return total,rows

def parse_vma_destinations(vma_dest_str: str) -> dict:
    """
    Parst 'vma_destinations' Feld in ein Dict {date: country_code}.
    Format: '2025-03-10:IN,2025-03-14:AE,2025-03-17:DE'
    Datum ist der erste Tag IN diesem Land (bis zum nächsten Eintrag).
    """
    if not vma_dest_str or not vma_dest_str.strip():
        return {}
    result = {}
    for part in vma_dest_str.split(","):
        part = part.strip()
        if ":" in part:
            try:
                d_str, cc = part.split(":", 1)
                result[date.fromisoformat(d_str.strip())] = cc.strip().upper()
            except ValueError:
                pass
    return result

def get_country_for_day(day: date, vma_dest: dict, default_cc: str) -> str:
    """Gibt das Land für einen bestimmten Tag zurück (letzter Eintrag <= day)."""
    if not vma_dest:
        return default_cc or "DE"
    applicable = [d for d in vma_dest if d <= day]
    if not applicable:
        return default_cc or "DE"
    return vma_dest[max(applicable)]

def calc_vma_multi(dep_d, ret_d, meals: list, vma_dest: dict, default_cc: str) -> tuple:
    """
    Berechnet VMA für Multidestination-Reisen.
    Gibt (total, rows) zurück wobei rows Liste von (lbl, cc, meals_str, betrag) ist.
    """
    if not dep_d or not ret_d:
        return 0.0, []
    days = (ret_d - dep_d).days + 1
    total = 0.0
    rows = []
    tag_list = (
        [("Anreisetag","partial")] +
        [("Reisetag","full")] * max(0, days-2) +
        [("Abreisetag","partial")]
    ) if days > 1 else [("Eintägig","partial")]

    for i, (lbl, dtype) in enumerate(tag_list):
        current_day = dep_d + timedelta(days=i)
        cc = get_country_for_day(current_day, vma_dest, default_cc)
        ml_abz = meals if (dtype=="partial" and i==len(tag_list)-1) else []
        v = get_vma(cc, dtype, ml_abz)
        total += v
        rows.append((lbl, cc, ", ".join(ml_abz) or "–", v))
    return total, rows

def trennungspauschale(dep_date, ret_date, dep_time_str="", ret_time_str=""):
    """
    Trennungspauschale Herrhammer:
    - Ganzer Sa/So/Feiertag auf Dienstreise: 80 EUR
    - Abreise vor 12:00 oder Rueckkehr nach 12:00 an Sa/So/Feiertag: 40 EUR
    """
    if not dep_date or not ret_date: return 0.0, []
    if isinstance(dep_date, str):
        try: dep_date = date.fromisoformat(dep_date)
        except: return 0.0, []
    if isinstance(ret_date, str):
        try: ret_date = date.fromisoformat(ret_date)
        except: return 0.0, []

    try: dep_h = int((dep_time_str or "08:00").split(":")[0])
    except: dep_h = 8
    try: ret_h = int((ret_time_str or "18:00").split(":")[0])
    except: ret_h = 18

    total = 0.0; details = []
    current = dep_date
    while current <= ret_date:
        if ist_feiertag_oder_wochenende(current):
            if current == dep_date:
                # Abreisetag: vor 12 Uhr abgereist → halbe Pauschale
                if dep_h < 12:
                    total += 40.0
                    details.append((str(current), "Abreise vor 12:00", 40.0))
            elif current == ret_date:
                # Rueckkehrtag: nach 12 Uhr zurueck → halbe Pauschale
                if ret_h >= 12:
                    total += 40.0
                    details.append((str(current), "Rueckkehr nach 12:00", 40.0))
            else:
                # ganzer Tag → volle Pauschale
                total += 80.0
                wd = ["Mo","Di","Mi","Do","Fr","Sa","So"][current.weekday()]
                details.append((str(current), wd, 80.0))
        current += timedelta(days=1)
    return total, details

# ── DB / S3 ────────────────────────────────────────────────────────────────────
def get_conn():
    return psycopg2.connect(DATABASE_URL)

def get_s3():
    return boto3.client("s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        region_name=S3_REGION)

def compute_status(dep, ret):
    today = date.today()
    if not dep: return "planned"
    if isinstance(dep,str):
        try: dep=date.fromisoformat(dep)
        except: return "planned"
    if isinstance(ret,str):
        try: ret=date.fromisoformat(ret)
        except: ret=None
    if today < dep: return "planned"
    if ret and today > ret: return "done"
    return "active"

def next_trip_code(cur):
    yr = str(date.today().year)[-2:]
    cur.execute("SELECT trip_code FROM trip_meta WHERE trip_code LIKE %s ORDER BY trip_code DESC LIMIT 1",(f"{yr}-%",))
    row=cur.fetchone()
    num = int(row[0].split("-")[1])+1 if row else 1
    return f"{yr}-{str(num).zfill(3)}"

def file_hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()[:16]

# =========================================================
# WÄHRUNGSKURSE – ECB Tageskurs (kostenlos, kein Key)
# =========================================================

_ecb_rates_cache: dict = {}  # {"USD": 1.08, ...}
_ecb_rates_ts: float = 0.0

async def get_ecb_rates() -> dict:
    """Holt aktuelle EUR-Wechselkurse von der EZB (täglich aktualisiert)."""
    global _ecb_rates_cache, _ecb_rates_ts
    # Cache: 4 Stunden gültig
    if _ecb_rates_cache and (time.time() - _ecb_rates_ts) < 14400:
        return _ecb_rates_cache
    fallback = {"EUR":1.0,"USD":1.08,"GBP":0.86,"INR":89.5,"CHF":0.96,
                "JPY":161.0,"AED":3.97,"CNY":7.8,"SGD":1.46,"TRY":35.0,
                "AZN":1.84,"PLN":4.25,"SEK":11.3,"NOK":11.5,"DKK":7.46}
    try:
        async with httpx.AsyncClient(timeout=8.0) as cl:
            resp = await cl.get(
                "https://data-api.ecb.europa.eu/service/data/EXR/D..EUR.SP00.A"
                "?lastNObservations=1&format=csvdata",
                headers={"Accept": "text/csv"})
        if resp.status_code == 200:
            rates = {"EUR": 1.0}
            for line in resp.text.splitlines()[1:]:
                parts = line.split(",")
                if len(parts) >= 8:
                    try:
                        currency = parts[2].strip()   # CURRENCY column
                        rate_str = parts[7].strip()   # OBS_VALUE
                        if currency and rate_str:
                            rates[currency] = float(rate_str)
                    except (ValueError, IndexError):
                        pass
            if len(rates) > 5:
                _ecb_rates_cache = rates
                _ecb_rates_ts = time.time()
                return rates
    except Exception as e:
        print(f"[ECB] Fehler: {e}")
    return fallback

async def convert_to_eur(amount: float, currency: str) -> tuple[float, str]:
    """Gibt (eur_betrag, kurs_info) zurück."""
    currency = (currency or "EUR").upper().strip()
    if currency == "EUR":
        return round(amount, 2), ""
    rates = await get_ecb_rates()
    # ECB gibt Einheiten pro EUR an → umkehren für EUR-Betrag
    rate = rates.get(currency)
    if rate and rate > 0:
        eur = round(amount / rate, 2)
        return eur, f"1 EUR = {rate:.4f} {currency}"
    return round(amount, 2), f"Kurs {currency} unbekannt"


# =========================================================
# MISTRAL KI  (DSGVO-konform, EU)
# =========================================================

async def mistral_ocr(file_bytes: bytes, filename: str) -> str:
    if not MISTRAL_API_KEY: return "KEIN_MISTRAL_KEY"
    ext = filename.lower().split(".")[-1]
    try:
        async with httpx.AsyncClient(timeout=60.0) as cl:
            if ext == "pdf":
                up = await cl.post(f"{MISTRAL_BASE}/files",
                    headers={"Authorization":f"Bearer {MISTRAL_API_KEY}"},
                    files={"file":(filename,file_bytes,"application/pdf")},
                    data={"purpose":"ocr"})
                if up.status_code!=200: return f"OCR_UPLOAD_FEHLER:{up.status_code}"
                fid = up.json().get("id","")
                ur  = await cl.get(f"{MISTRAL_BASE}/files/{fid}/url?expiry=60",
                    headers={"Authorization":f"Bearer {MISTRAL_API_KEY}"})
                signed = ur.json().get("url","")
                resp = await cl.post(f"{MISTRAL_BASE}/ocr",
                    headers={"Authorization":f"Bearer {MISTRAL_API_KEY}","Content-Type":"application/json"},
                    json={"model":MISTRAL_OCR_MODEL,"document":{"type":"document_url","document_url":signed},"include_image_base64":False})
                await cl.delete(f"{MISTRAL_BASE}/files/{fid}",headers={"Authorization":f"Bearer {MISTRAL_API_KEY}"})
            elif ext in ("jpg","jpeg","png","webp"):
                b64  = base64.b64encode(file_bytes).decode()
                mime = "image/jpeg" if ext in ("jpg","jpeg") else f"image/{ext}"
                resp = await cl.post(f"{MISTRAL_BASE}/ocr",
                    headers={"Authorization":f"Bearer {MISTRAL_API_KEY}","Content-Type":"application/json"},
                    json={"model":MISTRAL_OCR_MODEL,"document":{"type":"image_url","image_url":f"data:{mime};base64,{b64}"},"include_image_base64":False})
            else:
                return "NICHT_ANALYSIERBAR"
        if resp.status_code!=200: return f"OCR_FEHLER:{resp.status_code}"
        pages = resp.json().get("pages",[])
        text  = "\n\n".join(p.get("markdown","") for p in pages).strip()
        return text[:20000] if text else "KEIN_TEXT_GEFUNDEN"
    except Exception as e:
        return f"ERROR:{e}"


async def mistral_extract(text: str, known_codes: list, source: str = "anhang") -> dict:
    """
    Extrahiert aus OCR-Text oder Mail-Body:
    Betrag, Waehrung, Datum, Anbieter, Typ, Reisecode, PNR/AMADEUS-Code,
    Flugnummern, Zugnummern, Confidence.
    """
    if not MISTRAL_API_KEY or not text or text.startswith(("KEIN","ERROR","OCR_","NICHT")):
        return {}
    codes_str = ", ".join(known_codes) if known_codes else "keine"
    system = f"""Du bist Spezialist fuer Reisekostenbelege und Reisebestatigungen in deutschen Unternehmen.
Analysiere den {'E-Mail-Text' if source=='mail' else 'OCR-Text eines Belegs'} und extrahiere Felder als JSON.
Antworte NUR mit einem gueltigen JSON-Objekt ohne Markdown-Backticks.

Felder:
- betrag: Dezimalzahl als String z.B. "142.50" (Punkt), oder ""
  FLUGBUCHUNG: Nimm den GESAMTBETRAG der Buchung (inkl. Steuern/Gebuehren).
  Suche nach: "Gesamtpreis", "Total", "Gesamtbetrag", "zu zahlen", "charged", "Ticketpreis gesamt".
  NICHT nehmen: Einzelpreise pro Segment, Steuern allein, oder "pro Person" wenn mehrere Personen.
  Bei mehreren Personen: Gesamtbetrag durch Personenanzahl teilen falls erkennbar.
- waehrung: ISO-Code. Standard: "EUR" wenn kein Fremdwaehrungs-Symbol explizit im Text
- datum: Buchungsdatum oder Abflugdatum "DD.MM.YYYY" oder ""
- anbieter: Airline oder Reiseanbieter z.B. "Lufthansa", "Booking.com", "Expedia", oder ""
- beleg_typ: eines von: Flug, Hotel, Taxi, Bahn, Mietwagen, Essen, Sonstiges
  Regeln: Uber/Bolt/FreeNow/Lyft → Taxi | Hertz/Sixt/Avis/Europcar → Mietwagen
  Lufthansa/Swiss/Ryanair/Emirates/Air France/KLM/Alitalia/ITA/Turkish/Boarding Pass/eTicket → Flug
  DB/ICE/IC/Eurostar/Thalys/Trenitalia → Bahn
  Marriott/Hilton/Accor/Novotel/Booking.com/Hotels.com/HRS → Hotel
  Restaurant/Café/Bewirtung/Dinner/Lunch → Essen
- reisecode: Format YY-NNN z.B. "26-001" falls im Text, sonst ""
- pnr_code: AMADEUS PNR/Buchungscode (6-stellig alphanumerisch) z.B. "XY3K7M", oder ""
- flight_numbers: alle Flugnummern kommagetrennt z.B. "LH1234, LH4321", auch Rückflüge
  Format: Airline-IATA-Code (2 Buchstaben) + Zahl, z.B. "LH", "AZ", "AF", "EK"
- train_numbers: kommagetrennte Zugnummern z.B. "ICE 1234, IC 578", oder ""
- nights: Anzahl Uebernachtungen als Zahl z.B. 3, oder 0
- traveler_name: Vollstaendiger Name des Reisenden z.B. "Max Mustermann", sonst ""
  Suche nach: "Passagier", "Passenger", "Reisender", "Gebucht fuer", "Name:"
- destination: Hauptreiseziel z.B. "Lyon", "Mumbai", "Frankfurt Messe", sonst ""
  Bei Flug: Zielort des Hinflugs nehmen.
- confidence: "hoch" wenn Betrag+Typ+Datum sicher, "mittel" wenn 2 von 3, sonst "niedrig"
- bemerkung: kurze Notiz auf Deutsch, z.B. "Hin+Rueckflug", "2 Personen geteilt", sonst ""

WICHTIG: INR/USD/GBP nur wenn explizites Symbol/Code im Text, sonst immer EUR."""

    user = f"Bekannte Reisecodes: {codes_str}\n\nText:\n---\n{text[:8000]}\n---\nJSON:"
    try:
        async with httpx.AsyncClient(timeout=30.0) as cl:
            resp = await cl.post(f"{MISTRAL_BASE}/chat/completions",
                headers={"Authorization":f"Bearer {MISTRAL_API_KEY}","Content-Type":"application/json"},
                json={"model":MISTRAL_EXTRACT_MODEL,
                      "messages":[{"role":"system","content":system},{"role":"user","content":user}],
                      "temperature":0.0,"max_tokens":500,
                      "response_format":{"type":"json_object"}})
        if resp.status_code!=200: return {}
        content = resp.json()["choices"][0]["message"]["content"]
        content = content.strip().strip("```json").strip("```").strip()
        return json.loads(content)
    except Exception as e:
        return {"fehler":str(e)}


async def analyse_ki(att_id, storage_key, filename, conn, known_codes):
    ext = (filename or "").lower().split(".")[-1]
    cur = conn.cursor()

    # ICS-Dateien direkt parsen (kein OCR nötig)
    if ext == "ics":
        try:
            s3  = get_s3()
            obj = s3.get_object(Bucket=S3_BUCKET, Key=storage_key)
            ics_bytes = obj["Body"].read()
            ics_text  = ics_bytes.decode(errors="ignore")
            # ICS Zeilenfortsetzungen auflösen
            ics_text  = re.sub(r"\r?\n[ \t]", "", ics_text)
            ics_summary = re.search(r"^SUMMARY[^:]*:(.*)", ics_text, re.MULTILINE)
            ics_dtstart = re.search(r"^DTSTART[^:]*:([\dTZ]+)", ics_text, re.MULTILINE)
            ics_loc     = re.search(r"^LOCATION[^:]*:(.*)", ics_text, re.MULTILINE)
            ics_desc    = re.search(r"^DESCRIPTION[^:]*:(.*)", ics_text, re.MULTILINE)
            ics_full = " ".join(filter(None,[
                ics_summary.group(1).strip() if ics_summary else "",
                ics_desc.group(1).strip() if ics_desc else "",
            ]))
            ics_flight_m = re.search(r"\b([A-Z]{2}\d{3,4})\b", ics_full)
            ics_flight_nr = ics_flight_m.group(1) if ics_flight_m else ""
            # Zugnummern: ICE 597, IC 1234, RE 42, RB 65 etc.
            ics_train_m = re.search(r"\b(ICE|IC|EC|RE|RB|S)\s*(\d{1,4})\b", ics_full, re.IGNORECASE)
            ics_train_nr = f"{ics_train_m.group(1).upper()} {ics_train_m.group(2)}" if ics_train_m else ""
            raw_date = ics_dtstart.group(1).strip()[:8] if ics_dtstart else ""
            ics_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}" if len(raw_date)==8 else ""
            ics_bemerkung = " | ".join(filter(None,[
                f"Termin: {ics_summary.group(1).strip()}" if ics_summary else "",
                f"Datum: {ics_date}" if ics_date else "",
                f"Ort: {ics_loc.group(1).strip()}" if ics_loc else "",
                f"Flug: {ics_flight_nr}" if ics_flight_nr else "",
                f"Zug: {ics_train_nr}" if ics_train_nr else "",
            ]))
            cur.execute("""UPDATE mail_attachments SET
                analysis_status='ok', confidence='hoch', review_flag='ok',
                detected_date=%s, detected_flight_numbers=%s,
                detected_train_numbers=%s,
                ki_bemerkung=%s, detected_type='Kalendereintrag'
                WHERE id=%s""",
                (ics_date or None, ics_flight_nr or None,
                 ics_train_nr or None, ics_bemerkung or None, att_id))
            # Flugnummer in trip_meta übernehmen
            if ics_flight_nr or ics_train_nr:
                cur.execute("SELECT trip_code FROM mail_attachments WHERE id=%s",(att_id,))
                row_tc = cur.fetchone()
                if row_tc and row_tc[0]:
                    tc = row_tc[0]
                    if ics_flight_nr:
                        cur.execute("SELECT flight_numbers FROM trip_meta WHERE trip_code=%s",(tc,))
                        row_fn = cur.fetchone()
                        if row_fn:
                            existing = (row_fn[0] or "")
                            if ics_flight_nr not in existing:
                                cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s",
                                    (f"{existing},{ics_flight_nr}".strip(","), tc))
                                print(f"[ICS] Flugnummer {ics_flight_nr} → trip {tc}")
                    if ics_train_nr:
                        cur.execute("SELECT train_numbers FROM trip_meta WHERE trip_code=%s",(tc,))
                        row_tn = cur.fetchone()
                        if row_tn:
                            existing = (row_tn[0] or "")
                            if ics_train_nr not in existing:
                                cur.execute("UPDATE trip_meta SET train_numbers=%s WHERE trip_code=%s",
                                    (f"{existing},{ics_train_nr}".strip(","), tc))
                                print(f"[ICS] Zugnummer {ics_train_nr} → trip {tc}")
            conn.commit(); cur.close(); return
        except Exception as e:
            cur.execute("UPDATE mail_attachments SET analysis_status=%s WHERE id=%s",
                        (f"ics-fehler:{str(e)[:80]}", att_id))
            conn.commit(); cur.close(); return

    if ext not in ("pdf","jpg","jpeg","png","webp"):
        cur.execute("UPDATE mail_attachments SET analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    ("nicht analysierbar","niedrig","pruefen",att_id))
        cur.close(); return
    try:
        s3  = get_s3()
        obj = s3.get_object(Bucket=S3_BUCKET,Key=storage_key)
        file_bytes = obj["Body"].read()
    except Exception as e:
        cur.execute("UPDATE mail_attachments SET analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    (f"s3-fehler:{str(e)[:80]}","niedrig","pruefen",att_id))
        conn.commit(); cur.close(); return

    ocr_text = await mistral_ocr(file_bytes, filename)
    if not ocr_text or ocr_text in ("KEIN_TEXT_GEFUNDEN","NICHT_ANALYSIERBAR","KEIN_MISTRAL_KEY"):
        cur.execute("UPDATE mail_attachments SET extracted_text=%s,analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    (ocr_text,(ocr_text or "kein text").lower(),"niedrig","pruefen",att_id))
        conn.commit(); cur.close(); return
    if ocr_text.startswith(("ERROR","OCR_")):
        cur.execute("UPDATE mail_attachments SET extracted_text=%s,analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    (ocr_text,"analysefehler","niedrig","pruefen",att_id))
        conn.commit(); cur.close(); return

    fields = await mistral_extract(ocr_text, known_codes, "anhang")
    await _apply_fields(cur, att_id, fields, ocr_text)
    conn.commit(); cur.close()


async def _apply_fields(cur, att_id, fields, ocr_text=""):
    betrag     = fields.get("betrag","") or ""
    waehrung   = fields.get("waehrung","EUR") or "EUR"
    datum      = fields.get("datum","") or ""
    anbieter   = fields.get("anbieter","") or ""
    beleg_typ  = fields.get("beleg_typ","Sonstiges") or "Sonstiges"
    reisecode  = fields.get("reisecode","") or ""
    pnr        = fields.get("pnr_code","") or ""
    fns        = fields.get("flight_numbers","") or ""
    trains     = fields.get("train_numbers","") or ""
    nights     = int(fields.get("nights",0) or 0)
    confidence = fields.get("confidence","niedrig") or "niedrig"
    bemerkung  = fields.get("bemerkung","") or ""

    # Custom Rules: wenn KI "Sonstiges" oder kein Typ → eigene Regeln prüfen
    if beleg_typ in ("Sonstiges","") and anbieter:
        custom = load_custom_rules()
        ruled  = detect_type_with_rules(anbieter, "", "", custom)
        if ruled not in ("Unbekannt",""):
            beleg_typ = ruled

    betrag_eur=""; kurs_info=""
    if betrag:
        try:
            val = float(betrag.replace(",","."))
            eur, kurs_info = await convert_to_eur(val, waehrung)
            betrag_eur = f"{eur:.2f}".replace(".",",")
        except: pass

    if reisecode:
        cur.execute("UPDATE mail_attachments SET trip_code=%s WHERE id=%s AND (trip_code IS NULL OR trip_code='')",
                    (reisecode,att_id))
    if pnr:
        cur.execute("UPDATE mail_attachments SET pnr_code=%s WHERE id=%s AND (pnr_code IS NULL OR pnr_code='')",
                    (pnr,att_id))

    review = "ok" if confidence=="hoch" else "pruefen"
    status = "ok" if fields and "fehler" not in fields else "analysefehler"
    # Kurs-Info in Bemerkung aufnehmen wenn Fremdwährung
    if kurs_info:
        bemerkung = f"{bemerkung} [{kurs_info}]".strip() if bemerkung else f"[{kurs_info}]"
    cur.execute("""UPDATE mail_attachments SET
        extracted_text=%s,detected_amount=%s,detected_amount_eur=%s,detected_currency=%s,
        detected_date=%s,detected_vendor=%s,detected_type=%s,
        pnr_code=%s,detected_flight_numbers=%s,detected_train_numbers=%s,detected_nights=%s,
        analysis_status=%s,confidence=%s,review_flag=%s,ki_bemerkung=%s
        WHERE id=%s""",
        (ocr_text[:10000] if ocr_text else None,
         betrag,betrag_eur,waehrung,datum,anbieter,beleg_typ,
         pnr,fns,trains,nights,status,confidence,review,bemerkung,att_id))


# =========================================================
# MAIL-HILFSFUNKTIONEN
# =========================================================

def extract_trip_code(text):
    m = re.search(r"\b\d{2}-\d{3}\b", text or "")
    return m.group(0) if m else None

def extract_pnr(text):
    """AMADEUS PNR: 6 Zeichen alphanumerisch, typisch in Reisebestaetigung."""
    m = re.search(r"\b([A-Z0-9]{6})\b", text or "")
    return m.group(1) if m else None

def decode_mime_header(value):
    if not value: return ""
    parts = decode_header(value)
    return "".join(
        p.decode(enc or "utf-8",errors="ignore") if isinstance(p,bytes) else p
        for p,enc in parts)

def detect_mail_type(text):
    t=(text or "").lower()
    if any(x in t for x in ["flug","flight","boarding","pnr","ticket","airline","itinerary","eticket"]): return "Flug"
    if any(x in t for x in ["hotel","booking.com","check-in","reservation","zimmer","accommodation"]): return "Hotel"
    if any(x in t for x in ["taxi","uber","cab","ride"]): return "Taxi"
    if any(x in t for x in ["bahn","zug","train","ice","db ","bahnticket"]): return "Bahn"
    if any(x in t for x in ["restaurant","verpflegung","essen","dinner","lunch","breakfast"]): return "Essen"
    if any(x in t for x in ["mietwagen","rental","hertz","sixt","avis"]): return "Mietwagen"
    return "Unbekannt"

def load_custom_rules() -> dict:
    """Lädt benutzerdefinierte Kategorie-Regeln aus der DB."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT keyword,category FROM category_rules ORDER BY id")
        rows=cur.fetchall();cur.close();conn.close()
        rules: dict = {}
        for kw,cat in rows:
            rules.setdefault(cat,[]).append(kw.lower().strip())
        return rules
    except Exception:
        return {}

def detect_type_with_rules(filename, subject, body, custom_rules: dict = None) -> str:
    """Wie detect_attachment_type aber mit zusätzlichen benutzerdefinierten Regeln."""
    text=f"{filename or ''} {subject or ''} {body or ''}".lower()
    if (filename or "").lower().endswith(".ics"): return "Kalendereintrag"
    if (filename or "").lower().endswith(".emz"): return "Inline-Grafik"
    # Benutzerdefinierte Regeln zuerst
    if custom_rules:
        for typ, keywords in custom_rules.items():
            if any(kw in text for kw in keywords if kw):
                return typ
    # Standard-Regeln
    return detect_attachment_type(filename, subject, body)


def detect_attachment_type(filename,subject,body):
    text=f"{filename or ''} {subject or ''} {body or ''}".lower()
    if (filename or "").lower().endswith(".ics"): return "Kalendereintrag"
    if (filename or "").lower().endswith(".emz"): return "Inline-Grafik"
    # Bekannte Anbieter → feste Kategorie (Regel vor allgemeinem Keyword-Check)
    VENDOR_RULES = {
        "Taxi":     ["uber","bolt","free now","freenow","mytaxi","cabify","lyft","gett","taxi"],
        "Bahn":     ["deutsche bahn","db bahn","eurostar","thalys","railjet","westbahn",
                     "oebb","sbb","trenitalia","renfe","ice ","" ],
        "Flug":     ["lufthansa","lh ","swiss","austrian","ryanair","easyjet","wizz",
                     "eurowings","condor","tuifly","air berlin","transavia","vueling",
                     "iberia","klm","air france","british airways","alitalia","ita airways",
                     "turkish","emirates","qatar","etihad","flydubai","oman air",
                     "air india","indigo","jet2","norwegian","wizzair","boarding pass",
                     "eticket","e-ticket","itinerary","booking reference"],
        "Hotel":    ["marriott","hilton","accor","novotel","ibis","mercure","sofitel",
                     "hyatt","sheraton","radisson","holiday inn","intercontinental",
                     "booking.com","hotels.com","expedia","hrs","hotel reservation",
                     "check-in","check in","zimmerrechnung"],
        "Mietwagen":["hertz","sixt","avis","europcar","enterprise","budget","national",
                     "alamo","buchbinder","rental car","mietwagen"],
        "Essen":    ["restaurant","bistro","café","cafe","mcdonalds","starbucks",
                     "subway","vapiano","nordsee","burgerking","burger king","dean & david",
                     "lieferando","delivery","foodora","lunch","dinner","breakfast",
                     "verpflegung","bewirtung"],
    }
    for typ, keywords in VENDOR_RULES.items():
        if any(kw in text for kw in keywords if kw):
            return typ
    # Allgemeine Fallbacks
    if any(x in text for x in ["boarding","eticket","flight","flug","pnr","itinerary"]): return "Flug"
    if any(x in text for x in ["bahn","zug","train"]): return "Bahn"
    if any(x in text for x in ["cab","ride","fahrkosten"]): return "Taxi"
    return "Unbekannt"

def sanitize_filename(name):
    name=(name or "").replace("\\","_").replace("/","_").strip()
    name=re.sub(r"[^A-Za-z0-9._ -]","_",name)
    return name[:180] if name else "attachment.bin"

# =========================================================
# AUTO-IMAP HINTERGRUND-THREAD (alle 5 Minuten)
# =========================================================

_imap_lock = threading.Lock()

def _auto_fetch():
    """Laeuft als Daemon-Thread, prueft alle 5 Minuten den Posteingang."""
    time.sleep(30)  # Startup-Delay
    while True:
        if IMAP_HOST and IMAP_USER and IMAP_PASS:
            with _imap_lock:
                try:
                    _fetch_mails_internal()
                except Exception as e:
                    print(f"[AutoIMAPFehler] {e}")
        time.sleep(300)  # 5 Minuten

def _fetch_mails_internal():
    """Kern-Logik Mail-Import mit robustem Duplikat-Check (UID + Message-ID) und Löschen nach Import."""
    s3   = get_s3()
    conn = get_conn()
    cur  = conn.cursor()

    mail = imaplib.IMAP4_SSL(IMAP_HOST)
    mail.login(IMAP_USER, IMAP_PASS)
    mail.select("INBOX")
    _, data = mail.search(None, "ALL")  # Alle Mails – Duplikate via Message-ID gefiltert
    ids = data[0].split()
    if not ids:
        cur.close(); conn.close(); mail.logout(); return

    imported = att_count = dupl = deleted = 0
    ids_to_delete = []  # UIDs erfolgreich importierter Mails → werden danach gelöscht

    for i in ids:
        uid = i.decode()

        # Duplikat-Check 1: IMAP-UID
        cur.execute("SELECT id FROM mail_messages WHERE mail_uid=%s",(uid,))
        if cur.fetchone():
            # Mail war schon importiert → löschen (falls noch im Postfach)
            ids_to_delete.append(i)
            dupl += 1
            continue

        _, msg_data = mail.fetch(i,"(RFC822)")
        msg      = email.message_from_bytes(msg_data[0][1])
        subject  = decode_mime_header(msg.get("Subject",""))
        sender   = decode_mime_header(msg.get("From",""))
        msg_id   = (msg.get("Message-ID","") or "").strip()

        # Duplikat-Check 2: Message-ID Header (robuster als UID)
        if msg_id:
            cur.execute("SELECT id FROM mail_messages WHERE message_id=%s",(msg_id,))
            if cur.fetchone():
                ids_to_delete.append(i)
                dupl += 1
                continue

        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type()=="text/plain" and "attachment" not in str(part.get("Content-Disposition") or "").lower():
                    pl=part.get_payload(decode=True)
                    if pl: body=pl.decode(errors="ignore"); break
        else:
            pl=msg.get_payload(decode=True)
            if pl: body=pl.decode(errors="ignore")

        full  = f"{subject}\n{body}"
        code  = extract_trip_code(full)
        pnr   = extract_pnr(full)

        cur.execute("""INSERT INTO mail_messages
            (mail_uid,message_id,sender,subject,body,trip_code,detected_type,pnr_code)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (uid,msg_id,sender,subject,body,code,detect_mail_type(full),pnr))

        if code:
            cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(code,))
            # PNR in trip_meta speichern wenn gefunden
            if pnr:
                cur.execute("UPDATE trip_meta SET pnr_code=%s WHERE trip_code=%s AND (pnr_code IS NULL OR pnr_code='')",(pnr,code))

        # Anhaenge verarbeiten
        if msg.is_multipart():
            for part in msg.walk():
                fn = part.get_filename()
                cd = str(part.get("Content-Disposition") or "")
                if not fn and "attachment" not in cd.lower(): continue
                decoded_fn = decode_mime_header(fn) if fn else (
                    "attachment" + {"application/pdf":".pdf","image/jpeg":".jpg","image/png":".png","text/calendar":".ics"}.get(part.get_content_type(),".bin"))
                pl = part.get_payload(decode=True)
                if not pl: continue

                # Duplikat-Check per Hash
                h = file_hash(pl)
                cur.execute("SELECT id,trip_code FROM mail_attachments WHERE file_hash=%s",(h,))
                existing = cur.fetchone()
                if existing:
                    dupl += 1
                    print(f"[Duplikat] {decoded_fn} Hash:{h} – bereits als ID {existing[0]} vorhanden")
                    # Mail als gelesen markieren trotzdem
                    mail.store(i,"+FLAGS","\\Seen")
                    continue

                # ICS-Datei direkt parsen (Flugdaten ohne KI)
                ics_bemerkung = ""
                ics_detected_date = ""
                ics_flight_nr = ""
                if safe_fn.lower().endswith(".ics") and pl:
                    try:
                        ics_text = pl.decode(errors="ignore")
                        # ICS kann Zeilenfortsetzungen haben (Leerzeichen/Tab am Zeilenanfang)
                        ics_text = re.sub(r"\r?\n[ \t]", "", ics_text)
                        ics_summary = re.search(r"^SUMMARY[^:]*:(.*)", ics_text, re.MULTILINE)
                        ics_dtstart = re.search(r"^DTSTART[^:]*:([\dTZ]+)", ics_text, re.MULTILINE)
                        ics_loc     = re.search(r"^LOCATION[^:]*:(.*)", ics_text, re.MULTILINE)
                        ics_desc    = re.search(r"^DESCRIPTION[^:]*:(.*)", ics_text, re.MULTILINE)
                        ics_full = " ".join(filter(None,[
                            ics_summary.group(1).strip() if ics_summary else "",
                            ics_desc.group(1).strip() if ics_desc else "",
                        ]))
                        ics_flight_m = re.search(r"\b([A-Z]{2}\d{3,4})\b", ics_full)
                        ics_flight_nr = ics_flight_m.group(1) if ics_flight_m else ""
                        raw_date = ics_dtstart.group(1).strip()[:8] if ics_dtstart else ""
                        if raw_date and len(raw_date) == 8:
                            ics_detected_date = f"{raw_date[:4]}-{raw_date[4:6]}-{raw_date[6:8]}"
                        ics_bemerkung = " | ".join(filter(None,[
                            f"Termin: {ics_summary.group(1).strip()}" if ics_summary else "",
                            f"Datum: {ics_detected_date}" if ics_detected_date else "",
                            f"Ort: {ics_loc.group(1).strip()}" if ics_loc else "",
                            f"Flug: {ics_flight_nr}" if ics_flight_nr else "",
                        ]))
                        # Flugnummer in trip_meta übernehmen
                        if code and ics_flight_nr:
                            cur.execute("SELECT flight_numbers FROM trip_meta WHERE trip_code=%s",(code,))
                            row_fn = cur.fetchone()
                            if row_fn:
                                existing = (row_fn[0] or "")
                                if ics_flight_nr not in existing:
                                    cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s",
                                        (f"{existing},{ics_flight_nr}".strip(","), code))
                    except Exception as ics_err:
                        print(f"[ICS] Fehler: {ics_err}")

                cur.execute("""INSERT INTO mail_attachments
                    (mail_uid,trip_code,original_filename,saved_filename,content_type,
                     storage_key,detected_type,analysis_status,confidence,review_flag,
                     file_hash,ki_bemerkung,detected_date,detected_flight_numbers)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (uid,code,safe_fn,f"{uid}_{safe_fn}",part.get_content_type(),
                     storage_key,detect_type_with_rules(safe_fn,subject,body,load_custom_rules()),
                     "ok" if ics_bemerkung else "ausstehend",
                     "hoch" if ics_bemerkung else "niedrig",
                     "ok" if ics_bemerkung else "pruefen",
                     h, ics_bemerkung or None,
                     ics_detected_date or None,
                     ics_flight_nr or None))

        # Mail erfolgreich importiert → für Löschung vormerken
        ids_to_delete.append(i)
        imported += 1

    conn.commit(); cur.close(); conn.close()

    # Mails löschen (nach commit, damit Daten sicher in DB)
    for i in ids_to_delete:
        try:
            mail.store(i, "+FLAGS", "\\Deleted")
            deleted += 1
        except Exception:
            pass
    if ids_to_delete:
        mail.expunge()

    mail.logout()
    if imported or dupl:
        print(f"[AutoIMAP] {imported} neu, {att_count} Anhänge, {dupl} Duplikate, {deleted} gelöscht")

# Thread starten
_t = threading.Thread(target=_auto_fetch, daemon=True)
_t.start()


# =========================================================
# AVIATIONSTACK – Buchungsänderungs-Check (100 req/Tag Free)
# =========================================================

# Letzter Check-Zeitpunkt pro Flugnummer – verhindert zu viele API-Calls
_flight_check_cache: dict = {}  # key: "FN_DATUM" → last_checked timestamp

async def check_aviationstack(fn: str, dep_date: str) -> dict:
    """
    Prüft Flugstatus via AviationStack API.
    Erkennt: Routenänderung, Cancellation, Gate-Änderung, Verspätung.
    Benötigt AVIATIONSTACK_KEY (kostenlos bis 100 req/Tag).
    """
    if not AVIATIONSTACK_KEY:
        return {"status": "kein AVIATIONSTACK_KEY", "source": "AviationStack"}

    carrier = fn[:2].upper()
    num     = re.sub(r"[^0-9]", "", fn)
    try:
        async with httpx.AsyncClient(timeout=10.0) as cl:
            resp = await cl.get(
                "http://api.aviationstack.com/v1/flights",
                params={
                    "access_key":   AVIATIONSTACK_KEY,
                    "airline_iata": carrier,
                    "flight_number": num,
                    "flight_date":  dep_date,
                    "limit": 1,
                }
            )
        if resp.status_code != 200:
            return {"status": f"HTTP {resp.status_code}", "source": "AviationStack"}

        data = resp.json().get("data", [])
        if not data:
            return {"status": "nicht gefunden", "source": "AviationStack"}

        f = data[0]
        flight_status  = f.get("flight_status", "")   # scheduled/active/landed/cancelled
        dep_iata       = f.get("departure", {}).get("iata", "")
        arr_iata       = f.get("arrival", {}).get("iata", "")
        dep_delay      = f.get("departure", {}).get("delay")   # Minuten
        arr_delay      = f.get("arrival", {}).get("delay")
        dep_gate       = f.get("departure", {}).get("gate", "")
        dep_terminal   = f.get("departure", {}).get("terminal", "")
        cancelled      = (flight_status == "cancelled")

        route = f"{dep_iata}→{arr_iata}" if dep_iata and arr_iata else ""

        return {
            "status":        flight_status,
            "source":        "AviationStack",
            "route":         route,
            "dep_delay":     dep_delay,
            "arr_delay":     arr_delay,
            "gate":          dep_gate,
            "terminal":      dep_terminal,
            "cancelled":     cancelled,
            "delay_min":     dep_delay,
        }
    except Exception as e:
        return {"status": f"Fehler: {str(e)[:80]}", "source": "AviationStack"}


async def auto_check_active_flights():
    """
    Prüft alle Flugnummern aktiver Reisen via AviationStack.
    Nur Flüge heute/morgen, max. 1x pro 3h pro Flugnummer → bleibt unter 100 req/Tag.
    Schreibt Alerts in flight_alerts wenn Änderungen erkannt.
    """
    if not AVIATIONSTACK_KEY:
        return

    try:
        conn = get_conn(); cur = conn.cursor()
        today    = date.today()
        tomorrow = today + timedelta(days=1)

        # Alle aktiven Reisen mit Flugnummern
        cur.execute("""SELECT trip_code, flight_numbers, departure_date, return_date
                       FROM trip_meta
                       WHERE flight_numbers IS NOT NULL AND flight_numbers != ''
                       AND departure_date IS NOT NULL""")
        rows = cur.fetchall()

        checked = 0
        for tc, fns_raw, dep_d, ret_d in rows:
            # Nur aktive Reisen
            status = compute_status(dep_d, ret_d)
            if status != "active":
                continue

            fns = [f.strip() for f in (fns_raw or "").split(",") if f.strip()]
            for fn in fns:
                # Nur Flüge heute oder morgen
                flight_date = None
                if dep_d == today or dep_d == tomorrow:
                    flight_date = str(dep_d)
                elif ret_d and (ret_d == today or ret_d == tomorrow):
                    flight_date = str(ret_d)
                else:
                    continue

                # Rate-Limit: max 1x alle 3h pro Flugnummer
                cache_key = f"{fn}_{flight_date}"
                last = _flight_check_cache.get(cache_key, 0)
                if time.time() - last < 10800:  # 3 Stunden
                    continue

                if checked >= 90:  # Sicherheitspuffer vor 100
                    break

                result = await check_aviationstack(fn, flight_date)
                _flight_check_cache[cache_key] = time.time()
                checked += 1

                flight_status = result.get("status", "")
                cancelled     = result.get("cancelled", False)
                dep_delay     = result.get("dep_delay") or 0
                route         = result.get("route", "")
                gate          = result.get("gate", "")

                # Alert-Typen bestimmen
                alert_type = "ok"
                msg_parts  = []

                if cancelled:
                    alert_type = "cancelled"
                    msg_parts.append("⚠ FLUG STORNIERT")
                elif dep_delay and dep_delay > 30:
                    alert_type = "delay"
                    msg_parts.append(f"Verspätung +{dep_delay} Min.")
                elif flight_status in ("active", "landed"):
                    msg_parts.append(f"Status: {flight_status}")

                if route:
                    msg_parts.append(f"Route: {route}")
                if gate:
                    msg_parts.append(f"Gate: {gate}")

                message = " · ".join(msg_parts) if msg_parts else flight_status

                # Nur speichern wenn neu / geändert
                cur.execute("""SELECT message FROM flight_alerts
                               WHERE trip_code=%s AND flight_number=%s AND flight_date=%s
                               ORDER BY checked_at DESC LIMIT 1""",
                            (tc, fn, flight_date))
                last_alert = cur.fetchone()
                if not last_alert or last_alert[0] != message:
                    cur.execute("""INSERT INTO flight_alerts
                        (trip_code,flight_number,flight_date,alert_type,message,source,delay_min)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                        (tc, fn, flight_date, alert_type, message,
                         result.get("source","AviationStack"), dep_delay or None))
                    print(f"[FlightCheck] {fn} {flight_date}: {message}")

        conn.commit(); cur.close(); conn.close()
        if checked:
            print(f"[FlightCheck] {checked} Flüge geprüft")
    except Exception as e:
        print(f"[FlightCheckFehler] {e}")


def _auto_flight_check():
    """Daemon-Thread: prüft aktive Flüge alle 3 Stunden."""
    time.sleep(60)  # Startup-Delay
    while True:
        try:
            import asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(auto_check_active_flights())
            loop.close()
        except Exception as e:
            print(f"[FlightThreadFehler] {e}")
        time.sleep(10800)  # 3 Stunden


# Flight-Check Thread starten
_ft = threading.Thread(target=_auto_flight_check, daemon=True)
_ft.start()

# =========================================================

async def check_opensky(callsign: str) -> dict:
    """
    Prüft ob ein Flug gerade aktiv ist via OpenSky Network REST API.
    callsign: ICAO-Callsign z.B. 'DLH123' (Lufthansa LH123)
    Liefert: on_ground, altitude, velocity, last_seen
    """
    # IATA → ICAO Carrier Mapping (häufigste Airlines)
    iata_to_icao = {
        "LH":"DLH","AZ":"AZA","LX":"SWR","OS":"AUA","BA":"BAW",
        "AF":"AFR","KL":"KLM","IB":"IBE","EK":"UAE","EW":"EWG",
        "FR":"RYR","U2":"EZY","W6":"WZZ","TK":"THY","SK":"SAS",
        "AY":"FIN","SN":"BEL","VY":"VLG","VU":"VJT","QR":"QTR",
        "ET":"ETH","MS":"MSR","SV":"SVA","AI":"AIC","9W":"JAI",
        "6E":"IGO","SG":"SEJ","MH":"MAS","SQ":"SIA","CX":"CPA",
        "NH":"ANA","JL":"JAL","OZ":"AAR","KE":"KAL","CA":"CCA",
        "MU":"CES","CZ":"CSN","HU":"CHH","9C":"CQH",
    }
    try:
        # Callsign aus IATA-Flugnummer bauen: LH123 → DLH123
        m = re.match(r"^([A-Z]{2})(\d{1,4})$", callsign.upper().replace(" ",""))
        if not m:
            return {"status":"ungültige Flugnummer","source":"OpenSky","on_ground":None}
        iata_carrier = m.group(1)
        flight_num   = m.group(2)
        icao_carrier = iata_to_icao.get(iata_carrier, iata_carrier + "X")  # Fallback
        icao_callsign = f"{icao_carrier}{flight_num}"

        async with httpx.AsyncClient(timeout=10.0) as cl:
            resp = await cl.get(
                "https://opensky-network.org/api/states/all",
                params={"callsign": icao_callsign.ljust(8)},  # OpenSky padded auf 8 Zeichen
            )

        if resp.status_code == 429:
            return {"status":"Rate Limit (OpenSky)","source":"OpenSky","on_ground":None}
        if resp.status_code != 200:
            return {"status":f"HTTP {resp.status_code}","source":"OpenSky","on_ground":None}

        data = resp.json()
        states = data.get("states") or []

        # Suche nach Callsign (kann padded sein)
        match = None
        for s in states:
            cs = (s[1] or "").strip()
            if cs.upper() == icao_callsign.upper():
                match = s; break

        if not match:
            # Flug nicht live – entweder gelandet oder noch nicht gestartet
            return {
                "status": "nicht aktiv (gelandet/nicht gestartet)",
                "source": "OpenSky",
                "on_ground": None,
                "callsign": icao_callsign,
            }

        on_ground  = match[8]        # bool
        altitude   = match[7]        # Barometric altitude in m
        velocity   = match[9]        # m/s
        lat        = match[6]
        lon        = match[5]
        last_seen  = match[4]        # Unix timestamp

        if on_ground:
            status = "am Boden"
        else:
            alt_ft = int((altitude or 0) * 3.281)
            spd_kmh = int((velocity or 0) * 3.6)
            status = f"in der Luft · {alt_ft:,} ft · {spd_kmh} km/h"

        return {
            "status": status,
            "source": "OpenSky",
            "on_ground": on_ground,
            "altitude_m": altitude,
            "velocity_ms": velocity,
            "lat": lat,
            "lon": lon,
            "callsign": icao_callsign,
            "delay_min": None,  # OpenSky liefert keine Verspätungsminuten
        }

    except Exception as e:
        return {"status":f"Fehler: {str(e)[:80]}","source":"OpenSky","on_ground":None}


async def check_flight_status(fn: str, dep_date: str) -> dict:
    """
    Kombinierter Flugstatus:
    1. OpenSky: Live-Position (kostenlos, kein Key)
    2. Amadeus: Existenzcheck / Fahrplan (falls Key vorhanden)
    """
    # OpenSky immer zuerst
    result = await check_opensky(fn)

    # Amadeus als Ergänzung: Fahrplan-Existenzcheck
    if AMADEUS_CLIENT_ID and result.get("on_ground") is None:
        try:
            async with httpx.AsyncClient(timeout=8) as cl:
                tr = await cl.post(
                    "https://test.api.amadeus.com/v1/security/oauth2/token",
                    data={"grant_type":"client_credentials",
                          "client_id":AMADEUS_CLIENT_ID,
                          "client_secret":AMADEUS_CLIENT_SECRET})
                token = tr.json().get("access_token","")
                if token:
                    carrier = fn[:2].upper()
                    num     = re.sub(r"[^0-9]","",fn)
                    fr = await cl.get(
                        "https://test.api.amadeus.com/v2/schedule/flights",
                        headers={"Authorization":f"Bearer {token}"},
                        params={"carrierCode":carrier,"flightNumber":num,
                                "scheduledDepartureDate":dep_date})
                    if fr.status_code == 200 and fr.json().get("data"):
                        result["amadeus"] = "im Fahrplan ✓"
                    else:
                        result["amadeus"] = "nicht im Fahrplan"
        except Exception:
            pass

    return result


# =========================================================
# BAHN PUENKTLICHKEIT (DB API)
# =========================================================

async def check_bahn_puenktlichkeit(zug_nr: str, datum: str, eva_nr: str = "8000105") -> dict:
    """
    Prueft Zugverspaetung via DB Timetables API v1.
    Benoetigt DB_CLIENT_ID + DB_CLIENT_SECRET (developers.deutschebahn.com).
    eva_nr: EVA-Bahnhofsnummer, Standard Frankfurt Hbf = 8000105
    Nur Fernverkehr (ICE/IC/EC).
    """
    if not DB_CLIENT_ID or not DB_CLIENT_SECRET:
        return {"status": "kein DB_CLIENT_ID/SECRET", "delay_min": None, "source": "DB Timetables"}
    # Zugnummer normieren: "ICE 597" → "597", "ICE597" → "597"
    zug_clean = re.sub(r"[^0-9]", "", zug_nr)
    if not zug_clean:
        return {"status": "ungültige Zugnummer", "delay_min": None, "source": "DB Timetables"}
    try:
        # Datum für API: YYMMDD
        if datum and len(datum) == 10:  # YYYY-MM-DD
            api_date = datum[2:4] + datum[5:7] + datum[8:10]
        else:
            api_date = datetime.now().strftime("%y%m%d")
        # Stunde für fchg (changes): aktuelle Stunde
        api_hour = datetime.now().strftime("%H")

        headers = {
            "DB-Client-Id": DB_CLIENT_ID,
            "DB-Api-Key": DB_CLIENT_SECRET,
            "accept": "application/xml"
        }
        async with httpx.AsyncClient(timeout=10.0) as cl:
            # 1. Plan abrufen (planmäßige Abfahrten)
            plan_url = f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/plan/{eva_nr}/{api_date}/{api_hour}"
            resp_plan = await cl.get(plan_url, headers=headers)

            # 2. Änderungen abrufen (aktuelle Verspätungen)
            fchg_url = f"https://apis.deutschebahn.com/db-api-marketplace/apis/timetables/v1/fchg/{eva_nr}"
            resp_fchg = await cl.get(fchg_url, headers=headers)

        if resp_plan.status_code != 200:
            return {"status": f"Plan HTTP {resp_plan.status_code}", "delay_min": None, "source": "DB Timetables", "raw": resp_plan.text[:200]}

        plan_text = resp_plan.text
        fchg_text = resp_fchg.text if resp_fchg.status_code == 200 else ""

        # Zugnummer im Plan suchen
        if zug_clean not in plan_text:
            return {"status": "nicht im Fahrplan", "delay_min": None, "source": "DB Timetables"}

        # Verspätung aus fchg parsen: <dp ct="HHMM"> neben trip-id mit Zugnummer
        delay_min = None
        if fchg_text and zug_clean in fchg_text:
            # Trip-ID Zeilen mit unserer Zugnummer finden
            # Format: <s id="..."><tl c="ICE" n="597"...><dp ct="1423" ...>
            # Suche nach n="597" in der Nähe von ct=
            pattern = rf'n="{zug_clean}"[^>]*>.*?ct="(\d{{4}})"'
            m = re.search(pattern, fchg_text, re.DOTALL)
            if m:
                ct_time = m.group(1)  # aktuell geplante Abfahrt z.B. "1437"
                # Plan-Abfahrt finden
                pt_pattern = rf'n="{zug_clean}"[^>]*>.*?pt="(\d{{4}})"'
                pm = re.search(pt_pattern, plan_text, re.DOTALL)
                if pm:
                    pt_time = pm.group(1)
                    try:
                        ct_h, ct_m = int(ct_time[:2]), int(ct_time[2:])
                        pt_h, pt_m = int(pt_time[:2]), int(pt_time[2:])
                        delay_min = (ct_h * 60 + ct_m) - (pt_h * 60 + pt_m)
                        if delay_min < 0: delay_min += 1440  # Mitternacht überschritten
                    except Exception:
                        delay_min = None

        if delay_min is not None and delay_min > 15:
            return {"status": "verspätet", "delay_min": delay_min, "source": "DB Timetables"}
        elif delay_min is not None:
            return {"status": "pünktlich", "delay_min": delay_min, "source": "DB Timetables"}
        else:
            return {"status": "im Fahrplan", "delay_min": 0, "source": "DB Timetables"}

    except Exception as e:
        return {"status": f"Fehler: {str(e)[:80]}", "delay_min": None, "source": "DB Timetables"}


# =========================================================
# CSS + JS (identisch mit 6.1, Logo als Link)
# =========================================================

CSS = """
:root{
  --page:#f0f4f9;--white:#fff;
  --b900:#0e2650;--b700:#1a3d96;--b600:#2152c4;--b500:#2e63e8;
  --b400:#4d7ef5;--b300:#7aa3fa;--b100:#dde9ff;--b50:#eef4ff;
  --t900:#0d1b33;--t700:#2c3e5e;--t500:#5a6e8a;--t300:#9bafc8;
  --bd:#dde4ef;--bds:#eaeef5;
  --gr6:#0f9e6e;--gr1:#d4f5eb;
  --am6:#c97c0a;--am1:#fef3d6;
  --re6:#dc2626;--re1:#fee2e2;
  --sh-sm:0 1px 4px rgba(14,38,80,.07),0 1px 2px rgba(14,38,80,.04);
  --sh:0 4px 20px rgba(14,38,80,.10),0 2px 6px rgba(14,38,80,.05);
  --sh-lg:0 16px 48px rgba(14,38,80,.16),0 4px 16px rgba(14,38,80,.08);
  --r:12px;--rs:8px;
}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Inter',sans-serif;background:var(--page);color:var(--t900);min-height:100vh;font-size:13.5px;line-height:1.6;-webkit-font-smoothing:antialiased}
/* ── Topbar ── */
.topbar{position:sticky;top:0;z-index:100;background:rgba(255,255,255,.92);backdrop-filter:blur(12px);border-bottom:1px solid var(--bd);box-shadow:var(--sh-sm);height:60px;display:flex;align-items:center;padding:0 28px;gap:0}
.logo-wrap{display:flex;align-items:center;margin-right:32px;text-decoration:none;cursor:pointer}
.logo-wrap img{height:38px;width:auto;display:block;transition:opacity .15s}
.logo-wrap:hover img{opacity:.8}
.nav-tabs{display:flex;align-items:center;gap:3px;flex:1}
.nav-tab{padding:6px 16px;border-radius:var(--rs);font-size:13px;font-weight:400;color:var(--t500);cursor:pointer;transition:all .15s;text-decoration:none;border:none;background:none;white-space:nowrap;letter-spacing:.01em}
.nav-tab:hover{color:var(--t900);background:var(--b50)}
.nav-tab.active{color:var(--b600);background:var(--b50);font-weight:600;box-shadow:inset 0 -2px 0 var(--b500)}
.topbar-right{display:flex;align-items:center;gap:10px;margin-left:auto}
.ki-pill{font-size:11px;padding:3px 10px;border-radius:20px;border:1px solid;font-weight:500}
.ver-pill{font-family:'DM Mono',monospace;font-size:10px;color:var(--t300);background:var(--page);border:1px solid var(--bd);border-radius:4px;padding:2px 8px}
/* ── Dropdown ── */
.dd-wrap{position:relative}
.add-btn{display:flex;align-items:center;gap:6px;background:linear-gradient(135deg,var(--b600),var(--b500));color:white;border:none;border-radius:var(--rs);padding:8px 16px;font-family:'Inter',sans-serif;font-size:13px;font-weight:500;cursor:pointer;box-shadow:0 2px 8px rgba(33,82,196,.35);transition:all .15s}
.add-btn:hover{transform:translateY(-1px);box-shadow:0 4px 12px rgba(33,82,196,.4)}
.add-btn:active{transform:translateY(0)}
.dd-menu{position:absolute;top:calc(100% + 10px);right:0;background:var(--white);border:1px solid var(--bd);border-radius:var(--r);box-shadow:var(--sh-lg);min-width:240px;overflow:hidden;opacity:0;pointer-events:none;transform:translateY(-8px) scale(.97);transition:opacity .16s,transform .16s;z-index:200}
.dd-menu.open{opacity:1;pointer-events:all;transform:translateY(0) scale(1)}
.dd-item{display:flex;align-items:center;gap:12px;padding:11px 16px;cursor:pointer;transition:background .1s;border:none;background:none;width:100%;text-align:left;color:var(--t900);font-family:'Inter',sans-serif;font-size:13px;text-decoration:none}
.dd-item:hover{background:var(--b50)}
.dd-icon{width:32px;height:32px;border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:15px;flex-shrink:0}
.di-b{background:var(--b50)} .di-g{background:var(--gr1)} .di-a{background:var(--am1)}
.dd-sub{font-size:11.5px;color:var(--t300);margin-top:1px}
.dd-div{height:1px;background:var(--bds);margin:4px 0}
/* ── Layout ── */
.wrap{max-width:1400px;margin:0 auto;padding:28px 28px 60px;display:flex;flex-direction:column;gap:32px}
/* ── Summary Bar ── */
.sum-bar{display:flex;gap:12px;flex-wrap:wrap}
.sum-item{background:var(--white);border:1px solid var(--bd);border-radius:var(--r);padding:16px 22px;box-shadow:var(--sh-sm);min-width:130px;transition:all .15s}
.sum-item:hover{box-shadow:var(--sh);transform:translateY(-1px)}
.sum-val{font-family:'DM Mono',monospace;font-size:24px;font-weight:600;color:var(--t900);letter-spacing:-.5px}
.sum-val.blue{color:var(--b600)} .sum-val.green{color:var(--gr6)} .sum-val.red{color:var(--re6)}
.sum-lbl{font-size:11px;color:var(--t300);margin-top:4px;font-weight:500;text-transform:uppercase;letter-spacing:.04em}
/* ── Section headers ── */
.sec-hdr{display:flex;align-items:center;gap:10px;margin-bottom:14px}
.sec-dot{width:8px;height:8px;border-radius:50%;flex-shrink:0}
.sec-dot.active{background:var(--re6);box-shadow:0 0 0 3px rgba(220,38,38,.15)}
.sec-dot.planned{background:var(--b500);box-shadow:0 0 0 3px rgba(46,99,232,.15)}
.sec-dot.done{background:var(--gr6);box-shadow:0 0 0 3px rgba(15,158,110,.15)}
.sec-title{font-size:11px;font-weight:700;color:var(--t500);letter-spacing:.08em;text-transform:uppercase}
.sec-cnt{font-size:11px;font-family:'DM Mono',monospace;color:var(--t300);background:var(--white);border:1px solid var(--bd);border-radius:20px;padding:2px 10px;margin-left:auto}
/* ── Cards ── */
.cards{display:grid;grid-template-columns:repeat(auto-fill,minmax(380px,1fr));gap:14px}
.card{background:var(--white);border:1px solid var(--bd);border-radius:var(--r);box-shadow:var(--sh-sm);overflow:hidden;transition:box-shadow .18s,transform .18s,border-color .18s;cursor:pointer;position:relative}
.card:hover{box-shadow:var(--sh);transform:translateY(-3px);border-color:var(--b300)}
.card.alert{border-color:rgba(220,38,38,.4)}
.card.alert::before{content:'';position:absolute;top:0;left:0;right:0;height:3px;background:linear-gradient(90deg,var(--re6),#f87171)}
.c-top{padding:16px 16px 12px;display:flex;align-items:flex-start;gap:12px}
.c-code{font-family:'DM Mono',monospace;font-size:11.5px;font-weight:600;color:var(--b700);background:var(--b50);border:1px solid var(--b100);border-radius:6px;padding:4px 10px;white-space:nowrap;flex-shrink:0;letter-spacing:.02em}
.c-info{flex:1;min-width:0}
.c-traveler{font-size:14px;font-weight:600;color:var(--t900);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;line-height:1.4}
.c-dest{font-size:12px;color:var(--t500);margin-top:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.sbadge{font-size:11px;font-weight:600;padding:3px 10px;border-radius:20px;white-space:nowrap;flex-shrink:0;letter-spacing:.02em}
.sb-active{background:var(--re1);color:var(--re6);border:1px solid rgba(220,38,38,.2)}
.sb-planned{background:var(--b50);color:var(--b600);border:1px solid var(--b100)}
.sb-done{background:var(--gr1);color:var(--gr6);border:1px solid rgba(15,158,110,.2)}
.adot{display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--re6);margin-right:5px;animation:pr 1.5s ease-in-out infinite}
@keyframes pr{0%,100%{opacity:1;box-shadow:0 0 0 0 rgba(220,38,38,.4)}50%{box-shadow:0 0 0 4px rgba(220,38,38,0)}}
.alert-bar{margin:0 12px 10px;padding:8px 12px;background:#fff5f5;border:1px solid rgba(220,38,38,.2);border-radius:var(--rs);font-size:12px;color:var(--re6);display:flex;align-items:center;gap:8px;font-weight:500}
.pnr-bar{margin:0 12px 10px;padding:5px 12px;background:#f0fdf8;border:1px solid rgba(15,158,110,.2);border-radius:var(--rs);font-size:11.5px;color:var(--gr6);font-weight:500;font-family:'DM Mono',monospace}
.c-div{height:1px;background:var(--bds);margin:0 12px}
.c-meta{padding:10px 12px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.mpill{display:flex;align-items:center;gap:4px;font-size:11px;color:var(--t700);background:var(--page);border:1px solid var(--bd);border-radius:6px;padding:3px 9px;font-weight:500}
.mpill.ok{color:var(--gr6);background:#f0fdf8;border-color:rgba(15,158,110,.2)}
.mpill.warn{color:var(--am6);background:#fffbeb;border-color:rgba(201,124,10,.2)}
.mpill.err{color:var(--re6);background:#fff5f5;border-color:rgba(220,38,38,.2)}
.mdate{margin-left:auto;font-size:11px;color:var(--t300);white-space:nowrap}
.prog-wrap{padding:0 12px 10px}
.prog-lbl{font-size:11px;color:var(--t300);display:flex;justify-content:space-between;margin-bottom:5px}
.prog-bg{height:5px;background:var(--page);border-radius:3px;overflow:hidden}
.prog-fill{height:100%;border-radius:3px;transition:width .4s}
.pf-full{background:linear-gradient(90deg,var(--b500),var(--b300))}
.pf-mid{background:linear-gradient(90deg,var(--am6),#fbbf24)}
.pf-low{background:var(--re6)}
.c-foot{padding:10px 12px 14px;display:flex;align-items:center;gap:10px}
.c-amt{font-family:'DM Mono',monospace;font-size:15px;font-weight:600;color:var(--t900)}
.c-amt-sub{font-size:10px;color:var(--t300);margin-top:2px;font-weight:500}
.c-acts{margin-left:auto;display:flex;gap:6px}
.vma-row{padding:0 12px 10px;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.vma-tag{font-size:11px;font-weight:600;color:var(--gr6);background:#f0fdf8;border:1px solid rgba(15,158,110,.2);border-radius:4px;padding:2px 9px}
.vma-detail{font-family:'DM Mono',monospace;font-size:11.5px;color:var(--t500)}
.trenn-tag{font-size:11px;font-weight:600;color:var(--am6);background:var(--am1);border:1px solid rgba(201,124,10,.2);border-radius:4px;padding:2px 9px}
/* ── Buttons ── */
.btn-g{font-size:12px;font-weight:500;color:var(--b600);background:var(--b50);border:1px solid var(--b100);border-radius:6px;padding:5px 13px;cursor:pointer;transition:all .12s;text-decoration:none;font-family:'Inter',sans-serif}
.btn-g:hover{background:var(--b100);border-color:var(--b300)}
.btn-s{font-size:12px;font-weight:600;color:white;background:var(--b600);border:none;border-radius:6px;padding:5px 13px;cursor:pointer;transition:all .12s;font-family:'Inter',sans-serif}
.btn-s:hover{background:var(--b500)}
.btn-dg{font-size:12px;font-weight:500;color:var(--re6);background:#fff5f5;border:1px solid rgba(220,38,38,.2);border-radius:6px;padding:5px 13px;cursor:pointer;transition:all .12s;font-family:'Inter',sans-serif}
.btn-dg:hover{background:var(--re1)}
/* ── Page cards ── */
.page-card{background:var(--white);border:1px solid var(--bd);border-radius:var(--r);padding:28px;box-shadow:var(--sh-sm)}
.page-card h2{font-size:1.15rem;font-weight:700;margin-bottom:18px;color:var(--t900)}
.btn{background:linear-gradient(135deg,var(--b600),var(--b500));color:white;padding:9px 18px;border:none;border-radius:var(--rs);font-size:13px;font-weight:600;cursor:pointer;text-decoration:none;display:inline-block;transition:all .15s;font-family:'Inter',sans-serif;box-shadow:0 2px 6px rgba(33,82,196,.25)}
.btn:hover{transform:translateY(-1px);box-shadow:0 4px 10px rgba(33,82,196,.35)}
.btn-l{background:var(--white);color:var(--b600);padding:9px 18px;border:1.5px solid var(--b100);border-radius:var(--rs);font-size:13px;font-weight:500;cursor:pointer;text-decoration:none;display:inline-block;transition:all .12s;font-family:'Inter',sans-serif}
.btn-l:hover{background:var(--b50);border-color:var(--b300)}
.acts{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:16px}
/* ── Tables ── */
table{width:100%;border-collapse:collapse}
th,td{border:1px solid var(--bd);padding:9px 11px;text-align:left;vertical-align:top;font-size:12.5px}
th{background:linear-gradient(180deg,var(--b50),#e8f0fe);font-weight:600;color:var(--t700);font-size:12px;letter-spacing:.02em}
tr:hover td{background:#f7faff}
.cc{font-family:'DM Mono',monospace;font-weight:600;color:var(--b700)}
.ok-t{color:var(--gr6);font-weight:600} .warn-t{color:var(--am6);font-weight:600} .err-t{color:var(--re6);font-weight:600}
.bdg{padding:2px 9px;border-radius:20px;font-size:11px;font-weight:600}
.bdg-ok{background:var(--gr1);color:var(--gr6)} .bdg-w{background:var(--am1);color:var(--am6)} .bdg-e{background:var(--re1);color:var(--re6)}
/* ── Forms ── */
.fgrid{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.ff{grid-column:1/-1}
.fgrp{display:flex;flex-direction:column;gap:5px}
.flbl{font-size:11.5px;font-weight:600;color:var(--t700)}
.finp,.fsel{background:var(--page);border:1.5px solid var(--bd);border-radius:var(--rs);padding:9px 12px;color:var(--t900);font-family:'Inter',sans-serif;font-size:13px;transition:all .15s;width:100%}
.finp:focus,.fsel:focus{outline:none;border-color:var(--b400);background:var(--white);box-shadow:0 0 0 3px rgba(33,82,196,.1)}
.finp::placeholder{color:var(--t300)}
.mfooter{display:flex;gap:8px;justify-content:flex-end;padding-top:16px;border-top:1px solid var(--bds);margin-top:16px}
/* ── Modal ── */
.modal-ov{position:fixed;inset:0;z-index:300;background:rgba(14,38,80,.4);backdrop-filter:blur(6px);display:flex;align-items:center;justify-content:center;opacity:0;pointer-events:none;transition:opacity .2s}
.modal-ov.open{opacity:1;pointer-events:all}
.modal{background:var(--white);border:1px solid var(--bd);border-radius:16px;box-shadow:var(--sh-lg);width:100%;max-width:540px;transform:translateY(12px) scale(.98);transition:transform .22s;max-height:90vh;overflow-y:auto}
.modal-ov.open .modal{transform:translateY(0) scale(1)}
.m-hdr{padding:22px 26px 16px;display:flex;align-items:center;justify-content:space-between;border-bottom:1px solid var(--bds)}
.m-title{font-size:16px;font-weight:700;color:var(--t900)}
.m-close{width:30px;height:30px;border-radius:7px;display:flex;align-items:center;justify-content:center;cursor:pointer;color:var(--t300);background:none;border:none;font-size:18px;transition:all .12s}
.m-close:hover{background:var(--page);color:var(--t700)}
.m-body{padding:22px 26px 10px}
.code-prev{text-align:center;font-family:'DM Mono',monospace;font-size:24px;font-weight:600;color:var(--b700);background:linear-gradient(135deg,var(--b50),#e8f0fe);border:1px solid var(--b100);border-radius:var(--rs);padding:14px 0;margin-bottom:4px;letter-spacing:2px}
.code-sub{text-align:center;font-size:11px;color:var(--t300);margin-bottom:18px}
.btn-mp{background:linear-gradient(135deg,var(--b600),var(--b500));color:white;border:none;border-radius:var(--rs);padding:10px 24px;font-size:13px;font-weight:600;cursor:pointer;font-family:'Inter',sans-serif;box-shadow:0 2px 8px rgba(33,82,196,.3)}
.btn-mp:hover{transform:translateY(-1px);box-shadow:0 4px 12px rgba(33,82,196,.4)}
.btn-mc{background:var(--page);color:var(--t700);border:1.5px solid var(--bd);border-radius:var(--rs);padding:10px 20px;font-size:13px;cursor:pointer;font-family:'Inter',sans-serif}
.btn-mc:hover{background:var(--bds)}
/* ── Misc ── */
.empty{text-align:center;padding:36px;color:var(--t300);font-size:13px;border:2px dashed var(--bd);border-radius:var(--r);background:var(--white)}
.sub{color:var(--t500);font-size:12px}
.hint{font-size:11px;color:var(--t300);font-style:italic;margin-top:3px}
::-webkit-scrollbar{width:5px} ::-webkit-scrollbar-thumb{background:var(--bd);border-radius:3px}
@keyframes fu{from{opacity:0;transform:translateY(10px)}to{opacity:1;transform:translateY(0)}}
.sb{animation:fu .28s ease both}
.sb:nth-child(2){animation-delay:.07s} .sb:nth-child(3){animation-delay:.14s} .sb:nth-child(4){animation-delay:.21s}
"""
JS = """
function toggleDD(e){e.stopPropagation();document.getElementById('dd').classList.toggle('open')}
document.addEventListener('click',()=>document.getElementById('dd').classList.remove('open'));
function openM(t){
  document.getElementById('dd').classList.remove('open');
  if(t==='trip'){fetch('/api/next-code').then(r=>r.json()).then(d=>{document.getElementById('cprev').textContent=d.code}).catch(()=>{})}
  if(t==='event'||t==='upload'){
    fetch('/api/active-codes').then(r=>r.json()).then(d=>{
      const sel=document.getElementById(t==='event'?'ev-code':'up-code');
      if(sel){sel.innerHTML=(t==='upload'?'<option value="">– KI zuordnen lassen –</option>':'');d.codes.forEach(c=>{sel.innerHTML+=`<option>${c}</option>`});}
    }).catch(()=>{});
  }
  document.getElementById('m-'+t).classList.add('open');
  document.body.style.overflow='hidden';
}
function closeM(t){document.getElementById('m-'+t).classList.remove('open');document.body.style.overflow='';}
function submitTrip(){
  const req=['fi-employee-code','fi-trip-title','fi-departure-date','fi-return-date'];
  for(const id of req){
    const el=document.getElementById(id);
    if(!el||!el.value.trim()){
      el&&el.focus();
      el&&(el.style.borderColor='var(--re6)');
      alert('Bitte alle Pflichtfelder (*) ausfüllen: Mitarbeiterkürzel, Reisename, Abreise, Rückkehr');
      return;
    }
    el.style.borderColor='';
  }
  const f=new FormData();
  ['employee_code','trip_title','customer_code','traveler_name','colleagues',
   'departure_date','return_date','departure_time_home','arrival_time_home',
   'destinations','nights_planned','notes'].forEach(k=>{
    const el=document.getElementById('fi-'+k.replace(/_/g,'-'));
    if(el)f.append(k,el.value);
  });
  fetch('/new-trip',{method:'POST',body:f}).then(()=>{window.location.href='/';});
  closeM('trip');
}
function showFile(inp){if(inp.files[0])document.getElementById('fname').textContent='✓ '+inp.files[0].name;}
function dropFile(e){e.preventDefault();document.getElementById('uz').classList.remove('drag');const f=e.dataTransfer.files[0];if(f)document.getElementById('fname').textContent='✓ '+f.name;}
"""

def page_shell(title, content, active_tab=""):
    tabs=[("active","Laufende Reisen","/"),("planned","Vorplanung","/planned"),("done","Abgeschlossen","/done"),("stats","Statistik","/stats")]
    tab_html="".join(f'<a href="{href}" class="nav-tab{" active" if active_tab==k else ""}">{lbl}</a>' for k,lbl,href in tabs)
    ki_ok=bool(MISTRAL_API_KEY)
    ki_txt="✓ Mistral KI" if ki_ok else "⚠ Kein KI-Key"
    ki_style="color:#0f9e6e;background:#f0fdf8;border-color:rgba(15,158,110,.25)" if ki_ok else "color:#c97c0a;background:#fffbeb;border-color:rgba(201,124,10,.25)"
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} – Herrhammer Reisekosten</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>{CSS}</style>
</head>
<body>
<header class="topbar">
  <a href="/" class="logo-wrap"><img src="/static/herrhammer-logo.png" alt="Herrhammer"></a>
  <nav class="nav-tabs">{tab_html}</nav>
  <div class="topbar-right">
    <span class="ki-pill" style="{ki_style}">{ki_txt}</span>
    <span class="ver-pill">v{APP_VERSION}</span>
    <div class="dd-wrap">
      <button class="add-btn" onclick="toggleDD(event)">+ Neu</button>
      <div class="dd-menu" id="dd">
        <button class="dd-item" onclick="openM('trip')"><div class="dd-icon di-b">✈</div><div><div style="font-weight:500">Neue Reise anlegen</div><div class="dd-sub">Code wird automatisch vergeben</div></div></button>
        <button class="dd-item" onclick="openM('event')"><div class="dd-icon di-g">📋</div><div><div style="font-weight:500">Manuelles Ereignis</div><div class="dd-sub">Alert oder Notiz eintragen</div></div></button>
        <div class="dd-div"></div>
        <button class="dd-item" onclick="openM('upload')"><div class="dd-icon di-a">📎</div><div><div style="font-weight:500">Beleg hochladen</div><div class="dd-sub">KI-Analyse via Mistral OCR</div></div></button>
      </div>
    </div>
  </div>
</header>
<main class="wrap">{content}</main>

<!-- MODAL: NEUE REISE (vereinfacht, c) -->
<div class="modal-ov" id="m-trip" onclick="closeM('trip')">
  <div class="modal" onclick="event.stopPropagation()">
    <div class="m-hdr"><span class="m-title">Neue Reise anlegen</span><button class="m-close" onclick="closeM('trip')">×</button></div>
    <div class="m-body">
      <div class="code-prev" id="cprev">wird vergeben</div>
      <div class="code-sub">Reisecode automatisch</div>
      <div style="font-size:11px;color:var(--re6);margin-bottom:8px">* Pflichtfelder</div>
      <div class="fgrid">
        <div class="fgrp"><label class="flbl">Mitarbeiterkürzel * <span style="color:var(--t300);font-weight:400">(z.B. MH)</span></label><input class="finp" id="fi-employee-code" type="text" placeholder="MH" maxlength="5" required></div>
        <div class="fgrp"><label class="flbl">Reisename / Ziel *</label><input class="finp" id="fi-trip-title" type="text" placeholder="z.B. Lyon, Messe München" required></div>
        <div class="fgrp"><label class="flbl">Kundenkürzel <span style="color:var(--t300);font-weight:400">(optional)</span></label><input class="finp" id="fi-customer-code" type="text" placeholder="z.B. BMW, intern"></div>
        <div class="fgrp"><label class="flbl">Reisender (Klarname)</label><input class="finp" id="fi-traveler-name" type="text" placeholder="Vor- und Nachname"></div>
        <div class="fgrp"><label class="flbl">Abreise *</label><input class="finp" id="fi-departure-date" type="date" required></div>
        <div class="fgrp"><label class="flbl">Uhrzeit Abreise</label><input class="finp" id="fi-departure-time-home" type="time" value="08:00"></div>
        <div class="fgrp"><label class="flbl">Rückkehr *</label><input class="finp" id="fi-return-date" type="date" required></div>
        <div class="fgrp"><label class="flbl">Uhrzeit Ankunft</label><input class="finp" id="fi-arrival-time-home" type="time" value="18:00"></div>
        <div class="fgrp ff">
          <label class="flbl">Reiseziel(e) / Länder</label>
          <input class="finp" id="fi-destinations" type="text" list="country-list" placeholder="z.B. Indien, Dubai, Frankfurt Messe">
          <datalist id="country-list">
            <option value="Deutschland (DE)"><option value="Frankreich (FR)"><option value="Großbritannien (GB)">
            <option value="USA (US)"><option value="Indien (IN)"><option value="VAE / Dubai (AE)">
            <option value="Aserbaidschan / Baku (AZ)"><option value="China (CN)"><option value="Japan (JP)">
            <option value="Singapur (SG)"><option value="Schweiz (CH)"><option value="Österreich (AT)">
            <option value="Italien (IT)"><option value="Spanien (ES)"><option value="Türkei (TR)">
            <option value="Niederlande (NL)"><option value="Polen (PL)"><option value="Schweden (SE)">
            <option value="Norwegen (NO)"><option value="Dänemark (DK)"><option value="Belgien (BE)">
            <option value="Portugal (PT)"><option value="Tschechien (CZ)"><option value="Ungarn (HU)">
            <option value="Rumänien (RO)"><option value="Saudi-Arabien (SA)"><option value="Katar (QR)">
            <option value="Südkorea (KR)"><option value="Australien (AU)"><option value="Kanada (CA)">
            <option value="Brasilien (BR)"><option value="Mexiko (MX)"><option value="Indonesien (ID)">
          </datalist>
        </div>
        <div class="fgrp"><label class="flbl">Kollegen</label><input class="finp" id="fi-colleagues" type="text" placeholder="z.B. T. Moser"></div>
        <div class="fgrp"><label class="flbl">Geplante Nächte</label><input class="finp" id="fi-nights-planned" type="number" min="0" value="0"></div>
        <div class="fgrp ff"><label class="flbl">Notiz</label><input class="finp" id="fi-notes" type="text" placeholder="z.B. Messebesuch, Kundentermin..."></div>
      </div>
      <div class="mfooter"><button class="btn-mc" onclick="closeM('trip')">Abbrechen</button><button class="btn-mp" onclick="submitTrip()">Reise anlegen</button></div>
    </div>
  </div>
</div>

<!-- MODAL: MANUELLES EREIGNIS -->
<div class="modal-ov" id="m-event" onclick="closeM('event')">
  <div class="modal" onclick="event.stopPropagation()">
    <div class="m-hdr"><span class="m-title">Manuelles Ereignis</span><button class="m-close" onclick="closeM('event')">×</button></div>
    <div class="m-body">
      <div class="fgrid">
        <div class="fgrp ff"><label class="flbl">Reise</label><select class="fsel" id="ev-code"></select></div>
        <div class="fgrp ff"><label class="flbl">Ereignistyp</label>
          <select class="fsel"><option>Flugverspätung</option><option>Zugverspätung</option><option>Umbuchung</option><option>Hoteländerung</option><option>Mietwagen-Verlängerung</option><option>Sonstige Notiz</option></select></div>
        <div class="fgrp ff"><label class="flbl">Beschreibung</label><input class="finp" type="text" placeholder="z.B. AZ770 +47 Min."></div>
        <div class="fgrp"><label class="flbl">Schweregrad</label>
          <select class="fsel"><option>⚠ Warnung</option><option>🔴 Alert</option><option>ℹ Info</option></select></div>
        <div class="fgrp"><label class="flbl">Datum / Uhrzeit</label><input class="finp" type="datetime-local"></div>
      </div>
      <div class="mfooter"><button class="btn-mc" onclick="closeM('event')">Abbrechen</button><button class="btn-mp">Speichern</button></div>
    </div>
  </div>
</div>

<!-- MODAL: BELEG HOCHLADEN -->
<div class="modal-ov" id="m-upload" onclick="closeM('upload')">
  <div class="modal" onclick="event.stopPropagation()">
    <div class="m-hdr"><span class="m-title">Beleg hochladen</span><button class="m-close" onclick="closeM('upload')">×</button></div>
    <div class="m-body">
      <form id="upload-form" method="post" action="/upload-beleg" enctype="multipart/form-data">
        <div class="fgrid" style="margin-bottom:14px">
          <div class="fgrp ff"><label class="flbl">Reise zuordnen (optional)</label>
            <select class="fsel" id="up-code" name="trip_code"><option value="">– KI zuordnen lassen –</option></select></div>
        </div>
        <div style="border:2px dashed var(--bd);border-radius:var(--r);padding:28px 20px;text-align:center;color:var(--t300);cursor:pointer;background:var(--page)" id="uz"
             ondragover="event.preventDefault();this.style.borderColor='var(--b400)'" ondragleave="this.style.borderColor='var(--bd)'"
             ondrop="dropFile(event)" onclick="document.getElementById('fi').click()">
          <div style="font-size:26px;margin-bottom:6px">📎</div>
          <div style="font-size:13px;font-weight:500">Datei hierher ziehen oder klicken</div>
          <div style="font-size:11px;margin-top:3px">PDF, JPG, PNG, ICS – Mistral OCR analysiert automatisch</div>
        </div>
        <input type="file" id="fi" name="file" style="display:none" accept=".pdf,.jpg,.jpeg,.png,.ics" onchange="showFile(this)">
        <div id="fname" style="font-size:12px;color:var(--t500);margin-top:8px;min-height:18px"></div>
        <div style="font-size:11px;color:var(--t300);margin-top:6px">🔒 DSGVO: Mistral EU-API (Paris). Keine Datenspeicherung nach Analyse.</div>
        <div class="mfooter"><button type="button" class="btn-mc" onclick="closeM('upload')">Abbrechen</button><button type="submit" class="btn-mp">Hochladen &amp; KI-Analyse</button></div>
      </form>
    </div>
  </div>
</div>
<script>{JS}</script>
</body>
</html>"""


# =========================================================
# API HELPER
# =========================================================

@app.get("/api/next-code")
def api_next_code():
    try:
        conn=get_conn();cur=conn.cursor()
        code=next_trip_code(cur);cur.close();conn.close()
        return {"code":code}
    except Exception as e:
        return {"code":"–","error":str(e)}

@app.get("/api/active-codes")
def api_active_codes():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code DESC LIMIT 30")
        codes=[r[0] for r in cur.fetchall()];cur.close();conn.close()
        return {"codes":codes}
    except Exception as e:
        return {"codes":[],"error":str(e)}

@app.get("/version")
def version():
    return {"version":APP_VERSION,"ki":"mistral-eu" if MISTRAL_API_KEY else "keine","auto_imap":"aktiv"}


# =========================================================
# /init
# =========================================================

@app.get("/init")
def init():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("CREATE TABLE IF NOT EXISTS mail_messages (id SERIAL PRIMARY KEY, mail_uid TEXT UNIQUE)")
        for col in ["message_id TEXT","sender TEXT","subject TEXT","body TEXT","trip_code TEXT",
                    "detected_type TEXT","pnr_code TEXT",
                    "analysis_status TEXT DEFAULT 'ausstehend'",
                    "created_at TIMESTAMP DEFAULT now()"]:
            cur.execute(f"ALTER TABLE mail_messages ADD COLUMN IF NOT EXISTS {col}")
        # Index für schnellen Duplikat-Check per Message-ID
        cur.execute("CREATE INDEX IF NOT EXISTS idx_mail_message_id ON mail_messages(message_id)")

        cur.execute("CREATE TABLE IF NOT EXISTS mail_attachments (id SERIAL PRIMARY KEY, mail_uid TEXT)")
        for col in ["trip_code TEXT","original_filename TEXT","saved_filename TEXT","content_type TEXT",
                    "storage_key TEXT","detected_type TEXT","extracted_text TEXT",
                    "detected_amount TEXT","detected_amount_eur TEXT","detected_currency TEXT",
                    "detected_date TEXT","detected_vendor TEXT","pnr_code TEXT",
                    "detected_flight_numbers TEXT","detected_train_numbers TEXT","detected_nights INTEGER DEFAULT 0",
                    "analysis_status TEXT DEFAULT 'ausstehend'","confidence TEXT DEFAULT 'niedrig'",
                    "review_flag TEXT DEFAULT 'pruefen'","ki_bemerkung TEXT",
                    "file_hash TEXT","created_at TIMESTAMP DEFAULT now()"]:
            cur.execute(f"ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS {col}")

        cur.execute("""CREATE TABLE IF NOT EXISTS trip_meta (
            trip_code TEXT PRIMARY KEY, hotel_mode TEXT,
            departure_date DATE, return_date DATE,
            departure_time_home TEXT DEFAULT '08:00',
            arrival_time_home TEXT DEFAULT '18:00',
            destinations TEXT,
            country_code TEXT DEFAULT 'DE',
            traveler_name TEXT, colleagues TEXT,
            flight_numbers TEXT, train_numbers TEXT,
            car_rental_info TEXT, nights_planned INTEGER DEFAULT 0,
            nights_booked INTEGER DEFAULT 0,
            meals_reimbursed TEXT DEFAULT '',
            pnr_code TEXT, notes TEXT,
            created_at TIMESTAMP DEFAULT now())""")
        for col in ["departure_time_home TEXT DEFAULT '08:00'","arrival_time_home TEXT DEFAULT '18:00'",
                    "destinations TEXT","train_numbers TEXT","nights_booked INTEGER DEFAULT 0",
                    "pnr_code TEXT","country_code TEXT DEFAULT 'DE'",
                    "traveler_name TEXT","colleagues TEXT","flight_numbers TEXT",
                    "car_rental_info TEXT","nights_planned INTEGER DEFAULT 0",
                    "meals_reimbursed TEXT DEFAULT ''","notes TEXT",
                    "hotel_mode TEXT","departure_date DATE","return_date DATE",
                    "trip_title TEXT","customer_code TEXT","employee_code TEXT",
                    "vma_destinations TEXT",
                    "created_at TIMESTAMP DEFAULT now()"]:
            cur.execute(f"ALTER TABLE trip_meta ADD COLUMN IF NOT EXISTS {col}")

        cur.execute("""CREATE TABLE IF NOT EXISTS flight_alerts (
            id SERIAL PRIMARY KEY, trip_code TEXT, flight_number TEXT,
            flight_date TEXT, alert_type TEXT, message TEXT,
            source TEXT, delay_min INTEGER, checked_at TIMESTAMP DEFAULT now())""")

        # Benutzerdefinierte Kategorie-Regeln
        cur.execute("""CREATE TABLE IF NOT EXISTS category_rules (
            id SERIAL PRIMARY KEY,
            keyword TEXT NOT NULL,
            category TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT now())""")

        # Tagesbasierte Mahlzeiten-Erfassung für VMA
        cur.execute("""CREATE TABLE IF NOT EXISTS daily_meals (
            id SERIAL PRIMARY KEY,
            trip_code TEXT NOT NULL,
            meal_date DATE NOT NULL,
            breakfast BOOLEAN DEFAULT FALSE,
            lunch BOOLEAN DEFAULT FALSE,
            dinner BOOLEAN DEFAULT FALSE,
            notes TEXT,
            updated_at TIMESTAMP DEFAULT now(),
            UNIQUE(trip_code, meal_date))""")

        conn.commit();cur.close();conn.close()
        return {"status":"ok","version":APP_VERSION}
    except Exception as e:
        return {"status":"fehler","detail":str(e)}


# =========================================================
# TRIPS LADEN
# =========================================================

def load_trips(conn, filter_status=None):
    cur=conn.cursor()
    cur.execute("""SELECT trip_code,hotel_mode,departure_date,return_date,
                   departure_time_home,arrival_time_home,destinations,country_code,
                   traveler_name,colleagues,flight_numbers,train_numbers,car_rental_info,
                   nights_planned,nights_booked,meals_reimbursed,pnr_code,notes,
                   trip_title,customer_code,employee_code
                   FROM trip_meta ORDER BY trip_code""")
    raw=cur.fetchall()
    cur.execute("""SELECT COALESCE(trip_code,'') tc,detected_type,
                   COALESCE(detected_amount_eur,'') eur,review_flag,
                   detected_flight_numbers,detected_train_numbers,pnr_code
                   FROM mail_attachments""")
    att_rows=cur.fetchall()

    # Fallback: Reisender + Ziel aus Mail-Analyse wenn trip_meta leer
    cur.execute("""SELECT trip_code,
                   MAX(CASE WHEN traveler_name IS NOT NULL AND traveler_name!='' THEN traveler_name END),
                   MAX(CASE WHEN destination IS NOT NULL AND destination!='' THEN destination END)
                   FROM (
                     SELECT mm.trip_code,
                       NULL::text as traveler_name, NULL::text as destination
                     FROM mail_messages mm WHERE mm.trip_code IS NOT NULL
                   ) sub GROUP BY trip_code""")
    # Simpler Fallback: subject der ersten Mail als Hinweis
    cur.execute("""SELECT trip_code, subject FROM mail_messages
                   WHERE trip_code IS NOT NULL ORDER BY id""")
    mail_subjects = {}
    for tc_m, subj in cur.fetchall():
        if tc_m not in mail_subjects:
            mail_subjects[tc_m] = subj or ""

    # Aktuelle Flight-Alerts (letzte 48h, nur kritische)
    cur.execute("""SELECT trip_code,flight_number,alert_type,message
                   FROM flight_alerts
                   WHERE checked_at > now() - interval '48 hours'
                   AND alert_type IN ('cancelled','delay')
                   ORDER BY checked_at DESC""")
    alert_rows=cur.fetchall()
    cur.close()

    flight_alerts_by_trip: dict = {}
    for tc,fn,atype,msg in alert_rows:
        if tc not in flight_alerts_by_trip:
            flight_alerts_by_trip[tc] = []
        label = f"{'⚠ ' if atype=='cancelled' else '⏱ '}{fn}: {msg}"
        if label not in flight_alerts_by_trip[tc]:
            flight_alerts_by_trip[tc].append(label)

    att={}
    for tc,dt,eur,rf,fns,trains,pnr in att_rows:
        if tc not in att: att[tc]={"types":[],"sum":0.0,"review":0,"fns":[],"trains":[],"pnrs":[],"boarding":0}
        att[tc]["types"].append(dt)
        if rf=="pruefen": att[tc]["review"]+=1
        if dt=="Flug": att[tc]["boarding"]+=1  # Bordkarten zählen
        if eur:
            try: att[tc]["sum"]+=float(eur.replace(".","").replace(",","."))
            except: pass
        if fns: att[tc]["fns"].extend([f.strip() for f in fns.split(",") if f.strip()])
        if trains: att[tc]["trains"].extend([t.strip() for t in trains.split(",") if t.strip()])
        if pnr: att[tc]["pnrs"].append(pnr)

    trips=[]
    for row in raw:
        (tc,hm,dep,ret,dep_t,ret_t,destinations,cc,traveler,colleagues,
         fns,trains,car,nights_p,nights_b,meals,pnr,notes,
         trip_title,customer_code,employee_code) = row
        status=compute_status(dep,ret)
        if filter_status and status!=filter_status: continue
        a=att.get(tc,{"types":[],"sum":0.0,"review":0,"fns":[],"trains":[],"pnrs":[],"boarding":0})
        types=a["types"]
        all_fns = list(set(([f.strip() for f in (fns or "").split(",") if f.strip()] + a["fns"])))
        all_trains = list(set(([t.strip() for t in (trains or "").split(",") if t.strip()] + a["trains"])))
        all_pnrs = list(set(([pnr] if pnr else []) + a["pnrs"]))
        # Fallback: Mail-Betreff als Hinweis wenn Ziel/Name fehlt
        mail_hint = mail_subjects.get(tc,"")
        display_dest = destinations or trip_title or (mail_hint[:40] if mail_hint else "")
        display_traveler = traveler or ""
        trips.append(dict(tc=tc,status=status,hm=hm,dep=dep,ret=ret,
            dep_t=dep_t or "08:00",ret_t=ret_t or "18:00",
            destinations=display_dest,cc=cc or "DE",
            traveler=display_traveler,colleagues=colleagues or "",
            fns=", ".join(all_fns),trains=", ".join(all_trains),
            car=car or "",nights_p=nights_p or 0,nights_b=nights_b or 0,
            meals=meals or "",pnr=", ".join(all_pnrs),notes=notes or "",
            trip_title=trip_title or "",customer_code=customer_code or "",
            employee_code=employee_code or "",
            mail_hint=mail_hint,
            has_flight="Flug" in types,
            has_hotel="Hotel" in types or hm in ("customer","own"),
            has_car="Mietwagen" in types or bool(car),
            sum_eur=round(a["sum"],2),review=a["review"],
            flight_count=len(all_fns),
            boarding_count=a.get("boarding",0),
            flight_alerts=flight_alerts_by_trip.get(tc,[]),
            warnings=[w for w in [
                None if "Flug" in types else "Kein Flugbeleg",
                None if ("Hotel" in types or hm in ("customer","own")) or status=="done" else "Hotel fehlt",
            ] + flight_alerts_by_trip.get(tc,[]) if w]))
    return trips

def _pills(t):
    def p(ok,lbl): return f'<div class="mpill {"ok" if ok else "err"}"><span>{"✓" if ok else "✗"}</span> {lbl}</div>'
    dep=str(t["dep"])[:10] if t["dep"] else "–"
    ret=str(t["ret"])[:10] if t["ret"] else "–"
    return p(t["has_flight"],"Flug") + p(t["has_hotel"],"Hotel") + p(t["has_car"],"Mietwagen") + f'<div class="mdate">{dep} – {ret}</div>'

def _code_header(t):
    """Reisecode-Badge + Mitarbeiterkürzel · Reisename als Haupttitel der Karte."""
    emp   = t.get("employee_code","")
    title = t.get("trip_title","")
    ccode = t.get("customer_code","")
    # Hauptzeile: Kürzel · Reisename (oder Ziel als Fallback)
    main_parts = [x for x in [emp, title or t.get("destinations","")] if x]
    main_label = " · ".join(main_parts) if main_parts else ""
    # Kundencode klein dahinter
    ccode_html = f' <span style="font-size:10px;color:var(--t300);font-weight:400">{ccode}</span>' if ccode else ""
    label_html = (f' <span style="font-family:\'Inter\',sans-serif;font-size:12px;'
                  f'font-weight:500;color:var(--t700);letter-spacing:0">{main_label}</span>{ccode_html}') if main_label else ""
    return f'<div class="c-code">{t["tc"]}{label_html}</div>'

def _hotel_badge(t):
    np_=t["nights_p"]; nb=t["nights_b"]
    if not np_: return ""
    col = "ok" if nb>=np_ else ("warn" if nb>0 else "err")
    return f'<div class="mpill {col}">🏨 {nb}/{np_} Nächte</div>'

def _progress(t):
    sc=sum([t["has_flight"],t["has_hotel"],t["has_car"]])
    pct=int(sc/3*100)
    cls="pf-full" if sc==3 else ("pf-mid" if sc>=1 else "pf-low")
    lc="var(--gr6)" if sc==3 else ("var(--am6)" if sc>=1 else "var(--re6)")
    warn=t["warnings"][0] if t["warnings"] else "vollständig"
    return f'<div class="prog-wrap"><div class="prog-lbl"><span>Vollständigkeit</span><span style="color:{lc};font-weight:500">{warn if sc<3 else "vollständig"}</span></div><div class="prog-bg"><div class="prog-fill {cls}" style="width:{pct}%"></div></div></div>'


# =========================================================
# DASHBOARD
# =========================================================

@app.get("/", response_class=HTMLResponse)
@app.get("/active", response_class=HTMLResponse)
async def dashboard_active(request: Request):
    return await _dashboard(request, "active")

@app.get("/planned", response_class=HTMLResponse)
async def dashboard_planned(request: Request):
    return await _dashboard(request, "planned")

@app.get("/done", response_class=HTMLResponse)
async def dashboard_done(request: Request):
    return await _dashboard(request, "done")

async def _dashboard(request: Request, focus: str):
    try:
        conn=get_conn()
        all_trips=load_trips(conn)
        conn.close()
        active_t =[t for t in all_trips if t["status"]=="active"]
        planned_t=[t for t in all_trips if t["status"]=="planned"]
        done_t   =[t for t in all_trips if t["status"]=="done"]
        open_alerts=sum(1 for t in active_t if t["warnings"])

        # i) kein Belege-Betrag in Summary
        summary=f"""<div class="sum-bar sb">
          <div class="sum-item"><div class="sum-val blue">{len(active_t)}</div><div class="sum-lbl">Aktive Reisen</div></div>
          <div class="sum-item"><div class="sum-val">{len(planned_t)}</div><div class="sum-lbl">In Planung</div></div>
          <div class="sum-item"><div class="sum-val green">{len(done_t)}</div><div class="sum-lbl">Abgeschlossen</div></div>
          <div class="sum-item"><div class="sum-val {"red" if open_alerts else ""}">{open_alerts}</div><div class="sum-lbl">Offene Alerts</div></div>
        </div>"""

        def active_cards(trips):
            if not trips: return '<div class="empty">Keine laufenden Reisen.</div>'
            cards=""
            for t in trips:
                ha=bool(t["warnings"])
                fa=t.get("flight_alerts",[])
                pnr_bar=f'<div class="pnr-bar">✈ PNR: {t["pnr"]}</div>' if t["pnr"] else ""
                # Flight-Alert-Bar (rot, prominent) separat von Belege-Warnungen
                flight_alert_bar=""
                if fa:
                    for fal in fa[:2]:  # max 2 anzeigen
                        is_cancel = "STORNIERT" in fal.upper()
                        bg = "#fff0f0" if is_cancel else "#fff8f0"
                        bc = "rgba(220,38,38,.25)" if is_cancel else "rgba(201,124,10,.25)"
                        fc = "var(--re6)" if is_cancel else "var(--am6)"
                        flight_alert_bar += f'<div style="margin:0 12px 6px;padding:6px 12px;background:{bg};border:1px solid {bc};border-radius:var(--rs);font-size:12px;color:{fc};font-weight:500">{fal}</div>'
                # Normale Warnungen (Kein Flugbeleg / Hotel fehlt)
                doc_warnings=[w for w in t["warnings"] if w not in fa]
                dep_s=str(t["dep"])[:10] if t["dep"] else ""
                ret_s=str(t["ret"])[:10] if t["ret"] else ""
                date_range=f"{dep_s} – {ret_s}" if dep_s and ret_s else (dep_s or "Datum fehlt")
                dest_show=t["destinations"] or t["cc"] or "–"
                fn_line=f'<div style="font-family:DM Mono,monospace;font-size:11px;color:var(--b600);margin-top:2px">✈ {t["fns"]}</div>' if t["fns"] else ""
                pnr_inline=f' · <span style="font-family:DM Mono,monospace;color:var(--gr6)">{t["pnr"]}</span>' if t["pnr"] else ""
                cards+=f"""<div class="card {"alert" if ha else ""}" onclick="location.href='/trip/{t["tc"]}'">
                  <div class="c-top">{_code_header(t)}
                    <div class="c-info">
                      <div class="c-traveler">{dest_show}</div>
                      <div class="c-dest">{date_range}{pnr_inline}</div>
                      {fn_line}
                    </div>
                    <div class="sbadge sb-active">{"<span class='adot'></span>Alert" if ha else "Aktiv"}</div>
                  </div>
                  {flight_alert_bar}
                  {"<div class='alert-bar'>⚠ " + " · ".join(doc_warnings) + "</div>" if doc_warnings else ""}
                  <div class="c-div"></div>
                  <div class="c-meta">{_pills(t)}{_hotel_badge(t)}</div>
                  <div class="c-foot">
                    <div>
                      <div class="c-amt">{t["sum_eur"]:,.2f} €</div>
                      <div class="c-amt-sub">
                        {f'✈ {t["boarding_count"]}/{t["flight_count"]} Bordkarten' if t["flight_count"]>0 else "Keine Flüge"}
                        {" · ⚠" if t["flight_count"]>0 and t["boarding_count"]<t["flight_count"] else ""}
                      </div>
                    </div>
                    <div class="c-acts">
                      <button class="{"btn-dg" if ha else "btn-g"}" onclick="event.stopPropagation();location.href='/check-flights/{t["tc"]}'">✈ Flüge</button>
                      {"<button class='btn-g' onclick='event.stopPropagation();location.href=\"/check-trains/"+t["tc"]+"\"'>🚆 Bahn</button>" if t["trains"] else ""}
                      <button class="btn-s" onclick="event.stopPropagation();location.href='/trip/{t["tc"]}'">Detail</button>
                    </div>
                  </div>
                </div>"""
            return f'<div class="cards">{cards}</div>'

        def planned_cards(trips):
            if not trips: return '<div class="empty">Keine geplanten Reisen. Über &ldquo;+ Neu&rdquo; anlegen.</div>'
            cards=""
            for t in trips:
                dep=str(t["dep"])[:10] if t["dep"] else None
                ret=str(t["ret"])[:10] if t["ret"] else None
                # Kopfzeile: was wir wissen
                dest_show = t["destinations"] or t["trip_title"] or t["cc"] or '<span style="color:var(--am6);font-size:11px">⚠ Ziel fehlt</span>'
                date_line = f"{dep} – {ret}" if dep and ret else (f"Ab {dep}" if dep else '<span style="color:var(--am6);font-size:11px">⚠ Datum fehlt</span>')
                fn_line=f'<div style="font-family:DM Mono,monospace;font-size:11px;color:var(--b600);margin-top:3px">✈ {t["fns"]}</div>' if t["fns"] else ""
                pnr_line=f'<div style="font-family:DM Mono,monospace;font-size:11px;color:var(--gr6)">PNR: {t["pnr"]}</div>' if t["pnr"] else ""
                cards+=f"""<div class="card" onclick="location.href='/trip/{t["tc"]}'">
                  <div class="c-top">{_code_header(t)}
                    <div class="c-info">
                      <div class="c-traveler">{dest_show}</div>
                      <div class="c-dest">{date_line}</div>
                      {fn_line}{pnr_line}
                    </div>
                    <div class="sbadge sb-planned">Geplant</div>
                  </div>
                  {_progress(t)}
                  <div class="c-div"></div>
                  <div class="c-meta">{_pills(t)}{_hotel_badge(t)}</div>
                  <div class="c-foot">
                    <div><div class="c-amt">–</div><div class="c-amt-sub">Noch nicht aktiv</div></div>
                    <div class="c-acts">
                      <a class="btn-g" href="/edit-trip/{t["tc"]}">✏ Bearbeiten</a>
                      <a class="btn-s" href="/trip/{t["tc"]}">Detail</a>
                    </div>
                  </div>
                </div>"""
            return f'<div class="cards">{cards}</div>'

        def done_cards(trips):
            if not trips: return '<div class="empty">Keine abgeschlossenen Reisen.</div>'
            cards=""
            for t in trips:
                dep_d=t["dep"] if isinstance(t["dep"],date) else (date.fromisoformat(str(t["dep"])) if t["dep"] else None)
                ret_d=t["ret"] if isinstance(t["ret"],date) else (date.fromisoformat(str(t["ret"])) if t["ret"] else None)
                days=(ret_d-dep_d).days+1 if dep_d and ret_d else 0
                ml=[m.strip() for m in t["meals"].split(",") if m.strip()]
                vma=0.0
                if days>0:
                    if days==1: vma=get_vma(t["cc"],"partial",ml)
                    else:
                        vma+=get_vma(t["cc"],"partial",[])
                        vma+=get_vma(t["cc"],"full",[])*max(0,days-2)
                        vma+=get_vma(t["cc"],"partial",ml)
                trenn,_ = trennungspauschale(dep_d,ret_d,t["dep_t"],t["ret_t"])
                dep_s=str(dep_d)[:10] if dep_d else "–"
                gesamt=t["sum_eur"]+vma+trenn
                trenn_html=f'<span class="trenn-tag">Trenn. {trenn:.0f} €</span>' if trenn>0 else ""
                cards+=f"""<div class="card" onclick="location.href='/report/{t["tc"]}'">
                  <div class="c-top">{_code_header(t)}
                    <div class="c-info"><div class="c-traveler">{t["traveler"] or "–"}</div>
                      <div class="c-dest">{dep_s} · {days} Tage · {t["destinations"] or t["cc"]}</div></div>
                    <div class="sbadge sb-done">Abgerechnet</div>
                  </div>
                  <div class="c-div"></div>
                  <div class="vma-row">
                    <span class="vma-tag">VMA §9 EStG</span>
                    <span class="vma-detail">{vma:.2f} €</span>
                    {trenn_html}
                    <span style="margin-left:auto;font-size:11px;color:var(--t300)">{dep_s}</span>
                  </div>
                  <div class="c-foot">
                    <div>
                      <div class="c-amt">{gesamt:,.2f} €</div>
                      <div class="c-amt-sub">
                        {f'✈ {t["boarding_count"]}/{t["flight_count"]} Bordkarten · ' if t["flight_count"]>0 else ""}Belege + VMA + Trenn.
                      </div>
                    </div>
                    <div class="c-acts"><a class="btn-g" href="/report/{t["tc"]}">Abrechnung</a></div>
                  </div>
                </div>"""
            return f'<div class="cards">{cards}</div>'

        # Nur relevante Sektion zeigen je nach Tab
        if focus == "active":
            sections = f"""
        <div class="sb">
          <div class="sec-hdr"><div class="sec-dot active"></div><span class="sec-title">Laufende Reisen</span><span class="sec-cnt">{len(active_t)} aktiv</span></div>
          {active_cards(active_t)}
        </div>"""
        elif focus == "planned":
            sections = f"""
        <div class="sb">
          <div class="sec-hdr"><div class="sec-dot planned"></div><span class="sec-title">Vorplanung</span><span class="sec-cnt">{len(planned_t)} geplant</span></div>
          {planned_cards(planned_t)}
        </div>"""
        elif focus == "done":
            sections = f"""
        <div class="sb">
          <div class="sec-hdr"><div class="sec-dot done"></div><span class="sec-title">Abgeschlossen</span><span class="sec-cnt">{len(done_t)} Reisen</span></div>
          {done_cards(done_t)}
        </div>"""
        else:
            sections = f"""
        <div class="sb" id="active">
          <div class="sec-hdr"><div class="sec-dot active"></div><span class="sec-title">Laufende Reisen</span><span class="sec-cnt">{len(active_t)} aktiv</span></div>
          {active_cards(active_t)}
        </div>
        <div class="sb" id="planned">
          <div class="sec-hdr"><div class="sec-dot planned"></div><span class="sec-title">Vorplanung</span><span class="sec-cnt">{len(planned_t)} geplant</span></div>
          {planned_cards(planned_t)}
        </div>
        <div class="sb" id="done">
          <div class="sec-hdr"><div class="sec-dot done"></div><span class="sec-title">Abgeschlossen</span><span class="sec-cnt">{len(done_t)} Reisen</span></div>
          {done_cards(done_t)}
        </div>"""

        content=summary+sections+f"""
        <div class="sb">
          <div class="acts">
            <a class="btn" href="/fetch-mails">📥 Mails jetzt abrufen</a>
            <a class="btn" href="/analyze-attachments">🔍 KI-Analyse starten</a>
            <a class="btn-l" href="/attachment-log">Anhang-Log</a>
            <a class="btn-l" href="/mail-log">Mail-Log</a>
            <a class="btn-l" href="/rules">⚙ Kategorie-Regeln</a>
            <a class="btn-l" href="/reset-all" style="color:var(--re6)">Reset</a>
            <a class="btn-l" href="/init" style="color:var(--t300)">DB Init</a>
          </div>
        </div>"""

        return page_shell("Dashboard",content,active_tab=focus)
    except Exception as e:
        return HTMLResponse(page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Fehler</h2><p>{e}</p><a class="btn" href="/init">DB init</a></div>'),status_code=500)


# =========================================================
# NEUE REISE / EDIT
# =========================================================

@app.post("/new-trip")
async def new_trip(request: Request):
    try:
        form=await request.form()
        conn=get_conn();cur=conn.cursor()
        tc=next_trip_code(cur)
        cur.execute("""INSERT INTO trip_meta
            (trip_code,traveler_name,colleagues,departure_date,return_date,
             departure_time_home,arrival_time_home,destinations,
             nights_planned,notes,trip_title,customer_code,employee_code)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (trip_code) DO NOTHING""",
            (tc,form.get("traveler_name") or None,form.get("colleagues") or None,
             form.get("departure_date") or None,form.get("return_date") or None,
             form.get("departure_time_home") or "08:00",
             form.get("arrival_time_home") or "18:00",
             form.get("destinations") or None,
             int(form.get("nights_planned") or 0),
             form.get("notes") or None,
             form.get("trip_title") or None,
             form.get("customer_code") or None,
             form.get("employee_code") or None))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)

@app.get("/edit-trip/{tc}", response_class=HTMLResponse)
def edit_trip_form(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT traveler_name,colleagues,departure_date,return_date,
            departure_time_home,arrival_time_home,destinations,country_code,
            flight_numbers,train_numbers,nights_planned,nights_booked,
            car_rental_info,meals_reimbursed,notes,hotel_mode,pnr_code,
            trip_title,customer_code,vma_destinations,employee_code
            FROM trip_meta WHERE trip_code=%s""",(tc,))
        row=cur.fetchone();cur.close();conn.close()
        if not row: return HTMLResponse("Nicht gefunden",404)
        (traveler,colleagues,dep,ret,dep_t,ret_t,destinations,cc,
         fns,trains,nights_p,nights_b,car,meals,notes,hm,pnr,
         trip_title,customer_code,vma_dest_str,employee_code)=row
        dep_v=str(dep) if dep else ""; ret_v=str(ret) if ret else ""
        cc_opts="".join(f'<option value="{c}" {"selected" if cc==c else ""}>{c} – {l}</option>' for c,l in [
            ("DE","Deutschland"),("AZ","Aserbaidschan"),("AE","VAE/Dubai"),("FR","Frankreich"),
            ("GB","Großbritannien"),("US","USA"),("IN","Indien"),("CH","Schweiz"),
            ("AT","Österreich"),("IT","Italien"),("TR","Türkei"),("JP","Japan"),("SG","Singapur")])
        meal_chks="".join(f'<label style="margin-right:12px"><input type="checkbox" name="meals_reimbursed" value="{m}" {"checked" if m in (meals or "") else ""}> {m}</label>' for m in ["breakfast","lunch","dinner"])
        hm_opts="".join(f'<option value="{v}" {"selected" if hm==v else ""}>{l}</option>' for v,l in [("","– offen –"),("customer","Kunde stellt Hotel"),("own","Eigenes Hotel")])
        return page_shell(f"Bearbeiten {tc}",f"""
        <div class="page-card" style="max-width:760px">
          <h2>Reise {tc} bearbeiten</h2>
          <form method="post" action="/edit-trip/{tc}">
            <div class="fgrid">
              <div class="fgrp"><label class="flbl">Mitarbeiterkürzel *</label><input class="finp" name="employee_code" value="{employee_code or ''}" placeholder="z.B. MH"></div>
              <div class="fgrp"><label class="flbl">Reisetitel / Ziel *</label><input class="finp" name="trip_title" value="{trip_title or ''}" placeholder="z.B. Lyon, Messe München"></div>
              <div class="fgrp"><label class="flbl">Kundenkürzel</label><input class="finp" name="customer_code" value="{customer_code or ''}" placeholder="z.B. BMW, KD, intern"></div>
              <div class="fgrp"><label class="flbl">Reisender (Klarname)</label><input class="finp" name="traveler_name" value="{traveler or ''}"></div>
              <div class="fgrp ff"><label class="flbl">Kollegen</label><input class="finp" name="colleagues" value="{colleagues or ''}"></div>
              <div class="fgrp"><label class="flbl">Abreise (Datum)</label><input class="finp" type="date" name="departure_date" value="{dep_v}"></div>
              <div class="fgrp"><label class="flbl">Uhrzeit Abreise von Hause</label><input class="finp" type="time" name="departure_time_home" value="{dep_t or '08:00'}"></div>
              <div class="fgrp"><label class="flbl">Rückkehr (Datum)</label><input class="finp" type="date" name="return_date" value="{ret_v}"></div>
              <div class="fgrp"><label class="flbl">Uhrzeit Ankunft zu Hause</label><input class="finp" type="time" name="arrival_time_home" value="{ret_t or '18:00'}"></div>
              <div class="fgrp ff">
                <label class="flbl">Reiseziel(e) / Länder (Freitext)</label>
                <input class="finp" name="destinations" value="{destinations or ''}" placeholder="z.B. Baku (AZ) → Dubai (AE) → Los Angeles (US/CA)">
                <div class="hint">Mehrere Länder und Zeitzonen möglich</div>
              </div>
              <div class="fgrp"><label class="flbl">Hauptland für VMA (ISO)</label><select class="fsel" name="country_code">{cc_opts}</select></div>
              <div class="fgrp"><label class="flbl">Geplante Nächte gesamt</label><input class="finp" type="number" name="nights_planned" value="{nights_p or 0}"></div>
              <div class="fgrp"><label class="flbl">Davon gebucht (h: 4/6)</label><input class="finp" type="number" name="nights_booked" value="{nights_b or 0}"></div>
              <div class="fgrp ff"><label class="flbl">Flugnummern (kommagetrennt)</label><input class="finp" name="flight_numbers" value="{fns or ''}"></div>
              <div class="fgrp ff"><label class="flbl">Zugnummern (kommagetrennt)</label><input class="finp" name="train_numbers" value="{trains or ''}"></div>
              <div class="fgrp ff"><label class="flbl">PNR / AMADEUS-Code</label><input class="finp" name="pnr_code" value="{pnr or ''}" placeholder="z.B. XY3K7M"></div>
              <div class="fgrp ff"><label class="flbl">Mietwagen-Info</label><input class="finp" name="car_rental_info" value="{car or ''}"></div>
              <div class="fgrp ff"><label class="flbl">Hotel-Status</label><select class="fsel" name="hotel_mode">{hm_opts}</select></div>
              <div class="fgrp ff"><label class="flbl">Erstattete Mahlzeiten</label><div style="padding:6px 0">{meal_chks}</div></div>
              <div class="fgrp ff"><label class="flbl">Notizen</label><input class="finp" name="notes" value="{notes or ''}"></div>
              <div class="fgrp ff">
                <label class="flbl">🌍 Multidestination VMA (optional)</label>
                <input class="finp" name="vma_destinations" value="{vma_dest_str or ''}"
                  placeholder="2026-03-10:IN,2026-03-14:AE,2026-03-17:DE">
                <div class="hint" style="font-size:11px;color:var(--t300);margin-top:3px">Format: YYYY-MM-DD:ISO, … – erster Tag im jeweiligen Land. Leer = Hauptland für alle Tage.</div>
              </div>
            </div>
            <div class="mfooter"><a class="btn-mc" href="/">Abbrechen</a><button type="submit" class="btn-mp">Speichern</button></div>
          </form>
        </div>""")
    except Exception as e:
        return HTMLResponse(page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>'))

@app.post("/edit-trip/{tc}")
async def edit_trip_save(tc: str, request: Request):
    try:
        form=await request.form()
        meals=",".join(form.getlist("meals_reimbursed"))
        conn=get_conn();cur=conn.cursor()
        cur.execute("""UPDATE trip_meta SET
            traveler_name=%s,colleagues=%s,departure_date=%s,return_date=%s,
            departure_time_home=%s,arrival_time_home=%s,destinations=%s,country_code=%s,
            flight_numbers=%s,train_numbers=%s,pnr_code=%s,
            nights_planned=%s,nights_booked=%s,car_rental_info=%s,
            hotel_mode=%s,meals_reimbursed=%s,notes=%s,
            trip_title=%s,customer_code=%s,employee_code=%s,vma_destinations=%s WHERE trip_code=%s""",
            (form.get("traveler_name") or None,form.get("colleagues") or None,
             form.get("departure_date") or None,form.get("return_date") or None,
             form.get("departure_time_home") or "08:00",form.get("arrival_time_home") or "18:00",
             form.get("destinations") or None,form.get("country_code") or "DE",
             form.get("flight_numbers") or None,form.get("train_numbers") or None,
             form.get("pnr_code") or None,
             int(form.get("nights_planned") or 0),int(form.get("nights_booked") or 0),
             form.get("car_rental_info") or None,form.get("hotel_mode") or None,
             meals or None,form.get("notes") or None,
             form.get("trip_title") or None,form.get("customer_code") or None,
             form.get("employee_code") or None,
             form.get("vma_destinations") or None,
             tc))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)


# =========================================================
# KI-ANALYSE (Mail-Body + Anhaenge)
# =========================================================

@app.get("/analyze-attachments", response_class=HTMLResponse)
async def analyze_attachments():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code")
        known_codes=[r[0] for r in cur.fetchall()]

        # e) Auch Mail-Bodies analysieren
        cur.execute("""SELECT id,subject,body,trip_code FROM mail_messages
            WHERE (analysis_status IS NULL OR analysis_status='ausstehend')
            AND (body IS NOT NULL AND body != '') ORDER BY id LIMIT 50""")
        mail_rows=cur.fetchall()
        mail_processed=0
        for mid,subj,body,tc in mail_rows:
            full=f"{subj or ''}\n{body or ''}"
            fields=await mistral_extract(full,known_codes,"mail")
            if fields:
                pnr=fields.get("pnr_code","") or ""
                fns=fields.get("flight_numbers","") or ""
                trains=fields.get("train_numbers","") or ""
                rc=fields.get("reisecode","") or tc or ""
                traveler_ki=fields.get("traveler_name","") or ""
                dest_ki=fields.get("destination","") or ""
                if pnr and rc:
                    cur.execute("UPDATE trip_meta SET pnr_code=%s WHERE trip_code=%s AND (pnr_code IS NULL OR pnr_code='')",(pnr,rc))
                if fns and rc:
                    cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s AND (flight_numbers IS NULL OR flight_numbers='')",(fns,rc))
                if trains and rc:
                    cur.execute("UPDATE trip_meta SET train_numbers=%s WHERE trip_code=%s AND (train_numbers IS NULL OR train_numbers='')",(trains,rc))
                if traveler_ki and rc:
                    cur.execute("UPDATE trip_meta SET traveler_name=%s WHERE trip_code=%s AND (traveler_name IS NULL OR traveler_name='')",(traveler_ki,rc))
                if dest_ki and rc:
                    cur.execute("UPDATE trip_meta SET destinations=%s WHERE trip_code=%s AND (destinations IS NULL OR destinations='')",(dest_ki,rc))
            cur.execute("UPDATE mail_messages SET analysis_status='ok' WHERE id=%s",(mid,))
            mail_processed+=1
        conn.commit()

        # Anhänge analysieren – ausstehende + ICS ohne detected_date
        cur.execute("""SELECT id,storage_key,original_filename FROM mail_attachments
            WHERE analysis_status IN ('ausstehend','neu') OR analysis_status IS NULL
            OR (original_filename ILIKE '%.ics' AND (detected_date IS NULL OR detected_date = ''))
            ORDER BY id""")
        rows=cur.fetchall();cur.close()
        att_processed=0
        for row in rows:
            att_id,storage_key,filename=row
            if not storage_key or storage_key.startswith("S3-FEHLER"): continue
            try:
                await analyse_ki(att_id,storage_key,filename or "",conn,known_codes)
                att_processed+=1
            except Exception as e:
                cur2=conn.cursor()
                cur2.execute("UPDATE mail_attachments SET analysis_status=%s WHERE id=%s",(f"fehler:{str(e)[:80]}",att_id))
                conn.commit();cur2.close()
        conn.close()
        ki_info="Mistral OCR 3 + Small (EU)" if MISTRAL_API_KEY else "Kein Mistral Key"
        return page_shell("Analyse",f"""
        <div class="page-card">
          <h2 class="ok-t">✓ {mail_processed} Mails + {att_processed} Anhänge analysiert</h2>
          <p class="sub" style="margin-bottom:16px">{ki_info}</p>
          <div class="acts"><a class="btn" href="/">Dashboard</a><a class="btn-l" href="/attachment-log">Anhang-Log</a></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


# =========================================================
# MAIL-IMPORT (manuell aufrufbar)
# =========================================================

@app.get("/fetch-mails", response_class=HTMLResponse)
def fetch_mails():
    try:
        with _imap_lock:
            _fetch_mails_internal()
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT COUNT(*) FROM mail_messages")
        total_m=cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM mail_attachments")
        total_a=cur.fetchone()[0]
        cur.close();conn.close()
        return page_shell("Mails",f"""
        <div class="page-card">
          <h2 class="ok-t">✓ Mailabruf abgeschlossen</h2>
          <p style="margin-bottom:16px"><b>Gesamt im System:</b> {total_m} Mails · {total_a} Anhänge</p>
          <p class="sub" style="margin-bottom:16px">Neue Mails werden als gelesen markiert · Duplikate werden erkannt und übersprungen</p>
          <div class="acts"><a class="btn" href="/">Dashboard</a><a class="btn-l" href="/analyze-attachments">KI-Analyse starten</a><a class="btn-l" href="/attachment-log">Anhang-Log</a></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Fehler</h2><p>{e}</p></div>')


# =========================================================
# LOGS / DETAIL / ABRECHNUNG
# =========================================================

@app.get("/attachment-log", response_class=HTMLResponse)
def attachment_log():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT trip_code,original_filename,detected_type,detected_amount,detected_amount_eur,
            detected_currency,detected_date,detected_vendor,analysis_status,confidence,review_flag,ki_bemerkung,id,
            pnr_code,detected_flight_numbers,file_hash
            FROM mail_attachments ORDER BY id DESC LIMIT 200""")
        rows=cur.fetchall();cur.close();conn.close()
        def b(s,good="ok"):
            if s==good: return f'<span class="bdg bdg-ok">{s}</span>'
            if s in ("niedrig","pruefen","mittel"): return f'<span class="bdg bdg-w">{s}</span>'
            return f'<span class="bdg bdg-e">{s}</span>'
        html="".join(f"""<tr>
            <td class="cc">{r[0] or ''}</td>
            <td><a href="/beleg/{r[12]}" target="_blank" style="color:var(--b600);text-decoration:none;font-weight:500">📄 {r[1] or '–'}</a>
                <a href="/beleg-edit/{r[12]}" style="margin-left:6px;font-size:11px;color:var(--t300);text-decoration:none" title="Bearbeiten">✏</a></td>
            <td>{r[2] or ''}</td><td>{r[3] or ''}</td><td><b>{r[4] or ''}</b></td>
            <td>{r[5] or ''}</td><td>{r[6] or ''}</td><td>{r[7] or ''}</td>
            <td>{b(r[8] or '')}</td><td>{b(r[9] or '',"hoch")}</td><td>{b(r[10] or '')}</td>
            <td style="font-family:'DM Mono',monospace;font-size:11px;color:var(--gr6)">{r[13] or ''}</td>
            <td style="font-size:11px;color:var(--t300)">{r[11] or ''}</td>
            </tr>""" for r in rows)
        return page_shell("Anhang-Log",f"""
        <div class="page-card">
          <h2>Anhang-Log v{APP_VERSION}</h2>
          <div class="acts"><a class="btn-l" href="/">Zurück</a><a class="btn" href="/analyze-attachments">Erneut analysieren</a></div>
          <div style="overflow-x:auto"><table>
            <tr><th>Code</th><th>Datei</th><th>Typ</th><th>Betrag</th><th>EUR</th><th>Währung</th>
                <th>Datum</th><th>Anbieter</th><th>Status</th><th>Konfidenz</th><th>Review</th>
                <th>PNR</th><th>KI-Notiz</th></tr>
            {html}</table></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/mail-log", response_class=HTMLResponse)
def mail_log():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT id,sender,subject,trip_code,detected_type,pnr_code,
            analysis_status,created_at FROM mail_messages ORDER BY id DESC LIMIT 100""")
        rows=cur.fetchall();cur.close();conn.close()
        html="".join(f"""<tr>
            <td style="font-size:11px;color:var(--t300)">{str(r[7] or '')[:16]}</td>
            <td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{r[1] or ''}</td>
            <td><a href="/mail-detail/{r[0]}" style="color:var(--b600);text-decoration:none;font-weight:500">{r[2] or '–'}</a></td>
            <td class="cc">{r[3] or ''}</td><td>{r[4] or ''}</td>
            <td style="font-family:'DM Mono',monospace;color:var(--gr6)">{r[5] or ''}</td>
            <td><span class="bdg {"bdg-ok" if r[6]=="ok" else "bdg-w"}">{r[6] or 'ausstehend'}</span></td>
            </tr>""" for r in rows)
        return page_shell("Mail-Log",f"""
        <div class="page-card"><h2>Mail-Log ({len(rows)} Einträge)</h2>
          <div class="acts"><a class="btn-l" href="/">Zurück</a><a class="btn" href="/analyze-attachments">KI-Analyse</a></div>
          <div style="overflow-x:auto"><table>
            <tr><th>Zeit</th><th>Von</th><th>Betreff</th><th>Code</th><th>Typ</th><th>PNR</th><th>Status</th></tr>
            {html or '<tr><td colspan="7">Keine Mails</td></tr>'}
          </table></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


@app.get("/mail-detail/{mail_id}", response_class=HTMLResponse)
def mail_detail(mail_id: int):
    """Zeigt den vollständigen Mail-Body als Vorschau."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT sender,subject,body,trip_code,detected_type,
            pnr_code,analysis_status,created_at FROM mail_messages WHERE id=%s""",(mail_id,))
        row=cur.fetchone()
        # Anhänge dieser Mail
        cur.execute("""SELECT original_filename,detected_type,detected_amount_eur,
            analysis_status,id FROM mail_attachments WHERE mail_uid=(
                SELECT mail_uid FROM mail_messages WHERE id=%s)""",(mail_id,))
        att_rows=cur.fetchall()
        cur.close();conn.close()
        if not row: return HTMLResponse("Mail nicht gefunden",404)
        sender,subject,body,tc,dtype,pnr,astatus,created=row
        # Body sicher escapen für Vorschau
        import html as htmllib
        body_safe=htmllib.escape(body or "").replace("\n","<br>")
        att_html="".join(f"""<div style="display:flex;align-items:center;gap:8px;padding:6px 0;border-bottom:1px solid var(--bds)">
            <a href="/beleg/{a[4]}" target="_blank" style="color:var(--b600);text-decoration:none">📄 {a[0]}</a>
            <span class="bdg bdg-w" style="font-size:10px">{a[1] or ''}</span>
            {"<span style='font-family:DM Mono,monospace;font-size:12px'>"+a[2]+" €</span>" if a[2] else ""}
        </div>""" for a in att_rows) if att_rows else "<p class='sub'>Keine Anhänge</p>"
        return page_shell(f"Mail: {subject[:40]}",f"""
        <div class="page-card" style="max-width:800px">
          <div style="display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:16px">
            <div>
              <h2 style="margin-bottom:4px">{subject or '(kein Betreff)'}</h2>
              <p style="font-size:12px;color:var(--t500)">Von: {sender or '–'} · {str(created or '')[:16]}</p>
            </div>
            {"<span class='sbadge sb-active' style='font-family:DM Mono,monospace'>"+tc+"</span>" if tc else ""}
          </div>
          {"<div style='margin-bottom:12px'><span class='bdg bdg-ok' style='font-family:DM Mono,monospace'>PNR: "+pnr+"</span></div>" if pnr else ""}
          <div style="background:var(--page);border:1px solid var(--bd);border-radius:var(--rs);padding:16px;font-size:12px;line-height:1.7;max-height:400px;overflow-y:auto;margin-bottom:16px">
            {body_safe or '<span style="color:var(--t300)">Kein Body</span>'}
          </div>
          {"<h3 style='margin-bottom:8px;color:var(--t700)'>Anhänge</h3>"+att_html if att_rows else ""}
          <div class="acts" style="margin-top:16px">
            <a class="btn-l" href="/mail-log">← Zurück</a>
            {"<a class='btn' href='/trip/"+tc+"'>Reise "+tc+"</a>" if tc else ""}
          </div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/trip/{tc}", response_class=HTMLResponse)
def trip_detail(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT traveler_name,colleagues,departure_date,return_date,
            departure_time_home,arrival_time_home,destinations,country_code,
            flight_numbers,train_numbers,car_rental_info,nights_planned,nights_booked,
            meals_reimbursed,pnr_code,notes,hotel_mode,trip_title,customer_code,
            employee_code,vma_destinations
            FROM trip_meta WHERE trip_code=%s""",(tc,))
        meta=cur.fetchone()

        cur.execute("""SELECT original_filename,detected_type,detected_amount_eur,detected_currency,
            detected_date,detected_vendor,analysis_status,confidence,ki_bemerkung,id,
            pnr_code,detected_flight_numbers,detected_train_numbers,detected_amount
            FROM mail_attachments WHERE trip_code=%s ORDER BY detected_date,id""",(tc,))
        atts=cur.fetchall()

        cur.execute("""SELECT flight_number,flight_date,alert_type,message,source,checked_at
            FROM flight_alerts WHERE trip_code=%s ORDER BY checked_at DESC LIMIT 10""",(tc,))
        alerts=cur.fetchall()
        cur.close();conn.close()

        if meta:
            (traveler,colleagues,dep,ret,dep_t,ret_t,destinations,cc,
             fns,trains,car,nights_p,nights_b,meals,pnr,notes,hm,
             trip_title,customer_code,employee_code,vma_dest_str)=meta
            dep_d=dep if isinstance(dep,date) else (date.fromisoformat(str(dep)) if dep else None)
            ret_d=ret if isinstance(ret,date) else (date.fromisoformat(str(ret)) if ret else None)
            days=(ret_d-dep_d).days+1 if dep_d and ret_d else 0
            status=compute_status(dep_d,ret_d)
        else:
            traveler=colleagues=destinations=cc=fns=trains=car=""
            nights_p=nights_b=days=0; pnr=notes=hm=trip_title=customer_code=employee_code=vma_dest_str=""
            dep_d=ret_d=dep_t=ret_t=None; status="planned"

        status_badge={"active":"<span class='sbadge sb-active'><span class='adot'></span>Aktiv</span>",
                      "planned":"<span class='sbadge sb-planned'>Geplant</span>",
                      "done":"<span class='sbadge sb-done'>Abgeschlossen</span>"}.get(status,"")

        # Reise-Typ automatisch ermitteln
        if customer_code and customer_code.upper() not in ("INTERN","INT",""):
            trip_type="👤 Kundenprojekt"
            trip_type_detail=customer_code
        elif trip_title and any(w in trip_title.lower() for w in ["messe","expo","forum","congress","kongress","konferenz"]):
            trip_type="🎪 Messe / Veranstaltung"
            trip_type_detail=trip_title
        elif destinations:
            trip_type="✈ Dienstreise"
            trip_type_detail=destinations
        else:
            trip_type="📋 Reise"
            trip_type_detail=trip_title or "–"

        # Header-Titel
        title_parts=list(filter(None,[trip_title, customer_code]))
        header_title=f"{tc}"
        if title_parts: header_title+=f" · {' · '.join(title_parts)}"

        # ── CHRONOLOGISCHE TIMELINE ──────────────────────────────
        # Ereignisse sammeln: Abreise, Belege (nach Datum), Rückkehr
        timeline_events=[]

        # Abreise
        if dep_d:
            timeline_events.append({
                "date": dep_d,
                "time": dep_t or "08:00",
                "icon": "🏠→✈",
                "label": "Abreise von zu Hause",
                "detail": f"{dep_t or ''} · {fns.split(',')[0].strip() if fns else '–'}",
                "type": "journey"
            })

        # Flugnummern als Ereignisse
        fn_list=[f.strip() for f in (fns or "").split(",") if f.strip()]
        for fn in fn_list:
            timeline_events.append({
                "date": dep_d or date.today(),
                "time": "–",
                "icon": "✈",
                "label": f"Flug {fn}",
                "detail": pnr or "",
                "type": "flight"
            })

        # Zugnummern
        tn_list=[t.strip() for t in (trains or "").split(",") if t.strip()]
        for tn in tn_list:
            timeline_events.append({
                "date": dep_d or date.today(),
                "time": "–",
                "icon": "🚆",
                "label": f"Zug {tn}",
                "detail": "",
                "type": "train"
            })

        # Belege als Timeline-Einträge
        beleg_sum=0.0
        for a in atts:
            fn_a,dtype,amt_eur,curr,ddate,vendor,stat,conf,bemerk,att_id,apnr,afns,atrains,amt=a
            if amt_eur:
                try: beleg_sum+=float(amt_eur.replace(".","").replace(",","."))
                except: pass
            # Datum parsen
            ev_date=dep_d
            if ddate:
                try:
                    if "." in str(ddate):
                        parts=str(ddate).split(".")
                        if len(parts)==3:
                            ev_date=date(int(parts[2]),int(parts[1]),int(parts[0]))
                    else:
                        ev_date=date.fromisoformat(str(ddate)[:10])
                except: pass
            type_icons={"Flug":"✈","Hotel":"🏨","Taxi":"🚕","Bahn":"🚆","Mietwagen":"🚗",
                       "Essen":"🍽","Kalendereintrag":"📅","Sonstiges":"📄"}
            icon=type_icons.get(dtype,"📄")
            amount_str=f"{amt_eur} €" if amt_eur else (f"{amt} {curr}" if amt else "–")
            edit_url=f"/beleg-edit/{att_id}"
            view_url=f"/beleg/{att_id}"
            timeline_events.append({
                "date": ev_date or dep_d or date.today(),
                "time": "",
                "icon": icon,
                "label": f"{vendor or fn_a or dtype or 'Beleg'}",
                "detail": amount_str,
                "extra": f'<a href="{view_url}" target="_blank" style="color:var(--b600);margin-right:6px">📄</a><a href="{edit_url}" style="color:var(--t300)">✏</a>',
                "status": stat,
                "type": "beleg"
            })

        # Verpflegung pro Tag direkt in Timeline einbauen
        if dep_d and ret_d:
            daily = load_daily_meals(tc)
            days_range = (ret_d - dep_d).days + 1
            for i in range(days_range):
                d = dep_d + timedelta(days=i)
                ml = daily.get(d, [])
                # Mahlzeiten-Zeile immer zeigen (auch wenn leer → zum Anklicken)
                b_chk = "✅" if "breakfast" in ml else "☐"
                l_chk = "✅" if "lunch" in ml else "☐"
                d_chk = "✅" if "dinner" in ml else "☐"
                dtype = "partial" if i == 0 or i == days_range - 1 else "full"
                vma_day = get_vma(cc or "DE", dtype, ml)
                timeline_events.append({
                    "date": d,
                    "time": "",
                    "icon": "🍽",
                    "label": f"Verpflegung",
                    "detail": f"{b_chk} Frühstück &nbsp; {l_chk} Mittag &nbsp; {d_chk} Abend",
                    "extra": f'<span style="font-family:DM Mono,monospace;color:var(--b600);font-size:11px">{vma_day:.2f} € VMA</span> <a href="/meals/{tc}" style="margin-left:8px;font-size:11px;color:var(--t300)">✏</a>',
                    "type": "meal"
                })

        # Rückkehr
        if ret_d:
            timeline_events.append({
                "date": ret_d,
                "time": ret_t or "18:00",
                "icon": "✈→🏠",
                "label": "Rückkehr zu Hause",
                "detail": ret_t or "",
                "type": "journey"
            })

        # Chronologisch sortieren – Mahlzeiten nach Belegen, Journeys an Rand
        timeline_events.sort(key=lambda e: (
            e["date"] or date.today(),
            0 if e.get("type") == "journey" and e.get("icon","").startswith("🏠") else
            (99 if e.get("type") == "journey" else
             (50 if e.get("type") == "meal" else 10)),
            e.get("time","")
        ))

        # Timeline HTML
        tl_rows=""
        prev_date=None
        for ev in timeline_events:
            ev_date=ev["date"]
            # Datums-Trennzeile
            if ev_date != prev_date:
                wd=["Mo","Di","Mi","Do","Fr","Sa","So"][ev_date.weekday()] if ev_date else ""
                wkend=' style="color:var(--b600);font-weight:600"' if ev_date and ev_date.weekday()>=5 else ""
                tl_rows+=f'<tr><td colspan="5" style="background:var(--page);padding:8px 10px 4px;font-size:11px;color:var(--t300);border-bottom:1px solid var(--bds)"><span{wkend}>{str(ev_date)} {wd}</span></td></tr>'
                prev_date=ev_date

            type_colors={"journey":"var(--b600)","flight":"var(--b500)","train":"var(--gr6)","beleg":"var(--t700)"}
            col=type_colors.get(ev.get("type","beleg"),"var(--t700)")
            stat=ev.get("status","")
            stat_html=""
            if stat:
                sc="bdg-ok" if stat in ("ok","ok (manuell)") else "bdg-w"
                stat_html=f'<span class="bdg {sc}" style="font-size:10px">{stat}</span>'
            tl_rows+=f"""<tr>
                <td style="width:60px;color:var(--t300);font-size:11px;white-space:nowrap">{ev.get('time','')}</td>
                <td style="width:28px;text-align:center;font-size:16px">{ev['icon']}</td>
                <td style="font-weight:500;color:{col}">{ev['label']}</td>
                <td style="font-family:DM Mono,monospace;font-size:12px;color:var(--t500)">{ev.get('detail','')}</td>
                <td style="white-space:nowrap">{stat_html} {ev.get('extra','')}</td>
            </tr>"""

        # Header-Info-Leiste
        info_items=[]
        # Klarname prominent – mit Kürzel
        emp = employee_code or ""
        name_line = " · ".join(filter(None,[emp, traveler]))
        if name_line: info_items.append(f"<b>Reisender:</b> {name_line}{' · '+colleagues if colleagues else ''}")
        else: info_items.append('<span style="color:var(--am6)">⚠ Kein Reisender/Kürzel – bitte ergänzen</span>')
        if dep_d: info_items.append(f"<b>Zeitraum:</b> {str(dep_d)} {dep_t or ''} – {str(ret_d or '?')} {ret_t or ''} ({days} Tage)")
        if pnr: info_items.append(f"<b>PNR:</b> <span style='font-family:DM Mono,monospace;color:var(--gr6)'>{pnr}</span>")
        if hm: info_items.append(f"<b>Hotel:</b> {'Kunde stellt' if hm=='customer' else 'Eigenes Hotel'} · {nights_b or 0}/{nights_p or 0} Nächte")
        if notes: info_items.append(f"<b>Notiz:</b> {notes}")
        info_bar="<br>".join(info_items) if info_items else "<span class='sub'>Keine Metadaten – bitte bearbeiten</span>"

        # Warnung wenn Name/Ziel fehlt
        missing=[]
        if not employee_code: missing.append("Mitarbeiterkürzel fehlt")
        if not traveler: missing.append("Klarname fehlt")
        if not destinations and not trip_title: missing.append("Ziel/Titel fehlt")
        missing_html=f'<div class="alert-bar" style="margin-bottom:12px">⚠ {" · ".join(missing)} – <a href="/edit-trip/{tc}" style="color:var(--re6)">jetzt ergänzen</a></div>' if missing else ""

        # Flight-Alert-Zeilen
        alert_rows="".join(f"""<tr>
            <td class="cc">{a[0]}</td><td>{a[1]}</td>
            <td><span class="bdg {"bdg-e" if a[2]=="cancelled" else "bdg-w"}">{a[2]}</span></td>
            <td>{a[3]}</td><td style="font-size:11px;color:var(--t300)">{str(a[5])[:16]}</td>
            </tr>""" for a in alerts)

        return page_shell(f"Detail {tc}",f"""
        <div class="page-card">
          <!-- Header -->
          <div style="display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:12px">
            <div>
              <div style="display:flex;align-items:center;gap:10px;flex-wrap:wrap">
                <h2 style="margin:0">{header_title}</h2>
                {status_badge}
                <span style="font-size:12px;color:var(--t300)">{trip_type} · {trip_type_detail}</span>
              </div>
              <div style="font-size:12px;color:var(--t500);margin-top:6px;line-height:1.8">{info_bar}</div>
            </div>
          </div>
          {missing_html}

          <!-- Aktions-Buttons -->
          <div class="acts" style="margin-bottom:16px">
            <a class="btn" href="/report/{tc}">📊 Abrechnung</a>
            <a class="btn" href="/report-pdf/{tc}" target="_blank">📄 PDF</a>
            <a class="btn-l" href="/meals/{tc}">🍽 Mahlzeiten</a>
            <a class="btn-l" href="/check-flights/{tc}">✈ Flüge</a>
            {"<a class='btn-l' href='/check-trains/"+tc+"'>🚆 Züge</a>" if trains else ""}
            <a class="btn-l" href="/edit-trip/{tc}">✏ Bearbeiten</a>
            <a class="btn-l" href="/">Zurück</a>
          </div>

          <!-- Chronologische Timeline -->
          <h3 style="margin-bottom:8px;color:var(--t700)">📅 Reise-Timeline</h3>
          <div style="overflow-x:auto"><table style="table-layout:fixed;width:100%">
            <colgroup><col style="width:60px"><col style="width:32px"><col style="width:35%"><col style="width:25%"><col style="width:auto"></colgroup>
            {tl_rows or '<tr><td colspan="5" class="sub" style="padding:16px">Keine Ereignisse – Reisedaten und Mails zuordnen</td></tr>'}
          </table></div>

          <!-- Summe -->
          <div style="margin-top:12px;text-align:right;font-family:DM Mono,monospace;font-size:14px;font-weight:500;color:var(--b600)">
            Belege gesamt: {beleg_sum:.2f} €
          </div>

          <!-- Flight Alerts -->
          {f'''<h3 style="margin:20px 0 8px;color:var(--t700)">⚠ Flight-Alerts</h3>
          <div style="overflow-x:auto"><table>
            <tr><th>Flug</th><th>Datum</th><th>Typ</th><th>Meldung</th><th>Zeitpunkt</th></tr>
            {alert_rows}</table></div>''' if alerts else ""}
        </div>""")
    except Exception as e:
        import traceback
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Fehler in Trip-Detail</h2><pre style="font-size:11px;overflow-x:auto">{traceback.format_exc()}</pre></div>')
        # Anhänge
        cur.execute("""SELECT original_filename,detected_type,detected_amount_eur,detected_currency,
            detected_date,detected_vendor,analysis_status,confidence,ki_bemerkung,id,
            pnr_code,detected_flight_numbers,detected_train_numbers,detected_amount
            FROM mail_attachments WHERE trip_code=%s ORDER BY detected_date,id""",(tc,))
        atts=cur.fetchall()
        # Letzte Flight-Alerts
        cur.execute("""SELECT flight_number,flight_date,alert_type,message,source,checked_at
            FROM flight_alerts WHERE trip_code=%s ORDER BY checked_at DESC LIMIT 10""",(tc,))
        alerts=cur.fetchall()
        cur.close();conn.close()

        # Meta-Bereich
        if meta:
            (traveler,colleagues,dep,ret,dep_t,ret_t,destinations,cc,
             fns,trains,car,nights_p,nights_b,meals,pnr,notes,hm,
             trip_title,customer_code)=meta
            dep_d=dep if isinstance(dep,date) else (date.fromisoformat(str(dep)) if dep else None)
            ret_d=ret if isinstance(ret,date) else (date.fromisoformat(str(ret)) if ret else None)
            days=(ret_d-dep_d).days+1 if dep_d and ret_d else 0
            status=compute_status(dep_d,ret_d)
            status_badge={"active":"<span class='sbadge sb-active'><span class='adot'></span>Aktiv</span>",
                          "planned":"<span class='sbadge sb-planned'>Geplant</span>",
                          "done":"<span class='sbadge sb-done'>Abgeschlossen</span>"}.get(status,"")
            title_line=" · ".join(filter(None,[customer_code,trip_title]))
            meta_html=f"""
            <div style="display:grid;grid-template-columns:1fr 1fr;gap:8px 24px;margin-bottom:16px;font-size:13px">
              <div><span style="color:var(--t300);font-size:11px">Reisender</span><br><b>{traveler or '–'}</b>{f' · {colleagues}' if colleagues else ''}</div>
              <div><span style="color:var(--t300);font-size:11px">Zeitraum</span><br><b>{str(dep_d or '–')}</b> {dep_t or ''} – <b>{str(ret_d or '–')}</b> {ret_t or ''} · {days} Tage</div>
              <div><span style="color:var(--t300);font-size:11px">Reiseziel</span><br>{destinations or cc or '–'}</div>
              <div><span style="color:var(--t300);font-size:11px">Hotel</span><br>{'Kunde stellt' if hm=='customer' else 'Eigenes Hotel' if hm=='own' else '–'} · {nights_b or 0}/{nights_p or 0} Nächte</div>
              {"<div><span style='color:var(--t300);font-size:11px'>Flugnummern</span><br><span style='font-family:DM Mono,monospace'>"+fns+"</span></div>" if fns else ""}
              {"<div><span style='color:var(--t300);font-size:11px'>Zugnummern</span><br><span style='font-family:DM Mono,monospace'>"+trains+"</span></div>" if trains else ""}
              {"<div><span style='color:var(--t300);font-size:11px'>Mietwagen</span><br>"+car+"</div>" if car else ""}
              {"<div><span style='color:var(--t300);font-size:11px'>PNR</span><br><span style='font-family:DM Mono,monospace;color:var(--gr6)'>"+pnr+"</span></div>" if pnr else ""}
              {"<div><span style='color:var(--t300);font-size:11px'>Notiz</span><br>"+notes+"</div>" if notes else ""}
            </div>"""
        else:
            meta_html="<p class='sub'>Keine Metadaten gespeichert – <a href='/edit-trip/"+tc+"'>jetzt anlegen</a></p>"
            status_badge=""; title_line=""

        # Anhänge-Tabelle
        beleg_sum=0.0
        att_rows="".join(f"""<tr>
            <td><a href="/beleg/{a[9]}" target="_blank" style="color:var(--b600);text-decoration:none">📄 {a[0] or '–'}</a>
                <a href="/beleg-edit/{a[9]}" style="margin-left:5px;color:var(--t300);text-decoration:none" title="Korrigieren">✏</a></td>
            <td>{a[1] or ''}</td>
            <td style="font-family:'DM Mono',monospace"><b>{a[2] or ''}</b>{' '+a[3] if a[3] and a[3]!='EUR' else ' €'}</td>
            <td>{a[4] or ''}</td><td>{a[5] or ''}</td>
            <td><span class="bdg {"bdg-ok" if a[6] in ("ok","ok (manuell)") else "bdg-w"}">{a[6] or ''}</span></td>
            <td style="font-size:11px;color:var(--t300)">{(a[8] or '')[:60]}</td>
            </tr>""" + ("" if not a[2] or not (lambda x: beleg_sum.__add__(float(x.replace(".","").replace(",","."))) if x else 0)(a[2]) else "")
            for a in atts
            if not (a[2] and [beleg_sum := beleg_sum + float(a[2].replace(".","").replace(",",".")) for _ in [1]])
        )
        # beleg_sum sauber berechnen
        beleg_sum=0.0
        for a in atts:
            if a[2]:
                try: beleg_sum+=float(a[2].replace(".","").replace(",","."))
                except: pass

        att_html=f"""<div style="overflow-x:auto"><table>
            <tr><th>Datei</th><th>Typ</th><th>Betrag EUR</th><th>Datum</th><th>Anbieter</th><th>Status</th><th>KI-Notiz</th></tr>
            {"".join(f'''<tr>
            <td><a href="/beleg/{a[9]}" target="_blank" style="color:var(--b600);text-decoration:none">📄 {a[0] or "–"}</a>
                <a href="/beleg-edit/{a[9]}" style="margin-left:5px;color:var(--t300);text-decoration:none" title="Korrigieren">✏</a></td>
            <td>{a[1] or ""}</td>
            <td style="font-family:DM Mono,monospace"><b>{a[2] or ""}</b> {a[3] if a[3] and a[3]!="EUR" else "€"}</td>
            <td>{a[4] or ""}</td><td>{a[5] or ""}</td>
            <td><span class="bdg {"bdg-ok" if a[6] in ("ok","ok (manuell)") else "bdg-w"}">{a[6] or ""}</span></td>
            <td style="font-size:11px;color:var(--t300)">{(a[8] or "")[:60]}</td>
            </tr>''' for a in atts) or '<tr><td colspan="7">Keine Anhänge</td></tr>'}
            <tr style="background:var(--b50)"><td colspan="2"><b>Summe</b></td><td style="font-family:DM Mono,monospace"><b>{beleg_sum:.2f} €</b></td><td colspan="4"></td></tr>
        </table></div>"""

        # Flight-Alerts
        alerts_html=""
        if alerts:
            arows="".join(f"""<tr>
                <td class="cc">{a[0]}</td><td>{a[1]}</td>
                <td><span class="bdg {"bdg-e" if a[2]=="cancelled" else "bdg-w" if a[2]=="delay" else "bdg-ok"}">{a[2]}</span></td>
                <td>{a[3]}</td><td style="font-size:11px;color:var(--t300)">{a[4]}</td>
                <td style="font-size:11px;color:var(--t300)">{str(a[5])[:16]}</td>
                </tr>""" for a in alerts)
            alerts_html=f"""<h3 style="margin:20px 0 8px;color:var(--t700)">Flight-Alerts</h3>
            <div style="overflow-x:auto"><table>
              <tr><th>Flug</th><th>Datum</th><th>Typ</th><th>Meldung</th><th>Quelle</th><th>Zeitpunkt</th></tr>
              {arows}</table></div>"""

        title_display=f"{tc}" + (f" · {title_line}" if title_line else "")
        return page_shell(f"Detail {tc}",f"""
        <div class="page-card">
          <div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
            <h2 style="margin:0">{title_display}</h2>
            {status_badge}
          </div>
          {meta_html}
          <div class="acts" style="margin-bottom:16px">
            <a class="btn" href="/report/{tc}">📊 Abrechnung</a>
            <a class="btn" href="/report-pdf/{tc}" target="_blank">📄 PDF</a>
            <a class="btn-l" href="/check-flights/{tc}">✈ Flüge prüfen</a>
            {"<a class='btn-l' href='/check-trains/"+tc+"'>🚆 Züge prüfen</a>" if meta and trains else ""}
            <a class="btn-l" href="/meals/{tc}">🍽 Mahlzeiten</a>
            <a class="btn-l" href="/edit-trip/{tc}">✏ Bearbeiten</a>
            <a class="btn-l" href="/">Zurück</a>
          </div>
          <h3 style="margin-bottom:8px;color:var(--t700)">Anhänge ({len(atts)})</h3>
          {att_html}
          {alerts_html}
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/report/{tc}", response_class=HTMLResponse)
def report(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT traveler_name,departure_date,return_date,country_code,
            departure_time_home,arrival_time_home,destinations,meals_reimbursed,
            flight_numbers,train_numbers,colleagues,notes,pnr_code,nights_planned,nights_booked,
            vma_destinations
            FROM trip_meta WHERE trip_code=%s""",(tc,))
        meta=cur.fetchone()
        if not meta: return HTMLResponse("Nicht gefunden",404)
        (traveler,dep,ret,cc,dep_t,ret_t,destinations,meals_reimb,
         fns,trains,colleagues,notes,pnr,nights_p,nights_b,vma_dest_str)=meta

        cur.execute("""SELECT original_filename,detected_type,detected_amount,detected_amount_eur,
            detected_currency,detected_date,detected_vendor,analysis_status,detected_flight_numbers,id
            FROM mail_attachments WHERE trip_code=%s ORDER BY id""",(tc,))
        atts=cur.fetchall();cur.close();conn.close()

        dep_d=dep if isinstance(dep,date) else (date.fromisoformat(str(dep)) if dep else None)
        ret_d=ret if isinstance(ret,date) else (date.fromisoformat(str(ret)) if ret else None)
        days=(ret_d-dep_d).days+1 if dep_d and ret_d else 0
        ml=[m.strip() for m in (meals_reimb or "").split(",") if m.strip()]

        # VMA – tagesbasiert (daily_meals) wenn vorhanden, sonst Multidest., sonst Standard
        vma_dest = parse_vma_destinations(vma_dest_str or "")
        daily = load_daily_meals(tc)
        if days > 0:
            if daily:
                # Tagesgenaue Berechnung aus daily_meals Tabelle
                vma_total, vma_tag_rows = calc_vma_from_daily(dep_d, ret_d, daily, vma_dest, cc)
                vma_rows = "".join(f"<tr><td>{d}</td><td>{lbl}</td><td>{c_}</td><td>{m}</td><td>{v:.2f} €</td></tr>"
                                   for d,lbl,c_,m,v in vma_tag_rows)
                vma_source = f'<span style="font-size:11px;color:var(--gr6)">✓ Tagesgenaue Erfassung ({len(daily)} Tage gepflegt)</span>'
            else:
                # Fallback: alte Methode
                vma_total, vma_tag_rows = calc_vma_multi(dep_d, ret_d, ml, vma_dest, cc)
                vma_rows = "".join(f"<tr><td>{lbl}</td><td>{c_}</td><td>{m}</td><td>{v:.2f} €</td></tr>"
                                   for lbl,c_,m,v in vma_tag_rows)
                vma_source = f'<span style="font-size:11px;color:var(--am6)">⚠ Keine Tageserfassung – <a href="/meals/{tc}">jetzt pflegen</a></span>'
        else:
            vma_total = 0.0; vma_rows = ""; vma_source = ""

        # k) Trennungspauschale
        trenn_total,trenn_details=trennungspauschale(dep_d,ret_d,dep_t or "08:00",ret_t or "18:00")
        trenn_rows="".join(f"<tr><td>{d}</td><td>{lbl}</td><td>{amt:.2f} €</td></tr>" for d,lbl,amt in trenn_details)

        # Belege
        beleg_sum=0.0; beleg_rows=""
        all_fns_found=[]
        for a in atts:
            fn,dt,amt,amt_eur,curr,d,vendor,stat,det_fns,att_id=a
            if det_fns: all_fns_found.extend([f.strip() for f in det_fns.split(",") if f.strip()])
            if not amt_eur: continue
            try: beleg_sum+=float(amt_eur.replace(".","").replace(",","."))
            except: pass
            preview_link=f'<a href="/beleg/{att_id}" target="_blank" title="Vorschau" style="color:var(--b600);text-decoration:none">📄</a>'
            edit_link=f'<a href="/beleg-edit/{att_id}" title="Korrigieren" style="color:var(--t300);text-decoration:none;margin-left:6px">✏</a>'
            beleg_rows+=f"<tr><td>{preview_link}{edit_link} {fn}</td><td>{dt or '–'}</td><td>{vendor or '–'}</td><td>{d or '–'}</td><td>{amt or '–'} {curr or ''}</td><td><b>{amt_eur} €</b></td></tr>"

        # j) Fluganzahl
        all_fns_list = list(set(([f.strip() for f in (fns or "").split(",") if f.strip()] + all_fns_found)))
        flight_count = len(all_fns_list)

        # ── Fehlende Belege ermitteln ──────────────────────────────────────────
        belegte_typen = set(a[1] for a in atts if a[1])
        fehlende_belege = []

        # Flug: Bordkarte oder E-Ticket
        if all_fns_list and "Flug" not in belegte_typen and "Kalendereintrag" not in belegte_typen:
            fehlende_belege.append(f"✈ Flugbeleg / Bordkarte fehlt ({', '.join(all_fns_list)})")
        # Hotel: Rechnung
        hotel_beleg = "Hotel" in belegte_typen
        hotel_geplant = nights_p and nights_p > 0
        if hotel_geplant and not hotel_beleg:
            fehlende_belege.append(f"🏨 Hotel-Rechnung fehlt ({nights_b or nights_p} Nächte geplant)")
        # Mietwagen
        if trains and "Bahn" not in belegte_typen:
            fehlende_belege.append("🚆 Zugticket fehlt")

        fehlende_html = ""
        if fehlende_belege:
            items = "".join(f'<div style="padding:4px 0;border-bottom:1px solid rgba(220,38,38,.1)">'
                           f'<span style="color:var(--re6)">⚠</span> {b}</div>'
                           for b in fehlende_belege)
            fehlende_html = f"""
            <div style="margin-bottom:16px;padding:12px 16px;background:#fff5f5;border:1px solid rgba(220,38,38,.2);border-radius:var(--r)">
              <div style="font-weight:600;color:var(--re6);margin-bottom:6px">Fehlende Belege ({len(fehlende_belege)})</div>
              {items}
              <div style="font-size:11px;color:var(--t300);margin-top:6px">Bitte Belege hochladen oder per Mail einreichen vor Einreichung der Abrechnung.</div>
            </div>"""

        gesamt=beleg_sum+vma_total+trenn_total
        return page_shell(f"Abrechnung {tc}",f"""
        <div class="page-card" style="max-width:920px">
          <h2>Reisekostenabrechnung – {tc}</h2>
          <table style="width:auto;border:none;margin-bottom:16px">
            <tr style="border:none"><td style="border:none;padding:2px 12px 2px 0;font-weight:500">Reisender:</td><td style="border:none">{traveler or '–'}</td></tr>
            <tr style="border:none"><td style="border:none;padding:2px 12px 2px 0;font-weight:500">Zeitraum:</td><td style="border:none">{dep or '–'} {dep_t or ''} – {ret or '–'} {ret_t or ''} ({days} Tage)</td></tr>
            <tr style="border:none"><td style="border:none;padding:2px 12px 2px 0;font-weight:500">Reiseziel:</td><td style="border:none">{destinations or cc or '–'}</td></tr>
            <tr style="border:none"><td style="border:none;padding:2px 12px 2px 0;font-weight:500">Hotel:</td><td style="border:none">{nights_b or 0}/{nights_p or 0} Nächte gebucht</td></tr>
            {"<tr style='border:none'><td style='border:none;padding:2px 12px 2px 0;font-weight:500'>PNR:</td><td style='border:none;font-family:DM Mono,monospace;color:var(--gr6)'>"+pnr+"</td></tr>" if pnr else ""}
          </table>
          {fehlende_html}
          <h3 style="margin-bottom:10px;color:var(--t700)">Belege</h3>
          <table>
            <tr><th>Datei</th><th>Typ</th><th>Anbieter</th><th>Datum</th><th>Betrag orig.</th><th>Betrag EUR</th></tr>
            {beleg_rows or '<tr><td colspan="6">Keine analysierten Belege</td></tr>'}
            <tr><td colspan="5"><b>Summe Belege</b></td><td><b>{beleg_sum:.2f} €</b></td></tr>
          </table>
          <h3 style="margin:20px 0 6px;color:var(--t700)">Verpflegungsmehraufwand §9 EStG</h3>
          <div style="margin-bottom:8px">{vma_source}</div>
          <div style="margin-bottom:4px;text-align:right">
            <a class="btn-l" href="/meals/{tc}" style="font-size:11px">🍽 Mahlzeiten pflegen</a>
          </div>
          <table>
            <tr><th>Datum</th><th>Tag</th><th>Land</th><th>Erstattete Mahlzeiten</th><th>VMA</th></tr>
            {vma_rows or '<tr><td colspan="5">Keine Reisezeit erfasst</td></tr>'}
            <tr><td colspan="4"><b>Summe VMA</b></td><td><b>{vma_total:.2f} €</b></td></tr>
          </table>
          {"<h3 style='margin:20px 0 10px;color:var(--t700)'>Trennungspauschale (Herrhammer)</h3><table><tr><th>Datum</th><th>Grund</th><th>Betrag</th></tr>" + trenn_rows + f"<tr><td colspan='2'><b>Summe Trennungspauschale</b></td><td><b>{trenn_total:.2f} €</b></td></tr></table>" if trenn_total>0 else ""}
          <div style="margin-top:20px;padding:18px;background:var(--b50);border-radius:var(--r);border:1px solid var(--b100)">
            <div style="font-size:1.15rem;font-weight:600">Gesamtbetrag: {gesamt:,.2f} €</div>
            <div class="sub" style="margin-top:4px">Belege {beleg_sum:.2f} € + VMA {vma_total:.2f} € + Trennungspauschale {trenn_total:.2f} €</div>
          </div>
          <div style="margin-top:16px"><a class="btn-l" href="/">Zurück</a>
            <a class="btn" href="/report-pdf/{tc}" target="_blank" style="margin-left:8px">📄 PDF exportieren</a></div>
        </div>""",active_tab="done")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


# =========================================================
# PDF-EXPORT (druckfertiges HTML → Browser-PDF)
# =========================================================

@app.get("/report-pdf/{tc}", response_class=HTMLResponse)
def report_pdf(tc: str):
    """Druckfertiges HTML – Browser-Druckdialog öffnet sich automatisch."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT traveler_name,departure_date,return_date,country_code,
            departure_time_home,arrival_time_home,destinations,meals_reimbursed,
            flight_numbers,train_numbers,colleagues,notes,pnr_code,
            nights_planned,nights_booked,trip_title,customer_code,vma_destinations
            FROM trip_meta WHERE trip_code=%s""",(tc,))
        meta=cur.fetchone()
        if not meta: return HTMLResponse("Nicht gefunden",404)
        (traveler,dep,ret,cc,dep_t,ret_t,destinations,meals_reimb,
         fns,trains,colleagues,notes,pnr,nights_p,nights_b,
         trip_title,customer_code,vma_dest_str)=meta

        cur.execute("""SELECT original_filename,detected_type,detected_amount,
            detected_amount_eur,detected_currency,detected_date,detected_vendor,
            analysis_status,ki_bemerkung
            FROM mail_attachments WHERE trip_code=%s ORDER BY detected_date,id""",(tc,))
        atts=cur.fetchall();cur.close();conn.close()

        dep_d=dep if isinstance(dep,date) else (date.fromisoformat(str(dep)) if dep else None)
        ret_d=ret if isinstance(ret,date) else (date.fromisoformat(str(ret)) if ret else None)
        days=(ret_d-dep_d).days+1 if dep_d and ret_d else 0
        ml=[m.strip() for m in (meals_reimb or "").split(",") if m.strip()]

        # VMA – tagesbasiert (daily_meals) wenn vorhanden, sonst Multidest.
        vma_dest = parse_vma_destinations(vma_dest_str or "")
        daily = load_daily_meals(tc)
        if days > 0:
            if daily:
                vma_total, vma_tag_rows = calc_vma_from_daily(dep_d, ret_d, daily, vma_dest, cc)
                vma_rows = "".join(
                    f"<tr><td>{d}</td><td>{lbl}</td><td>{c_}</td><td>{m}</td><td style='text-align:right'>{v:.2f} €</td></tr>"
                    for d,lbl,c_,m,v in vma_tag_rows)
                vma_header = "<tr><th>Datum</th><th>Tag</th><th>Land</th><th>Erstattete Mahlzeiten</th><th>VMA</th></tr>"
                vma_source_note = f"Tagesgenaue Erfassung ({len(daily)} Tage)"
            else:
                vma_total, vma_tag_rows = calc_vma_multi(dep_d, ret_d, ml, vma_dest, cc)
                vma_rows = "".join(
                    f"<tr><td>{lbl}</td><td>{c_}</td><td>{m}</td><td style='text-align:right'>{v:.2f} €</td></tr>"
                    for lbl,c_,m,v in vma_tag_rows)
                vma_header = "<tr><th>Tag</th><th>Land</th><th>Mahlzeiten-Abzug</th><th>VMA</th></tr>"
                vma_source_note = "Pauschale (keine Tageserfassung)"
        else:
            vma_total=0.0; vma_rows=""; vma_header=""; vma_source_note=""
        trenn_total,trenn_details=trennungspauschale(dep_d,ret_d,dep_t or "08:00",ret_t or "18:00")
        trenn_rows="".join(f"<tr><td>{d}</td><td>{lbl}</td><td style='text-align:right'>{amt:.2f} €</td></tr>" for d,lbl,amt in trenn_details)

        beleg_sum=0.0; beleg_rows=""
        for a in atts:
            fn,dt,amt,amt_eur,curr,d,vendor,stat,bemerk=a
            if not amt_eur: continue
            try: beleg_sum+=float(amt_eur.replace(".","").replace(",","."))
            except: pass
            beleg_rows+=f"<tr><td>{fn}</td><td>{dt or '–'}</td><td>{vendor or '–'}</td><td>{d or '–'}</td><td>{amt or '–'} {curr or ''}</td><td style='text-align:right'><b>{amt_eur} €</b></td></tr>"

        gesamt=beleg_sum+vma_total+trenn_total
        title_line = " · ".join(filter(None,[trip_title,customer_code])) or ""

        html=f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<title>Abrechnung {tc}</title>
<style>
  @page {{ margin: 20mm; }}
  @media print {{ .no-print {{ display:none }} body {{ font-size:11pt }} }}
  body {{ font-family: Arial, sans-serif; font-size:12px; color:#1a1a2e; margin:0; padding:20px }}
  h1 {{ font-size:18px; color:#1a3d96; margin-bottom:4px }}
  h2 {{ font-size:13px; color:#2c3e5e; margin:18px 0 6px; border-bottom:1px solid #dde4ef; padding-bottom:4px }}
  .meta-grid {{ display:grid; grid-template-columns:140px 1fr; gap:2px 0; margin-bottom:12px }}
  .meta-label {{ font-weight:600; color:#5a6e8a; font-size:11px }}
  .meta-val {{ color:#1a1a2e }}
  table {{ width:100%; border-collapse:collapse; margin-bottom:8px }}
  th {{ background:#eef4ff; font-size:11px; padding:5px 8px; text-align:left; border:1px solid #dde4ef }}
  td {{ padding:4px 8px; border:1px solid #eaeef5; font-size:11px; vertical-align:top }}
  .sum-row td {{ background:#f0f4f9; font-weight:600 }}
  .total-box {{ background:#eef4ff; border:1px solid #2152c4; border-radius:6px;
                padding:12px 16px; margin-top:16px; display:flex; justify-content:space-between }}
  .total-label {{ font-size:13px; font-weight:600; color:#1a3d96 }}
  .total-val {{ font-size:16px; font-weight:700; color:#1a3d96 }}
  .footer {{ margin-top:24px; font-size:10px; color:#9bafc8; border-top:1px solid #dde4ef; padding-top:8px }}
  .print-btn {{ background:#2152c4; color:white; border:none; padding:8px 20px; border-radius:6px;
                cursor:pointer; font-size:13px; margin-bottom:16px }}
</style>
</head>
<body>
<button class="print-btn no-print" onclick="window.print()">🖨 Drucken / Als PDF speichern</button>
<h1>Reisekostenabrechnung – {tc}{f" · {title_line}" if title_line else ""}</h1>
<p style="font-size:11px;color:#5a6e8a;margin-bottom:12px">Erstellt: {date.today().strftime('%d.%m.%Y')} · Herrhammer Kürschner Kerzenmaschinen</p>

<h2>Reisedaten</h2>
<div class="meta-grid">
  <span class="meta-label">Reisender:</span><span class="meta-val">{traveler or '–'}</span>
  {"<span class='meta-label'>Kollegen:</span><span class='meta-val'>"+colleagues+"</span>" if colleagues else ""}
  <span class="meta-label">Zeitraum:</span><span class="meta-val">{str(dep_d or '–')} {dep_t or ''} – {str(ret_d or '–')} {ret_t or ''} ({days} Tage)</span>
  <span class="meta-label">Reiseziel:</span><span class="meta-val">{destinations or cc or '–'}</span>
  <span class="meta-label">Land (VMA):</span><span class="meta-val">{cc or 'DE'}</span>
  {"<span class='meta-label'>PNR:</span><span class='meta-val' style='font-family:monospace'>"+pnr+"</span>" if pnr else ""}
  {"<span class='meta-label'>Flüge:</span><span class='meta-val'>"+fns+"</span>" if fns else ""}
  <span class="meta-label">Hotel:</span><span class="meta-val">{nights_b or 0}/{nights_p or 0} Nächte</span>
</div>

<h2>Belege</h2>
<table>
  <tr><th>Datei</th><th>Typ</th><th>Anbieter</th><th>Datum</th><th>Betrag orig.</th><th>Betrag EUR</th></tr>
  {beleg_rows or '<tr><td colspan="6">Keine analysierten Belege</td></tr>'}
  <tr class="sum-row"><td colspan="5">Summe Belege</td><td style="text-align:right">{beleg_sum:.2f} €</td></tr>
</table>

<h2>Verpflegungsmehraufwand §9 EStG</h2>
<p style="font-size:10px;color:#9bafc8;margin-bottom:4px">{vma_source_note}</p>
<table>
  {vma_header}
  {vma_rows or '<tr><td colspan="5">Keine Reisezeit erfasst</td></tr>'}
  <tr class="sum-row"><td colspan="{4 if not daily else 4}"><b>Summe VMA</b></td><td style="text-align:right">{vma_total:.2f} €</td></tr>
</table>

{f"""<h2>Trennungspauschale (Herrhammer)</h2>
<table>
  <tr><th>Datum</th><th>Grund</th><th>Betrag</th></tr>
  {trenn_rows}
  <tr class="sum-row"><td colspan="2">Summe Trennungspauschale</td><td style="text-align:right">{trenn_total:.2f} €</td></tr>
</table>""" if trenn_total > 0 else ""}

<div class="total-box">
  <span class="total-label">Gesamtbetrag</span>
  <span class="total-val">{gesamt:,.2f} €</span>
</div>
<p style="font-size:10px;color:#9bafc8;margin-top:6px">Belege {beleg_sum:.2f} € + VMA {vma_total:.2f} € + Trennungspauschale {trenn_total:.2f} €</p>

<div class="footer">
  Herrhammer Kürschner Kerzenmaschinen · Reisekosten-System v{APP_VERSION} ·
  Abrechnung {tc} · {date.today().strftime('%d.%m.%Y')}
</div>
<script>window.onload=function(){{if(window.location.search!=='?noprint')setTimeout(()=>window.print(),400);}}</script>
</body></html>"""
        return HTMLResponse(html)
    except Exception as e:
        return HTMLResponse(f"<pre>Fehler: {e}</pre>",status_code=500)


# =========================================================
# FLUG + BAHN CHECK
# =========================================================

@app.get("/check-flights/{tc}", response_class=HTMLResponse)
async def check_flights(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT flight_numbers,train_numbers,departure_date,pnr_code FROM trip_meta WHERE trip_code=%s",(tc,))
        row=cur.fetchone()
        if not row: return page_shell("Fehler",f'<div class="page-card"><p>{tc} nicht gefunden</p></div>')
        fns_raw,trains_raw,dep_date,pnr=row
        dep_str=str(dep_date) if dep_date else str(date.today())
        results_html=""

        # Flüge: OpenSky (Live-Position) + AviationStack (Buchungsstatus)
        fns=[f.strip() for f in (fns_raw or "").split(",") if f.strip()]
        for fn in fns:
            # OpenSky: Live-Position
            si      = await check_flight_status(fn, dep_str)
            on_ground = si.get("on_ground")
            opensky_status = si.get("status","–")

            # AviationStack: Buchungsstatus (Umbuchung/Stornierung/Verspätung)
            av = await check_aviationstack(fn, dep_str)
            av_status   = av.get("status","")
            av_route    = av.get("route","")
            av_delay    = av.get("dep_delay") or 0
            av_gate     = av.get("gate","")
            av_cancelled = av.get("cancelled", False)

            # Alert-Logik
            if av_cancelled:
                alert = "cancelled"
                display_status = "⚠ STORNIERT"
                cls = "bdg-e"
            elif av_delay > 30:
                alert = "delay"
                display_status = f"Verspätung +{av_delay} Min."
                cls = "bdg-e"
            elif on_ground is False:
                alert = "ok"
                display_status = opensky_status
                cls = "bdg-ok"
            elif on_ground is True:
                alert = "ok"
                display_status = "am Boden"
                cls = "bdg-w"
            else:
                alert = "ok"
                display_status = av_status or opensky_status or "–"
                cls = "bdg-w"

            cur.execute("""INSERT INTO flight_alerts
                (trip_code,flight_number,flight_date,alert_type,message,source,delay_min)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (tc,fn,dep_str,alert,display_status,"OpenSky+AviationStack",av_delay or si.get("delay_min")))

            # Zusatzinfos zusammenbauen
            extras = []
            if av_route:
                extras.append(f"🛤 {av_route}")
            if av_gate:
                extras.append(f"Gate {av_gate}")
            if si.get("lat") and si.get("lon"):
                extras.append(f"📍 {si['lat']:.1f}°N {si['lon']:.1f}°E")
            if av.get("source") == "kein AVIATIONSTACK_KEY":
                extras.append('<span style="color:var(--am6)">⚠ kein AviationStack Key</span>')
            extra_html = " · ".join(extras)

            results_html+=f"""<tr>
                <td class='cc'>✈ {fn}</td>
                <td>{dep_str}</td>
                <td><span class='bdg {cls}'>{display_status}</span>
                    {"<br>" if extra_html else ""}<span style='font-size:11px;color:var(--t500)'>{extra_html}</span></td>
                <td style='font-size:11px'>OpenSky<br>AviationStack</td>
                <td>{av_delay or '–'}</td>
            </tr>"""

        # g) Züge
        trains=[t.strip() for t in (trains_raw or "").split(",") if t.strip()]
        for tn in trains:
            bi=await check_bahn_puenktlichkeit(tn,dep_str)
            cls="bdg-e" if (bi.get("delay_min") or 0)>5 else "bdg-ok"
            results_html+=f"<tr><td class='cc'>🚆 {tn}</td><td>{dep_str}</td><td><span class='bdg {cls}'>{bi.get('status','–')}</span></td><td>{bi.get('source','–')}</td><td>{bi.get('delay_min','–')}</td></tr>"

        if pnr:
            results_html=f"<tr style='background:var(--gr1)'><td colspan='5' style='color:var(--gr6);font-weight:500;font-family:DM Mono,monospace'>PNR/AMADEUS: {pnr}</td></tr>"+results_html

        conn.commit();cur.close();conn.close()
        return page_shell("Flug+Bahn",f"""
        <div class="page-card"><h2>Flug- & Zugstatus – {tc}</h2>
          <div class="acts"><a class="btn-l" href="/">Zurück</a></div>
          <table><tr><th>Verbindung</th><th>Datum</th><th>Status</th><th>Quelle</th><th>Verspätung (Min.)</th></tr>
          {results_html or '<tr><td colspan="5">Keine Flug-/Zugnummern hinterlegt</td></tr>'}
          </table>
        </div>""",active_tab="active")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


# =========================================================
# BELEG VORSCHAU (Signed URL aus Hetzner)
# =========================================================

@app.get("/beleg/{att_id}")
def beleg_vorschau(att_id: int):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT storage_key,original_filename,content_type FROM mail_attachments WHERE id=%s",(att_id,))
        row=cur.fetchone();cur.close();conn.close()
        if not row: return HTMLResponse("Beleg nicht gefunden",status_code=404)
        storage_key,filename,content_type=row
        if not storage_key or storage_key.startswith("S3-FEHLER"):
            return HTMLResponse(f"Datei nicht im Bucket: {storage_key}",status_code=404)
        s3=get_s3()
        signed_url=s3.generate_presigned_url("get_object",
            Params={"Bucket":S3_BUCKET,"Key":storage_key,
                    "ResponseContentDisposition":f'inline; filename="{filename or "beleg"}"',
                    "ResponseContentType":content_type or "application/octet-stream"},
            ExpiresIn=300)
        return RedirectResponse(url=signed_url)
    except Exception as e:
        return HTMLResponse(f"Fehler: {e}",status_code=500)


@app.get("/beleg-edit/{att_id}", response_class=HTMLResponse)
def beleg_edit_form(att_id: int):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT id,trip_code,original_filename,detected_type,
            detected_amount,detected_amount_eur,detected_currency,
            detected_date,detected_vendor,ki_bemerkung,confidence,analysis_status
            FROM mail_attachments WHERE id=%s""",(att_id,))
        row=cur.fetchone()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code")
        all_codes=[r[0] for r in cur.fetchall()]
        cur.close();conn.close()
        if not row: return HTMLResponse("Nicht gefunden",404)
        _,tc,fname,dtype,amt,amt_eur,curr,ddate,vendor,bemerk,conf,astatus=row
        type_opts="".join(f'<option {"selected" if dtype==t else ""}>{t}</option>'
            for t in ["Flug","Hotel","Taxi","Bahn","Mietwagen","Essen","Sonstiges","Kalendereintrag"])
        code_opts="".join(f'<option value="{c}" {"selected" if tc==c else ""}>{c}</option>'
            for c in all_codes)
        return page_shell(f"Beleg bearbeiten #{att_id}",f"""
        <div class="page-card" style="max-width:600px">
          <h2>Beleg #{att_id} korrigieren</h2>
          <p class="sub" style="margin-bottom:16px">{fname or "–"}</p>
          <form method="post" action="/beleg-edit/{att_id}">
            <div class="fgrid">
              <div class="fgrp ff"><label class="flbl">Reisecode</label>
                <select class="fsel" name="trip_code"><option value="">– nicht zugeordnet –</option>{code_opts}</select></div>
              <div class="fgrp"><label class="flbl">Belegtyp</label>
                <select class="fsel" name="detected_type">{type_opts}</select></div>
              <div class="fgrp"><label class="flbl">Anbieter</label>
                <input class="finp" name="detected_vendor" value="{vendor or ''}"></div>
              <div class="fgrp"><label class="flbl">Datum (DD.MM.YYYY)</label>
                <input class="finp" name="detected_date" value="{ddate or ''}"></div>
              <div class="fgrp"><label class="flbl">Betrag (Original)</label>
                <input class="finp" name="detected_amount" value="{amt or ''}"></div>
              <div class="fgrp"><label class="flbl">Währung (ISO)</label>
                <input class="finp" name="detected_currency" value="{curr or 'EUR'}" maxlength="3"></div>
              <div class="fgrp ff"><label class="flbl">Betrag EUR (Komma)</label>
                <input class="finp" name="detected_amount_eur" value="{amt_eur or ''}"></div>
              <div class="fgrp ff"><label class="flbl">Notiz / KI-Bemerkung</label>
                <input class="finp" name="ki_bemerkung" value="{bemerk or ''}"></div>
            </div>
            <div class="mfooter">
              <a class="btn-mc" href="javascript:history.back()">Abbrechen</a>
              <button type="submit" class="btn-mp">Speichern</button>
            </div>
          </form>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.post("/beleg-edit/{att_id}")
async def beleg_edit_save(att_id: int, request: Request):
    try:
        form=await request.form()
        conn=get_conn();cur=conn.cursor()
        cur.execute("""UPDATE mail_attachments SET
            trip_code=%s,detected_type=%s,detected_vendor=%s,detected_date=%s,
            detected_amount=%s,detected_currency=%s,detected_amount_eur=%s,
            ki_bemerkung=%s,review_flag='ok',analysis_status='ok (manuell)'
            WHERE id=%s""",
            (form.get("trip_code") or None,
             form.get("detected_type") or None,
             form.get("detected_vendor") or None,
             form.get("detected_date") or None,
             form.get("detected_amount") or None,
             (form.get("detected_currency") or "EUR").upper(),
             form.get("detected_amount_eur") or None,
             form.get("ki_bemerkung") or None,
             att_id))
        conn.commit()
        # Reisecode updaten falls gesetzt
        tc = form.get("trip_code")
        if tc:
            cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(tc,))
            conn.commit()
        cur.close();conn.close()
        return RedirectResponse(url=f"/attachment-log",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)


# =========================================================
# RESET / HILFSFUNKTIONEN
# =========================================================

@app.get("/reset-all", response_class=HTMLResponse)
def reset_all(confirm: str = ""):
    if confirm != "ja":
        return page_shell("Reset","""
        <div class="page-card" style="max-width:500px">
          <h2 class="err-t">⚠ Alle Daten löschen?</h2>
          <p style="margin:12px 0 20px;color:var(--t500)">Löscht alle Reisen, Mails, Anhänge und Alerts unwiderruflich.</p>
          <div class="acts">
            <a class="btn" style="background:var(--re6)" href="/reset-all?confirm=ja">Ja, alles löschen</a>
            <a class="btn-l" href="/">Abbrechen</a>
          </div>
        </div>""")
    try:
        conn=get_conn();cur=conn.cursor()
        for tbl in ["mail_attachments","mail_messages","flight_alerts","trip_meta"]:
            cur.execute(f"TRUNCATE TABLE {tbl} RESTART IDENTITY CASCADE")
        conn.commit();cur.close();conn.close()
        return page_shell("Reset","""
        <div class="page-card" style="max-width:500px">
          <h2 class="ok-t">✓ Datenbank geleert</h2>
          <p style="margin:12px 0 20px;color:var(--t500)">Alle Daten gelöscht. Bereit für Echtdaten.</p>
          <div class="acts"><a class="btn" href="/">Dashboard</a></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Fehler</h2><p>{e}</p></div>')


@app.get("/check-trains/{tc}", response_class=HTMLResponse)
async def check_trains(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT train_numbers,departure_date FROM trip_meta WHERE trip_code=%s",(tc,))
        row=cur.fetchone()
        if not row or not row[0]:
            cur.close();conn.close()
            return page_shell("Bahnprüfung",f'<div class="page-card"><h2>Keine Zugnummern für {tc}</h2><a class="btn-l" href="/edit-trip/{tc}">Bearbeiten</a></div>')
        trains=[z.strip() for z in (row[0] or "").split(",") if z.strip()]
        dep_date=str(row[1]) if row[1] else str(date.today())
        if not DB_CLIENT_ID or not DB_CLIENT_SECRET:
            cur.close();conn.close()
            return page_shell("Bahnprüfung",f"""<div class="page-card">
              <h2 class="warn-t">⚠ DB API nicht konfiguriert</h2>
              <p class="sub">Bitte DB_CLIENT_ID und DB_CLIENT_SECRET in Render eintragen.</p>
              <p class="sub">Portal: developers.deutschebahn.com → Timetables 1.0.274 → Anwendung → Keys</p>
              <a class="btn-l" href="/">Zurück</a></div>""")
        results_html=""
        for zug in trains:
            si = await check_bahn_puenktlichkeit(zug, dep_date)
            alert = "delay" if (si.get("delay_min") or 0) > 15 else "ok"
            cur.execute("""INSERT INTO flight_alerts
                (trip_code,flight_number,flight_date,alert_type,message,source,delay_min)
                VALUES (%s,%s,%s,%s,%s,%s,%s)""",
                (tc,zug,dep_date,alert,si.get("status","–"),si.get("source","DB Timetables"),si.get("delay_min")))
            cls="bdg-e" if alert=="delay" else "bdg-ok"
            delay_txt = f'+{si["delay_min"]} Min.' if si.get("delay_min") else "–"
            results_html+=f"<tr><td class='cc'>{zug}</td><td>{dep_date}</td><td><span class='bdg {cls}'>{si.get('status','–')}</span></td><td>{delay_txt}</td><td>{si.get('source','–')}</td></tr>"
            if "raw" in si:
                results_html+=f"<tr><td colspan='5' style='font-size:11px;color:var(--t300)'>API-Antwort: {si['raw'][:200]}</td></tr>"
        conn.commit();cur.close();conn.close()
        bahn_status = "✓ DB Timetables API aktiv" if DB_CLIENT_ID else "⚠ Kein API Key"
        return page_shell("Bahnprüfung",f"""
        <div class="page-card"><h2>Zugstatus – {tc}</h2>
          <p class="sub" style="margin-bottom:12px">{bahn_status} · EVA 8000105 (Frankfurt Hbf)</p>
          <div class="acts"><a class="btn-l" href="/">Zurück</a></div>
          <table><tr><th>Zug</th><th>Datum</th><th>Status</th><th>Verspätung</th><th>Quelle</th></tr>
          {results_html or "<tr><td colspan='5'>Keine Ergebnisse</td></tr>"}</table>
        </div>""",active_tab="active")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


# =========================================================
# BELEG MANUELL HOCHLADEN
# =========================================================

@app.post("/upload-beleg", response_class=HTMLResponse)
async def upload_beleg(
    request: Request,
    file: UploadFile = File(...),
    trip_code: str = Form(default="")
):
    try:
        if not file or not file.filename:
            return page_shell("Upload",f'<div class="page-card"><h2 class="err-t">Keine Datei</h2><a class="btn-l" href="/">Zurück</a></div>')

        ext = (file.filename or "").lower().split(".")[-1]
        if ext not in ("pdf","jpg","jpeg","png","webp","ics"):
            return page_shell("Upload",f'<div class="page-card"><h2 class="err-t">Dateityp .{ext} nicht unterstützt</h2><p class="sub">Erlaubt: PDF, JPG, PNG, WEBP, ICS</p><a class="btn-l" href="/">Zurück</a></div>')

        file_bytes = await file.read()
        h = file_hash(file_bytes)

        conn=get_conn();cur=conn.cursor()

        # Duplikat-Check
        cur.execute("SELECT id,trip_code FROM mail_attachments WHERE file_hash=%s",(h,))
        existing = cur.fetchone()
        if existing:
            cur.close();conn.close()
            return page_shell("Upload",f'<div class="page-card"><h2 class="warn-t">⚠ Duplikat</h2><p>Diese Datei wurde bereits als Anhang ID {existing[0]} gespeichert (Reise {existing[1] or "–"}).</p><a class="btn-l" href="/">Zurück</a></div>')

        # Reisecode aus Dateiname / Formular ermitteln
        code = trip_code.strip() or extract_trip_code(file.filename)
        if code:
            cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(code,))

        safe_fn = sanitize_filename(file.filename)
        uid = f"manual_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        storage_key = f"mail_attachments/{uid}_{safe_fn}"

        try:
            s3 = get_s3()
            s3.put_object(Bucket=S3_BUCKET, Key=storage_key, Body=file_bytes,
                          ContentType=file.content_type or "application/octet-stream")
        except Exception as s3e:
            storage_key = f"S3-FEHLER:{s3e}"

        cur.execute("""INSERT INTO mail_attachments
            (mail_uid,trip_code,original_filename,saved_filename,content_type,
             storage_key,detected_type,analysis_status,confidence,review_flag,file_hash)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (uid,code,safe_fn,f"{uid}_{safe_fn}",file.content_type,
             storage_key,detect_type_with_rules(safe_fn,"","",load_custom_rules()),
             "ausstehend","niedrig","pruefen",h))

        conn.commit();cur.close();conn.close()

        return page_shell("Upload",f"""
        <div class="page-card">
          <h2 class="ok-t">✓ {safe_fn} hochgeladen</h2>
          <p style="margin-bottom:16px">Reise: <b>{code or '– noch nicht zugeordnet –'}</b> · Jetzt KI-Analyse starten.</p>
          <div class="acts">
            <a class="btn" href="/analyze-attachments">KI-Analyse starten</a>
            <a class="btn-l" href="/">Dashboard</a>
            {f'<a class="btn-l" href="/trip/{code}">Reise {code}</a>' if code else ''}
          </div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Upload-Fehler</h2><p>{e}</p><a class="btn-l" href="/">Zurück</a></div>')


def reset_mail_log():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("TRUNCATE TABLE mail_attachments RESTART IDENTITY")
        cur.execute("TRUNCATE TABLE mail_messages RESTART IDENTITY")
        conn.commit();cur.close();conn.close()
        return {"status":"ok"}
    except Exception as e:
        return {"status":"fehler","detail":str(e)}


# =========================================================
# STATISTIK-SEITE
# =========================================================

@app.get("/stats", response_class=HTMLResponse)
def stats():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT t.traveler_name,COUNT(DISTINCT t.trip_code),
            COALESCE(SUM(CASE WHEN a.detected_amount_eur IS NOT NULL
                THEN CAST(REPLACE(a.detected_amount_eur,',','.') AS NUMERIC)
                ELSE 0 END),0)
            FROM trip_meta t LEFT JOIN mail_attachments a ON a.trip_code=t.trip_code
            WHERE t.traveler_name IS NOT NULL GROUP BY t.traveler_name ORDER BY 3 DESC LIMIT 20""")
        by_person=cur.fetchall()
        cur.execute("""SELECT t.country_code,COUNT(DISTINCT t.trip_code),
            COALESCE(SUM(CASE WHEN a.detected_amount_eur IS NOT NULL
                THEN CAST(REPLACE(a.detected_amount_eur,',','.') AS NUMERIC)
                ELSE 0 END),0)
            FROM trip_meta t LEFT JOIN mail_attachments a ON a.trip_code=t.trip_code
            WHERE t.country_code IS NOT NULL GROUP BY t.country_code ORDER BY 3 DESC LIMIT 20""")
        by_country=cur.fetchall()
        cur.execute("""SELECT TO_CHAR(t.departure_date,'YYYY-MM'),COUNT(DISTINCT t.trip_code),
            COALESCE(SUM(CASE WHEN a.detected_amount_eur IS NOT NULL
                THEN CAST(REPLACE(a.detected_amount_eur,',','.') AS NUMERIC)
                ELSE 0 END),0)
            FROM trip_meta t LEFT JOIN mail_attachments a ON a.trip_code=t.trip_code
            WHERE t.departure_date >= now()-interval '12 months' AND t.departure_date IS NOT NULL
            GROUP BY 1 ORDER BY 1 DESC""")
        by_month=cur.fetchall()
        cur.execute("""SELECT detected_type,COUNT(*),
            COALESCE(SUM(CASE WHEN detected_amount_eur IS NOT NULL
                THEN CAST(REPLACE(detected_amount_eur,',','.') AS NUMERIC)
                ELSE 0 END),0)
            FROM mail_attachments WHERE detected_type IS NOT NULL AND detected_type!=''
            GROUP BY detected_type ORDER BY 3 DESC""")
        by_type=cur.fetchall()
        cur.execute("""SELECT COUNT(DISTINCT trip_code),
            COALESCE(SUM(CASE WHEN detected_amount_eur IS NOT NULL
                THEN CAST(REPLACE(detected_amount_eur,',','.') AS NUMERIC)
                ELSE 0 END),0) FROM mail_attachments""")
        total=cur.fetchone()
        cur.close();conn.close()

        def tbl(rows, headers):
            ths="".join(f"<th>{h}</th>" for h in headers)
            trs=""
            for r in rows:
                cells=[]
                for i,v in enumerate(r):
                    if i==2:
                        cells.append(f"<td style='text-align:right;font-family:DM Mono,monospace'>{float(v or 0):,.2f} \u20ac</td>")
                    else:
                        cells.append(f"<td>{v or '\u2013'}</td>")
                trs+=f"<tr>{'  '.join(cells)}</tr>"
            return f"<div style='overflow-x:auto'><table><tr>{ths}</tr>{trs or '<tr><td colspan=3>Keine Daten</td></tr>'}</table></div>"

        return page_shell("Statistik",f"""
        <div style="display:flex;flex-direction:column;gap:20px;max-width:1100px">
          <div class="sum-bar sb">
            <div class="sum-item"><div class="sum-val blue">{total[0] if total else 0}</div><div class="sum-lbl">Reisen gesamt</div></div>
            <div class="sum-item"><div class="sum-val">{float(total[1] if total else 0):,.2f} \u20ac</div><div class="sum-lbl">Gesamtkosten (Belege)</div></div>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">
            <div class="page-card"><h2 style="margin-bottom:12px">\U0001f4bc Nach Mitarbeiter</h2>
              {tbl(by_person,["Mitarbeiter","Reisen","EUR"])}</div>
            <div class="page-card"><h2 style="margin-bottom:12px">\U0001f30d Nach Land</h2>
              {tbl(by_country,["Land","Reisen","EUR"])}</div>
          </div>
          <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px">
            <div class="page-card"><h2 style="margin-bottom:12px">\U0001f4c5 Nach Monat (12 Mon.)</h2>
              {tbl(by_month,["Monat","Reisen","EUR"])}</div>
            <div class="page-card"><h2 style="margin-bottom:12px">\U0001f9fe Nach Belegtyp</h2>
              {tbl(by_type,["Typ","Anzahl","EUR"])}</div>
          </div>
          <div><a class="btn-l" href="/">\u2190 Dashboard</a></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


# =========================================================
# KATEGORIE-REGELN ADMIN
# =========================================================

@app.get("/rules", response_class=HTMLResponse)
def rules_page():
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT id,keyword,category,created_at FROM category_rules ORDER BY category,keyword")
        rows=cur.fetchall();cur.close();conn.close()
        categories=["Flug","Hotel","Taxi","Bahn","Mietwagen","Essen","Sonstiges"]
        cat_opts="".join(f'<option>{c}</option>' for c in categories)
        rule_rows="".join(f"""<tr>
            <td style="font-family:DM Mono,monospace">{r[1]}</td>
            <td><span class="bdg bdg-w">{r[2]}</span></td>
            <td style="font-size:11px;color:var(--t300)">{str(r[3] or '')[:10]}</td>
            <td><a href="/rules/delete/{r[0]}" onclick="return confirm('Regel löschen?')"
               style="color:var(--re6);font-size:12px;text-decoration:none">✕ löschen</a></td>
            </tr>""" for r in rows)
        # Standard-Regeln zur Ansicht
        std_html=""
        std_rules={"Taxi":["uber","bolt","free now","mytaxi"],"Flug":["lufthansa","ryanair","boarding pass"],
                   "Hotel":["marriott","booking.com","hilton"],"Bahn":["deutsche bahn","eurostar"],
                   "Mietwagen":["hertz","sixt","avis"],"Essen":["restaurant","starbucks","lieferando"]}
        for cat,kws in std_rules.items():
            std_html+=f'<div style="margin-bottom:4px"><span class="bdg bdg-ok" style="font-size:10px">{cat}</span> <span style="font-size:11px;color:var(--t300)">{", ".join(kws[:4])} …</span></div>'
        return page_shell("Kategorie-Regeln",f"""
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;max-width:1000px">
          <div class="page-card">
            <h2 style="margin-bottom:4px">Eigene Regeln</h2>
            <p class="sub" style="margin-bottom:14px">Schlüsselwort → Kategorie. Groß-/Kleinschreibung egal. Wird VOR Standard-Regeln geprüft.</p>
            <form method="post" action="/rules/add">
              <div class="fgrid" style="margin-bottom:12px">
                <div class="fgrp"><label class="flbl">Schlüsselwort</label>
                  <input class="finp" name="keyword" placeholder="z.B. taxifahrt münchen" required></div>
                <div class="fgrp"><label class="flbl">Kategorie</label>
                  <select class="fsel" name="category">{cat_opts}</select></div>
              </div>
              <button type="submit" class="btn-mp" style="width:100%">+ Regel hinzufügen</button>
            </form>
            <div style="margin-top:20px">
              {"<table><tr><th>Schlüsselwort</th><th>Kategorie</th><th>Erstellt</th><th></th></tr>"+rule_rows+"</table>" if rows else "<p class='sub'>Noch keine eigenen Regeln.</p>"}
            </div>
            <div style="margin-top:16px;padding-top:12px;border-top:1px solid var(--bds)">
              <a class="btn" href="/reclassify">🔄 Alle «Unbekannt»-Belege neu klassifizieren</a>
            </div>
          </div>
          <div class="page-card">
            <h2 style="margin-bottom:12px">Standard-Regeln (integriert)</h2>
            <p class="sub" style="margin-bottom:12px">Diese Regeln sind fest eingebaut und können nicht gelöscht werden. Eigene Regeln haben Vorrang.</p>
            {std_html}
          </div>
        </div>
        <div style="margin-top:12px"><a class="btn-l" href="/">← Dashboard</a></div>
        """)
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.post("/rules/add")
async def rules_add(request: Request):
    try:
        form=await request.form()
        kw=(form.get("keyword") or "").strip().lower()
        cat=(form.get("category") or "").strip()
        if not kw or not cat:
            return RedirectResponse(url="/rules",status_code=303)
        conn=get_conn();cur=conn.cursor()
        cur.execute("INSERT INTO category_rules (keyword,category) VALUES (%s,%s)",(kw,cat))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/rules",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)

@app.get("/rules/delete/{rule_id}")
def rules_delete(rule_id: int):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("DELETE FROM category_rules WHERE id=%s",(rule_id,))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/rules",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)

@app.get("/reclassify", response_class=HTMLResponse)
def reclassify():
    """Klassifiziert alle 'Unbekannt' Belege neu mit Standard + eigenen Regeln."""
    try:
        custom_rules=load_custom_rules()
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT id,original_filename,detected_type,extracted_text,ki_bemerkung
            FROM mail_attachments
            WHERE detected_type IN ('Unbekannt','') OR detected_type IS NULL""")
        rows=cur.fetchall()
        updated=0
        for att_id,fname,old_type,extext,bemerk in rows:
            new_type=detect_type_with_rules(fname,"",extext or "",custom_rules)
            if new_type and new_type!=old_type:
                cur.execute("UPDATE mail_attachments SET detected_type=%s WHERE id=%s",(new_type,att_id))
                updated+=1
        conn.commit();cur.close();conn.close()
        return page_shell("Nachklassifizierung",f"""
        <div class="page-card">
          <h2 class="ok-t">✓ {updated} Belege neu klassifiziert</h2>
          <p class="sub" style="margin-bottom:16px">{len(rows)} «Unbekannt»-Belege geprüft · {updated} Kategorien aktualisiert</p>
          <div class="acts">
            <a class="btn" href="/">Dashboard</a>
            <a class="btn-l" href="/rules">Regeln verwalten</a>
            <a class="btn-l" href="/attachment-log">Anhang-Log</a>
          </div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')



# =========================================================
# TAGESBASIERTE MAHLZEITEN-ERFASSUNG (VMA-Grundlage)
# =========================================================

@app.get("/meals/{tc}", response_class=HTMLResponse)
def meals_page(tc: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT departure_date,return_date,traveler_name,trip_title FROM trip_meta WHERE trip_code=%s",(tc,))
        meta=cur.fetchone()
        if not meta: return HTMLResponse("Reise nicht gefunden",404)
        dep,ret,traveler,title=meta
        dep_d=dep if isinstance(dep,date) else (date.fromisoformat(str(dep)) if dep else None)
        ret_d=ret if isinstance(ret,date) else (date.fromisoformat(str(ret)) if ret else None)
        if not dep_d or not ret_d:
            return page_shell(f"Mahlzeiten {tc}",f"""
            <div class="page-card"><h2>Mahlzeiten {tc}</h2>
            <p class="sub">Bitte zuerst Abreise- und Rückkehrdatum in der Reise hinterlegen.</p>
            <a class="btn" href="/edit-trip/{tc}">Reise bearbeiten</a></div>""")

        # Bestehende Einträge laden
        cur.execute("SELECT meal_date,breakfast,lunch,dinner,notes FROM daily_meals WHERE trip_code=%s ORDER BY meal_date",(tc,))
        existing={row[0]: row for row in cur.fetchall()}
        cur.close();conn.close()

        # Tabelle für alle Reisetage generieren
        days=(ret_d-dep_d).days+1
        rows_html=""
        vma_preview=0.0
        for i in range(days):
            d=dep_d+timedelta(days=i)
            e=existing.get(d)
            b_chk="checked" if e and e[1] else ""
            l_chk="checked" if e and e[2] else ""
            di_chk="checked" if e and e[3] else ""
            notes_val=e[4] if e and e[4] else ""
            # Tagestyp für VMA
            dtype="partial" if i==0 or i==days-1 else "full"
            ml=[]
            if e:
                if e[1]: ml.append("breakfast")
                if e[2]: ml.append("lunch")
                if e[3]: ml.append("dinner")
            vma_day=get_vma("DE",dtype,ml)
            vma_preview+=vma_day
            wd=["Mo","Di","Mi","Do","Fr","Sa","So"][d.weekday()]
            wkend_style=' style="background:var(--b50)"' if d.weekday()>=5 else ""
            rows_html+=f"""<tr{wkend_style}>
                <td style="font-weight:500;white-space:nowrap">{str(d)} {wd}</td>
                <td style="text-align:center"><input type="checkbox" name="b_{d}" {b_chk} onchange="this.form.submit()"></td>
                <td style="text-align:center"><input type="checkbox" name="l_{d}" {l_chk} onchange="this.form.submit()"></td>
                <td style="text-align:center"><input type="checkbox" name="d_{d}" {di_chk} onchange="this.form.submit()"></td>
                <td><input type="text" class="finp" name="n_{d}" value="{notes_val}" placeholder="Notiz..." style="padding:3px 6px;font-size:12px"></td>
                <td style="text-align:right;font-family:DM Mono,monospace;color:var(--b600)">{vma_day:.2f} €</td>
            </tr>"""

        title_str=f" · {title}" if title else ""
        return page_shell(f"Mahlzeiten {tc}",f"""
        <div class="page-card" style="max-width:800px">
          <h2>Mahlzeiten-Erfassung – {tc}{title_str}</h2>
          <p class="sub" style="margin-bottom:4px">Reisender: {traveler or '–'} · {days} Tage · {str(dep_d)} bis {str(ret_d)}</p>
          <p class="sub" style="margin-bottom:16px">Haken setzen = Mahlzeit wurde <b>vom Kunden/Hotel gestellt</b> → wird vom VMA abgezogen</p>
          <form method="post" action="/meals/{tc}">
            <div style="overflow-x:auto"><table>
              <tr>
                <th>Datum</th>
                <th style="text-align:center">🍳 Frühstück<br><span style="font-size:10px;font-weight:400">−5,60 €</span></th>
                <th style="text-align:center">🥗 Mittagessen<br><span style="font-size:10px;font-weight:400">−11,20 €</span></th>
                <th style="text-align:center">🍽 Abendessen<br><span style="font-size:10px;font-weight:400">−11,20 €</span></th>
                <th>Notiz</th>
                <th style="text-align:right">VMA</th>
              </tr>
              {rows_html}
              <tr style="background:var(--b50)">
                <td colspan="5"><b>Summe VMA (Vorschau, Land DE)</b></td>
                <td style="text-align:right;font-family:DM Mono,monospace;font-weight:600">{vma_preview:.2f} €</td>
              </tr>
            </table></div>
            <div class="mfooter">
              <a class="btn-mc" href="/trip/{tc}">Zurück</a>
              <button type="submit" class="btn-mp">💾 Speichern</button>
            </div>
          </form>
          <p class="sub" style="margin-top:12px">💡 VMA-Vorschau gilt für DE. Länderspezifische Sätze werden in der Abrechnung berechnet.</p>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


@app.post("/meals/{tc}")
async def meals_save(tc: str, request: Request):
    try:
        form=await request.form()
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT departure_date,return_date FROM trip_meta WHERE trip_code=%s",(tc,))
        meta=cur.fetchone()
        if not meta: return RedirectResponse(url=f"/trip/{tc}",status_code=303)
        dep,ret=meta
        dep_d=dep if isinstance(dep,date) else date.fromisoformat(str(dep))
        ret_d=ret if isinstance(ret,date) else date.fromisoformat(str(ret))
        days=(ret_d-dep_d).days+1

        for i in range(days):
            d=dep_d+timedelta(days=i)
            b=bool(form.get(f"b_{d}"))
            l=bool(form.get(f"l_{d}"))
            di=bool(form.get(f"d_{d}"))
            notes=form.get(f"n_{d}","").strip() or None
            cur.execute("""INSERT INTO daily_meals (trip_code,meal_date,breakfast,lunch,dinner,notes,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,now())
                ON CONFLICT (trip_code,meal_date) DO UPDATE SET
                breakfast=EXCLUDED.breakfast, lunch=EXCLUDED.lunch,
                dinner=EXCLUDED.dinner, notes=EXCLUDED.notes, updated_at=now()""",
                (tc,d,b,l,di,notes))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url=f"/meals/{tc}",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)


@app.get("/set-hotel")
def set_hotel(code: str, mode: str):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("INSERT INTO trip_meta (trip_code,hotel_mode) VALUES (%s,%s) ON CONFLICT (trip_code) DO UPDATE SET hotel_mode=%s",(code,mode,mode))
        conn.commit();cur.close();conn.close()
        return {"status":"ok"}
    except Exception as e:
        return {"status":"fehler","detail":str(e)}
