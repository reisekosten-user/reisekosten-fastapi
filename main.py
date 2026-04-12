# Herrhammer Reisekosten – v1.0 (Neustart)
# Python 3.11 kompatibel
import os, re, io, json, base64, hashlib, imaplib, email, threading, time
from datetime import date, datetime, timedelta
from email.header import decode_header
from typing import Optional

import psycopg2
import boto3
import httpx
from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

# ─── Konstanten ───────────────────────────────────────────────────────────────
APP_VERSION     = "1.3"
DATABASE_URL    = os.getenv("DATABASE_URL", "")
IMAP_HOST       = os.getenv("IMAP_HOST", "")
IMAP_USER       = os.getenv("IMAP_USER", "")
IMAP_PASS       = os.getenv("IMAP_PASS", "")
S3_ENDPOINT     = os.getenv("S3_ENDPOINT", "")
S3_BUCKET       = os.getenv("S3_BUCKET", "")
S3_ACCESS_KEY   = os.getenv("S3_ACCESS_KEY", "")
S3_SECRET_KEY   = os.getenv("S3_SECRET_KEY", "")
MISTRAL_KEY     = os.getenv("MISTRAL_API_KEY", "")
MISTRAL_URL     = "https://api.mistral.ai/v1/chat/completions"
MISTRAL_MODEL   = "mistral-small-latest"

# ─── DB ───────────────────────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(DATABASE_URL)

def get_s3():
    return boto3.client("s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY)

# ─── Hilfsfunktionen ──────────────────────────────────────────────────────────
def file_hash(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()

def decode_header_value(val: str) -> str:
    if not val: return ""
    parts = decode_header(val)
    result = []
    for part, enc in parts:
        if isinstance(part, bytes):
            result.append(part.decode(enc or "utf-8", errors="ignore"))
        else:
            result.append(part)
    return "".join(result)

def html_to_text(html: str) -> str:
    import html as _html
    t = _html.unescape(html)
    t = re.sub(r'<style[^>]*>.*?</style>', ' ', t, flags=re.DOTALL|re.IGNORECASE)
    t = re.sub(r'<script[^>]*>.*?</script>', ' ', t, flags=re.DOTALL|re.IGNORECASE)
    t = re.sub(r'<br\s*/?>', '\n', t, flags=re.IGNORECASE)
    t = re.sub(r'<[^>]+>', ' ', t)
    t = re.sub(r'[ \t]+', ' ', t)
    t = re.sub(r'\n{3,}', '\n\n', t)
    return t.strip()[:30000]

# ─── Beleg-Analyse ────────────────────────────────────────────────────────────
IATA = {
    "FRA":"Frankfurt","MUC":"München","NUE":"Nürnberg","BER":"Berlin","HAM":"Hamburg",
    "STR":"Stuttgart","DUS":"Düsseldorf","CGN":"Köln","LYS":"Lyon","CDG":"Paris",
    "LHR":"London Heathrow","ZRH":"Zürich","VIE":"Wien","FCO":"Rom","MAD":"Madrid",
    "BCN":"Barcelona","AMS":"Amsterdam","BRU":"Brüssel","GVA":"Genf","MXP":"Mailand",
    "SJO":"San José (Costa Rica)","PTY":"Panama City","JFK":"New York","LAX":"Los Angeles",
    "ORD":"Chicago","MIA":"Miami","SFO":"San Francisco","DXB":"Dubai","SIN":"Singapur",
    "NRT":"Tokio","PEK":"Peking","HKG":"Hongkong","SYD":"Sydney","GRU":"São Paulo",
    "EZE":"Buenos Aires","MEX":"Mexico City","YYZ":"Toronto","YVR":"Vancouver",
    "DOH":"Doha","AUH":"Abu Dhabi","IST":"Istanbul","ATH":"Athen","WAW":"Warschau",
    "PRG":"Prag","BUD":"Budapest","OSL":"Oslo","ARN":"Stockholm","HEL":"Helsinki",
    "CPH":"Kopenhagen","LIS":"Lissabon","OPO":"Porto","DUB":"Dublin",
}

MONTHS_DE = {"januar":1,"februar":2,"märz":3,"april":4,"mai":5,"juni":6,
             "juli":7,"august":8,"september":9,"oktober":10,"november":11,"dezember":12}
MONTHS_EN = {"january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
             "july":7,"august":8,"september":9,"october":10,"november":11,"december":12}

def parse_date(s: str) -> Optional[date]:
    """Wandelt einen Datumsstring in ein date-Objekt um."""
    if not s: return None
    s = s.strip()
    # ISO: 2026-04-22
    m = re.match(r'(\d{4})-(\d{2})-(\d{2})', s)
    if m:
        try: return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        except: pass
    # DE: 22.04.2026
    m = re.match(r'(\d{1,2})[./](\d{1,2})[./](\d{4})', s)
    if m:
        try: return date(int(m.group(3)), int(m.group(2)), int(m.group(1)))
        except: pass
    # EN: April 22, 2026
    m = re.match(r'([A-Za-z]+)\s+(\d{1,2}),?\s+(\d{4})', s)
    if m:
        mon = MONTHS_EN.get(m.group(1).lower()) or MONTHS_DE.get(m.group(1).lower())
        if mon:
            try: return date(int(m.group(3)), mon, int(m.group(2)))
            except: pass
    # DE: 22. April 2026
    m = re.match(r'(\d{1,2})\.\s*([A-Za-z]+)\s+(\d{4})', s)
    if m:
        mon = MONTHS_DE.get(m.group(2).lower()) or MONTHS_EN.get(m.group(2).lower())
        if mon:
            try: return date(int(m.group(3)), mon, int(m.group(1)))
            except: pass
    return None

def extract_dates(text: str) -> list:
    """Findet alle Datumswerte im Text."""
    results = set()
    patterns = [
        r'\b(\d{4}-\d{2}-\d{2})\b',
        r'\b(\d{1,2}[./]\d{1,2}[./]\d{4})\b',
        r'\b(\d{1,2}\.\s*(?:Januar|Februar|März|April|Mai|Juni|Juli|August|September|Oktober|November|Dezember)\s+\d{4})\b',
        r'\b((?:January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},?\s+\d{4})\b',
    ]
    for p in patterns:
        for m in re.finditer(p, text, re.IGNORECASE):
            d = parse_date(m.group(1))
            if d and 2020 <= d.year <= 2030:
                results.add(d)
    return sorted(results)

def extract_flight_numbers(text: str) -> list:
    """Findet Flugnummern wie LH3463, LX 3613."""
    AIRLINES = {"LH","LX","OS","SK","AF","KL","BA","IB","EW","TK","AY","QR","EK","EY",
                "DL","AA","UA","AC","NH","JL","CX","SQ","TG","CM","AM","LA","4Y","WK"}
    clean = re.sub(r'\b\d{3}\s*-\s*\d{7,}\b', '', text)
    found = []
    seen = set()
    for m in re.finditer(r'\b([A-Z]{2})\s*(\d{3,4})\b', clean):
        al, num = m.group(1), m.group(2)
        if al in AIRLINES:
            fn = al + num
            if fn not in seen:
                seen.add(fn)
                found.append(fn)
    return found

def extract_iata(text: str) -> list:
    """Findet IATA-Codes im Text."""
    found = []
    for m in re.finditer(r'\b([A-Z]{3})\b', text):
        code = m.group(1)
        if code in IATA:
            found.append(code)
    return list(dict.fromkeys(found))

def extract_amount(text: str) -> tuple:
    """Findet Gesamtbetrag und Währung."""
    # Explizite Totals zuerst
    patterns = [
        (r'(?:Total|Grand Total|Gesamtpreis|Endpreis|Gesamtbetrag|Total amount)[^\d]{0,20}([\d,\.]+)\s*(EUR|USD|CHF|GBP)', 1, 2),
        (r'(EUR|USD|CHF|GBP)\s*([\d,\.]+)', 2, 1),
        (r'([\d,\.]+)\s*(EUR|USD|CHF|GBP)', 1, 2),
    ]
    for pattern, ai, ci in patterns:
        for m in re.finditer(pattern, text, re.IGNORECASE):
            try:
                val_s = m.group(ai).replace(',', '.')
                parts = val_s.split('.')
                if len(parts) > 2:
                    val_s = ''.join(parts[:-1]) + '.' + parts[-1]
                val = float(val_s)
                if 1 < val < 999999:
                    curr = m.group(ci).upper()
                    return f"{val:.2f}", curr
            except: pass
    return "", "EUR"

def extract_vendor(text: str) -> str:
    """Erkennt Anbieter/Vendor aus Text."""
    VENDORS = [
        "Lufthansa","Swiss","Austrian Airlines","Air France","KLM","British Airways",
        "Edelweiss","Eurowings","Ryanair","easyJet","FLY AWAY","Condor",
        "Marriott","Sheraton","Hilton","Hyatt","Radisson","InterContinental",
        "Booking.com","Hotels.com","Expedia","Ibis","Novotel","NH Hotels","Melia",
        "Hertz","Sixt","Avis","Europcar","Enterprise",
        "Deutsche Bahn","DB","Eurostar","Thalys",
        "Uber","Lyft","Bolt","FreeNow",
        "Shell","BP","Aral","Total","Esso",
    ]
    tl = text.lower()
    for v in VENDORS:
        if v.lower() in tl:
            return v
    return ""

def detect_type(text: str, filename: str = "") -> str:
    """Erkennt Belegtyp. Flug hat höchste Priorität."""
    t = text.lower()
    fn = filename.lower()
    fns = extract_flight_numbers(text)
    # Flug: Flugnummern ODER explizite Flug-Keywords
    if fns:
        return "Flug"
    if any(x in t for x in ['e-ticket','eticket','boarding pass','flight confirmation',
                              'flugbestätigung','flugticket','ihr flug','your flight',
                              'itinerary','reiseangebot']):
        return "Flug"
    if any(x in fn for x in ['itinerary','ticket','flug','flight']):
        return "Flug"
    # Bahn VOR Hotel (hat auch "ticket")
    if any(x in t for x in ['deutsche bahn','db bahn','bahnticket','sitzplatz reservierung',
                              'zugticket','ice ','ice-']):
        return "Bahn"
    # Hotel: NUR wenn keine Flugnummern und explizite Hotel-Keywords
    if any(x in t for x in ['marriott','sheraton','hilton','hyatt','radisson',
                              'intercontinental','ibis','novotel','booking.com']):
        return "Hotel"
    if any(x in t for x in ['hotel reservation','hotel booking','zimmer reservierung',
                              'your stay at','ihr aufenthalt']):
        return "Hotel"
    if any(x in t for x in ['taxi','uber','lyft','bolt','fahrschein']):
        return "Taxi"
    if any(x in t for x in ['mietwagen','rental car','car rental','hertz','sixt','avis']):
        return "Mietwagen"
    if any(x in t for x in ['restaurant','dinner','lunch','speisen','bewirtung']):
        return "Bewirtung"
    if any(x in t for x in ['tankstelle','tanken','kraftstoff','shell','bp','aral']):
        return "Tanken"
    return "Sonstiges"

def extract_hotel_dates(text: str) -> tuple:
    """
    Findet Check-in und Check-out Datum.
    Unterstützt: "Check-in: Wednesday, April 22, 2026"
                 "Anreise: 22.04.2026"
                 "Check-in: 2026-04-22"
    """
    # Datum-Pattern: erlaubt Wochentag davor
    DATE_PAT = (r'(?:(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday|'
                r'Montag|Dienstag|Mittwoch|Donnerstag|Freitag|Samstag|Sonntag),?\s+)?'
                r'(\w+\s+\d{1,2},?\s+\d{4}'  # April 22, 2026
                r'|\d{1,2}[./]\d{1,2}[./]\d{4}'  # 22.04.2026
                r'|\d{4}-\d{2}-\d{2}'             # 2026-04-22
                r'|\d{1,2}\.?\s+\w+\s+\d{4})'  # 22. April 2026
               )
    ci_m = re.search(r'(?:Check.?in|Arrival|Anreise|Eincheck)[^\n]{0,50}?' + DATE_PAT,
                     text, re.IGNORECASE)
    co_m = re.search(r'(?:Check.?out|Departure|Abreise|Auscheck)[^\n]{0,50}?' + DATE_PAT,
                     text, re.IGNORECASE)
    checkin = parse_date(ci_m.group(1)) if ci_m else None
    checkout = parse_date(co_m.group(1)) if co_m else None
    return checkin, checkout

def extract_pnr(text: str) -> str:
    """Findet PNR/Buchungsreferenz – nur wenn explizit gelabelt."""
    m = re.search(
        r'(?:Buchungsreferenz|Buchungscode|PNR|Booking\s*Ref(?:erence)?|'
        r'Record\s*Locator|Confirmation\s*(?:Number|Code|#))\s*[:\s#]*([A-Z0-9]{5,8})\b',
        text, re.IGNORECASE)
    if m:
        val = m.group(1).upper()
        if not re.match(r'^(HOTEL|FLUG|BAHN|TRAIN|FLIGHT|BOOKING|REISE|ECMA)$', val):
            return val
    return ""

def extract_trip_code(text: str) -> str:
    """Findet Reisecode 26-xxx direkt im Text."""
    m = re.search(r'\b(\d{2}-\d{3})\b', text)
    return m.group(1) if m else ""

MONTHS_SHORT_MAP = {
    "jan":1,"feb":2,"mar":3,"mrz":3,"apr":4,"may":5,"mai":5,"jun":6,
    "jul":7,"aug":8,"sep":9,"okt":10,"oct":10,"nov":11,"dec":12,"dez":12
}

CITY_TO_IATA = {
    "frankfurt":"FRA","muenchen":"MUC","munich":"MUC","nuernberg":"NUE","nuremberg":"NUE",
    "nürnberg":"NUE","münchen":"MUC",
    "berlin":"BER","hamburg":"HAM","stuttgart":"STR","duesseldorf":"DUS","koeln":"CGN",
    "düsseldorf":"DUS","köln":"CGN","cologne":"CGN",
    "lyon":"LYS","paris":"CDG","london":"LHR","zuerich":"ZRH","zurich":"ZRH",
    "zürich":"ZRH","genf":"GVA","geneva":"GVA",
    "wien":"VIE","vienna":"VIE","rom":"FCO","rome":"FCO",
    "madrid":"MAD","barcelona":"BCN","amsterdam":"AMS",
    "san jose":"SJO","san josé":"SJO","costa rica":"SJO",
    "panama city":"PTY","panama":"PTY",
    "new york":"JFK","los angeles":"LAX","miami":"MIA","chicago":"ORD",
    "dubai":"DXB","singapur":"SIN","singapore":"SIN","tokio":"NRT","tokyo":"NRT",
    "doha":"DOH","istanbul":"IST","athen":"ATH","athens":"ATH",
}

IATA_TO_CITY = {
    "FRA":"Frankfurt","MUC":"München","NUE":"Nürnberg","BER":"Berlin","HAM":"Hamburg",
    "STR":"Stuttgart","DUS":"Düsseldorf","CGN":"Köln","LYS":"Lyon","CDG":"Paris",
    "LHR":"London Heathrow","ZRH":"Zürich","GVA":"Genf","VIE":"Wien","FCO":"Rom",
    "MAD":"Madrid","BCN":"Barcelona","AMS":"Amsterdam","BRU":"Brüssel",
    "SJO":"San José (CR)","PTY":"Panama City","JFK":"New York","LAX":"Los Angeles",
    "MIA":"Miami","ORD":"Chicago","SFO":"San Francisco","BOS":"Boston",
    "DXB":"Dubai","AUH":"Abu Dhabi","DOH":"Doha","SIN":"Singapur",
    "NRT":"Tokio","HKG":"Hongkong","PEK":"Peking","PVG":"Shanghai",
    "IST":"Istanbul","ATH":"Athen","WAW":"Warschau","PRG":"Prag",
    "OSL":"Oslo","ARN":"Stockholm","HEL":"Helsinki","CPH":"Kopenhagen",
    "LIS":"Lissabon","DUB":"Dublin","SYD":"Sydney","MEL":"Melbourne",
    "YYZ":"Toronto","YVR":"Vancouver","GRU":"São Paulo","EZE":"Buenos Aires",
    "MEX":"Mexico City","BOG":"Bogotá","SCL":"Santiago",
}

def city_name_to_iata(name: str) -> str:
    key = name.lower().strip()
    return CITY_TO_IATA.get(key, "")

def iata_to_name(code: str) -> str:
    return IATA_TO_CITY.get(code.upper(), "")

def extract_segments(text: str) -> list:
    """
    Extrahiert Flugsegmente aus beliebigen Formaten.
    Gibt Liste von dicts zurück: fn, von, von_name, nach, nach_name, datum, abflug, ankunft
    Unterstützt beliebig viele Segmente.
    """
    segs = []
    seen = set()

    # Jahr aus Text ermitteln
    jm = re.search(r"\b(202\d)\b", text)
    default_year = jm.group(1) if jm else "2026"

    # ── Pattern 1: Itinerary-Format ──────────────────────────────────────────
    # "20 Apr Nürnberg - Frankfurt LH 3463 13:00 - 14:15"
    # "25 Mai Frankfurt - Zurich LX 3613 06:35 - 07:30"
    p1 = re.compile(
        r"(\d{1,2})\s+(Jan|Feb|M[äa]r|Apr|Mai|May|Jun|Jul|Aug|Sep|Okt|Oct|Nov|Dez|Dec)\s+"
        r"([A-Za-z\xc4\xd6\xdc\xe4\xf6\xfc\xdf\s]+?)\s*-\s*([A-Za-z\xc4\xd6\xdc\xe4\xf6\xfc\xdf\s]+?)\s+"
        r"([A-Z]{2})\s*(\d{3,4})\s+"
        r"(\d{1,2}:\d{2})\s*[-–]\s*(\d{1,2}:\d{2})",
        re.IGNORECASE)
    for m in p1.finditer(text):
        tag, mon, von_s, nach_s, al, num, ab, an = m.groups()
        fn = al.upper() + num
        mon_num = MONTHS_SHORT_MAP.get(mon.lower()[:3], 1)
        dat = f"{int(tag):02d}.{mon_num:02d}.{default_year}"
        von_iata = city_name_to_iata(von_s.strip()) or von_s.strip()[:3].upper()
        nach_iata = city_name_to_iata(nach_s.strip()) or nach_s.strip()[:3].upper()
        key = fn + dat
        if key not in seen:
            seen.add(key)
            segs.append({
                "fn": fn, "datum": dat,
                "von": von_iata, "von_name": von_s.strip(),
                "nach": nach_iata, "nach_name": nach_s.strip(),
                "abflug": ab, "ankunft": an
            })

    # ── Pattern 2: IATA-Code Format ──────────────────────────────────────────
    # "LH3463 NUE→FRA 13:00→14:15" oder "LH3463 NUE->FRA 13:00->14:15"
    # Optional: mit Datum danach oder davor
    p2 = re.compile(
        r"([A-Z]{2}\d{3,4})\s+([A-Z]{3})\s*[→>\->–]+\s*([A-Z]{3})\s+"
        r"(\d{1,2}:\d{2})\s*[→>\->–]+\s*(\d{1,2}:\d{2})"
        r"(?:\s+(\d{1,2}[./]\d{1,2}[./]\d{4}|\d{4}-\d{2}-\d{2}))?",
        re.IGNORECASE)
    for m in p2.finditer(text):
        fn, von, nach, ab, an, dat = m.groups()
        dat = dat or default_year
        key = fn + (dat or ab)
        if key not in seen:
            seen.add(key)
            segs.append({
                "fn": fn, "datum": dat or "",
                "von": von, "von_name": iata_to_name(von),
                "nach": nach, "nach_name": iata_to_name(nach),
                "abflug": ab, "ankunft": an
            })

    # ── Pattern 3: Pipe-Format ───────────────────────────────────────────────
    # "LH3463|NUE|FRA|20.04.2026|13:00|14:15"
    p3 = re.compile(
        r"([A-Z]{2}\d{3,4})[|]([A-Z]{3})[|]([A-Z]{3})[|]"
        r"(\d{1,2}[./]\d{1,2}[./]\d{4}|\d{4}-\d{2}-\d{2})[|]"
        r"(\d{1,2}:\d{2})[|](\d{1,2}:\d{2})")
    for m in p3.finditer(text):
        fn, von, nach, dat, ab, an = m.groups()
        key = fn + dat
        if key not in seen:
            seen.add(key)
            segs.append({
                "fn": fn, "datum": dat,
                "von": von, "von_name": iata_to_name(von),
                "nach": nach, "nach_name": iata_to_name(nach),
                "abflug": ab, "ankunft": an
            })

    # ── Pattern 4: Tabellenformat mit Datum vorne ────────────────────────────
    # "20.04.2026  LH3463  NUE  FRA  13:00  14:15"
    p4 = re.compile(
        r"(\d{1,2}[./]\d{1,2}[./]\d{4}|\d{4}-\d{2}-\d{2})\s+"
        r"([A-Z]{2}\d{3,4})\s+([A-Z]{3})\s+([A-Z]{3})\s+"
        r"(\d{1,2}:\d{2})\s+(\d{1,2}:\d{2})")
    for m in p4.finditer(text):
        dat, fn, von, nach, ab, an = m.groups()
        key = fn + dat
        if key not in seen:
            seen.add(key)
            segs.append({
                "fn": fn, "datum": dat,
                "von": von, "von_name": iata_to_name(von),
                "nach": nach, "nach_name": iata_to_name(nach),
                "abflug": ab, "ankunft": an
            })

    # ── Pattern 5: Lufthansa Bestätigungsmail ───────────────────────────────
    # Blöcke mit: "Datum der Abreise XX.XX.XXXX Abflugzeit HH:MM"
    #             "IATA-Code des Abflughafens XXX"
    #             "Abflugsort STADTNAME"
    if not segs and "IATA-Code des Abflughafens" in text:
        blocks = re.split(r"(?=\d{2}\.\d{2}\.\d{4}\s*-\s*\d{2}:\d{2})", text)
        for block in blocks:
            dm = re.search(r"(\d{2}\.\d{2}\.\d{4})\s*[–\-]\s*(\d{2}:\d{2})", block)
            if not dm: continue
            datum, abflug = dm.group(1), dm.group(2)
            id_m = re.search(r"IATA-Code des Abflughafens\s+([A-Z]{3})", block)
            ia_m = re.search(r"IATA-Code des Ankunftsflughafens\s+([A-Z]{3})", block)
            vn_m = re.search(r"Abflugsort\s+([^\r\n]+)", block)
            nm_m = re.search(r"Ankunftsort\s+([^\r\n]+)", block)
            iata_dep = id_m.group(1) if id_m else ""
            iata_arr = ia_m.group(1) if ia_m else ""
            von_name = vn_m.group(1).strip() if vn_m else iata_to_name(iata_dep)
            nach_name = nm_m.group(1).strip() if nm_m else iata_to_name(iata_arr)
            key = datum + iata_dep + iata_arr
            if key not in seen:
                seen.add(key)
                segs.append({
                    "fn": "",
                    "datum": datum,
                    "von": iata_dep, "von_name": von_name,
                    "nach": iata_arr, "nach_name": nach_name,
                    "abflug": abflug, "ankunft": "–"
                })

    return segs


def analyse_beleg(text: str, filename: str = "") -> dict:
    """
    Analysiert einen Belegtext und gibt alle erkannten Felder zurück.
    Nur was wirklich gefunden wurde – kein Raten.
    """
    typ = detect_type(text, filename)
    vendor = extract_vendor(text)
    betrag, waehrung = extract_amount(text)
    pnr = extract_pnr(text)
    trip_code = extract_trip_code(text)
    fns = extract_flight_numbers(text)
    iatas = extract_iata(text)
    alle_daten = extract_dates(text)

    # Hotel-Daten
    checkin = checkout = None
    naechte = 0
    if typ == "Hotel":
        checkin, checkout = extract_hotel_dates(text)
        if checkin and checkout:
            naechte = (checkout - checkin).days

    # Konfirmationsnummer (Hotels)
    conf_m = re.search(r'(?:Confirmation|Bestätigungs|Reservierungs)(?:\s*Number|\s*Nr\.?|#)\s*[:\s]*([A-Z0-9]{6,12})', text, re.IGNORECASE)
    konfirmation = conf_m.group(1) if conf_m else ""

    # Name des Reisenden
    name_m = re.search(r'(?:Guest|Gast|Passenger|Reisender|Name)[:\s]+([A-ZÜÖÄ][a-züöä]+\s+[A-ZÜÖÄ][a-züöä]+)', text)
    reisender = name_m.group(1) if name_m else ""

    segs = extract_segments(text) if (typ == "Flug" or fns) else []
    return {
        "typ": typ,
        "vendor": vendor,
        "reisender": reisender,
        "betrag": betrag,
        "waehrung": waehrung,
        "pnr": pnr,
        "trip_code": trip_code,
        "flugnummern": fns,
        "iata_codes": iatas,
        "alle_daten": [str(d) for d in alle_daten],
        "checkin": str(checkin) if checkin else "",
        "checkout": str(checkout) if checkout else "",
        "naechte": naechte,
        "konfirmation": konfirmation,
        "segmente": segs,
    }

# ─── Datenbank-Schema ─────────────────────────────────────────────────────────
SCHEMA = """
CREATE TABLE IF NOT EXISTS reisen (
    code        TEXT PRIMARY KEY,
    kuerzel     TEXT,
    klarname    TEXT,
    titel       TEXT,
    abreise     DATE,
    rueckkehr   DATE,
    erstellt    TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS belege (
    id              SERIAL PRIMARY KEY,
    mail_uid        TEXT,
    reise_code      TEXT REFERENCES reisen(code) ON DELETE SET NULL,
    dateiname       TEXT,
    typ             TEXT,
    vendor          TEXT,
    reisender       TEXT,
    betrag          TEXT,
    waehrung        TEXT DEFAULT 'EUR',
    betrag_eur      TEXT,
    pnr             TEXT,
    konfirmation    TEXT,
    flugnummern     TEXT,
    iata_codes      TEXT,
    checkin         DATE,
    checkout        DATE,
    naechte         INTEGER,
    belegdatum      DATE,
    alle_daten      TEXT,
    rohtext         TEXT,
    storage_key     TEXT,
    analyse_json    TEXT,
    status          TEXT DEFAULT 'neu',
    notiz           TEXT,
    erstellt        TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mails (
    id          SERIAL PRIMARY KEY,
    uid         TEXT UNIQUE,
    message_id  TEXT UNIQUE,
    absender    TEXT,
    betreff     TEXT,
    body        TEXT,
    erstellt    TIMESTAMP DEFAULT now()
);
"""

# ─── EUR Umrechnung ───────────────────────────────────────────────────────────
RATES = {"EUR": 1.0, "USD": 0.92, "CHF": 1.03, "GBP": 1.17, "JPY": 0.006}

def to_eur(betrag: str, waehrung: str) -> str:
    if not betrag: return ""
    try:
        val = float(betrag)
        rate = RATES.get(waehrung.upper(), 1.0)
        return f"{val * rate:.2f}"
    except: return ""

# ─── IMAP Mail-Import ─────────────────────────────────────────────────────────
_imap_lock = threading.Lock()

def fetch_mails_now() -> dict:
    """Holt alle Mails vom IMAP-Server und speichert sie in der DB."""
    if not all([IMAP_HOST, IMAP_USER, IMAP_PASS]):
        return {"error": "IMAP nicht konfiguriert"}

    db = get_db(); cur = db.cursor()
    imported = dupl = fehler = belege_erstellt = 0
    fehler_details = []

    try:
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")
        _, data = mail.search(None, "ALL")
        ids = data[0].split() if data and data[0] else []
    except Exception as e:
        cur.close(); db.close()
        return {"error": str(e)}

    ids_to_delete = []

    for mid in ids:
        uid = mid.decode()
        try:
            # Duplikat-Check
            cur.execute("SELECT id FROM mails WHERE uid=%s", (uid,))
            if cur.fetchone():
                ids_to_delete.append(mid)
                dupl += 1
                continue

            _, msg_data = mail.fetch(mid, "(RFC822)")
            if not msg_data or not msg_data[0]: continue
            msg = email.message_from_bytes(msg_data[0][1])

            betreff = decode_header_value(msg.get("Subject", ""))
            absender = decode_header_value(msg.get("From", ""))
            msg_id = (msg.get("Message-ID", "") or "").strip()

            # Duplikat via Message-ID
            if msg_id:
                cur.execute("SELECT id FROM mails WHERE message_id=%s", (msg_id,))
                if cur.fetchone():
                    ids_to_delete.append(mid)
                    dupl += 1
                    continue

            # Body extrahieren
            body = ""
            html_body = ""
            attachments = []

            if msg.is_multipart():
                for part in msg.walk():
                    ct = part.get_content_type()
                    cd = str(part.get("Content-Disposition") or "").lower()
                    fn = part.get_filename()
                    payload = part.get_payload(decode=True)
                    if not payload: continue

                    if fn and ("attachment" in cd or fn):
                        # Anhang
                        decoded_fn = decode_header_value(fn)
                        attachments.append({
                            "filename": decoded_fn,
                            "data": payload,
                            "content_type": ct
                        })
                    elif ct == "text/plain" and not body:
                        body = payload.decode(errors="ignore")
                    elif ct == "text/html" and not html_body:
                        html_body = payload.decode(errors="ignore")
            else:
                payload = msg.get_payload(decode=True)
                if payload:
                    ct = msg.get_content_type()
                    if ct == "text/html":
                        html_body = payload.decode(errors="ignore")
                    else:
                        body = payload.decode(errors="ignore")

            if not body and html_body:
                body = html_to_text(html_body)

            # Mail speichern
            cur.execute(
                "INSERT INTO mails (uid, message_id, absender, betreff, body) "
                "VALUES (%s,%s,%s,%s,%s)",
                (uid, msg_id or None, absender, betreff, body[:50000]))

            # Reisecode aus Betreff+Body
            full_text = betreff + "\n" + body
            rcode_raw = extract_trip_code(full_text)
            # Nur zuordnen wenn Reise existiert
            rcode = None
            if rcode_raw:
                cur.execute("SELECT code FROM reisen WHERE code=%s", (rcode_raw,))
                if cur.fetchone():
                    rcode = rcode_raw

            # Beleg aus Mail-Body erstellen
            info = analyse_beleg(full_text, betreff)
            if info["typ"] not in ("Sonstiges",) or info["vendor"] or info["flugnummern"]:
                eur = to_eur(info["betrag"], info["waehrung"])
                ci = parse_date(info["checkin"]) if info["checkin"] else None
                co = parse_date(info["checkout"]) if info["checkout"] else None
                bd = parse_date(info["alle_daten"][0]) if info["alle_daten"] else None
                if bd and ci and bd.year == ci.year:
                    bd = ci  # Belegdatum = Check-in wenn selbes Jahr
                cur.execute("""INSERT INTO belege
                    (mail_uid, reise_code, dateiname, typ, vendor, reisender,
                     betrag, waehrung, betrag_eur, pnr, konfirmation,
                     flugnummern, iata_codes, checkin, checkout, naechte,
                     belegdatum, alle_daten, rohtext, analyse_json, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (uid, rcode or None,
                     "Mail: " + betreff[:80], info["typ"], info["vendor"], info["reisender"],
                     info["betrag"] or None, info["waehrung"], eur or None,
                     info["pnr"] or None, info["konfirmation"] or None,
                     ",".join(info["flugnummern"]) or None,
                     ",".join(info["iata_codes"]) or None,
                     ci, co, info["naechte"] or None,
                     bd, ",".join(info["alle_daten"]) or None,
                     full_text[:10000], json.dumps(info, ensure_ascii=False),
                     "ausstehend" if not rcode else "zugeordnet"))
                belege_erstellt += 1

            # PDF/Bild-Anhänge verarbeiten
            for att in attachments:
                fn_lower = att["filename"].lower()
                # Inline-Bilder überspringen
                if re.match(r'image\d+\.(png|jpg|jpeg|gif|emz|wmz)$', fn_lower): continue
                if fn_lower.endswith((".ics", ".emz", ".wmz")): continue

                h = file_hash(att["data"])
                cur.execute("SELECT id FROM belege WHERE storage_key=%s", ("hash:" + h,))
                if cur.fetchone(): continue

                # PDF analysieren
                att_text = ""
                if fn_lower.endswith(".pdf"):
                    try:
                        import pypdf as _pypdf
                        reader = _pypdf.PdfReader(io.BytesIO(att["data"]))
                        att_text = "\n".join(p.extract_text() or "" for p in reader.pages)
                    except: pass

                # S3 Upload
                skey = f"belege/{uid}/{att['filename']}"
                try:
                    s3 = get_s3()
                    s3.put_object(Bucket=S3_BUCKET, Key=skey, Body=att["data"],
                                  ContentType=att["content_type"])
                except Exception as s3e:
                    skey = "s3-fehler:" + str(s3e)[:60]

                analyse_text = att_text or full_text
                info2 = analyse_beleg(analyse_text, att["filename"])
                eur2 = to_eur(info2["betrag"], info2["waehrung"])
                ci2 = parse_date(info2["checkin"]) if info2["checkin"] else None
                co2 = parse_date(info2["checkout"]) if info2["checkout"] else None
                bd2 = parse_date(info2["alle_daten"][0]) if info2["alle_daten"] else None
                if bd2 and ci2 and bd2.year == ci2.year: bd2 = ci2

                cur.execute("""INSERT INTO belege
                    (mail_uid, reise_code, dateiname, typ, vendor, reisender,
                     betrag, waehrung, betrag_eur, pnr, konfirmation,
                     flugnummern, iata_codes, checkin, checkout, naechte,
                     belegdatum, alle_daten, rohtext, storage_key, analyse_json, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (uid, rcode or None,
                     att["filename"], info2["typ"], info2["vendor"], info2["reisender"],
                     info2["betrag"] or None, info2["waehrung"], eur2 or None,
                     info2["pnr"] or None, info2["konfirmation"] or None,
                     ",".join(info2["flugnummern"]) or None,
                     ",".join(info2["iata_codes"]) or None,
                     ci2, co2, info2["naechte"] or None,
                     bd2, ",".join(info2["alle_daten"]) or None,
                     att_text[:10000] or None, "hash:" + h,
                     json.dumps(info2, ensure_ascii=False),
                     "ausstehend" if not rcode else "zugeordnet"))
                belege_erstellt += 1

            db.commit()
            ids_to_delete.append(mid)
            imported += 1

        except Exception as e:
            import traceback
            msg = f"{type(e).__name__}: {e}"
            print(f"[Mail Fehler] {msg}")
            fehler_details.append(msg[:200])
            fehler += 1
            try: db.rollback()
            except: pass

    cur.close(); db.close()

    for mid in ids_to_delete:
        try: mail.store(mid, "+FLAGS", "\\Deleted")
        except: pass
    try:
        if ids_to_delete: mail.expunge()
        mail.logout()
    except: pass

    return {"importiert": imported, "duplikate": dupl, "belege": belege_erstellt, "fehler": fehler, "fehler_details": fehler_details}

# Auto-Fetch deaktiviert - nur manuell via /mails-abrufen

# ─── FastAPI App ──────────────────────────────────────────────────────────────
app = FastAPI(title="Herrhammer Reisekosten", version=APP_VERSION)

if not os.path.exists("static"):
    os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ─── CSS ──────────────────────────────────────────────────────────────────────
CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
       background: #f1f5f9; color: #0f172a; font-size: 14px; }
nav { background: #1e293b; padding: 0 24px; display: flex; align-items: center;
      gap: 24px; position: sticky; top: 0; z-index: 100;
      box-shadow: 0 2px 8px rgba(0,0,0,.25); }
.logo img { height: 28px; width: auto; display: block; padding: 10px 0; }
.nav-link { color: #94a3b8; text-decoration: none; font-size: 13px; font-weight: 500;
            padding: 16px 4px; border-bottom: 2px solid transparent; transition: color .15s; }
.nav-link:hover, .nav-link.on { color: white; border-bottom-color: #3b82f6; }
.nav-right { margin-left: auto; display: flex; align-items: center; gap: 8px; }
.version { font-size: 11px; color: #475569; }
main { padding: 24px; max-width: 1100px; margin: 0 auto; }
h1 { font-size: 20px; font-weight: 700; margin-bottom: 20px; color: #0f172a; }
h2 { font-size: 16px; font-weight: 600; margin-bottom: 12px; color: #1e293b; }
.card { background: white; border: 1px solid #e2e8f0; border-radius: 10px;
        padding: 20px; box-shadow: 0 1px 3px rgba(0,0,0,.06); margin-bottom: 16px; }
.btn { display: inline-flex; align-items: center; gap: 6px; padding: 8px 18px;
       background: #2563eb; color: white; border: none; border-radius: 6px;
       font-size: 13px; font-weight: 600; cursor: pointer; text-decoration: none;
       transition: background .15s; }
.btn:hover { background: #1d4ed8; }
.btn-g { background: #059669; }
.btn-g:hover { background: #047857; }
.btn-o { background: #d97706; }
.btn-o:hover { background: #b45309; }
.btn-s { background: white; color: #374151; border: 1px solid #d1d5db; }
.btn-s:hover { background: #f9fafb; }
.acts { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 20px; }
table { width: 100%; border-collapse: collapse; }
th { text-align: left; padding: 8px 12px; font-size: 11px; font-weight: 600;
     color: #64748b; border-bottom: 2px solid #e2e8f0; white-space: nowrap; }
td { padding: 10px 12px; font-size: 13px; border-bottom: 1px solid #f1f5f9;
     vertical-align: middle; }
tr:hover td { background: #f8fafc; }
.badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
         font-size: 11px; font-weight: 700; }
.b-flug { background: #dbeafe; color: #1e40af; }
.b-hotel { background: #dcfce7; color: #166534; }
.b-taxi { background: #fef3c7; color: #92400e; }
.b-bahn { background: #e0e7ff; color: #3730a3; }
.b-mietwagen { background: #fce7f3; color: #9d174d; }
.b-bewirtung { background: #fff7ed; color: #9a3412; }
.b-tanken { background: #f0fdf4; color: #14532d; }
.b-sonstiges { background: #f1f5f9; color: #475569; }
.inp { width: 100%; padding: 8px 10px; border: 1px solid #d1d5db; border-radius: 6px;
       font-size: 13px; }
.inp:focus { outline: none; border-color: #2563eb;
             box-shadow: 0 0 0 3px rgba(37,99,235,.1); }
.sel { width: 100%; padding: 8px 10px; border: 1px solid #d1d5db; border-radius: 6px;
       font-size: 13px; background: white; }
.grid2 { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
.grid3 { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 12px; }
.full { grid-column: 1 / -1; }
label { display: block; font-size: 12px; font-weight: 600; color: #374151;
        margin-bottom: 4px; }
.hint { font-size: 11px; color: #94a3b8; margin-top: 2px; }
.empty { text-align: center; padding: 48px 20px; color: #94a3b8; }
.alert { padding: 12px 16px; border-radius: 8px; font-size: 13px; margin-bottom: 16px; }
.alert-w { background: #fef3c7; border: 1px solid #fbbf24; color: #92400e; }
.alert-ok { background: #dcfce7; border: 1px solid #4ade80; color: #166534; }
.kv { display: grid; grid-template-columns: 160px 1fr; gap: 4px 16px; }
.kv dt { font-size: 12px; color: #64748b; padding: 4px 0; }
.kv dd { font-size: 13px; font-weight: 500; padding: 4px 0; border-bottom: 1px solid #f1f5f9; }
.postbox { background: #fffbeb; border: 1px solid #f59e0b; border-radius: 10px;
           padding: 16px 20px; margin-bottom: 20px; }
"""

# ─── HTML Shell ───────────────────────────────────────────────────────────────
def shell(title: str, body: str, page: str = "") -> str:
    def nl(p, lbl, url):
        on = ' on' if page == p else ''
        return f'<a href="{url}" class="nav-link{on}">{lbl}</a>'
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title} – Herrhammer Reisekosten</title>
<style>{CSS}</style>
</head>
<body>
<nav>
  <div class="logo"><a href="/"><img src="/static/herrhammer-logo.png" alt="Herrhammer"></a></div>
  {nl("start","Dashboard","/")}
  {nl("belege","Belege","/belege")}
  {nl("reisen","Reisen","/reisen")}
  {nl("posteingang","📬 Posteingang","/posteingang")}
  <div class="nav-right">
    <span class="version">v{APP_VERSION}</span>
  </div>
</nav>
<main>
{body}
</main>
</body>
</html>"""

# ─── Routen ───────────────────────────────────────────────────────────────────

@app.get("/init")
def init():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute(SCHEMA)
        db.commit(); cur.close(); db.close()
        return {"status": "ok", "version": APP_VERSION}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}

@app.get("/version")
def version():
    return {"version": APP_VERSION}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    try:
        db = get_db(); cur = db.cursor()

        # Reisen
        cur.execute("SELECT code, titel, klarname, abreise, rueckkehr FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()

        # Belege ohne Reise
        cur.execute("SELECT COUNT(*) FROM belege WHERE reise_code IS NULL")
        unzugeordnet = cur.fetchone()[0]

        # Belege gesamt
        cur.execute("SELECT COUNT(*) FROM belege")
        belege_gesamt = cur.fetchone()[0]

        cur.close(); db.close()

        # Posteingang-Banner
        postbox = ""
        if unzugeordnet > 0:
            postbox = (f'<div class="postbox">'
                       f'<b>📬 {unzugeordnet} Beleg{"e" if unzugeordnet>1 else ""} im Posteingang</b> '
                       f'– noch keiner Reise zugeordnet. '
                       f'<a href="/posteingang" style="color:#92400e;font-weight:600">Jetzt zuordnen →</a>'
                       f'</div>')

        # Reise-Karten
        today = date.today()
        aktiv = []; geplant = []; fertig = []
        for r in reisen:
            code, titel, name, ab, zu = r
            ab_d = ab if isinstance(ab, date) else (date.fromisoformat(str(ab)) if ab else None)
            zu_d = zu if isinstance(zu, date) else (date.fromisoformat(str(zu)) if zu else None)
            if not ab_d: geplant.append(r)
            elif today < ab_d: geplant.append(r)
            elif zu_d and today > zu_d: fertig.append(r)
            else: aktiv.append(r)

        def karte(r, farbe):
            code, titel, name, ab, zu = r
            ab_s = ab.strftime("%d.%m.%Y") if isinstance(ab, date) else str(ab or "")
            zu_s = zu.strftime("%d.%m.%Y") if isinstance(zu, date) else str(zu or "")
            return (f'<a href="/reise/{code}" style="text-decoration:none;color:inherit;display:block">'
                    f'<div style="background:white;border:1px solid #e2e8f0;border-left:4px solid {farbe};'
                    f'border-radius:8px;padding:14px 18px;margin-bottom:8px;'
                    f'box-shadow:0 1px 2px rgba(0,0,0,.05);transition:box-shadow .15s" '
                    f'onmouseover="this.style.boxShadow=\'0 4px 12px rgba(0,0,0,.1)\'" '
                    f'onmouseout="this.style.boxShadow=\'0 1px 2px rgba(0,0,0,.05)\'">'
                    f'<div style="font-size:11px;color:#94a3b8;font-family:monospace">{code}</div>'
                    f'<div style="font-weight:700;font-size:15px;margin:4px 0 2px">{titel or code}</div>'
                    f'<div style="font-size:12px;color:#64748b">👤 {name or ""} &nbsp;·&nbsp; '
                    f'📅 {ab_s} – {zu_s}</div>'
                    f'</div></a>')

        aktiv_html = "".join(karte(r, "#10b981") for r in aktiv) or '<p class="empty">Keine aktiven Reisen</p>'
        geplant_html = "".join(karte(r, "#3b82f6") for r in geplant) or '<p style="color:#94a3b8;padding:8px 0">Keine geplanten Reisen</p>'
        fertig_html = "".join(karte(r, "#94a3b8") for r in fertig) or '<p style="color:#94a3b8;padding:8px 0">Keine abgeschlossenen Reisen</p>'

        html = f"""
        {postbox}
        <div class="acts">
          <a href="/mails-abrufen" class="btn">📥 Mails abrufen</a>
          <a href="/beleg-upload" class="btn btn-s">📎 Beleg hochladen</a>
          <a href="/reise-neu" class="btn btn-g">+ Neue Reise</a>
        </div>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:12px;margin-bottom:8px">
          <div class="card" style="padding:16px;text-align:center">
            <div style="font-size:28px;font-weight:700;color:#2563eb">{belege_gesamt}</div>
            <div style="font-size:11px;color:#64748b">Belege gesamt</div>
          </div>
          <div class="card" style="padding:16px;text-align:center">
            <div style="font-size:28px;font-weight:700;color:#d97706">{unzugeordnet}</div>
            <div style="font-size:11px;color:#64748b">Unzugeordnet</div>
          </div>
        </div>
        <h2>🟢 Aktive Reisen ({len(aktiv)})</h2>
        {aktiv_html}
        <h2 style="margin-top:20px">📋 Geplant ({len(geplant)})</h2>
        {geplant_html}
        <h2 style="margin-top:20px">✓ Abgeschlossen ({len(fertig)})</h2>
        {fertig_html}
        """
        return HTMLResponse(shell("Dashboard", html, "start"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler", f'<div class="card"><h2>Fehler</h2><p>{e}</p>'
                                  f'<pre style="font-size:11px;margin-top:12px">{traceback.format_exc()[:600]}</pre>'
                                  f'<p style="margin-top:12px"><a href="/init" class="btn">→ /init aufrufen</a></p></div>'))

@app.get("/mails-abrufen", response_class=HTMLResponse)
def mails_abrufen_page():
    with _imap_lock:
        result = fetch_mails_now()
    if "error" in result:
        body = f'<div class="card"><div class="alert alert-w"><b>Fehler:</b> {result["error"]}</div><a href="/" class="btn btn-s" style="margin-top:12px">← Zurück</a></div>'
    else:
        details = result.get("fehler_details", [])
        details_html = ""
        if details:
            items = "".join(f"<li style='margin:4px 0'>{d}</li>" for d in details)
            details_html = f'<div class="alert alert-w" style="margin-top:12px"><b>Fehlerdetails:</b><ul style="margin-top:8px;padding-left:16px">{items}</ul></div>'
        body = (f'<div class="card">'
                f'<h1>📥 Mails abgerufen</h1>'
                f'<div class="alert alert-ok" style="margin-bottom:12px">'
                f'✓ {result["importiert"]} neue Mails · {result["belege"]} Belege erstellt · '
                f'{result["duplikate"]} Duplikate · {result["fehler"]} Fehler'
                f'</div>'
                f'{details_html}'
                f'<div class="acts" style="margin-top:16px">'
                f'<a href="/belege" class="btn">📋 Belege ansehen</a>'
                f'<a href="/posteingang" class="btn btn-o">📬 Posteingang</a>'
                f'<a href="/" class="btn btn-s">← Dashboard</a>'
                f'</div></div>')
    return HTMLResponse(shell("Mails abrufen", body))

@app.get("/belege", response_class=HTMLResponse)
def belege_liste():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT id, dateiname, typ, vendor, reisender, betrag, waehrung,
            betrag_eur, checkin, checkout, belegdatum, reise_code, status, pnr, flugnummern
            FROM belege ORDER BY id DESC LIMIT 100""")
        rows = cur.fetchall()
        cur.close(); db.close()

        TYPE_BADGE = {
            "Flug":"b-flug","Hotel":"b-hotel","Taxi":"b-taxi","Bahn":"b-bahn",
            "Mietwagen":"b-mietwagen","Bewirtung":"b-bewirtung","Tanken":"b-tanken",
        }

        def fmt_date(d):
            if not d: return ""
            if isinstance(d, date): return d.strftime("%d.%m.%Y")
            return str(d)[:10]

        zeilen = ""
        for r in rows:
            bid, datei, typ, vendor, reisend, bet, curr, eur, ci, co, bd, rcode, stat, pnr, fns = r
            bc = TYPE_BADGE.get(typ, "b-sonstiges")
            betrag_s = f"{eur} €" if eur else (f"{bet} {curr}" if bet else "–")
            datum_s = fmt_date(ci or bd)
            name_s = vendor or reisend or "–"
            farbe = "#10b981" if stat == "zugeordnet" else "#f59e0b"
            zeilen += (f'<tr>'
                       f'<td><a href="/beleg/{bid}" style="color:#2563eb;font-weight:600">#{bid}</a></td>'
                       f'<td><span class="badge {bc}">{typ}</span></td>'
                       f'<td style="max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{name_s}</td>'
                       f'<td style="font-weight:600;color:#059669">{betrag_s}</td>'
                       f'<td style="font-family:monospace;font-size:12px">{datum_s}</td>'
                       f'<td style="font-family:monospace;font-size:12px;color:#2563eb">{rcode or ""}</td>'
                       f'<td><span style="font-size:11px;color:{farbe};font-weight:600">{stat}</span></td>'
                       f'<td><a href="/beleg/{bid}" class="btn btn-s" style="padding:4px 10px;font-size:11px">Ansehen</a></td>'
                       f'</tr>')

        if not zeilen:
            zeilen = '<tr><td colspan="8" class="empty">Keine Belege vorhanden</td></tr>'

        body = f"""
        <h1>📋 Belege ({len(rows)})</h1>
        <div class="acts">
          <a href="/mails-abrufen" class="btn">📥 Mails abrufen</a>
          <a href="/posteingang" class="btn btn-o">📬 Posteingang</a>
          <a href="/beleg-upload" class="btn btn-s">📎 Hochladen</a>
        </div>
        <div class="card" style="padding:0;overflow:hidden">
        <table>
          <thead><tr>
            <th>#</th><th>Typ</th><th>Anbieter/Name</th><th>Betrag</th>
            <th>Datum</th><th>Reise</th><th>Status</th><th></th>
          </tr></thead>
          <tbody>{zeilen}</tbody>
        </table>
        </div>"""
        return HTMLResponse(shell("Belege", body, "belege"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="card"><p>{e}</p></div>'))

@app.get("/beleg/{bid}", response_class=HTMLResponse)
def beleg_detail(bid: int):
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT id, dateiname, typ, vendor, reisender, betrag, waehrung,
            betrag_eur, pnr, konfirmation, flugnummern, iata_codes,
            checkin, checkout, naechte, belegdatum, alle_daten,
            reise_code, status, notiz, analyse_json, rohtext, storage_key
            FROM belege WHERE id=%s""", (bid,))
        r = cur.fetchone()

        # Alle Reisen für Dropdown
        cur.execute("SELECT code, titel FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.close(); db.close()

        if not r:
            return HTMLResponse(shell("Nicht gefunden", '<div class="card">Beleg nicht gefunden</div>'))

        (bid2, datei, typ, vendor, reisend, bet, curr, eur, pnr, konf, fns, iatas,
         ci, co, naechte, bd, alle_d, rcode, stat, notiz, analyse_json, rohtext, skey) = r

        def fmt(d):
            if not d: return "–"
            if isinstance(d, date): return d.strftime("%d.%m.%Y")
            return str(d)[:10]

        TYPE_BADGE = {"Flug":"b-flug","Hotel":"b-hotel","Taxi":"b-taxi","Bahn":"b-bahn",
                      "Mietwagen":"b-mietwagen","Bewirtung":"b-bewirtung","Tanken":"b-tanken"}
        bc = TYPE_BADGE.get(typ, "b-sonstiges")

        TYPE_ICON = {"Flug":"✈","Hotel":"🏨","Taxi":"🚕","Bahn":"🚆",
                     "Mietwagen":"🚗","Bewirtung":"🍽","Tanken":"⛽","Sonstiges":"📄"}
        icon = TYPE_ICON.get(typ, "📄")

        # IATA mit Klarname
        iata_list = [i.strip() for i in (iatas or "").split(",") if i.strip()]
        iata_html = " → ".join(f'<b>{code2}</b> <span style="color:#64748b;font-size:12px">({IATA.get(code2,"")})</span>'
                                for code2 in iata_list) if iata_list else "–"

        # Segmente aus analyse_json
        segs = []
        if analyse_json:
            try:
                aj = json.loads(analyse_json)
                segs = aj.get("segmente", [])
            except: pass

        seg_table = ""
        if segs:
            rows_s = ""
            for s in segs:
                von_s = s.get("von","")
                von_n = s.get("von_name","")
                nach_s = s.get("nach","")
                nach_n = s.get("nach_name","")
                von_full = f"{von_s} ({von_n})" if von_n else von_s
                nach_full = f"{nach_s} ({nach_n})" if nach_n else nach_s
                rows_s += (f"<tr>"
                           f"<td style='font-family:monospace;font-weight:700;color:#1d4ed8'>{s.get('fn','')}</td>"
                           f"<td>{von_full or '–'}</td>"
                           f"<td style='color:#64748b'>→</td>"
                           f"<td>{nach_full or '–'}</td>"
                           f"<td style='font-family:monospace'>{s.get('datum','')}</td>"
                           f"<td style='font-family:monospace'>{s.get('abflug','')}</td>"
                           f"<td style='font-family:monospace'>{s.get('ankunft','')}</td>"
                           f"</tr>")
            seg_table = (f"<div style='margin-top:16px'>"
                         f"<h2 style='margin-bottom:8px'>✈ Flugsegmente ({len(segs)})</h2>"
                         f"<div style='overflow-x:auto'><table>"
                         f"<thead><tr><th>Flug</th><th>Von</th><th></th><th>Nach</th>"
                         f"<th>Datum</th><th>Abflug</th><th>Ankunft</th></tr></thead>"
                         f"<tbody>{rows_s}</tbody></table></div></div>")

        # Reise-Dropdown Options
        opts = '<option value="">– Keine Reise –</option>'
        for rc, rt in reisen:
            sel = ' selected' if rc == rcode else ''
            opts += f'<option value="{rc}"{sel}>{rc} · {rt or rc}</option>'

        # Qualitätsanzeige
        felder = [bool(vendor), bool(bet), bool(ci or bd), bool(fns or iatas)]
        qual = sum(felder)
        qual_farbe = "#10b981" if qual >= 3 else "#f59e0b" if qual >= 2 else "#ef4444"
        qual_text = ["Niedrig – bitte prüfen", "Niedrig – bitte prüfen",
                     "Mittel – OK", "Gut", "Sehr gut"][qual]

        body = f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
          <a href="/belege" class="btn btn-s">← Belege</a>
          <a href="/beleg/{bid}/reanalyse" class="btn btn-s">🔄 Neu analysieren</a>
          <a href="/alle-reanalyse" class="btn btn-s" onclick="return confirm('Alle Belege neu analysieren?')">🔄 Alle neu</a>
          <h1 style="margin:0">{icon} Beleg #{bid} – {typ}</h1>
          <span class="badge {bc}">{typ}</span>
        </div>

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">

        <!-- Erkannte Daten -->
        <div class="card">
          <h2>📊 Erkannte Informationen</h2>
          <dl class="kv">
            <dt>Typ</dt><dd><span class="badge {bc}">{typ}</span></dd>
            <dt>Anbieter</dt><dd>{vendor or '<span style="color:#ef4444">nicht erkannt</span>'}</dd>
            <dt>Reisender</dt><dd>{reisend or '–'}</dd>
            <dt>Betrag</dt><dd style="font-weight:700;color:#059669">{bet or '–'} {curr if bet else ''}{(' = ' + eur + ' €') if eur and curr != 'EUR' else ''}</dd>
            <dt>PNR</dt><dd style="font-family:monospace">{pnr or '–'}</dd>
            <dt>Konfirmation</dt><dd style="font-family:monospace">{konf or '–'}</dd>
            <dt>Flugnummern</dt><dd style="font-family:monospace;color:#2563eb">{fns or '–'}</dd>
            <dt>Route</dt><dd>{iata_html}</dd>
            <dt>Check-in</dt><dd>{fmt(ci)}</dd>
            <dt>Check-out</dt><dd>{fmt(co)}</dd>
            <dt>Nächte</dt><dd>{naechte or '–'}</dd>
            <dt>Belegdatum</dt><dd>{fmt(bd)}</dd>
            <dt>Alle Daten</dt><dd style="font-size:11px;color:#64748b">{alle_d or '–'}</dd>
          </dl>
          <div style="margin-top:12px;padding:8px 12px;border-radius:6px;border:1px solid {qual_farbe};
               background:{qual_farbe}11;font-size:12px;color:{qual_farbe}">
            <b>Erkennungsqualität: {qual_text}</b>
            ({qual}/4 Felder erkannt)
          </div>
        </div>

        <!-- Zuordnung & Bearbeitung -->
        <div class="card">
          <h2>✏ Bearbeiten & Zuordnen</h2>
          <form method="post" action="/beleg/{bid}/speichern">
            <div style="display:grid;gap:10px">
              <div>
                <label>Reise zuordnen</label>
                <select name="reise_code" class="sel">{opts}</select>
              </div>
              <div>
                <label>Typ</label>
                <select name="typ" class="sel">
                  {''.join(f'<option{"selected" if t==typ else ""}>{t}</option>'
                   for t in ["Flug","Hotel","Taxi","Bahn","Mietwagen","Bewirtung","Tanken","Sonstiges"])}
                </select>
              </div>
              <div>
                <label>Anbieter / Name</label>
                <input class="inp" name="vendor" value="{vendor or ''}">
              </div>
              <div class="grid2">
                <div>
                  <label>Betrag</label>
                  <input class="inp" name="betrag" value="{bet or ''}">
                </div>
                <div>
                  <label>Währung</label>
                  <select name="waehrung" class="sel">
                    {''.join(f'<option{"selected" if c==curr else ""}>{c}</option>'
                     for c in ["EUR","USD","CHF","GBP","JPY"])}
                  </select>
                </div>
              </div>
              <div class="grid2">
                <div>
                  <label>Check-in / Datum</label>
                  <input class="inp" type="date" name="checkin"
                         value="{ci.isoformat() if isinstance(ci,date) else (str(ci)[:10] if ci else '')}">
                </div>
                <div>
                  <label>Check-out</label>
                  <input class="inp" type="date" name="checkout"
                         value="{co.isoformat() if isinstance(co,date) else (str(co)[:10] if co else '')}">
                </div>
              </div>
              <div>
                <label>Notiz</label>
                <input class="inp" name="notiz" value="{notiz or ''}" placeholder="Optionale Notiz">
              </div>
              <button type="submit" class="btn btn-g">✓ Speichern</button>
            </div>
          </form>
        </div>
        </div>

        {seg_table}

        <!-- Rohtext -->
        <div class="card" style="margin-top:16px">
          <h2>📄 Mail-Text / Dokumentinhalt</h2>
          <pre style="font-size:12px;white-space:pre-wrap;color:#374151;max-height:300px;
               overflow-y:auto;background:#f8fafc;padding:12px;border-radius:6px">{(rohtext or '').replace('<','&lt;').replace('>','&gt;')[:3000]}</pre>
        </div>
        """
        return HTMLResponse(shell(f"Beleg #{bid}", body, "belege"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler", f'<div class="card"><p>{e}</p><pre>{traceback.format_exc()[:400]}</pre></div>'))

@app.post("/beleg/{bid}/speichern")
async def beleg_speichern(bid: int, request: Request):
    try:
        form = await request.form()
        rcode = (form.get("reise_code") or "").strip() or None
        typ = (form.get("typ") or "Sonstiges").strip()
        vendor = (form.get("vendor") or "").strip()
        betrag = (form.get("betrag") or "").strip()
        waehrung = (form.get("waehrung") or "EUR").strip()
        checkin_s = (form.get("checkin") or "").strip()
        checkout_s = (form.get("checkout") or "").strip()
        notiz = (form.get("notiz") or "").strip()

        ci = parse_date(checkin_s) if checkin_s else None
        co = parse_date(checkout_s) if checkout_s else None
        naechte = (co - ci).days if ci and co else None
        eur = to_eur(betrag, waehrung) if betrag else None
        stat = "zugeordnet" if rcode else "ausstehend"

        db = get_db(); cur = db.cursor()
        cur.execute("""UPDATE belege SET
            reise_code=%s, typ=%s, vendor=%s, betrag=%s, waehrung=%s, betrag_eur=%s,
            checkin=%s, checkout=%s, naechte=%s, notiz=%s, status=%s
            WHERE id=%s""",
            (rcode, typ, vendor or None, betrag or None, waehrung, eur,
             ci, co, naechte, notiz or None, stat, bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"status": "fehler", "detail": str(e)}, status_code=500)

@app.get("/posteingang", response_class=HTMLResponse)
def posteingang():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT b.id, b.dateiname, b.typ, b.vendor, b.reisender,
            b.betrag, b.waehrung, b.betrag_eur, b.checkin, b.belegdatum,
            b.flugnummern, b.iata_codes, b.pnr
            FROM belege b WHERE b.reise_code IS NULL ORDER BY b.id DESC""")
        rows = cur.fetchall()
        cur.execute("SELECT code, titel, abreise FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.close(); db.close()

        if not rows:
            return HTMLResponse(shell("Posteingang",
                '<div class="card"><div class="alert alert-ok">✓ Alle Belege zugeordnet!</div>'
                '<a href="/" class="btn btn-s">← Dashboard</a></div>', "posteingang"))

        opts = '<option value="">– Reise wählen –</option>'
        for rc, rt, ab in reisen:
            ab_s = ab.strftime("%d.%m.%Y") if isinstance(ab, date) else str(ab or "")
            opts += f'<option value="{rc}">{rc} · {rt or rc} · {ab_s}</option>'

        TYPE_BADGE = {"Flug":"b-flug","Hotel":"b-hotel","Taxi":"b-taxi","Bahn":"b-bahn",
                      "Mietwagen":"b-mietwagen","Bewirtung":"b-bewirtung","Tanken":"b-tanken"}
        TYPE_ICON = {"Flug":"✈","Hotel":"🏨","Taxi":"🚕","Bahn":"🚆",
                     "Mietwagen":"🚗","Bewirtung":"🍽","Tanken":"⛽","Sonstiges":"📄"}

        karten = ""
        for r in rows:
            bid, datei, typ, vendor, reisend, bet, curr, eur, ci, bd, fns, iatas, pnr = r
            bc = TYPE_BADGE.get(typ, "b-sonstiges")
            icon = TYPE_ICON.get(typ, "📄")
            betrag_s = f"{eur} €" if eur else (f"{bet} {curr}" if bet else "–")

            def fmt(d):
                if not d: return "–"
                if isinstance(d, date): return d.strftime("%d.%m.%Y")
                return str(d)[:10]

            iata_list = [i.strip() for i in (iatas or "").split(",") if i.strip()]
            route_s = " → ".join(f'{c} ({IATA.get(c,c)})' for c in iata_list) if iata_list else ""

            infos = []
            if vendor: infos.append(f"<b>{vendor}</b>")
            if betrag_s != "–": infos.append(f'<span style="color:#059669;font-weight:700">{betrag_s}</span>')
            if fmt(ci) != "–": infos.append(f"📅 {fmt(ci)}")
            if route_s: infos.append(f"✈ {route_s}")
            if fns: infos.append(f'<span style="font-family:monospace;color:#2563eb">{fns}</span>')
            if pnr: infos.append(f'PNR: <span style="font-family:monospace">{pnr}</span>')

            karten += (f'<div class="card" style="border-left:4px solid #f59e0b">'
                       f'<div style="display:flex;align-items:flex-start;justify-content:space-between;gap:12px">'
                       f'<div style="flex:1">'
                       f'<div style="display:flex;align-items:center;gap:8px;margin-bottom:8px">'
                       f'<span style="font-size:20px">{icon}</span>'
                       f'<span class="badge {bc}">{typ}</span>'
                       f'<span style="font-size:12px;color:#64748b;font-style:italic">{datei[:50]}</span>'
                       f'</div>'
                       f'<div style="display:flex;gap:16px;flex-wrap:wrap;margin-bottom:12px">'
                       f'{"&nbsp;·&nbsp;".join(infos) if infos else "<span style=color:#94a3b8>Keine Daten erkannt</span>"}'
                       f'</div>'
                       f'<form method="post" action="/beleg/{bid}/zuordnen" '
                       f'style="display:flex;gap:8px;align-items:center;flex-wrap:wrap">'
                       f'<select name="reise_code" class="sel" style="max-width:350px">{opts}</select>'
                       f'<button type="submit" class="btn btn-g" style="white-space:nowrap">✓ Zuordnen</button>'
                       f'</form>'
                       f'</div>'
                       f'<a href="/beleg/{bid}" class="btn btn-s" style="white-space:nowrap">Details →</a>'
                       f'</div></div>')

        body = f"""
        <h1>📬 Posteingang ({len(rows)} Belege)</h1>
        <p style="color:#64748b;margin-bottom:20px;font-size:13px">
          Diese Belege wurden analysiert aber noch keiner Reise zugeordnet.
        </p>
        <div class="acts">
          <a href="/mails-abrufen" class="btn">📥 Neue Mails</a>
          <a href="/" class="btn btn-s">← Dashboard</a>
        </div>
        {karten}"""
        return HTMLResponse(shell("Posteingang", body, "posteingang"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler", f'<div class="card"><p>{e}</p><pre>{traceback.format_exc()[:300]}</pre></div>'))

@app.post("/beleg/{bid}/zuordnen")
async def beleg_zuordnen(bid: int, request: Request):
    form = await request.form()
    rcode = (form.get("reise_code") or "").strip() or None
    if rcode:
        db = get_db(); cur = db.cursor()
        cur.execute("UPDATE belege SET reise_code=%s, status='zugeordnet' WHERE id=%s", (rcode, bid))
        db.commit(); cur.close(); db.close()
    return RedirectResponse("/posteingang", status_code=303)

@app.get("/reisen", response_class=HTMLResponse)
def reisen_liste():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT r.code, r.titel, r.klarname, r.abreise, r.rueckkehr,
            COUNT(b.id) as belege
            FROM reisen r LEFT JOIN belege b ON b.reise_code=r.code
            GROUP BY r.code, r.titel, r.klarname, r.abreise, r.rueckkehr
            ORDER BY r.abreise DESC""")
        rows = cur.fetchall()
        cur.close(); db.close()

        def fmt(d):
            if not d: return "–"
            if isinstance(d, date): return d.strftime("%d.%m.%Y")
            return str(d)[:10]

        zeilen = ""
        for r in rows:
            code, titel, name, ab, zu, cnt = r
            zeilen += (f'<tr>'
                       f'<td style="font-family:monospace;color:#2563eb">'
                       f'<a href="/reise/{code}" style="color:#2563eb;font-weight:600">{code}</a></td>'
                       f'<td style="font-weight:600">{titel or "–"}</td>'
                       f'<td>{name or "–"}</td>'
                       f'<td>{fmt(ab)}</td>'
                       f'<td>{fmt(zu)}</td>'
                       f'<td style="text-align:center">{cnt}</td>'
                       f'<td><a href="/reise/{code}" class="btn btn-s" '
                       f'style="padding:4px 10px;font-size:11px">Ansehen</a></td>'
                       f'</tr>')

        body = f"""
        <h1>✈ Reisen ({len(rows)})</h1>
        <div class="acts">
          <a href="/reise-neu" class="btn btn-g">+ Neue Reise</a>
        </div>
        <div class="card" style="padding:0;overflow:hidden">
        <table>
          <thead><tr>
            <th>Code</th><th>Titel</th><th>Reisender</th>
            <th>Abreise</th><th>Rückkehr</th><th>Belege</th><th></th>
          </tr></thead>
          <tbody>{zeilen or '<tr><td colspan="7" class="empty">Keine Reisen</td></tr>'}</tbody>
        </table>
        </div>"""
        return HTMLResponse(shell("Reisen", body, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="card"><p>{e}</p></div>'))

@app.get("/reise-neu", response_class=HTMLResponse)
def reise_neu_form():
    body = """
    <h1>✈ Neue Reise anlegen</h1>
    <div class="card" style="max-width:500px">
      <form method="post" action="/reise-neu">
        <div style="display:grid;gap:12px">
          <div>
            <label>Kürzel * <span style="color:#94a3b8;font-weight:400">(z.B. RD)</span></label>
            <input class="inp" name="kuerzel" required maxlength="5" placeholder="RD">
          </div>
          <div>
            <label>Klarname *</label>
            <input class="inp" name="klarname" required placeholder="Ralf Diesslin">
          </div>
          <div>
            <label>Reisebeschreibung *</label>
            <input class="inp" name="titel" required placeholder="z.B. ECMA Lyon">
          </div>
          <div class="grid2">
            <div>
              <label>Abreise *</label>
              <input class="inp" type="date" name="abreise" required>
            </div>
            <div>
              <label>Rückkehr *</label>
              <input class="inp" type="date" name="rueckkehr" required>
            </div>
          </div>
          <button type="submit" class="btn btn-g">Reise anlegen</button>
        </div>
      </form>
    </div>"""
    return HTMLResponse(shell("Neue Reise", body, "reisen"))

@app.post("/reise-neu")
async def reise_neu(request: Request):
    try:
        form = await request.form()
        kuerzel = (form.get("kuerzel") or "").strip().upper()
        klarname = (form.get("klarname") or "").strip()
        titel = (form.get("titel") or "").strip()
        abreise = (form.get("abreise") or "").strip()
        rueckkehr = (form.get("rueckkehr") or "").strip()

        if not all([kuerzel, klarname, titel, abreise, rueckkehr]):
            return JSONResponse({"error": "Pflichtfelder fehlen"}, status_code=400)

        db = get_db(); cur = db.cursor()
        cur.execute("SELECT MAX(code) FROM reisen")
        last = cur.fetchone()[0]
        if last:
            year = str(date.today().year)[-2:]
            m = re.match(r'(\d{2})-(\d{3})', last)
            if m and m.group(1) == year:
                num = int(m.group(2)) + 1
            else:
                num = 1
        else:
            year = str(date.today().year)[-2:]
            num = 1
        code = f"{year}-{num:03d}"

        cur.execute(
            "INSERT INTO reisen (code, kuerzel, klarname, titel, abreise, rueckkehr) "
            "VALUES (%s,%s,%s,%s,%s,%s)",
            (code, kuerzel, klarname, titel, abreise, rueckkehr))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{code}", status_code=303)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/reise/{code}", response_class=HTMLResponse)
def reise_detail(code: str):
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT code, kuerzel, klarname, titel, abreise, rueckkehr FROM reisen WHERE code=%s", (code,))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Nicht gefunden", '<div class="card">Reise nicht gefunden</div>'))

        cur.execute("""SELECT id, dateiname, typ, vendor, betrag, waehrung, betrag_eur,
            checkin, belegdatum, status, pnr, flugnummern, iata_codes
            FROM belege WHERE reise_code=%s ORDER BY belegdatum NULLS LAST, checkin NULLS LAST, id""", (code,))
        belege = cur.fetchall()
        cur.close(); db.close()

        rcode, kuerzel, klarname, titel, ab, zu = r

        def fmt(d):
            if not d: return "–"
            if isinstance(d, date): return d.strftime("%d.%m.%Y")
            return str(d)[:10]

        TYPE_BADGE = {"Flug":"b-flug","Hotel":"b-hotel","Taxi":"b-taxi","Bahn":"b-bahn",
                      "Mietwagen":"b-mietwagen","Bewirtung":"b-bewirtung","Tanken":"b-tanken"}
        TYPE_ICON = {"Flug":"✈","Hotel":"🏨","Taxi":"🚕","Bahn":"🚆",
                     "Mietwagen":"🚗","Bewirtung":"🍽","Tanken":"⛽","Sonstiges":"📄"}

        gesamtbetrag = 0.0
        zeilen = ""
        for b in belege:
            bid, datei, typ, vendor, bet, curr, eur, ci, bd, stat, pnr, fns, iatas = b
            bc = TYPE_BADGE.get(typ, "b-sonstiges")
            icon = TYPE_ICON.get(typ, "📄")
            betrag_s = f"{eur} €" if eur else (f"{bet} {curr}" if bet else "–")
            datum_s = fmt(ci or bd)
            if eur:
                try: gesamtbetrag += float(eur)
                except: pass
            iata_list = [i.strip() for i in (iatas or "").split(",") if i.strip()]
            route_s = " → ".join(f'{c}' for c in iata_list[:4]) if iata_list else ""
            zeilen += (f'<tr>'
                       f'<td>{icon} <span class="badge {bc}">{typ}</span></td>'
                       f'<td style="font-weight:600">{vendor or datei[:30]}</td>'
                       f'<td style="font-weight:600;color:#059669">{betrag_s}</td>'
                       f'<td>{datum_s}</td>'
                       f'<td style="font-family:monospace;font-size:11px;color:#2563eb">'
                       f'{fns or route_s or pnr or ""}</td>'
                       f'<td><a href="/beleg/{bid}" class="btn btn-s" '
                       f'style="padding:4px 10px;font-size:11px">📋 Detail</a></td>'
                       f'</tr>')

        body = f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
          <a href="/reisen" class="btn btn-s">← Reisen</a>
          <h1 style="margin:0">{titel or rcode}</h1>
          <span style="font-family:monospace;font-size:12px;background:#f1f5f9;
                padding:2px 10px;border-radius:4px;color:#475569">{rcode}</span>
        </div>
        <div class="card" style="margin-bottom:16px">
          <div class="grid3">
            <div><span style="font-size:11px;color:#64748b">Reisender</span>
                 <div style="font-weight:600">{kuerzel} · {klarname}</div></div>
            <div><span style="font-size:11px;color:#64748b">Zeitraum</span>
                 <div style="font-weight:600">{fmt(ab)} – {fmt(zu)}</div></div>
            <div><span style="font-size:11px;color:#64748b">Gesamtkosten</span>
                 <div style="font-weight:700;font-size:18px;color:#059669">{gesamtbetrag:.2f} €</div></div>
          </div>
        </div>
        <h2>Belege ({len(belege)})</h2>
        <div class="card" style="padding:0;overflow:hidden">
        <table>
          <thead><tr>
            <th>Typ</th><th>Anbieter</th><th>Betrag</th>
            <th>Datum</th><th>Details</th><th></th>
          </tr></thead>
          <tbody>{zeilen or '<tr><td colspan="6" class="empty">Keine Belege – Mails importieren oder Beleg hochladen</td></tr>'}</tbody>
        </table>
        </div>
        <div class="acts" style="margin-top:16px">
          <a href="/beleg-upload?reise={code}" class="btn btn-s">📎 Beleg hochladen</a>
        </div>"""
        return HTMLResponse(shell(f"Reise {rcode}", body, "reisen"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler", f'<div class="card"><p>{e}</p><pre>{traceback.format_exc()[:400]}</pre></div>'))

@app.get("/beleg-upload", response_class=HTMLResponse)
def beleg_upload_form(reise: str = ""):
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT code, titel FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.close(); db.close()
    except: reisen = []

    opts = '<option value="">– Keine Reise –</option>'
    for rc, rt in reisen:
        sel = ' selected' if rc == reise else ''
        opts += f'<option value="{rc}"{sel}>{rc} · {rt or rc}</option>'

    body = f"""
    <h1>📎 Beleg hochladen</h1>
    <div class="card" style="max-width:500px">
      <form method="post" action="/beleg-upload" enctype="multipart/form-data">
        <div style="display:grid;gap:12px">
          <div>
            <label>Reise (optional)</label>
            <select name="reise_code" class="sel">{opts}</select>
          </div>
          <div>
            <label>Datei (PDF, JPG, PNG)</label>
            <input type="file" name="file" accept=".pdf,.jpg,.jpeg,.png" required
                   style="width:100%;padding:8px;border:1px solid #d1d5db;border-radius:6px">
          </div>
          <button type="submit" class="btn btn-g">Hochladen & Analysieren</button>
        </div>
      </form>
    </div>"""
    return HTMLResponse(shell("Beleg hochladen", body))

@app.post("/beleg-upload")
async def beleg_upload(file: UploadFile = File(...), reise_code: str = Form("")):
    try:
        data = await file.read()
        fn = file.filename or "upload"
        fn_lower = fn.lower()

        # Text extrahieren
        text = ""
        if fn_lower.endswith(".pdf"):
            try:
                import pypdf as _pypdf
                reader = _pypdf.PdfReader(io.BytesIO(data))
                text = "\n".join(p.extract_text() or "" for p in reader.pages)
            except: pass

        info = analyse_beleg(text or fn, fn)
        eur = to_eur(info["betrag"], info["waehrung"]) if info["betrag"] else None
        ci = parse_date(info["checkin"]) if info["checkin"] else None
        co = parse_date(info["checkout"]) if info["checkout"] else None
        bd = parse_date(info["alle_daten"][0]) if info["alle_daten"] else None
        if bd and ci and bd.year == ci.year: bd = ci

        h = file_hash(data)
        skey = f"uploads/{fn}"
        try:
            s3 = get_s3()
            s3.put_object(Bucket=S3_BUCKET, Key=skey, Body=data, ContentType=file.content_type or "application/octet-stream")
        except Exception as s3e:
            skey = "s3-fehler:" + str(s3e)[:60]

        rcode = reise_code.strip() or info["trip_code"] or None
        stat = "zugeordnet" if rcode else "ausstehend"

        db = get_db(); cur = db.cursor()
        cur.execute("""INSERT INTO belege
            (dateiname, typ, vendor, reisender, betrag, waehrung, betrag_eur,
             pnr, konfirmation, flugnummern, iata_codes, checkin, checkout,
             naechte, belegdatum, alle_daten, rohtext, storage_key,
             analyse_json, reise_code, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            RETURNING id""",
            (fn, info["typ"], info["vendor"] or None, info["reisender"] or None,
             info["betrag"] or None, info["waehrung"], eur,
             info["pnr"] or None, info["konfirmation"] or None,
             ",".join(info["flugnummern"]) or None,
             ",".join(info["iata_codes"]) or None,
             ci, co, info["naechte"] or None, bd,
             ",".join(info["alle_daten"]) or None,
             text[:10000] or None, skey,
             json.dumps(info, ensure_ascii=False), rcode, stat))
        new_id = cur.fetchone()[0]
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{new_id}", status_code=303)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

@app.get("/beleg/{bid}/reanalyse", response_class=HTMLResponse)
def beleg_reanalyse(bid: int):
    """Analysiert einen Beleg neu aus dem gespeicherten Rohtext."""
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT rohtext, dateiname, reise_code FROM belege WHERE id=%s", (bid,))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="card">Beleg nicht gefunden</div>'))

        rohtext, dateiname, rcode = r
        if not rohtext:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="card">Kein Rohtext vorhanden – Beleg kann nicht neu analysiert werden</div>'))

        info = analyse_beleg(rohtext, dateiname or "")
        eur = to_eur(info["betrag"], info["waehrung"]) if info["betrag"] else None
        ci = parse_date(info["checkin"]) if info["checkin"] else None
        co = parse_date(info["checkout"]) if info["checkout"] else None
        bd = parse_date(info["alle_daten"][0]) if info["alle_daten"] else None
        if bd and ci and bd.year == ci.year: bd = ci
        naechte = (co - ci).days if ci and co else None

        cur.execute("""UPDATE belege SET
            typ=%s, vendor=%s, reisender=%s,
            betrag=%s, waehrung=%s, betrag_eur=%s,
            pnr=%s, konfirmation=%s,
            flugnummern=%s, iata_codes=%s,
            checkin=%s, checkout=%s, naechte=%s,
            belegdatum=%s, alle_daten=%s,
            analyse_json=%s
            WHERE id=%s""",
            (info["typ"], info["vendor"] or None, info["reisender"] or None,
             info["betrag"] or None, info["waehrung"], eur,
             info["pnr"] or None, info["konfirmation"] or None,
             ",".join(info["flugnummern"]) or None,
             ",".join(info["iata_codes"]) or None,
             ci, co, naechte, bd,
             ",".join(info["alle_daten"]) or None,
             json.dumps(info, ensure_ascii=False),
             bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler", f'<div class="card"><p>{e}</p><pre>{traceback.format_exc()[:300]}</pre></div>'))


@app.get("/alle-reanalyse", response_class=HTMLResponse)
def alle_reanalyse():
    """Analysiert ALLE Belege neu aus ihrem Rohtext."""
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT id, rohtext, dateiname FROM belege WHERE rohtext IS NOT NULL ORDER BY id")
        rows = cur.fetchall()
        ok = fehler = 0
        for bid, rohtext, dateiname in rows:
            try:
                info = analyse_beleg(rohtext, dateiname or "")
                eur = to_eur(info["betrag"], info["waehrung"]) if info["betrag"] else None
                ci = parse_date(info["checkin"]) if info["checkin"] else None
                co = parse_date(info["checkout"]) if info["checkout"] else None
                bd = parse_date(info["alle_daten"][0]) if info["alle_daten"] else None
                if bd and ci and bd.year == ci.year: bd = ci
                naechte = (co - ci).days if ci and co else None
                cur.execute("""UPDATE belege SET
                    typ=%s, vendor=%s, reisender=%s,
                    betrag=%s, waehrung=%s, betrag_eur=%s,
                    pnr=%s, konfirmation=%s,
                    flugnummern=%s, iata_codes=%s,
                    checkin=%s, checkout=%s, naechte=%s,
                    belegdatum=%s, alle_daten=%s, analyse_json=%s
                    WHERE id=%s""",
                    (info["typ"], info["vendor"] or None, info["reisender"] or None,
                     info["betrag"] or None, info["waehrung"], eur,
                     info["pnr"] or None, info["konfirmation"] or None,
                     ",".join(info["flugnummern"]) or None,
                     ",".join(info["iata_codes"]) or None,
                     ci, co, naechte, bd,
                     ",".join(info["alle_daten"]) or None,
                     json.dumps(info, ensure_ascii=False),
                     bid))
                ok += 1
            except Exception as e2:
                fehler += 1
                print(f"[Reanalyse] Beleg {bid}: {e2}")
        db.commit(); cur.close(); db.close()
        return HTMLResponse(shell("Reanalyse", f'''
        <div class="card">
          <h1>🔄 Reanalyse abgeschlossen</h1>
          <div class="alert alert-ok" style="margin:16px 0">
            ✓ {ok} Belege neu analysiert · {fehler} Fehler
          </div>
          <div class="acts">
            <a href="/belege" class="btn">📋 Belege ansehen</a>
            <a href="/" class="btn btn-s">← Dashboard</a>
          </div>
        </div>'''))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="card"><p>{e}</p></div>'))


@app.get("/debug-mail/{uid_or_id}")
def debug_mail(uid_or_id: str):
    """Zeigt gespeicherten Mail-Body zur Diagnose."""
    try:
        db = get_db(); cur = db.cursor()
        # Versuche als Beleg-ID
        try:
            bid = int(uid_or_id)
            cur.execute("SELECT mail_uid FROM belege WHERE id=%s", (bid,))
            r = cur.fetchone()
            if r:
                cur.execute("SELECT uid, absender, betreff, body FROM mails WHERE uid=%s", (r[0],))
            else:
                cur.execute("SELECT uid, absender, betreff, body FROM mails WHERE id=%s", (bid,))
        except:
            cur.execute("SELECT uid, absender, betreff, body FROM mails WHERE uid=%s", (uid_or_id,))
        row = cur.fetchone()
        cur.close(); db.close()
        if not row:
            return {"error": "nicht gefunden"}
        uid, absender, betreff, body = row
        return {
            "uid": uid,
            "absender": absender,
            "betreff": betreff,
            "body_laenge": len(body or ""),
            "body_preview": (body or "")[:2000],
            "hat_html": "<html" in (body or "").lower(),
        }
    except Exception as e:
        return {"error": str(e)}

@app.get("/reset-all")
def reset_all(confirm: str = ""):
    if confirm != "ja":
        return HTMLResponse(shell("Reset", """
        <div class="card" style="max-width:400px">
          <h1>⚠ Alle Daten löschen?</h1>
          <p style="margin:12px 0;color:#64748b">Löscht alle Belege, Mails und Reisen.</p>
          <div class="acts">
            <a href="/reset-all?confirm=ja" class="btn" style="background:#ef4444">Ja, löschen</a>
            <a href="/" class="btn btn-s">Abbrechen</a>
          </div>
        </div>"""))
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("TRUNCATE belege, mails RESTART IDENTITY CASCADE")
        cur.execute("DELETE FROM reisen")
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/", status_code=303)
    except Exception as e:
        return JSONResponse({"error": str(e)})

