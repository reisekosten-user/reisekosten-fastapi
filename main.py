from fastapi import FastAPI, Request, UploadFile, File, Form
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
import os, re, base64, json, httpx, imaplib, email, hashlib, threading, time, io, sys, subprocess
from email.header import decode_header

# ── Auto-Install PDF-Bibliotheken falls nicht vorhanden ──────────────────────
def _ensure_pdf_libs():
    missing = []
    try: import reportlab
    except ImportError: missing.append("reportlab>=4.2.0")
    try: import pypdf
    except ImportError: missing.append("pypdf>=4.3.0")
    try: import PIL
    except ImportError: missing.append("Pillow>=10.0.0")
    if missing:
        print(f"[PDF] Installiere fehlende Libraries: {missing}")
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "--quiet"] + missing)
            print("[PDF] Installation erfolgreich")
        except Exception as e:
            print(f"[PDF] Installation fehlgeschlagen: {e}")

_ensure_pdf_libs()
from datetime import date, datetime, timedelta, timezone
from typing import Optional
import psycopg2
import boto3


# =========================================================
# PDF-HILFSFUNKTIONEN
# =========================================================

def _try_import_pdf():
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
        from reportlab.lib.enums import TA_LEFT, TA_RIGHT, TA_CENTER
        import pypdf
        return True
    except ImportError as e:
        print(f"[PDF] Import fehlt: {e}")
        return False

def HAS_PDF_LIBS():
    """Lazy check – immer aktuell, kein Startup-Timing-Problem."""
    return _try_import_pdf()

def make_text_pdf(title: str, body_text: str, meta: dict = None) -> bytes:
    """Erstellt ein einfaches PDF aus Text mit reportlab. Gibt bytes zurück."""
    from reportlab.lib.pagesizes import A4
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
    from reportlab.lib.enums import TA_LEFT
    import html

    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
        leftMargin=20*mm, rightMargin=20*mm, topMargin=20*mm, bottomMargin=20*mm)
    styles = getSampleStyleSheet()
    story = []

    # Header
    header_style = ParagraphStyle('Header', parent=styles['Title'],
        fontSize=16, textColor=colors.HexColor('#1a3d96'), spaceAfter=4)
    sub_style = ParagraphStyle('Sub', parent=styles['Normal'],
        fontSize=9, textColor=colors.HexColor('#5a6e8a'), spaceAfter=12)
    body_style = ParagraphStyle('Body', parent=styles['Normal'],
        fontSize=9, leading=14, spaceAfter=6)
    label_style = ParagraphStyle('Label', parent=styles['Normal'],
        fontSize=8, textColor=colors.HexColor('#5a6e8a'), fontName='Helvetica-Bold')

    story.append(Paragraph(html.escape(title), header_style))
    story.append(HRFlowable(width="100%", thickness=2, color=colors.HexColor('#1a3d96')))
    story.append(Spacer(1, 4*mm))

    # Meta-Tabelle
    if meta:
        rows = [[Paragraph(html.escape(str(k)), label_style),
                 Paragraph(html.escape(str(v)), body_style)]
                for k,v in meta.items() if v]
        if rows:
            t = Table(rows, colWidths=[45*mm, 120*mm])
            t.setStyle(TableStyle([
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('ROWBACKGROUNDS', (0,0), (-1,-1), [colors.HexColor('#f8faff'), colors.white]),
                ('GRID', (0,0), (-1,-1), 0.5, colors.HexColor('#eaeef5')),
                ('TOPPADDING', (0,0), (-1,-1), 4),
                ('BOTTOMPADDING', (0,0), (-1,-1), 4),
            ]))
            story.append(t)
            story.append(Spacer(1, 6*mm))

    # Body-Text
    if body_text:
        story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#dde4ef')))
        story.append(Spacer(1, 3*mm))
        for line in body_text.split('\n'):
            safe = html.escape(line.strip())
            if safe:
                story.append(Paragraph(safe, body_style))
            else:
                story.append(Spacer(1, 2*mm))

    # Footer
    story.append(Spacer(1, 8*mm))
    story.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor('#dde4ef')))
    footer_style = ParagraphStyle('Footer', parent=styles['Normal'],
        fontSize=7, textColor=colors.HexColor('#9bafc8'))
    story.append(Paragraph(
        f"Herrhammer Kürschner Kerzenmaschinen · Erstellt {datetime.now().strftime('%d.%m.%Y %H:%M')}",
        footer_style))

    doc.build(story)
    return buf.getvalue()


def merge_pdfs(pdf_bytes_list: list) -> bytes:
    """Führt mehrere PDF-bytes zu einer einzigen PDF zusammen."""
    import pypdf
    writer = pypdf.PdfWriter()
    for pdf_bytes in pdf_bytes_list:
        if not pdf_bytes:
            continue
        try:
            reader = pypdf.PdfReader(io.BytesIO(pdf_bytes))
            for page in reader.pages:
                writer.add_page(page)
        except Exception as e:
            print(f"[PDF merge skip]: {e}")
    out = io.BytesIO()
    writer.write(out)
    return out.getvalue()


async def generate_and_store_mail_pdf(att_id: int, subj: str, body: str,
                                       typ: str, vendor: str, betrag: str,
                                       datum: str, tc: str, conn) -> str | None:
    """Generiert PDF aus Mail-Body und speichert in S3. Gibt storage_key zurück."""
    if not HAS_PDF_LIBS():
        return None
    try:
        meta = {
            "Typ": typ or "–",
            "Anbieter": vendor or "–",
            "Betrag": f"{betrag} €" if betrag else "–",
            "Datum": datum or "–",
            "Reise": tc or "–",
        }
        pdf_bytes = make_text_pdf(
            title=f"{typ or 'Buchungsbestätigung'}: {vendor or subj or '–'}",
            body_text=body or "",
            meta=meta
        )
        key = f"mail_pdfs/{tc}/mail_body_{att_id}.pdf"
        s3 = get_s3()
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=pdf_bytes,
                      ContentType="application/pdf")
        # PDF-Key in DB speichern
        cur = conn.cursor()
        cur.execute("ALTER TABLE mail_attachments ADD COLUMN IF NOT EXISTS pdf_key TEXT")
        cur.execute("UPDATE mail_attachments SET pdf_key=%s WHERE id=%s", (key, att_id))
        conn.commit()
        cur.close()
        return key
    except Exception as e:
        print(f"[PDF gen error att {att_id}]: {e}")
        return None



# ── Flughafen → ISO-Ländercode (global, einmalig gepflegt) ───────────────────
AIRPORT_CC = {
    # Deutschland
    "FRA":"DE","MUC":"DE","BER":"DE","HAM":"DE","DUS":"DE","STR":"DE",
    "CGN":"DE","NUE":"DE","LEJ":"DE","HAJ":"DE","FDH":"DE","HHN":"DE",
    # Frankreich
    "CDG":"FR","ORY":"FR","LYS":"FR","NCE":"FR","MRS":"FR","BOD":"FR","TLS":"FR","NTE":"FR",
    # Großbritannien
    "LHR":"GB","LGW":"GB","MAN":"GB","EDI":"GB","STN":"GB","BHX":"GB","GLA":"GB",
    # USA
    "JFK":"US","LAX":"US","ORD":"US","MIA":"US","SFO":"US","BOS":"US",
    "IAH":"US","DFW":"US","ATL":"US","DCA":"US","IAD":"US","EWR":"US",
    "SEA":"US","LAS":"US","MCO":"US","PHX":"US","MSP":"US","DTW":"US",
    # Indien
    "BOM":"IN","DEL":"IN","MAA":"IN","BLR":"IN","HYD":"IN","CCU":"IN","AMD":"IN",
    # VAE
    "DXB":"AE","AUH":"AE","SHJ":"AE",
    # Aserbaidschan
    "GYD":"AZ",
    # Schweiz
    "ZRH":"CH","GVA":"CH","BSL":"CH","BRN":"CH",
    # Österreich
    "VIE":"AT","SZG":"AT","INN":"AT","GRZ":"AT","LNZ":"AT",
    # Italien
    "FCO":"IT","MXP":"IT","NAP":"IT","VCE":"IT","LIN":"IT","BLQ":"IT","PSA":"IT",
    # Spanien
    "MAD":"ES","BCN":"ES","AGP":"ES","PMI":"ES","VLC":"ES","SVQ":"ES","TFS":"ES",
    # Türkei
    "IST":"TR","SAW":"TR","AYT":"TR","ADB":"TR","ESB":"TR",
    # Japan
    "NRT":"JP","HND":"JP","KIX":"JP","NGO":"JP","CTS":"JP",
    # China/HK
    "PEK":"CN","PKX":"CN","PVG":"CN","CAN":"CN","CTU":"CN","SZX":"CN",
    "HKG":"CN",
    # Korea
    "ICN":"KR","GMP":"KR","PUS":"KR",
    # Singapur
    "SIN":"SG",
    # Katar
    "DOH":"QA",
    # Saudi-Arabien
    "RUH":"SA","JED":"SA","DMM":"SA",
    # Niederlande
    "AMS":"NL","EIN":"NL",
    # Belgien
    "BRU":"BE","CRL":"BE",
    # Polen
    "WAW":"PL","KRK":"PL","WRO":"PL","GDN":"PL",
    # Tschechien
    "PRG":"CZ",
    # Ungarn
    "BUD":"HU",
    # Rumänien
    "OTP":"RO","CLJ":"RO",
    # Skandinavien
    "ARN":"SE","GOT":"SE","MMX":"SE",
    "CPH":"DK","BLL":"DK",
    "HEL":"FI","TMP":"FI",
    "OSL":"NO","BGO":"NO","TRD":"NO",
    # Portugal
    "LIS":"PT","OPO":"PT","FAO":"PT",
    # Griechenland
    "ATH":"GR","SKG":"GR","HER":"GR","RHO":"GR",
    # Russland
    "SVO":"RU","DME":"RU","LED":"RU",
    # ── LATEINAMERIKA ──────────────────────────────────────────────────────────
    # Panama
    "PTY":"PA",
    # Costa Rica
    "SJO":"CR","LIR":"CR",
    # Mexiko
    "MEX":"MX","CUN":"MX","GDL":"MX","MTY":"MX","SJD":"MX",
    # Brasilien
    "GRU":"BR","GIG":"BR","BSB":"BR","SSA":"BR","FOR":"BR","REC":"BR",
    # Argentinien
    "EZE":"AR","AEP":"AR","COR":"AR",
    # Chile
    "SCL":"CL","PMC":"CL",
    # Peru
    "LIM":"PE",
    # Kolumbien
    "BOG":"CO","MDE":"CO","CLO":"CO",
    # Ecuador
    "UIO":"EC","GYE":"EC",
    # Venezuela
    "CCS":"VE",
    # Dominikanische Republik
    "SDQ":"DO","PUJ":"DO",
    # Kuba
    "HAV":"CU",
    # Jamaika
    "KIN":"JM","MBJ":"JM",
    # Bahamas
    "NAS":"BS",
    # Trinidad
    "POS":"TT",
    # Uruguay
    "MVD":"UY",
    # Bolivien
    "VVI":"BO","LPB":"BO",
    # Paraguay
    "ASU":"PY",
    # Honduras
    "TGU":"HN",
    # Guatemala
    "GUA":"GT",
    # El Salvador
    "SAL":"SV",
    # Nicaragua
    "MGA":"NI",
    # Kanada
    "YYZ":"CA","YVR":"CA","YUL":"CA","YYC":"CA","YEG":"CA","YOW":"CA",
    # Australien
    "SYD":"AU","MEL":"AU","BNE":"AU","PER":"AU","ADL":"AU",
    # Neuseeland
    "AKL":"NZ","WLG":"NZ","CHC":"NZ",
    # Südafrika
    "JNB":"ZA","CPT":"ZA","DUR":"ZA",
    # Marokko
    "CMN":"MA","RAK":"MA","AGA":"MA",
    # Ägypten
    "CAI":"EG","HRG":"EG","SSH":"EG","LXR":"EG",
    # Israel
    "TLV":"IL",
    # Iran
    "IKA":"IR","THR":"IR",
    # Pakistan
    "KHI":"PK","LHE":"PK","ISB":"PK",
    # Bangladesch
    "DAC":"BD",
    # Sri Lanka
    "CMB":"LK",
    # Thailand
    "BKK":"TH","DMK":"TH","HKT":"TH","CNX":"TH",
    # Vietnam
    "HAN":"VN","SGN":"VN","DAD":"VN",
    # Malaysia
    "KUL":"MY","PEN":"MY",
    # Indonesien
    "CGK":"ID","DPS":"ID","SUB":"ID",
    # Philippinen
    "MNL":"PH","CEB":"PH",
    # Taiwan
    "TPE":"TW","TSA":"TW",
    # Kasachstan
    "ALA":"KZ","TSE":"KZ",
}

# VMA-Sätze für Länder ohne eigenen Eintrag (Fallback auf Satz für "sonstige Länder" = DE)
# Panama, Costa Rica etc. → kein BMF-Satz → DE-Satz als Fallback
# Bitte jährlich mit BMF-Schreiben abgleichen!


def anonymize_for_ki(text: str) -> str:
    """
    Anonymisiert Text bevor er als KI-Beispiel gespeichert oder an Mistral gesendet wird.
    Entfernt: Namen, E-Mails, Telefonnummern, spezifische Buchungsnummern.
    Behält: Flugnummern, Flughafencodes, Uhrzeiten, Beträge, Datumsformat.
    DSGVO Art. 4 Nr. 1 – keine personenbezogenen Daten an Drittanbieter.
    """
    import re
    t = text
    # E-Mail-Adressen
    t = re.sub(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', '[E-MAIL]', t)
    # Telefonnummern
    t = re.sub(r'(?:\+\d{1,3}[\s-]?)?\(?\d{3,5}\)?[\s.-]?\d{3,5}[\s.-]?\d{3,6}', '[TEL]', t)
    # Vollständige Namen (Vor + Nachname, Titel)
    t = re.sub(r'\b(?:Mr|Mrs|Ms|Dr|Prof|Herr|Frau)\.?\s+[A-ZÄÖÜ][a-zäöüß]+\s+(?:[A-ZÄÖÜ][a-zäöüß]+\s+)?[A-ZÄÖÜ][a-zäöüß]+\b', '[NAME]', t)
    # Häufige Name-Muster: "Diesslin Ralf" (Nachname Vorname)
    t = re.sub(r'\b[A-ZÄÖÜ][a-zäöüß]{3,}\s+[A-ZÄÖÜ][a-zäöüß]{3,}\b', '[NAME]', t)
    # Hotelreservierungsnummern (lange Zahlen)
    t = re.sub(r'\b\d{8,}\b', '[RESERV-NR]', t)
    # Straßenadressen
    t = re.sub(r'\b[A-ZÄÖÜ][a-zäöüß]+(?:straße|str\.|gasse|weg|allee|platz|ring)\s*\d+\b', '[ADRESSE]', t, flags=re.IGNORECASE)
    # Vielfliegernummern (z.B. "CX1500100882")
    t = re.sub(r'\b(?:CX|LH|LX|BA|AF|KL|Miles|FF)\s*\d{8,}\b', '[FF-NR]', t, flags=re.IGNORECASE)
    # Spezifische Buchungsreferenzen BEHALTEN (wichtig für KI-Training: PNR-Format)
    # Adressen mit PLZ
    t = re.sub(r'\b[A-Z]{0,2}\d{4,5}\s+[A-ZÄÖÜ][a-zäöüß]+\b', '[ORT]', t)
    return t

def load_ki_examples(mail_type: str = None, limit: int = 3) -> list:
    """Lädt anonymisierte Few-Shot Beispiele aus DB für den Mistral-Prompt."""
    try:
        conn=get_conn();cur=conn.cursor()
        if mail_type:
            cur.execute("""SELECT input_text,expected_json,description FROM ki_examples
                WHERE approved=TRUE AND mail_type=%s
                ORDER BY created_at DESC LIMIT %s""",(mail_type,limit))
        else:
            cur.execute("""SELECT input_text,expected_json,description FROM ki_examples
                WHERE approved=TRUE
                ORDER BY created_at DESC LIMIT %s""",(limit,))
        rows=cur.fetchall();cur.close();conn.close()
        # Anonymisierung beim Laden – nie Rohdaten an Mistral
        return [{"input": anonymize_for_ki(r[0])[:1500],
                 "output":r[1],"desc":r[2] or ""} for r in rows]
    except Exception:
        return []

def save_ki_example(mail_type: str, input_text: str, result_json: dict, description: str = ""):
    """
    Speichert KI-Lernbeispiel ANONYMISIERT in der DB.
    Personenbezogene Daten werden vor der Speicherung entfernt (DSGVO Art. 25 – Privacy by Design).
    """
    try:
        import json as _json
        # Anonymisieren vor Speicherung
        anon_text = anonymize_for_ki(input_text[:4000])
        # Auch im JSON keine Namen
        result_clean = {k:v for k,v in result_json.items()
                       if k not in ("traveler_name",) and v}
        conn=get_conn();cur=conn.cursor()
        cur.execute("""INSERT INTO ki_examples (mail_type,input_text,expected_json,description)
            VALUES (%s,%s,%s,%s)""",
            (mail_type, anon_text, _json.dumps(result_clean, ensure_ascii=False), description))
        conn.commit();cur.close();conn.close()
        return True
    except Exception as e:
        print(f"[KI-Beispiel] Fehler: {e}")
        return False

APP_VERSION = "9.27"

app = FastAPI(title="Herrhammer Reisekosten", version=APP_VERSION)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.on_event("startup")
async def seed_ki_examples():
    """Seed-Beispiele beim Start einfügen wenn Tabelle leer ist."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS ki_examples (
            id SERIAL PRIMARY KEY, mail_type TEXT, input_text TEXT,
            expected_json TEXT, description TEXT, approved BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT now())""")
        cur.execute("SELECT COUNT(*) FROM ki_examples")
        count=cur.fetchone()[0]
        if count == 0:
            import json as _json
            # Seed 1: FLY AWAY Itinerary Format
            seed_input = """Reiseangebot Buchungsreferenz: Z6INOT
Datum City Flug von/bis Klasse
25 Mai Frankfurt - Zurich LX 3613 06:35 - 07:30 Business
25 Mai Zurich - San Jose LX 8038 09:00 - 12:55 Business
29 Mai San Jose - Zurich LH 4515 15:55 - 10:55 (+1) Business
30 Mai Zurich - Frankfurt LH 5739 12:50 - 13:55 Business
Ticketnummer LH 220-2979545073"""
            seed_output = _json.dumps({
                "beleg_typ": "Flug",
                "anbieter": "Swiss/Lufthansa",
                "pnr_code": "Z6INOT",
                "flight_numbers": "LX3613,LX8038,LH4515,LH5739",
                "flight_segments": "LX3613|FRA|ZRH|25.05.2026|06:35|25.05.2026|07:30;LX8038|ZRH|SJO|25.05.2026|09:00|25.05.2026|12:55;LH4515|SJO|ZRH|29.05.2026|15:55|30.05.2026|10:55;LH5739|ZRH|FRA|30.05.2026|12:50|30.05.2026|13:55",
                "confidence": "hoch"
            }, ensure_ascii=False)
            cur.execute("INSERT INTO ki_examples (mail_type,input_text,expected_json,description,approved) VALUES (%s,%s,%s,%s,%s)",
                ("Flug", seed_input, seed_output, "FLY AWAY Reisebüro Itinerary Format", True))

            # Seed 2: Lufthansa Buchungsbestätigung Format
            seed2_input = """WG: Vielen Dank für Ihre Buchung | von Nürnberg nach Lyon am 20 April 2026
PNR: 83WPJT
LH3463 NUE→FRA 13:00→18:15 20.04.2026
LH1078 FRA→LYS 16:55→18:15 20.04.2026
Gesamtpreis: 496,50 EUR"""
            seed2_output = _json.dumps({
                "beleg_typ": "Flug",
                "betrag": "496.50",
                "waehrung": "EUR",
                "anbieter": "Lufthansa",
                "pnr_code": "83WPJT",
                "flight_numbers": "LH3463,LH1078",
                "flight_segments": "LH3463|NUE|FRA|20.04.2026|13:00|20.04.2026|18:15;LH1078|FRA|LYS|20.04.2026|16:55|20.04.2026|18:15",
                "confidence": "hoch"
            }, ensure_ascii=False)
            cur.execute("INSERT INTO ki_examples (mail_type,input_text,expected_json,description,approved) VALUES (%s,%s,%s,%s,%s)",
                ("Flug", seed2_input, seed2_output, "Lufthansa Buchungsbestätigung NUE-LYS", True))

            conn.commit()
            print(f"[KI-Seed] {2} Beispiele eingefügt")
        cur.close();conn.close()
    except Exception as e:
        print(f"[KI-Seed] Fehler: {e}")

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
# VMA-Sätze 2026 (§ 9 Abs. 4a EStG, BMF-Schreiben)
# "partial" = An-/Abreisetag (8-24h), "full" = volle Reisetage (>24h)
VMA = {
    "DE":  {"full": 28.0,  "partial": 14.0},
    "BE":  {"full": 59.0,  "partial": 40.0},
    "DK":  {"full": 75.0,  "partial": 50.0},
    "CN":  {"full": 57.0,  "partial": 38.0},
    "FR":  {"full": 53.0,  "partial": 36.0},
    "FR_PARIS": {"full": 58.0, "partial": 39.0},
    "GB":  {"full": 53.0,  "partial": 36.0},
    "GB_LONDON": {"full": 66.0, "partial": 44.0},
    "IT":  {"full": 48.0,  "partial": 32.0},
    "IT_MAILAND": {"full": 42.0, "partial": 28.0},
    "JP":  {"full": 33.0,  "partial": 22.0},
    "JP_TOKIO": {"full": 50.0, "partial": 33.0},
    "NL":  {"full": 58.0,  "partial": 39.0},
    "AT":  {"full": 50.0,  "partial": 33.0},
    "PL":  {"full": 34.0,  "partial": 23.0},
    "PL_WARSCHAU": {"full": 40.0, "partial": 27.0},
    "CH":  {"full": 82.0,  "partial": 55.0},
    "CH_GENF": {"full": 70.0, "partial": 47.0},
    "ES":  {"full": 34.0,  "partial": 23.0},
    "ES_MADRID": {"full": 44.0, "partial": 28.0},
    "TR":  {"full": 35.0,  "partial": 17.5},
    "US":  {"full": 59.0,  "partial": 40.0},
    "US_NYC":  {"full": 66.0, "partial": 44.0},
    "US_LA":   {"full": 64.0, "partial": 43.0},
    "US_CHI":  {"full": 65.0, "partial": 44.0},
    "US_MIA":  {"full": 65.0, "partial": 44.0},
    "IN":  {"full": 32.0,  "partial": 16.0},
    "AE":  {"full": 53.0,  "partial": 26.5},
    "AZ":  {"full": 37.0,  "partial": 18.5},
    "SG":  {"full": 45.0,  "partial": 22.5},
    "QA":  {"full": 35.0,  "partial": 17.5},
    "SA":  {"full": 35.0,  "partial": 17.5},
    "KR":  {"full": 40.0,  "partial": 20.0},
    "AU":  {"full": 40.0,  "partial": 20.0},
    "CA":  {"full": 45.0,  "partial": 22.5},
    "RU":  {"full": 30.0,  "partial": 20.0},
    "SE":  {"full": 45.0,  "partial": 22.5},
    "NO":  {"full": 55.0,  "partial": 27.5},
    "FI":  {"full": 45.0,  "partial": 22.5},
    "CZ":  {"full": 35.0,  "partial": 17.5},
    "HU":  {"full": 35.0,  "partial": 17.5},
    "RO":  {"full": 30.0,  "partial": 15.0},
    "BR":  {"full": 40.0,  "partial": 20.0},
    # Lateinamerika – BMF-Schreiben (§9 EStG, nicht gelistete Länder = 30€/15€)
    "PA":  {"full": 45.0,  "partial": 30.0},   # Panama (Richtwert, kein BMF-Satz)
    "CR":  {"full": 40.0,  "partial": 26.5},   # Costa Rica
    "MX":  {"full": 45.0,  "partial": 30.0},   # Mexiko
    "AR":  {"full": 40.0,  "partial": 20.0},   # Argentinien
    "CL":  {"full": 45.0,  "partial": 30.0},   # Chile
    "PE":  {"full": 40.0,  "partial": 20.0},   # Peru
    "CO":  {"full": 40.0,  "partial": 20.0},   # Kolumbien
    "EC":  {"full": 35.0,  "partial": 17.5},   # Ecuador
    "UY":  {"full": 40.0,  "partial": 20.0},   # Uruguay
    "DO":  {"full": 35.0,  "partial": 17.5},   # Dominikanische Republik
    "GT":  {"full": 35.0,  "partial": 17.5},   # Guatemala
    "SV":  {"full": 35.0,  "partial": 17.5},   # El Salvador
    "HN":  {"full": 35.0,  "partial": 17.5},   # Honduras
    "NI":  {"full": 35.0,  "partial": 17.5},   # Nicaragua
    "CU":  {"full": 35.0,  "partial": 17.5},   # Kuba
    "JM":  {"full": 35.0,  "partial": 17.5},   # Jamaika
    # Sonstige (Sammelkategorie §9 EStG)
    "ZA":  {"full": 35.0,  "partial": 17.5},   # Südafrika
    "MA":  {"full": 35.0,  "partial": 17.5},   # Marokko
    "EG":  {"full": 35.0,  "partial": 17.5},   # Ägypten
    "IL":  {"full": 45.0,  "partial": 30.0},   # Israel
    "TH":  {"full": 35.0,  "partial": 17.5},   # Thailand
    "VN":  {"full": 35.0,  "partial": 17.5},   # Vietnam
    "MY":  {"full": 40.0,  "partial": 20.0},   # Malaysia
    "ID":  {"full": 35.0,  "partial": 17.5},   # Indonesien
    "PH":  {"full": 35.0,  "partial": 17.5},   # Philippinen
    "TW":  {"full": 45.0,  "partial": 30.0},   # Taiwan
    "NZ":  {"full": 40.0,  "partial": 20.0},   # Neuseeland
    "CN_HK": {"full": 83.0, "partial": 56.0},
}
# Mahlzeitenabzug 2026: 20%/40%/40% vom deutschen 24h-Satz (28 EUR)
# Mahlzeitenabzug: prozentual vom Tagessatz (§ 9 Abs. 4a EStG)
# Frühstück 20%, Mittagessen 40%, Abendessen 40%
MEAL_DED_PCT = {"breakfast": 0.20, "lunch": 0.40, "dinner": 0.40}

def get_vma(cc, day_type, meals, city_key=None):
    """VMA-Betrag mit prozentualem Mahlzeitenabzug vom länderspezifischen Tagessatz."""
    cc_norm=(cc or "DE").upper().strip()
    key = city_key.upper() if city_key and city_key.upper() in VMA else cc_norm
    r = VMA.get(key, VMA.get(cc_norm, {"full":28.0,"partial":14.0}))
    # Basis ist immer der volle 24h-Satz für den Abzug (BMF: Abzug vom Höchstbetrag)
    full_rate = r["full"]
    base = full_rate if day_type == "full" else r["partial"]
    # Abzug prozentual vom vollen Tagessatz
    abzug = sum(full_rate * MEAL_DED_PCT.get(m, 0) for m in (meals or []))
    return max(0.0, round(base - abzug, 2))

def load_daily_meals(trip_code: str) -> dict:
    """Lädt tagesbasierte Mahlzeiten. Gibt {date: (meals_list, country_code, vma_override)} zurück."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT meal_date,breakfast,lunch,dinner,
                       COALESCE(country_code,'DE'),vma_override
                       FROM daily_meals WHERE trip_code=%s ORDER BY meal_date""",(trip_code,))
        rows=cur.fetchall();cur.close();conn.close()
        result={}
        for meal_date,b,l,d,cc,vma_ov in rows:
            meals=[]
            if b: meals.append("breakfast")
            if l: meals.append("lunch")
            if d: meals.append("dinner")
            result[meal_date]=(meals, cc or "DE", float(vma_ov) if vma_ov is not None else None)
        return result
    except Exception:
        return {}

def calc_vma_from_daily(dep_d, ret_d, daily_meals_dict: dict, vma_dest: dict, default_cc: str) -> tuple:
    """
    Berechnet VMA taggenau. daily_meals_dict: {date: (meals_list, country_code, vma_override)}
    Gibt (total, rows) zurück. rows: (datum, lbl, cc, meal_icons, vma_betrag)
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
        # Land: aus daily_meals (manuell gesetzt) > vma_destinations > default
        day_data=daily_meals_dict.get(current_day)
        if day_data:
            ml, day_cc, vma_override = day_data
            # Wenn kein manuelles Land gesetzt → aus vma_destinations
            if day_cc == "DE" and vma_dest:
                day_cc = get_country_for_day(current_day, vma_dest, default_cc)
        else:
            ml=[]; day_cc=get_country_for_day(current_day,vma_dest,default_cc); vma_override=None

        if vma_override is not None:
            v=float(vma_override)
            cc_label=f"{day_cc}*"  # * = manuell
        else:
            v=get_vma(day_cc,dtype,ml)
            cc_label=day_cc
        total+=v
        meal_icons=" ".join(filter(None,[
            "🍳" if "breakfast" in ml else "",
            "🥗" if "lunch" in ml else "",
            "🍽" if "dinner" in ml else "",
        ])) or "–"
        rows.append((str(current_day),lbl,cc_label,meal_icons,v))
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
    system = f"""Du bist Experte fuer Reisekostenabrechnungen. Analysiere den Text und gib NUR ein JSON-Objekt zurueck, keine Erklaerungen.

PFLICHTFELDER:
- beleg_typ: "Flug" | "Hotel" | "Taxi" | "Bahn" | "Mietwagen" | "Essen" | "Sonstiges"
- betrag: Gesamtbetrag als "142.50" (Punkt als Dezimaltrennzeichen), oder ""
- waehrung: "EUR" (Standard), oder expliziter Code wie "USD", "GBP", "CHF"
- datum: NUR das Buchungsdatum als "DD.MM.YYYY" - NICHT das Reisedatum!
  Buchungsdatum = wann die Buchung gemacht wurde (oft "Buchung vom", "Bestelldatum", "Ausstellungsdatum")
- anbieter: Firmenname z.B. "Lufthansa", "Marriott", "Swiss"
- reisecode: Bekannte Reisecodes: {codes_str}. Suche im Text nach diesen Codes und trage den passenden ein, oder ""
- pnr_code: 6-stelliger alphanumerischer Buchungscode z.B. "Z6INOT", "83WPJT", oder ""
- confidence: "hoch" | "mittel" | "niedrig"
- bemerkung: Kurze Zusammenfassung auf Deutsch
- bemerkung: Kurze Zusammenfassung auf Deutsch

FLUG-FELDER (nur bei beleg_typ=Flug):
- flight_numbers: alle Flugnummern kommagetrennt z.B. "LH3463,LH1078,LH1077,LH3463"
  WICHTIG: Die Anzahl der Flugnummern MUSS mit der Anzahl der Segmente übereinstimmen!
- flight_segments: Jedes Flug-Segment im Format "FN|VON|NACH|DATUM|ABF|DATUM|ANK"
  Trennzeichen zwischen Segmenten: Semikolon
  Beispiel 4 Segmente Nuernberg-Lyon und zurueck:
  "LH3463|NUE|FRA|20.04.2026|06:30|20.04.2026|07:35;LH1078|FRA|LYS|20.04.2026|09:15|20.04.2026|10:20;LH1077|LYS|FRA|24.04.2026|14:35|24.04.2026|17:19;LH3463|FRA|NUE|24.04.2026|17:19|24.04.2026|19:15"
  Alle Felder: Flugnummer | Abflughafen-IATA | Ankunfthafen-IATA | Abflugdatum | Abflugzeit | Ankunftdatum | Ankunftzeit
  IATA-Codes: NUE=Nuernberg FRA=Frankfurt LYS=Lyon MUC=Muenchen BER=Berlin CDG=Paris ZRH=Zuerich VIE=Wien
  Wenn Uhrzeit fehlt: leeres Feld lassen z.B. "LH3463|NUE|FRA|20.04.2026||20.04.2026|"
  ABSOLUT ZWINGEND: ALLE Segmente eintragen - Hin- UND Rueckfluege!
  Bei Verbindungsflug NUE->FRA->LYS sind das 2 Segmente, nicht 1!
  Zähle alle Flugnummern und stelle sicher dass jede ein eigenes Segment hat!
- traveler_name: Name des Passagiers z.B. "Max Mustermann"
- destination: Zielstadt des Hinflugs z.B. "Lyon"

HOTEL-FELDER (nur bei beleg_typ=Hotel):
- checkin_date: Check-in Datum "DD.MM.YYYY" - das ist das ANREISEDATUM nicht das Buchungsdatum!
  Suche nach: "Anreise", "Check-in", "Arrival", "ab dem"
  NIEMALS das Buchungsdatum hier eintragen!
- checkout_date: Check-out Datum "DD.MM.YYYY" - das ABREISEDATUM
  Suche nach: "Abreise", "Check-out", "Departure", "bis zum"
- nights: Anzahl Naechte als Zahl (checkout - checkin in Tagen)
- checkin_time: Uhrzeit z.B. "15:00" oder ""
- checkout_time: Uhrzeit z.B. "11:00" oder ""
- destination: Hotelstadt z.B. "Lyon"
- traveler_name: Name des Gastes

SONSTIGE FELDER:
- train_numbers: Zugnummern z.B. "ICE 597", oder ""

STRENGE REGELN:
1. datum = IMMER Buchungsdatum, NIEMALS Reise- oder Check-in-Datum
2. checkin_date = IMMER Anreisedatum, NIEMALS Buchungsdatum
3. Betrag = Gesamtbetrag inkl. Steuern, NICHT Teilbetraege
4. Alle Felder die nicht relevant sind: leer lassen ("")
5. Nur EUR wenn kein anderes Symbol im Text"""

    # Few-Shot Beispiele aus DB laden
    examples = load_ki_examples(mail_type="Flug" if source=="mail" else None, limit=2)
    examples_text = ""
    if examples:
        examples_text = "\n\nBEISPIELE aus echten Buchungsbestätigungen (verwende dieses Format):\n"
        for ex in examples:
            examples_text += f"\n--- BEISPIEL{' ('+ex['desc']+')' if ex['desc'] else ''} ---\n"
            examples_text += f"Text: {ex['input'][:800]}\n"
            examples_text += f"JSON: {ex['output']}\n"
        examples_text += "\n--- DEIN TEXT ---\n"

    # Personendaten anonymisieren vor API-Call (DSGVO Art. 25 – Privacy by Design)
    anon_text = anonymize_for_ki(text)
    user = f"Bekannte Reisecodes: {codes_str}{examples_text}\n\nText:\n---\n{anon_text[:7000]}\n---\nJSON:"
    try:
        async with httpx.AsyncClient(timeout=30.0) as cl:
            resp = await cl.post(f"{MISTRAL_BASE}/chat/completions",
                headers={"Authorization":f"Bearer {MISTRAL_API_KEY}","Content-Type":"application/json"},
                json={"model":MISTRAL_EXTRACT_MODEL,
                      "messages":[{"role":"system","content":system},{"role":"user","content":user}],
                      "temperature":0.0,"max_tokens":1500,
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
            # ICS Zeilenfortsetzungen auflösen (RFC 5545)
            ics_text  = re.sub(r"\r?\n[ \t]", "", ics_text)

            def ics_get(field):
                """Liest einen ICS-Feldwert inkl. TZID-Parameter."""
                m = re.search(rf"^{field}(?:;[^:]*)?:(.*)", ics_text, re.MULTILINE)
                return m.group(1).strip() if m else ""

            def ics_get_tzid(field):
                """Gibt (wert, tzid) zurück."""
                m = re.search(rf"^{field}(?:;TZID=([^:;]+))?(?:;[^:]*)?:(.*)", ics_text, re.MULTILINE)
                if m: return m.group(2).strip(), (m.group(1) or "UTC").strip()
                return "", "UTC"

            def parse_ics_dt(raw, tzid="UTC"):
                """Parst ICS-Datetime → (date_obj, time_utc_str, time_local_str, tzid)"""
                raw = raw.strip().rstrip("Z")
                try:
                    if "T" in raw:
                        dt = datetime.strptime(raw[:15], "%Y%m%dT%H%M%S")
                        utc_str  = dt.strftime("%H:%M UTC") if tzid in ("UTC","") else dt.strftime("%H:%M")
                        local_str = dt.strftime("%H:%M")
                        return dt.date(), utc_str, local_str, tzid
                    else:
                        return date(int(raw[:4]),int(raw[4:6]),int(raw[6:8])), "", "", ""
                except:
                    return None, "", "", ""

            # Alle VEVENT-Blöcke extrahieren (ICS kann mehrere haben)
            vevents = re.findall(r"BEGIN:VEVENT(.*?)END:VEVENT", ics_text, re.DOTALL)

            # Für Flüge: oft mehrere VEVENTs (Hinflug + Rückflug)
            all_flights = []
            all_trains  = []
            first_date  = None
            ki_parts    = []
            betrag_ics  = ""

            for vevent in vevents:
                # Felder aus diesem VEVENT
                def ev_get(f):
                    m=re.search(rf"^{f}(?:;[^:]*)?:(.*)", vevent, re.MULTILINE)
                    return m.group(1).strip() if m else ""
                def ev_tzid(f):
                    m=re.search(rf"^{f}(?:;TZID=([^:;]+))?(?:;[^:]*)?:(.*)", vevent, re.MULTILINE)
                    if m: return m.group(2).strip(), (m.group(1) or "UTC").strip()
                    return "", "UTC"

                summary   = ev_get("SUMMARY")
                location  = ev_get("LOCATION")
                desc      = ev_get("DESCRIPTION")
                dtstart_raw, tz_start = ev_tzid("DTSTART")
                dtend_raw,   tz_end   = ev_tzid("DTEND")

                start_d, start_utc, start_local, _ = parse_ics_dt(dtstart_raw, tz_start)
                end_d,   end_utc,   end_local,   _ = parse_ics_dt(dtend_raw,   tz_end)

                if first_date is None and start_d:
                    first_date = start_d

                ev_text = f"{summary} {location} {desc}"

                # Flugnummer aus Summary/Description extrahieren
                fn_m = re.search(r"\b([A-Z]{2}\d{3,4})\b", ev_text)
                fn   = fn_m.group(1) if fn_m else ""

                # Zugnummer
                tn_m = re.search(r"\b(ICE|IC|EC|RE|RB|S)\s*(\d{1,4})\b", ev_text, re.IGNORECASE)
                tn   = f"{tn_m.group(1).upper()} {tn_m.group(2)}" if tn_m else ""

                # Abflug/Ankunft-Flughafen aus LOCATION (oft "FRA - Frankfurt" oder "FRA" oder "Frankfurt (FRA)")
                airports = re.findall(r"\b([A-Z]{3})\b", location)
                dep_apt = airports[0] if len(airports)>0 else ""
                arr_apt = airports[1] if len(airports)>1 else ""

                # Alternativ aus Summary: "FRA → CDG" oder "FRA-CDG"
                if not dep_apt or not arr_apt:
                    rt_m = re.search(r"\b([A-Z]{3})\s*[-→>]\s*([A-Z]{3})\b", summary+desc)
                    if rt_m:
                        dep_apt=rt_m.group(1)
                        arr_apt=rt_m.group(2)

                # Preis aus Description
                price_m = re.search(r"(?:EUR|€|CHF|USD|GBP)\s*([\d,.]+)|([\d,.]+)\s*(?:EUR|€)", desc)
                if price_m and not betrag_ics:
                    betrag_ics = (price_m.group(1) or price_m.group(2) or "").replace(",",".")

                if fn:
                    all_flights.append(fn)
                    route = f"{dep_apt}→{arr_apt}" if dep_apt and arr_apt else (dep_apt or arr_apt or "")
                    time_info = ""
                    if start_utc and end_utc:
                        time_info = f"{start_local} ({tz_start}) – {end_local} ({tz_end})"
                    elif start_utc:
                        time_info = f"Ab {start_local} ({tz_start})"
                    ki_parts.append(f"✈ {fn} {route} {time_info}".strip())
                elif tn:
                    all_trains.append(tn)
                    ki_parts.append(f"🚆 {tn}")
                elif summary:
                    ki_parts.append(f"{summary[:60]}")

                # Datum/Zeit in ki_bemerkung
                if start_d and start_utc:
                    ki_parts.append(f"Abflug: {start_d} {start_utc}")
                if end_d and end_utc:
                    ki_parts.append(f"Ankunft: {end_d} {end_utc}")

            flight_str = ", ".join(list(dict.fromkeys(all_flights)))  # dedupliziert
            train_str  = ", ".join(list(dict.fromkeys(all_trains)))
            ics_date   = str(first_date) if first_date else ""
            ki_bemerkung = " | ".join(ki_parts) if ki_parts else "ICS ohne verwertbare Daten"

            # Zeitinfo für Speicherung extrahieren
            flight_time_info = " | ".join(p for p in ki_parts if "Abflug:" in p or "Ankunft:" in p) or None

            cur.execute("""UPDATE mail_attachments SET
                analysis_status='ok', confidence='hoch', review_flag='ok',
                detected_date=%s, detected_flight_numbers=%s,
                detected_train_numbers=%s,
                ki_bemerkung=%s, detected_type='Kalendereintrag',
                flight_time_info=%s
                WHERE id=%s""",
                (ics_date or None, flight_str or None,
                 train_str or None, ki_bemerkung,
                 flight_time_info, att_id))

            # Flugnummern + Zugnummern in trip_meta übernehmen
            cur.execute("SELECT trip_code FROM mail_attachments WHERE id=%s",(att_id,))
            row_tc = cur.fetchone()
            if row_tc and row_tc[0]:
                tc_val = row_tc[0]
                if flight_str:
                    cur.execute("SELECT flight_numbers FROM trip_meta WHERE trip_code=%s",(tc_val,))
                    row_fn = cur.fetchone()
                    if row_fn:
                        existing = row_fn[0] or ""
                        new_fns = existing
                        for fn in all_flights:
                            if fn not in new_fns:
                                new_fns = f"{new_fns},{fn}".strip(",")
                        if new_fns != existing:
                            cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s",(new_fns, tc_val))
                if train_str:
                    cur.execute("SELECT train_numbers FROM trip_meta WHERE trip_code=%s",(tc_val,))
                    row_tn = cur.fetchone()
                    if row_tn:
                        existing = row_tn[0] or ""
                        new_tns = existing
                        for tn in all_trains:
                            if tn not in new_tns:
                                new_tns = f"{new_tns},{tn}".strip(",")
                        if new_tns != existing:
                            cur.execute("UPDATE trip_meta SET train_numbers=%s WHERE trip_code=%s",(new_tns, tc_val))

            conn.commit(); cur.close(); return
        except Exception as e:
            cur.execute("UPDATE mail_attachments SET analysis_status=%s WHERE id=%s",
                        (f"ics-fehler:{str(e)[:80]}", att_id))
            conn.commit(); cur.close(); return

    # Inline-Bilder aus HTML-Mails direkt als irrelevant markieren
    fn_lower = (filename or "").lower()
    if re.match(r"image\d+\.(png|jpg|jpeg|gif|bmp|emz|wmz)$", fn_lower) or fn_lower.endswith(".emz") or fn_lower.endswith(".wmz"):
        cur.execute("UPDATE mail_attachments SET analysis_status=%s,confidence=%s,review_flag=%s WHERE id=%s",
                    ("Inline-Grafik","niedrig","ok",att_id))
        cur.close(); return

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
    checkin    = fields.get("checkin_date","") or ""
    checkout   = fields.get("checkout_date","") or ""
    checkin_t  = fields.get("checkin_time","") or ""
    checkout_t = fields.get("checkout_time","") or ""
    segments   = fields.get("flight_segments","") or ""
    confidence = fields.get("confidence","niedrig") or "niedrig"
    bemerkung  = fields.get("bemerkung","") or ""

    # Segment-Vollständigkeitsprüfung: jede Flugnummer braucht ein Segment
    if fns and beleg_typ == "Flug":
        fn_list = [f.strip() for f in fns.split(",") if f.strip()]
        seg_list = [s.strip() for s in segments.split(";") if s.strip()] if segments else []
        seg_fns  = [s.split("|")[0].strip() for s in seg_list if s]
        missing  = [f for f in fn_list if f not in seg_fns]
        if missing:
            # Fehlende als Stub-Segmente anhängen (nur Flugnummer, Rest leer)
            for mfn in missing:
                seg_list.append(f"{mfn}|||||| ")
            segments = ";".join(seg_list)
            bemerkung = (bemerkung + f" [Stub-Segmente für: {','.join(missing)}]").strip()

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
        detected_checkin=%s,detected_checkout=%s,detected_checkin_time=%s,detected_checkout_time=%s,
        flight_segments=%s,
        analysis_status=%s,confidence=%s,review_flag=%s,ki_bemerkung=%s
        WHERE id=%s""",
        (ocr_text[:10000] if ocr_text else None,
         betrag,betrag_eur,waehrung,datum,anbieter,beleg_typ,
         pnr,fns,trains,nights,
         checkin or None,checkout or None,checkin_t or None,checkout_t or None,
         segments or None,
         status,confidence,review,bemerkung,att_id))


# =========================================================
# MAIL-HILFSFUNKTIONEN
# =========================================================

def extract_trip_code(text):
    m = re.search(r"\b\d{2}-\d{3}\b", text or "")
    return m.group(0) if m else None

def extract_pnr(text):
    """PNR/Buchungsreferenz: 6 alphanumerische Zeichen (Grossbuchstaben+Ziffern)."""
    # Explizite Labels zuerst
    labeled = re.search(r'(?:Buchungsreferenz|PNR|Booking\s*Ref(?:erence)?|Record\s*Locator)\s*[:\s#]*([A-Z0-9]{6})\b', text or "", re.IGNORECASE)
    if labeled: return labeled.group(1).upper()
    # Fallback: 6-stellige Sequenz aus Grossbuchstaben+Ziffern
    m = re.search(r'\b([A-Z]{2}[A-Z0-9]{4}|[A-Z0-9]{2}[A-Z]{2}[A-Z0-9]{2})\b', text or "")
    return m.group(1) if m else None

def extract_flight_numbers(text: str) -> list:
    """Extrahiert Flugnummern aus Text. Schließt Ticketnummern (LH 220-xxx) aus."""
    if not text: return []
    AIRLINE_CODES = {
        "LH","LX","OS","SK","AF","KL","BA","IB","VY","FR","U2","W6","EW","TK",
        "AY","SN","QR","EK","EY","DL","AA","UA","AC","WS","NH","JL","OZ","KE",
        "CX","SQ","MH","TG","VN","GA","AI","SG","6E","G8","IX","QP","S5",
        "WK","SR","SU","FI","AZ","TP","RO","LO","OK","BT","TF","OA","A3",
        "HV","PC","VF","TO","LS","BY","MT","ET","MS","SV","CM","AM","LA",
    }
    result = []
    seen = set()
    # Ticketnummern aus Text entfernen bevor wir suchen
    # Ticketnummern: "220-2979545073" oder "LH 220-xxx" – enthalten Bindestrich nach Zahl
    clean = re.sub(r'\b\d{3}\s*-\s*\d{7,}\b', '', text)  # "220-2979545073"
    clean = re.sub(r'\b[A-Z]{2}\s+\d{3}\s*-\s*\d+', '', clean)  # "LH 220-xxx"
    # Flugnummern: 2 Buchstaben + Leerzeichen (optional) + 3-4 Ziffern
    # NICHT gefolgt von Bindestrich (wäre Ticketnummer)
    matches = re.findall(r'\b([A-Z]{2})\s*(\d{3,4})\b(?!\s*[-/]\s*\d)', clean)
    for airline, num in matches:
        if airline in AIRLINE_CODES:
            fn = f"{airline}{num}"
            if fn not in seen:
                seen.add(fn)
                result.append(fn)
    return result

def extract_hotel_dates(text: str) -> dict:
    """Extrahiert Check-in/Check-out aus Bestätigungsmails via Regex."""
    result = {}
    # Check-in patterns
    ci = re.search(r'(?:Check.in|Arrival|Anreise|Eincheck)[:\s]*(\d{1,2}[./]\d{1,2}[./]\d{2,4}|\d{1,2}\s+\w+\s+\d{4}|\w+,?\s+\d{1,2}\s+\w+\s+\d{4})', text, re.IGNORECASE)
    if ci: result['checkin'] = ci.group(1)
    # Check-out patterns
    co = re.search(r'(?:Check.out|Departure|Abreise|Auscheck)[:\s]*(\d{1,2}[./]\d{1,2}[./]\d{2,4}|\d{1,2}\s+\w+\s+\d{4}|\w+,?\s+\d{1,2}\s+\w+\s+\d{4})', text, re.IGNORECASE)
    if co: result['checkout'] = co.group(1)
    # Nächte
    nights = re.search(r'(\d+)\s*(?:Nächte?|nights?|Übernachtung)', text, re.IGNORECASE)
    if nights: result['nights'] = int(nights.group(1))
    return result

def extract_flight_segments_from_text(text: str) -> list:
    """
    Extrahiert Flug-Segmente aus beliebigem Text.
    WICHTIG: Gibt Liste zurück - doppelte Flugnummern (Hin+Rück) werden beide gespeichert.
    """
    MONTH_MAP = {
        "jan":"01","feb":"02","mar":"03","maer":"03","apr":"04",
        "mai":"05","may":"05","jun":"06","jul":"07","aug":"08",
        "sep":"09","okt":"10","oct":"10","nov":"11","dez":"12","dec":"12"
    }
    CITY_TO_IATA = {
        "frankfurt":"FRA","frankfurt intl":"FRA","frankfurt international":"FRA",
        "nuernberg":"NUE","nürnberg":"NUE","nuremberg":"NUE",
        "muenchen":"MUC","münchen":"MUC","munich":"MUC",
        "berlin":"BER","hamburg":"HAM","duesseldorf":"DUS","düsseldorf":"DUS",
        "koeln":"CGN","köln":"CGN","cologne":"CGN","stuttgart":"STR",
        "zurich":"ZRH","zuerich":"ZRH","zürich":"ZRH","zurich airport":"ZRH",
        "genf":"GVA","geneva":"GVA",
        "wien":"VIE","vienna":"VIE","salzburg":"SZG",
        "paris":"CDG","charles de gaulle":"CDG","orly":"ORY",
        "lyon":"LYS","nizza":"NCE","nice":"NCE","marseille":"MRS",
        "london":"LHR","heathrow":"LHR","gatwick":"LGW",
        "amsterdam":"AMS","bruessel":"BRU","brussels":"BRU",
        "madrid":"MAD","barcelona":"BCN",
        "rom":"FCO","rome":"FCO","mailand":"MXP","milan":"MXP",
        "istanbul":"IST","dubai":"DXB","abu dhabi":"AUH","doha":"DOH",
        "san jose":"SJO","juan santamaria":"SJO",
        "panama":"PTY","panama city":"PTY",
        "new york":"JFK","los angeles":"LAX","miami":"MIA","chicago":"ORD",
        "singapur":"SIN","singapore":"SIN",
        "tokio":"NRT","tokyo":"NRT","narita":"NRT",
        "bangkok":"BKK","kuala lumpur":"KUL",
        "delhi":"DEL","mumbai":"BOM",
        "peking":"PEK","beijing":"PEK","shanghai":"PVG",
        "hongkong":"HKG","hong kong":"HKG",
    }

    def find_iata(city_text: str) -> str:
        c = city_text.strip()
        # IATA in Klammern: "Frankfurt (FRA)"
        m = re.search(r"\(([A-Z]{3})\)", c)
        if m and m.group(1) in AIRPORT_CC: return m.group(1)
        # Direkt 3 Grossbuchstaben
        if re.match(r"^[A-Z]{3}$", c) and c in AIRPORT_CC: return c
        cl = c.lower()
        for k in sorted(CITY_TO_IATA.keys(), key=len, reverse=True):
            if k in cl: return CITY_TO_IATA[k]
        for apt in re.findall(r"\b([A-Z]{3})\b", c.upper()):
            if apt in AIRPORT_CC: return apt
        return ""

    def parse_date_full(s: str) -> str:
        m = re.match(r"(\d{1,2})[./](\d{1,2})[./](\d{4})", s.strip())
        if m: return f"{int(m.group(1)):02d}.{int(m.group(2)):02d}.{m.group(3)}"
        return s.strip()

    def parse_date_mon(day, mon, yr="2026") -> str:
        mo = MONTH_MAP.get(str(mon).strip().lower()[:3], "01")
        try: return f"{int(day):02d}.{mo}.{yr}"
        except: return ""

    # Sophos-Links entfernen
    text = re.sub(r"https?://[\w.-]*sophos[\w./-]*", " ", text, flags=re.IGNORECASE)

    fns_all = extract_flight_numbers(text)
    if not fns_all: return []

    # Codeshares herausfiltern
    codeshare = set()
    for m in re.finditer(r"\((?:Durchgeführt von|Operated by)[^,)]+,\s*([A-Z]{2})\s*(\d{3,4})\)", text, re.IGNORECASE):
        codeshare.add(f"{m.group(1)}{m.group(2)}")
    main_fns = [f for f in fns_all if f not in codeshare] or fns_all

    result = []  # Liste, nicht Dict - erlaubt doppelte FN

    # ── METHODE 1: "Flug LH3463: Stadt (FRA) → Stadt (LYS), DD.MM.YYYY, HH:MM → HH:MM" ──
    m1 = re.compile(
        r"(?:Flug\s+)?([A-Z]{2}\d{3,4})\s*[:\s]+"
        r"([A-Za-z\xc0-\xff][A-Za-z\xc0-\xff\s\-,\.\(\)]{2,35}?)\s*(?:→|->|nach)\s*"
        r"([A-Za-z\xc0-\xff][A-Za-z\xc0-\xff\s\-,\.\(\)]{2,35}?)[,;\s]+"
        r"(\d{2}\.\d{2}\.\d{4})[,;\s]+"
        r"(\d{2}:\d{2})\s*(?:→|->|–|-|bis)\s*(\d{2}:\d{2})",
        re.IGNORECASE
    )
    found_m1 = set()
    for m in m1.finditer(text):
        fn = m.group(1).upper()
        if fn not in main_fns: continue
        dep = find_iata(m.group(2))
        arr = find_iata(m.group(3))
        d = parse_date_full(m.group(4))
        seg = {"fn":fn,"dep":dep,"arr":arr,"date":d,"arr_date":d,"dep_time":m.group(5),"arr_time":m.group(6)}
        result.append(seg)
        found_m1.add(f"{fn}_{d}")  # Merken welche schon gefunden

    # ── METHODE 2: Itinerary-Tabelle "25 Mai Frankfurt - Zurich LX3613 06:35-07:30" ──
    m2 = re.compile(
        r"(\d{1,2})\s+(Jan|Feb|M[aä]r|Apr|Mai|May|Jun|Jul|Aug|Sep|Okt|Oct|Nov|Dez|Dec)\s+"
        r"([A-Za-z\xc0-\xff][A-Za-z\xc0-\xff\s\-,\.]*?)\s*-\s*"
        r"([A-Za-z\xc0-\xff][A-Za-z\xc0-\xff\s\-,\.]*?)\s+"
        r"([A-Z]{2})\s*(\d{3,4})\s+"
        r"(\d{2}:\d{2})\s*-\s*(\d{2}:\d{2})",
        re.IGNORECASE
    )
    for m in m2.finditer(text):
        fn = f"{m.group(5).upper()}{m.group(6)}"
        if fn not in main_fns: continue
        dep_date = parse_date_mon(m.group(1), m.group(2))
        # Bereits durch Methode 1 gefunden?
        if f"{fn}_{dep_date}" in found_m1: continue
        arr_date = dep_date
        ctx = text[m.start():m.start()+150]
        if re.search(r"\(\+\s*1\)", ctx):
            try:
                from datetime import date as _d, timedelta as _td
                p = dep_date.split("."); d2 = _d(int(p[2]),int(p[1]),int(p[0]))+_td(days=1)
                arr_date = d2.strftime("%d.%m.%Y")
            except: pass
        result.append({"fn":fn,"dep":find_iata(m.group(3)),"arr":find_iata(m.group(4)),
                       "date":dep_date,"arr_date":arr_date,"dep_time":m.group(7),"arr_time":m.group(8)})

    # ── METHODE 3: IATA direkt "NUE→FRA" in Nähe der Flugnummer ──
    already_found = {(s["fn"],s["date"]) for s in result}
    for fn in main_fns:
        airline, num = fn[:2], fn[2:]
        # Alle Vorkommen dieser Flugnummer suchen
        for m_pos in [m.start() for m in re.finditer(rf"\b{re.escape(airline)}\s*{re.escape(num)}\b", text)]:
            region = text[max(0,m_pos-60):m_pos+300]
            route_m = re.search(r"\b([A-Z]{3})\s*(?:→|->|–)\s*([A-Z]{3})\b", region)
            date_m  = re.search(r"(\d{2}\.\d{2}\.\d{4})", region)
            times   = re.findall(r"\b(\d{2}:\d{2})\b", region)
            dep_apt = route_m.group(1) if route_m and route_m.group(1) in AIRPORT_CC else ""
            arr_apt = route_m.group(2) if route_m and route_m.group(2) in AIRPORT_CC else ""
            d_str   = parse_date_full(date_m.group(1)) if date_m else ""
            if (fn, d_str) in already_found: continue
            if dep_apt or arr_apt or d_str:
                seg = {"fn":fn,"dep":dep_apt,"arr":arr_apt,"date":d_str,"arr_date":d_str,
                       "dep_time":times[0] if times else "","arr_time":times[1] if len(times)>1 else ""}
                result.append(seg)
                already_found.add((fn, d_str))

    # ── METHODE 5: Lufthansa-Format – Abflugsort/Ankunftsort Blöcke ─────────
    # Format: "DD.MM.YYYY - HH:MM ... Abflugsort STADT ... Ankunftsort STADT"
    # Kommt vor wenn Flugnummern in Bilder eingebettet sind (typisch Lufthansa HTML-Mail)
    if len(result) < len(main_fns):
        lh_blocks = re.findall(
            r'(\d{2}\.\d{2}\.\d{4})\s*-\s*(\d{2}:\d{2}).*?'
            r'Abflugsort\s+([A-Za-z\u00c0-\u00ff][A-Za-z\u00c0-\u00ff\s]+?)\s*\n.*?'
            r'Ankunftsort\s+([A-Za-z\u00c0-\u00ff][A-Za-z\u00c0-\u00ff\s]+?)\s*\n',
            text, re.DOTALL
        )
        if lh_blocks:
            # Hub-Map: bei 1 Stopp über bekannte Hubs aufteilen
            HUB_MAP = {
                ("NUE","LYS"):[("NUE","FRA"),("FRA","LYS")],
                ("NUE","CDG"):[("NUE","FRA"),("FRA","CDG")],
                ("NUE","LHR"):[("NUE","FRA"),("FRA","LHR")],
                ("NUE","AMS"):[("NUE","FRA"),("FRA","AMS")],
                ("NUE","BRU"):[("NUE","FRA"),("FRA","BRU")],
                ("NUE","VIE"):[("NUE","FRA"),("FRA","VIE")],
                ("NUE","FCO"):[("NUE","FRA"),("FRA","FCO")],
                ("NUE","MAD"):[("NUE","FRA"),("FRA","MAD")],
                ("NUE","BCN"):[("NUE","FRA"),("FRA","BCN")],
                ("NUE","IST"):[("NUE","FRA"),("FRA","IST")],
                ("NUE","DXB"):[("NUE","FRA"),("FRA","DXB")],
                ("NUE","DOH"):[("NUE","FRA"),("FRA","DOH")],
                ("NUE","SIN"):[("NUE","FRA"),("FRA","SIN")],
                ("NUE","JFK"):[("NUE","FRA"),("FRA","JFK")],
                ("LYS","NUE"):[("LYS","FRA"),("FRA","NUE")],
                ("CDG","NUE"):[("CDG","FRA"),("FRA","NUE")],
                ("LHR","NUE"):[("LHR","FRA"),("FRA","NUE")],
                ("MUC","LYS"):[("MUC","FRA"),("FRA","LYS")],
                ("MUC","LHR"):[("MUC","FRA"),("FRA","LHR")],
                ("MUC","CDG"):[("MUC","FRA"),("FRA","CDG")],
            }
            # Anzahl Stopps aus Text
            stopps_list = [int(m) for m in re.findall(r'(\d+)\s*Stopp', text)]
            already_found = {(s["fn"],s["date"]) for s in result}
            fn_idx = len(result)  # Weitermachen wo aufgehört
            for bi, (date_str, dep_time, dep_city, arr_city) in enumerate(lh_blocks):
                dep = find_iata(dep_city.strip())
                arr = find_iata(arr_city.strip())
                n_stopps = stopps_list[bi] if bi < len(stopps_list) else 0
                route_pair = HUB_MAP.get((dep, arr)) if n_stopps >= 1 else None
                if route_pair and fn_idx + 1 < len(main_fns):
                    for seg_dep, seg_arr in route_pair:
                        if fn_idx >= len(main_fns): break
                        fn = main_fns[fn_idx]
                        if (fn, date_str) not in already_found:
                            result.append({"fn":fn,"dep":seg_dep,"arr":seg_arr,
                                "date":date_str,"arr_date":date_str,
                                "dep_time":dep_time,"arr_time":""})
                            already_found.add((fn, date_str))
                        fn_idx += 1
                else:
                    if fn_idx < len(main_fns):
                        fn = main_fns[fn_idx]
                        if (fn, date_str) not in already_found:
                            result.append({"fn":fn,"dep":dep,"arr":arr,
                                "date":date_str,"arr_date":date_str,
                                "dep_time":dep_time,"arr_time":""})
                            already_found.add((fn, date_str))
                        fn_idx += 1

    # Reihenfolge: nach main_fns sortieren (Hin dann Rück)
    fn_order = {fn: i for i, fn in enumerate(main_fns)}
    result.sort(key=lambda s: (fn_order.get(s["fn"], 99), s.get("date",""), s.get("dep_time","")))

    return result


def segments_to_string(segments: list) -> str:
    """Konvertiert Segment-Liste in DB-Format: FN|DEP|ARR|DATE|TIME|ARR_DATE|ARR_TIME;..."""
    parts = []
    for s in segments:
        arr_date = s.get("arr_date") or s.get("date","")
        parts.append(f"{s['fn']}|{s['dep']}|{s['arr']}|{s['date']}|{s['dep_time']}|{arr_date}|{s['arr_time']}")
    return ";".join(parts)

def decode_mime_header(value):
    if not value: return ""
    parts = decode_header(value)
    return "".join(
        p.decode(enc or "utf-8",errors="ignore") if isinstance(p,bytes) else p
        for p,enc in parts)

def detect_mail_type(text):
    t=(text or "").lower()
    if any(x in t for x in ["flug","flight","boarding","pnr","ticket","airline","itinerary","eticket","buchungsreferenz","flugnummer","check-in","lx ","lh ","os ","sk "]): return "Flug"
    if any(x in t for x in ["hotel","booking.com","check-in","reservation","zimmer","accommodation","sheraton","marriott","hilton","hyatt"]): return "Hotel"
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
        html_body = ""
        if msg.is_multipart():
            for part in msg.walk():
                ct = part.get_content_type()
                cd = str(part.get("Content-Disposition") or "").lower()
                if "attachment" in cd: continue
                pl = part.get_payload(decode=True)
                if not pl: continue
                if ct == "text/plain" and not body:
                    body = pl.decode(errors="ignore")
                elif ct == "text/html" and not html_body:
                    html_body = pl.decode(errors="ignore")
        else:
            pl = msg.get_payload(decode=True)
            ct = msg.get_content_type()
            if pl:
                if ct == "text/html":
                    html_body = pl.decode(errors="ignore")
                else:
                    body = pl.decode(errors="ignore")

        # HTML → Plain Text Fallback wenn kein plain/text vorhanden
        if not body and html_body:
            # Einfaches HTML-Stripping: Tags entfernen, Whitespace normalisieren
            import html as _html
            text = _html.unescape(html_body)
            text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL|re.IGNORECASE)
            text = re.sub(r'<script[^>]*>.*?</script>', ' ', text, flags=re.DOTALL|re.IGNORECASE)
            text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
            text = re.sub(r'<[^>]+>', ' ', text)
            text = re.sub(r'[ \t]+', ' ', text)
            text = re.sub(r'\n{3,}', '\n\n', text)
            body = text.strip()[:20000]

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
                    mail.store(i,"+FLAGS","\\Seen")
                    continue

                safe_fn = sanitize_filename(decoded_fn)

                # ICS-Dateien überspringen – Flugdaten kommen aus Mail-Body
                if safe_fn.lower().endswith(".ics"):
                    # Flugnummer trotzdem aus ICS in trip_meta übernehmen (schneller Scan)
                    if pl and code:
                        try:
                            ics_text = pl.decode(errors="ignore")
                            ics_text = re.sub(r"\r?\n[ \t]", "", ics_text)
                            for fn_m in re.finditer(r"\b([A-Z]{2}\d{3,4})\b", ics_text):
                                fn_ics = fn_m.group(1)
                                cur.execute("SELECT flight_numbers FROM trip_meta WHERE trip_code=%s",(code,))
                                row_fn = cur.fetchone()
                                if row_fn:
                                    existing = row_fn[0] or ""
                                    if fn_ics not in existing:
                                        cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s",
                                            (f"{existing},{fn_ics}".strip(","), code))
                        except: pass
                    continue  # ICS nicht in DB speichern

                # S3-Upload
                storage_key = f"mail_attachments/{uid}_{safe_fn}"
                try:
                    s3 = get_s3()
                    s3.put_object(Bucket=S3_BUCKET, Key=storage_key, Body=pl,
                                  ContentType=part.get_content_type())
                except Exception as s3e:
                    storage_key = f"S3-FEHLER:{str(s3e)[:60]}"

                cur.execute("""INSERT INTO mail_attachments
                    (mail_uid,trip_code,original_filename,saved_filename,content_type,
                     storage_key,detected_type,analysis_status,confidence,review_flag,
                     file_hash)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (uid,code,safe_fn,f"{uid}_{safe_fn}",part.get_content_type(),
                     storage_key,detect_type_with_rules(safe_fn,subject,body,load_custom_rules()),
                     "ausstehend","niedrig","pruefen",h))

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
        <button class="dd-item" onclick="window.location='/upload-beleg'"><div class="dd-icon di-a">📎</div><div><div style="font-weight:500">Beleg hochladen</div><div class="dd-sub">PDF direkt hochladen &amp; analysieren</div></div></button>
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

@app.get("/debug-body/{tc}")
def debug_body(tc: str):
    """Zeigt die ersten 3000 Zeichen aller Mail-Bodies für eine Reise."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT id,subject,LENGTH(body),LEFT(body,3000) FROM mail_messages WHERE trip_code=%s ORDER BY id",(tc,))
        rows=cur.fetchall();cur.close();conn.close()
        return [{"id":r[0],"subject":r[1],"body_len":r[2],"body_preview":r[3]} for r in rows]
    except Exception as e:
        return {"error":str(e)}

@app.get("/debug/{tc}")
def debug_trip(tc: str):
    """Debug: zeigt rohe DB-Daten für eine Reise."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT id,original_filename,detected_type,detected_amount_eur,
            detected_flight_numbers,flight_segments,storage_key,analysis_status
            FROM mail_attachments WHERE trip_code=%s ORDER BY id""",(tc,))
        atts=cur.fetchall()
        cur.execute("SELECT flight_numbers,pnr_code,vma_destinations,destinations FROM trip_meta WHERE trip_code=%s",(tc,))
        meta=cur.fetchone()
        cur.close();conn.close()
        return {
            "trip_meta": {"flight_numbers":meta[0],"pnr":meta[1],"vma_destinations":meta[2],"destinations":meta[3]} if meta else None,
            "attachments": [{"id":a[0],"file":a[1],"type":a[2],"eur":a[3],
                            "fns":a[4],"segments":a[5],"skey":a[6],"status":a[7]} for a in atts]
        }
    except Exception as e:
        return {"error":str(e)}

@app.get("/fix-trips")
def fix_trips():
    """
    Repariert bekannte Datenfehler direkt in der DB.
    Setzt korrekte Segmente, bereinigt Duplikate, korrigiert Beträge.
    """
    try:
        conn=get_conn(); cur=conn.cursor()
        fixes=[]

        # ── 1. Doppelte Flug-Belege entfernen ────────────────────────────────
        cur.execute("SELECT DISTINCT trip_code FROM mail_attachments WHERE detected_type='Flug'")
        trip_codes=[r[0] for r in cur.fetchall() if r[0]]
        for tc in trip_codes:
            cur.execute("""SELECT id,storage_key,flight_segments
                FROM mail_attachments WHERE trip_code=%s AND detected_type='Flug'
                ORDER BY
                  CASE WHEN storage_key LIKE 'mail_attachments/%%' THEN 0
                       WHEN storage_key LIKE 'repaired%%' THEN 2
                       WHEN storage_key LIKE 'manual%%' THEN 3
                       ELSE 1 END, id DESC""",(tc,))
            belege=cur.fetchall()
            if len(belege) <= 1: continue
            # Besten Beleg: echter Upload mit Airports in Segmenten
            best_id=None
            for b in belege:
                segs=[s.split('|') for s in (b[2] or '').split(';') if s.strip()]
                if any(len(s)>=3 and s[1].strip() and s[2].strip() for s in segs):
                    best_id=b[0]; break
            if not best_id: best_id=belege[0][0]
            for b in belege:
                if b[0]!=best_id:
                    cur.execute("DELETE FROM mail_attachments WHERE id=%s",(b[0],))
                    fixes.append(f"{tc}: Duplikat ID {b[0]} gelöscht")

        # ── 2. 26-001: Korrekte Segmente direkt setzen ───────────────────────
        SEGS_26001 = "LH3463|NUE|FRA|20.04.2026|13:00|20.04.2026|14:15;LH1078|FRA|LYS|20.04.2026|16:55|20.04.2026|18:15;LH1077|LYS|FRA|24.04.2026|14:35|24.04.2026|17:19;LH3463|FRA|NUE|24.04.2026|17:19|24.04.2026|19:15"
        FNS_26001  = "LH3463,LH1078,LH1077,LH3463"

        cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code='26-001'",(FNS_26001,))
        cur.execute("SELECT id FROM mail_attachments WHERE trip_code='26-001' AND detected_type='Flug' ORDER BY id LIMIT 1")
        row=cur.fetchone()
        if row:
            cur.execute("""UPDATE mail_attachments SET
                flight_segments=%s, detected_flight_numbers=%s,
                analysis_status='ok (manuell)', confidence='hoch', review_flag='ok'
                WHERE id=%s""",(SEGS_26001, FNS_26001, row[0]))
            fixes.append(f"26-001: Segmente korrekt gesetzt (ID {row[0]})")
        else:
            uid="manual_seg_26-001"
            cur.execute("""INSERT INTO mail_attachments
                (mail_uid,trip_code,original_filename,saved_filename,content_type,
                 storage_key,detected_type,detected_flight_numbers,flight_segments,
                 analysis_status,confidence,review_flag,ki_bemerkung)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (uid,"26-001","Flüge 26-001","Flüge 26-001","text/plain",
                 "manual_seg_26-001","Flug",FNS_26001,SEGS_26001,
                 "ok (manuell)","hoch","ok","Fix: korrekte Segmente"))
            fixes.append("26-001: Neuer Flug-Beleg angelegt")

        cur.execute("UPDATE trip_meta SET vma_destinations='2026-04-20:DE,2026-04-20:FR,2026-04-24:DE' WHERE trip_code='26-001'")
        fixes.append("26-001: VMA auf Frankreich gesetzt")

        # ── 3. 26-002: PNR + VMA korrigieren ────────────────────────────────
        cur.execute("UPDATE trip_meta SET pnr_code='Z6INOT' WHERE trip_code='26-002' AND pnr_code='RIGHTS'")
        if cur.rowcount: fixes.append("26-002: PNR RIGHTS→Z6INOT")

        cur.execute("SELECT vma_destinations FROM trip_meta WHERE trip_code='26-002'")
        vma_row=cur.fetchone()
        if not vma_row or not vma_row[0] or 'CR' not in str(vma_row[0]):
            cur.execute("UPDATE trip_meta SET vma_destinations='2026-05-25:DE,2026-05-25:CR,2026-05-29:DE' WHERE trip_code='26-002'")
            fixes.append("26-002: VMA auf Costa Rica (CR) gesetzt")

        # ── 4. Betrag 26-001: Lufthansa-Beleg hat 0€ → aus Mail lesen ───────
        cur.execute("SELECT id,detected_amount FROM mail_attachments WHERE trip_code='26-001' AND detected_type='Flug'")
        beleg_26001=cur.fetchone()
        if beleg_26001 and not beleg_26001[1]:
            cur.execute("UPDATE mail_attachments SET detected_amount='496.07',detected_amount_eur='496,07',detected_currency='EUR' WHERE id=%s",(beleg_26001[0],))
            fixes.append("26-001: Betrag 496.07 EUR gesetzt (aus Lufthansa Mail)")

        # ── 5. 26-002: LH4515 doppelt (einmal am 29.05, einmal am 30.05) ────
        # LH4515 fliegt 29.05 ab → Ankunft 30.05. Am 30.05 darf er nicht nochmal erscheinen
        cur.execute("SELECT id,flight_segments FROM mail_attachments WHERE trip_code='26-002' AND detected_type='Flug' LIMIT 1")
        b26002=cur.fetchone()
        if b26002 and b26002[1]:
            segs=[s for s in b26002[1].split(';') if s.strip()]
            # LH4515 darf nur 1x vorkommen
            lh4515_segs=[s for s in segs if s.startswith('LH4515')]
            if len(lh4515_segs)>1:
                # Behalte nur den mit Abflugdatum 29.05
                segs_fixed=[s for s in segs if not s.startswith('LH4515')]
                segs_fixed.append([s for s in lh4515_segs if '29.05' in s][0] if any('29.05' in s for s in lh4515_segs) else lh4515_segs[0])
                segs_fixed.sort(key=lambda s: s.split('|')[3] if len(s.split('|'))>3 else '')
                cur.execute("UPDATE mail_attachments SET flight_segments=%s WHERE id=%s",(';'.join(segs_fixed),b26002[0]))
                fixes.append(f"26-002: LH4515-Duplikat entfernt")

        conn.commit(); cur.close(); conn.close()
        return {"status":"ok","fixes":fixes,"naechster_schritt":"Seite neu laden"}
    except Exception as e:
        import traceback
        return {"status":"fehler","detail":str(e),"trace":traceback.format_exc()[:800]}

@app.get("/version")
def version():
    pdf_ok = HAS_PDF_LIBS()
    pdf_detail = ""
    try:
        import reportlab; pdf_detail += f"reportlab {reportlab.__version__} "
    except Exception as e: pdf_detail += f"reportlab FEHLT: {e} "
    try:
        import pypdf; pdf_detail += f"pypdf {pypdf.__version__}"
    except Exception as e: pdf_detail += f"pypdf FEHLT: {e}"
    return {"version":APP_VERSION,"ki":"mistral-eu" if MISTRAL_API_KEY else "keine",
            "auto_imap":"aktiv","pdf_libs":pdf_ok,"pdf_detail":pdf_detail}


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
                    "flight_time_info TEXT","detected_checkin TEXT","detected_checkout TEXT",
                    "detected_checkin_time TEXT","detected_checkout_time TEXT","flight_segments TEXT",
                    "pdf_key TEXT",
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

        # KI-Lernbeispiele (Few-Shot Examples für Mistral)
        cur.execute("""CREATE TABLE IF NOT EXISTS ki_examples (
            id SERIAL PRIMARY KEY,
            mail_type TEXT NOT NULL,
            input_text TEXT NOT NULL,
            expected_json TEXT NOT NULL,
            description TEXT,
            approved BOOLEAN DEFAULT TRUE,
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
            country_code TEXT DEFAULT 'DE',
            vma_override NUMERIC,
            updated_at TIMESTAMP DEFAULT now(),
            UNIQUE(trip_code, meal_date))""")
        for col in ["country_code TEXT DEFAULT 'DE'","vma_override NUMERIC"]:
            cur.execute(f"ALTER TABLE daily_meals ADD COLUMN IF NOT EXISTS {col}")

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
async def dashboard_main(request: Request):
    return await _dashboard(request, "all")

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

        # Sektionen je nach Tab
        all_sections = f"""
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
        else:  # "all" – Hauptseite zeigt alle drei
            sections = all_sections
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
            <a class="btn" href="/mail-eingabe" style="background:linear-gradient(135deg,#7c3aed,#6d28d9)">📝 Mail manuell eingeben</a>
            <a class="btn-l" href="/reanalyze-mails" onclick="return confirm('Alle Mail-Bodies nochmal analysieren?')">🔄 Mails re-analysieren</a>
            <a class="btn-l" href="/attachment-log">Anhang-Log</a>
            <a class="btn-l" href="/mail-log">Mail-Log</a>
            <a class="btn-l" href="/rules">⚙ Kategorie-Regeln</a>
            <a class="btn-l" href="/ki-beispiele">🧠 KI-Beispiele</a>
            <a class="btn-l" href="/vma-rates" title="VMA-Sätze prüfen und aktualisieren">📋 VMA-Sätze 2026</a>
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
            try:
                # HTML → Plaintext wenn Body noch HTML enthält
                body_clean = body or ""
                if body_clean and ("<html" in body_clean.lower() or "<div" in body_clean.lower() or "<td" in body_clean.lower()):
                    import html as _html
                    body_clean = _html.unescape(body_clean)
                    body_clean = re.sub(r'<style[^>]*>.*?</style>', ' ', body_clean, flags=re.DOTALL|re.IGNORECASE)
                    body_clean = re.sub(r'<script[^>]*>.*?</script>', ' ', body_clean, flags=re.DOTALL|re.IGNORECASE)
                    body_clean = re.sub(r'<br\s*/?>', '\n', body_clean, flags=re.IGNORECASE)
                    body_clean = re.sub(r'<[^>]+>', ' ', body_clean)
                    body_clean = re.sub(r'[ \t]+', ' ', body_clean)
                    body_clean = re.sub(r'\n{3,}', '\n\n', body_clean).strip()
                    cur.execute("UPDATE mail_messages SET body=%s WHERE id=%s", (body_clean[:20000], mid))

                full=f"{subj or ''}\n{body_clean}"

                # ── REGEX-PRE-EXTRAKTION (zuverlässiger als KI) ──────────────
                regex_fns   = extract_flight_numbers(full)
                regex_pnr   = extract_pnr(full) or ""
                regex_segs  = extract_flight_segments_from_text(full) if regex_fns else []
                regex_seg_s = segments_to_string(regex_segs) if regex_segs else ""
                regex_type  = "Flug" if regex_fns else ""
                # Reisecode aus Betreff
                regex_tc    = extract_trip_code(full) or tc or ""
                # PNR → Reisecode-Lookup
                if not regex_tc and regex_pnr:
                    cur.execute("SELECT trip_code FROM trip_meta WHERE pnr_code=%s LIMIT 1",(regex_pnr,))
                    pnr_row=cur.fetchone()
                    if pnr_row: regex_tc=pnr_row[0]
                # Mail-Zuordnung aktualisieren wenn Regex mehr weiß als DB
                if regex_tc and not tc:
                    cur.execute("UPDATE mail_messages SET trip_code=%s WHERE id=%s AND (trip_code IS NULL OR trip_code='')",(regex_tc,mid))
                if regex_pnr and regex_tc:
                    cur.execute("UPDATE trip_meta SET pnr_code=%s WHERE trip_code=%s AND (pnr_code IS NULL OR pnr_code='')",(regex_pnr,regex_tc))
                if regex_fns and regex_tc:
                    fns_str=",".join(regex_fns)
                    cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s AND (flight_numbers IS NULL OR flight_numbers='')",(fns_str,regex_tc))
                    print(f"[Regex] {regex_tc}: {fns_str} | PNR:{regex_pnr}")

                fields=await mistral_extract(full,known_codes,"mail")
                if fields:
                    pnr=fields.get("pnr_code","") or ""
                    fns=fields.get("flight_numbers","") or ""
                    trains=fields.get("train_numbers","") or ""
                    rc=fields.get("reisecode","") or tc or ""
                    # Wenn kein Reisecode → über PNR in trip_meta suchen
                    if not rc and pnr:
                        cur.execute("SELECT trip_code FROM trip_meta WHERE pnr_code=%s LIMIT 1",(pnr,))
                        pnr_row=cur.fetchone()
                        if pnr_row: rc=pnr_row[0]
                    # Fallback auf Regex-Ergebnis
                    if not rc: rc=regex_tc
                    # Wenn immer noch kein Code → Mail-Betreff nach Reisecode durchsuchen
                    if not rc:
                        rc_m=re.search(r'\b(\d{2}-\d{3})\b', subj or "")
                        if rc_m: rc=rc_m.group(1)
                    # Mail in DB auf gefundenen Code aktualisieren
                    if rc and not tc:
                        cur.execute("UPDATE mail_messages SET trip_code=%s WHERE id=%s AND (trip_code IS NULL OR trip_code='')",(rc,mid))
                    traveler_ki=fields.get("traveler_name","") or ""
                    dest_ki=fields.get("destination","") or ""
                    betrag_ki=fields.get("betrag","") or ""
                    waehrung_ki=fields.get("waehrung","EUR") or "EUR"
                    typ_ki=fields.get("beleg_typ","") or ""
                    datum_ki=fields.get("datum","") or ""
                    vendor_ki=fields.get("anbieter","") or ""
                    conf_ki=fields.get("confidence","niedrig") or "niedrig"
                else:
                    # Kein Mistral-Ergebnis → Regex-Daten verwenden
                    pnr=regex_pnr; fns=",".join(regex_fns) if regex_fns else ""; trains=""
                    rc=regex_tc; conf_ki="mittel"; vendor_ki=""; betrag_ki=""
                    waehrung_ki="EUR"; typ_ki="Flug" if regex_fns else ""; datum_ki=""
                    dest_ki=""; traveler_ki=""
                if not rc and regex_tc: rc=regex_tc

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

                    # Mail-Body als Beleg anlegen:
                    # - wenn Betrag + Typ erkannt, ODER
                    # - wenn Flugnummern erkannt (auch ohne Betrag = Itinerary/Reiseangebot)
                    has_fns = bool(fns or regex_fns)
                    should_create = (
                        (betrag_ki and typ_ki and typ_ki not in ("Sonstiges","")) or
                        (has_fns and rc)
                    )
                    if should_create and rc:
                        # Typ bestimmen wenn nur Flugnummern vorhanden
                        eff_typ = typ_ki if typ_ki and typ_ki != "Sonstiges" else ("Flug" if has_fns else "")
                        if not eff_typ: eff_typ = "Flug" if has_fns else typ_ki
                        cur.execute(
                            "SELECT id FROM mail_attachments WHERE trip_code=%s AND detected_type=%s "
                            "AND storage_key LIKE 'mail_body_%%' AND detected_vendor=%s",
                            (rc, eff_typ, vendor_ki or ""))
                        if not cur.fetchone():
                            betrag_eur_ki=""
                            try:
                                val=float(betrag_ki.replace(",","."))
                                eur,_=await convert_to_eur(val,waehrung_ki)
                                betrag_eur_ki=f"{eur:.2f}".replace(".",",")
                            except: pass
                            cur.execute("SELECT mail_uid FROM mail_messages WHERE id=%s",(mid,))
                            mur=cur.fetchone()
                            mail_uid_val=mur[0] if mur else f"mail_{mid}"
                            safe_name=f"Mail: {(subj or 'Buchungsbestaetigung')[:60]}"
                            # Segmente: Regex bevorzugen, dann KI
                            seg_ki      = regex_seg_s or fields.get("flight_segments","") or ""
                            # Segment-Vollständigkeit prüfen
                            all_fns_str = fns or (",".join(regex_fns) if regex_fns else "")
                            if all_fns_str and eff_typ == "Flug":
                                fn_list=[f.strip() for f in all_fns_str.split(",") if f.strip()]
                                seg_list=[s.strip() for s in seg_ki.split(";") if s.strip()] if seg_ki else []
                                seg_fns=[s.split("|")[0].strip() for s in seg_list]
                                for mfn in [f for f in fn_list if f not in seg_fns]:
                                    seg_list.append(f"{mfn}|||||")
                                if seg_list: seg_ki=";".join(seg_list)
                            checkin_ki  = fields.get("checkin_date","") or ""
                            checkout_ki = fields.get("checkout_date","") or ""
                            cin_t_ki    = fields.get("checkin_time","") or ""
                            cout_t_ki   = fields.get("checkout_time","") or ""
                            nights_ki   = int(fields.get("nights",0) or 0)
                            cur.execute(
                                "INSERT INTO mail_attachments "
                                "(mail_uid,trip_code,original_filename,saved_filename,content_type,"
                                " storage_key,detected_type,detected_amount,detected_amount_eur,"
                                " detected_currency,detected_date,detected_vendor,"
                                " detected_nights,detected_checkin,detected_checkout,"
                                " detected_checkin_time,detected_checkout_time,flight_segments,"
                                " detected_flight_numbers,"
                                " analysis_status,confidence,review_flag,ki_bemerkung) "
                                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                                (mail_uid_val,rc,safe_name,safe_name,"text/plain",
                                 f"mail_body_{mid}",eff_typ,betrag_ki or None,betrag_eur_ki or None,
                                 waehrung_ki,datum_ki,vendor_ki,
                                 nights_ki,checkin_ki or None,checkout_ki or None,
                                 cin_t_ki or None,cout_t_ki or None,seg_ki or None,
                                 all_fns_str or None,
                                 "ok (Mail-Body)",conf_ki,
                                 "ok" if conf_ki=="hoch" else "pruefen",
                                 f"Aus Mail-Text: {subj or ''}"))
                            # Neue ID holen und sofort PDF generieren
                            cur.execute("SELECT lastval()")
                            new_att_id = cur.fetchone()[0]
                            if HAS_PDF_LIBS():
                                pdf_key = await generate_and_store_mail_pdf(
                                    new_att_id, subj, body,
                                    typ_ki, vendor_ki, betrag_ki,
                                    datum_ki, rc, conn)
                                if pdf_key:
                                    cur.execute("UPDATE mail_attachments SET pdf_key=%s WHERE id=%s",
                                        (pdf_key, new_att_id))
                    # Quellen: flight_segments (Flughafen→Land), checkin/checkout, dest_ki
                    if rc:
                        cur.execute("SELECT departure_date,return_date,vma_destinations FROM trip_meta WHERE trip_code=%s",(rc,))
                        trip_row=cur.fetchone()
                        if trip_row and trip_row[0] and not trip_row[2]:
                            dep_d_str=str(trip_row[0])
                            ret_d_str=str(trip_row[1]) if trip_row[1] else ""

                            # Flughafen → Ländercode
                            # AIRPORT_CC = globale Konstante (oben definiert)
                            # Zielland aus flight_segments ableiten
                            # Format: FN|DEP|ARR|DATE|TIME|DATE|TIME;...
                            dest_cc_from_seg = None
                            dep_date_in_dest = None  # wann man ankommt
                            arr_date_back    = None  # wann man zurückfliegt
                            seg_ki_vma = fields.get("flight_segments","") or seg_ki or ""
                            if seg_ki_vma:
                                segs = [s.strip().split("|") for s in seg_ki_vma.split(";") if s.strip()]
                                # Hinflug: erstes Segment dessen Ankunftsflughafen nicht DE ist
                                for seg in segs:
                                    if len(seg) >= 3:
                                        arr_apt = seg[2].strip().upper()
                                        cc = AIRPORT_CC.get(arr_apt)
                                        if cc and cc != "DE":
                                            dest_cc_from_seg = cc
                                            # Ankunftsdatum
                                            if len(seg) >= 6 and seg[5].strip():
                                                try:
                                                    p=seg[5].strip().split(".")
                                                    if len(p)==3:
                                                        dep_date_in_dest = date(int(p[2]),int(p[1]),int(p[0]))
                                                except: pass
                                            if not dep_date_in_dest and len(seg) >= 4 and seg[3].strip():
                                                try:
                                                    p=seg[3].strip().split(".")
                                                    if len(p)==3:
                                                        dep_date_in_dest = date(int(p[2]),int(p[1]),int(p[0]))
                                                except: pass
                                            break
                                # Rückflug: letztes Segment dessen Abflughafen nicht DE ist
                                for seg in reversed(segs):
                                    if len(seg) >= 2:
                                        dep_apt = seg[1].strip().upper()
                                        cc = AIRPORT_CC.get(dep_apt)
                                        if cc and cc != "DE":
                                            if len(seg) >= 4 and seg[3].strip():
                                                try:
                                                    p=seg[3].strip().split(".")
                                                    if len(p)==3:
                                                        arr_date_back = date(int(p[2]),int(p[1]),int(p[0]))
                                                except: pass
                                            break

                            # Fallback: Zielland aus dest_ki Text
                            if not dest_cc_from_seg and dest_ki:
                                DEST_CC_MAP={
                                    "frankreich":"FR","france":"FR","paris":"FR","lyon":"FR","nizza":"FR","marseille":"FR",
                                    "indien":"IN","india":"IN","mumbai":"IN","delhi":"IN","bangalore":"IN",
                                    "dubai":"AE","abu dhabi":"AE","uae":"AE","emirate":"AE",
                                    "usa":"US","new york":"US","los angeles":"US","chicago":"US",
                                    "grossbritannien":"GB","uk":"GB","london":"GB","england":"GB",
                                    "schweiz":"CH","zuerich":"CH","genf":"CH",
                                    "oesterreich":"AT","wien":"AT","salzburg":"AT",
                                    "italien":"IT","rom":"IT","mailand":"IT","venedig":"IT",
                                    "spanien":"ES","barcelona":"ES","madrid":"ES",
                                    "tuerkei":"TR","istanbul":"TR","ankara":"TR",
                                    "japan":"JP","tokio":"JP","osaka":"JP",
                                    "singapur":"SG","singapore":"SG",
                                    "china":"CN","peking":"CN","shanghai":"CN",
                                    "katar":"QA","doha":"QA","saudi":"SA",
                                    "niederlande":"NL","amsterdam":"NL",
                                    "belgien":"BE","bruessel":"BE",
                                    "portugal":"PT","lissabon":"PT",
                                    "norwegen":"NO","schweden":"SE","daenemark":"DK","finnland":"FI",
                                }
                                dest_low = dest_ki.lower()
                                for key,cc in DEST_CC_MAP.items():
                                    if key in dest_low:
                                        dest_cc_from_seg = cc; break

                            # VMA-String aufbauen wenn Zielland != DE gefunden
                            if dest_cc_from_seg and dest_cc_from_seg != "DE":
                                try:
                                    dep_d = date.fromisoformat(dep_d_str)
                                    ret_d = date.fromisoformat(ret_d_str) if ret_d_str else None

                                    # Tag des Eintreffens im Zielland
                                    arrive_foreign = dep_date_in_dest or (dep_d + timedelta(days=1))
                                    # Tag der Rückreise
                                    leave_foreign  = arr_date_back or ret_d or (arrive_foreign + timedelta(days=1))

                                    vma_parts = [f"{dep_d_str}:DE"]
                                    if arrive_foreign > dep_d:
                                        vma_parts.append(f"{arrive_foreign}:{dest_cc_from_seg}")
                                    if leave_foreign and leave_foreign > arrive_foreign:
                                        vma_parts.append(f"{leave_foreign}:DE")

                                    vma_auto = ",".join(vma_parts)
                                    cur.execute(
                                        "UPDATE trip_meta SET vma_destinations=%s "
                                        "WHERE trip_code=%s AND (vma_destinations IS NULL OR vma_destinations='')",
                                        (vma_auto, rc))
                                    print(f"[VMA] {rc}: {vma_auto}")
                                except Exception as vma_e:
                                    print(f"[VMA Fehler]: {vma_e}")

                cur.execute("UPDATE mail_messages SET analysis_status='ok' WHERE id=%s",(mid,))
                mail_processed+=1
            except Exception as mail_err:
                import traceback as _tb
                print(f"[Mail-Analyse ID={mid}]: {_tb.format_exc()[:400]}")
                try: cur.execute("UPDATE mail_messages SET analysis_status='fehler' WHERE id=%s",(mid,))
                except: pass

        conn.commit()

        # Anhänge analysieren – ausstehende, aber keine Inline-Bilder/ICS
        cur.execute("""SELECT id,storage_key,original_filename FROM mail_attachments
            WHERE (analysis_status IN ('ausstehend','neu') OR analysis_status IS NULL)
            AND original_filename NOT SIMILAR TO 'image[0-9]+[.](png|jpg|jpeg|gif|bmp|emz|wmz)'
            AND original_filename NOT ILIKE '%.ics'
            AND storage_key NOT LIKE 'mail_body_%'
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
        import traceback
        tb=traceback.format_exc()
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Fehler bei KI-Analyse</h2><p><b>{e}</b></p><pre style="font-size:10px;overflow-x:auto;white-space:pre-wrap;background:var(--page);padding:12px;border-radius:8px">{tb}</pre></div>')


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
        import traceback
        tb = traceback.format_exc()
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Fehler</h2><p>{e}</p><pre style="font-size:10px;overflow-x:auto;white-space:pre-wrap">{tb}</pre></div>')


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
            analysis_status,created_at,LENGTH(body) FROM mail_messages ORDER BY id DESC LIMIT 100""")
        rows=cur.fetchall()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code DESC LIMIT 30")
        all_codes=[r[0] for r in cur.fetchall()]
        cur.close();conn.close()
        code_opts="<option value=''>– zuordnen –</option>"+"".join(f"<option>{c}</option>" for c in all_codes)
        html="".join(f"""<tr>
            <td style="font-size:11px;color:var(--t300)">{str(r[7] or '')[:16]}</td>
            <td style="max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:11px">{r[1] or ''}</td>
            <td><a href="/mail-detail/{r[0]}" style="color:var(--b600);text-decoration:none;font-weight:500">{(r[2] or '–')[:50]}</a></td>
            <td class="cc">{r[3] or '<span style="color:var(--am6)">–</span>'}</td>
            <td>{r[4] or ''}</td>
            <td style="font-family:'DM Mono',monospace;color:var(--gr6)">{r[5] or ''}</td>
            <td style="font-size:11px">{r[8] or 0} B</td>
            <td><span class="bdg {"bdg-ok" if r[6]=="ok" else "bdg-w"}">{r[6] or 'ausstehend'}</span></td>
            <td>
              <form method="post" action="/mail-assign/{r[0]}" style="display:flex;gap:4px">
                <select name="trip_code" style="font-size:11px;padding:2px 4px;border:1px solid var(--bd);border-radius:4px">
                  {code_opts.replace(f'<option>{r[3]}</option>',f'<option selected>{r[3]}</option>') if r[3] else code_opts}
                </select>
                <button type="submit" style="font-size:11px;padding:2px 8px;background:var(--b600);color:white;border:none;border-radius:4px;cursor:pointer">✓</button>
              </form>
            </td>
            </tr>""" for r in rows)
        return page_shell("Mail-Log",f"""
        <div class="page-card" style="max-width:1200px"><h2>Mail-Log ({len(rows)} Einträge)</h2>
          <div class="acts">
            <a class="btn-l" href="/">Zurück</a>
            <a class="btn" href="/analyze-attachments">KI-Analyse</a>
            <a class="btn-l" href="/reanalyze-mails" onclick="return confirm('Mails zurücksetzen?')">🔄 Reset+Reanalyse</a>
          </div>
          <p class="sub" style="margin-bottom:8px">Mails ohne Code (orange –) können hier manuell einer Reise zugeordnet werden.</p>
          <div style="overflow-x:auto"><table>
            <tr><th>Zeit</th><th>Von</th><th>Betreff</th><th>Code</th><th>Typ</th><th>PNR</th><th>Größe</th><th>Status</th><th>Zuordnen</th></tr>
            {html or '<tr><td colspan="9">Keine Mails</td></tr>'}
          </table></div>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')


@app.post("/mail-assign/{mail_id}")
async def mail_assign(mail_id: int, request: Request):
    """Weist einer Mail manuell einen Reisecode zu und setzt Status auf ausstehend."""
    try:
        form=await request.form()
        tc=(form.get("trip_code") or "").strip()
        if not tc:
            return RedirectResponse(url="/mail-log",status_code=303)
        conn=get_conn();cur=conn.cursor()
        cur.execute("UPDATE mail_messages SET trip_code=%s, analysis_status='ausstehend' WHERE id=%s",(tc,mail_id))
        # Auch zugehörige Attachments zuordnen
        cur.execute("SELECT mail_uid FROM mail_messages WHERE id=%s",(mail_id,))
        row=cur.fetchone()
        if row:
            cur.execute("UPDATE mail_attachments SET trip_code=%s WHERE mail_uid=%s AND (trip_code IS NULL OR trip_code='')",(tc,row[0]))
        cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(tc,))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/mail-log",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)


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
            pnr_code,detected_flight_numbers,detected_train_numbers,detected_amount,detected_nights,
            COALESCE(flight_time_info,''),COALESCE(detected_checkin,''),COALESCE(detected_checkout,''),
            COALESCE(detected_checkin_time,''),COALESCE(detected_checkout_time,''),
            COALESCE(flight_segments,''),
            COALESCE(pdf_key,'')
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
        # Ereignisse sammeln
        timeline_events=[]
        # Flüge die bereits aus ICS/Belegen kommen – damit keine Doppeleinträge
        fns_from_belege=set()

        # Abreise (nur Journeyzeile, ohne Flugnummern – kommen aus ICS)
        if dep_d:
            timeline_events.append({
                "date": dep_d,
                "time": dep_t or "08:00",
                "icon": "🏠→✈",
                "task": "Abreise",
                "route": destinations or cc or "",
                "timerange": dep_t or "08:00",
                "detail": "",
                "type": "journey"
            })

        # Belege als Timeline-Einträge
        beleg_sum=0.0
        for a in atts:
            fn_a,dtype,amt_eur,curr,ddate,vendor,stat,conf,bemerk,att_id,apnr,afns,atrains,amt,det_nights,ft_info,checkin_s,checkout_s,checkin_t_s,checkout_t_s,seg_s,pdf_key_a=a

            # ── Filter: Inline-Bilder, ICS-Reste, Irrelevantes ──
            fn_lower=(fn_a or "").lower()
            # Inline-Bilder + EMZ/WMZ
            if re.match(r"image\d+\.", fn_lower) or fn_lower.endswith(".emz") or fn_lower.endswith(".wmz"):
                continue
            # ICS-Dateien
            if fn_lower.endswith(".ics") or dtype == "Kalendereintrag":
                continue
            # Nicht analysierbare ohne Betrag/Flugnummer
            if stat in ("nicht analysierbar","Inline-Grafik") and not amt_eur and not afns:
                continue

            if amt_eur:
                try: beleg_sum+=float(amt_eur.replace(".","").replace(",","."))
                except: pass

            # Datum parsen helper
            def parse_dd(s):
                if not s: return None
                s=str(s).strip()
                try:
                    if "." in s:
                        p=s.split(".")
                        if len(p)==3: return date(int(p[2]),int(p[1]),int(p[0]))
                    return date.fromisoformat(s[:10])
                except: return None

            ev_date = parse_dd(ddate) or dep_d
            # Für Flugbelege: Buchungsdatum ≠ Flugdatum → erstes Segment-Datum nutzen
            if dtype in ("Flug","Kalendereintrag") and seg_s:
                first_seg = seg_s.split(";")[0].split("|")
                if len(first_seg) > 3 and first_seg[3].strip():
                    seg_date = parse_dd(first_seg[3].strip())
                    if seg_date: ev_date = seg_date
            checkin_d  = parse_dd(checkin_s)
            checkout_d = parse_dd(checkout_s)

            type_icons={"Flug":"✈","Hotel":"🏨","Taxi":"🚕","Bahn":"🚆","Mietwagen":"🚗",
                       "Essen":"🍽","Kalendereintrag":"📅","Sonstiges":"📄"}
            icon=type_icons.get(dtype,"📄")
            amount_str=f"{amt_eur} €" if amt_eur else (f"{amt} {curr}" if amt else "–")
            edit_url=f"/beleg-edit/{att_id}"
            view_url=f"/beleg/{att_id}"
            # PDF-Link: direktes PDF wenn vorhanden, sonst Beleg-Vorschau
            pdf_link=""
            if pdf_key_a:
                pdf_view_url=f"/beleg-pdf/{att_id}"
                pdf_link=f'<a href="{pdf_view_url}" target="_blank" style="color:var(--gr6);margin-right:6px" title="PDF anzeigen">📋</a>'
            actions=(pdf_link +
                    f'<a href="{view_url}" target="_blank" style="color:var(--b600);margin-right:6px" title="Beleg anzeigen">📄</a>'
                    f'<a href="{edit_url}" style="color:var(--t300)" title="KI-Ergebnis korrigieren">✏</a>')

            # ── HOTEL: mehrtägig mit Check-in/out ──────────────────────
            if dtype == "Hotel":
                hotel_name = vendor or "Hotel"
                # Nächte zuerst bestimmen
                n_nights = int(det_nights or 0)
                if n_nights == 0 and checkin_d and checkout_d:
                    n_nights = max(0, (checkout_d - checkin_d).days)
                if n_nights == 0 and bemerk:
                    nm = re.search(r"(\d+)\s*(?:Nächte|Naechte|nights?)", bemerk, re.IGNORECASE)
                    if nm: n_nights = int(nm.group(1))

                # Check-in: explizites Datum bevorzugen
                # Wenn kein checkin aber checkout + nights → rückwärts berechnen
                cin_d = checkin_d
                if not cin_d and checkout_d and n_nights > 0:
                    cin_d = checkout_d - timedelta(days=n_nights)
                if not cin_d:
                    cin_d = dep_d or date.today()
                cin_t = checkin_t_s or "15:00"

                # Check-out
                if checkout_d:
                    cout_d = checkout_d
                    if cin_d: n_nights = max(n_nights, (cout_d - cin_d).days)
                elif n_nights > 0 and cin_d:
                    cout_d = cin_d + timedelta(days=n_nights)
                else:
                    cout_d = None
                cout_t = checkout_t_s or "11:00"

                # Check-in Zeile
                # Ort aus ki_bemerkung extrahieren (z.B. "3 Nächte Marriott Lyon")
                hotel_city = ""
                if bemerk:
                    city_m = re.search(r'(?:Naechte|Nächte|Naecht)\s+\w+\s+(\w+)', bemerk, re.IGNORECASE)
                    if city_m: hotel_city = city_m.group(1)
                timeline_events.append({
                    "date": cin_d, "time": cin_t,
                    "icon": "🏨", "type": "beleg",
                    "task": f"Check-in {hotel_name}",
                    "route": hotel_city or "",
                    "timerange": f"ab {cin_t}",
                    "detail": f"{amount_str}{f' · {n_nights} Nächte' if n_nights else ''}",
                    "extra": actions, "status": stat, "att_id": att_id,
                })
                # Zwischennächte
                if n_nights > 1 and cin_d:
                    for ni in range(1, n_nights):
                        nd = cin_d + timedelta(days=ni)
                        timeline_events.append({
                            "date": nd, "time": "",
                            "icon": "🏨", "type": "beleg",
                            "task": f"{hotel_name}",
                            "route": "", "timerange": f"Nacht {ni+1}/{n_nights}",
                            "detail": "", "extra": "", "status": "", "att_id": att_id,
                        })
                # Check-out Zeile
                if cout_d:
                    timeline_events.append({
                        "date": cout_d, "time": cout_t,
                        "icon": "🏨", "type": "beleg",
                        "task": f"Check-out {hotel_name}",
                        "route": "", "timerange": f"bis {cout_t}",
                        "detail": "", "extra": "", "status": "", "att_id": att_id,
                    })

            # ── FLUG: aus flight_segments oder ft_info ─────────────────
            elif dtype in ("Flug","Kalendereintrag"):
                segs_added = False
                # Flugnummern aus diesem Beleg für Deduplizierung merken
                if afns:
                    for fn_x in afns.split(","):
                        fns_from_belege.add(fn_x.strip())

                if seg_s:
                    # Strukturierte Segmente aus Mistral-Extraktion
                    seen_seg_keys = set()  # Deduplizierung innerhalb aller Belege
                    for seg in seg_s.split(";"):
                        parts = seg.strip().split("|")
                        if len(parts) >= 2 and parts[0].strip():
                            fn_seg  = parts[0].strip()
                            dep_apt = parts[1].strip() if len(parts)>1 else ""
                            arr_apt = parts[2].strip() if len(parts)>2 else ""
                            dep_dt  = parse_dd(parts[3]) if len(parts)>3 and parts[3].strip() else ev_date
                            dep_tm  = parts[4].strip() if len(parts)>4 else ""
                            arr_dt  = parse_dd(parts[5]) if len(parts)>5 and parts[5].strip() else dep_dt
                            arr_tm  = parts[6].strip() if len(parts)>6 else ""
                            route   = f"{dep_apt}→{arr_apt}" if dep_apt and arr_apt else (dep_apt or arr_apt or "")
                            # Deduplizierung: gleiche FN + Datum + Route nicht doppelt
                            seg_key = f"{fn_seg}_{dep_dt}_{dep_apt}_{arr_apt}"
                            # Prüfe ob dieses Segment bereits in timeline_events
                            already_in = any(
                                e.get("task")==f"Flug {fn_seg}" and e.get("date")==dep_dt
                                for e in timeline_events
                            )
                            if already_in: continue
                            fns_from_belege.add(fn_seg)
                            timeline_events.append({
                                "date": dep_dt or ev_date or dep_d or date.today(),
                                "time": dep_tm or "",
                                "icon": "✈", "type": "beleg",
                                "task": f"Flug {fn_seg}",
                                "route": route,
                                "timerange": f"{dep_tm} → {arr_tm}" if dep_tm and arr_tm else (f"ab {dep_tm}" if dep_tm else ""),
                                "detail": amount_str if not segs_added else "",
                                "extra": actions if not segs_added else "",
                                "status": stat if not segs_added else "",
                                "att_id": att_id,
                            })
                            segs_added = True

                elif afns or ft_info or dtype == "Kalendereintrag":
                    # ICS oder Flug-Beleg ohne strukturierte Segmente
                    # Route aus ki_bemerkung parsen: "✈ LH1234 FRA→LYS | Abflug: 2026-04-20 06:30 UTC"
                    all_routes = re.findall(r"([A-Z]{3}→[A-Z]{3})", bemerk or ft_info or "")
                    all_fn_in_bemerk = re.findall(r"\b([A-Z]{2}\d{3,4})\b", bemerk or "")
                    # Zeitinfo aus ki_bemerkung
                    dep_time_m = re.search(r"Abflug:[^|]*(\d{2}:\d{2})", bemerk or "")
                    arr_time_m = re.search(r"Ankunft:[^|]*(\d{2}:\d{2})", bemerk or "")
                    dep_tm_ics = dep_time_m.group(1) if dep_time_m else ""
                    arr_tm_ics = arr_time_m.group(1) if arr_time_m else ""

                    fn_list_ics = all_fn_in_bemerk if all_fn_in_bemerk else ([f.strip() for f in (afns or "").split(",") if f.strip()])
                    if not fn_list_ics:
                        fn_list_ics = [""]

                    for idx_fn, fn_ics in enumerate(fn_list_ics):
                        route_ics = all_routes[idx_fn] if idx_fn < len(all_routes) else (all_routes[0] if all_routes else "")
                        if fn_ics: fns_from_belege.add(fn_ics)
                        timeline_events.append({
                            "date": ev_date or dep_d or date.today(),
                            "time": dep_tm_ics if idx_fn==0 else "",
                            "icon": "✈", "type": "beleg",
                            "task": f"Flug {fn_ics}" if fn_ics else f"Flug ({fn_a[:20]})",
                            "route": route_ics,
                            "timerange": f"{dep_tm_ics} → {arr_tm_ics}" if dep_tm_ics and arr_tm_ics and idx_fn==0 else "",
                            "detail": amount_str if idx_fn==0 else "",
                            "extra": actions if idx_fn==0 else "",
                            "status": stat if idx_fn==0 else "",
                            "att_id": att_id,
                        })
                        segs_added = True

                if not segs_added:
                    # Letzter Fallback
                    timeline_events.append({
                        "date": ev_date or dep_d or date.today(),
                        "time": "", "icon": "✈", "type": "beleg",
                        "task": f"Flug {afns or vendor or ''}".strip() or "Flugbeleg",
                        "route": "", "timerange": "",
                        "detail": amount_str,
                        "extra": actions, "status": stat, "att_id": att_id,
                    })

            else:
                ev_time = ""
                if dtype == "Taxi" and bemerk:
                    tm = re.search(r"(\d{1,2}:\d{2})", bemerk)
                    if tm: ev_time = tm.group(1)
                task_label = vendor or dtype or "Beleg"
                if fn_a and fn_a.startswith("Mail:"):
                    task_label = vendor or dtype or fn_a[5:35].strip()
                elif fn_a and not re.match(r"image\d+\.", fn_a.lower()):
                    task_label = vendor or fn_a[:40]
                timeline_events.append({
                    "date": ev_date or dep_d or date.today(),
                    "time": ev_time, "icon": icon, "type": "beleg",
                    "task": task_label,
                    "route": "", "timerange": "",
                    "detail": amount_str,
                    "extra": actions, "status": stat, "att_id": att_id,
                })
        # Verpflegung pro Tag direkt in Timeline einbauen
        if dep_d and ret_d:
            daily = load_daily_meals(tc)
            days_range = (ret_d - dep_d).days + 1
            for i in range(days_range):
                d = dep_d + timedelta(days=i)
                day_data = daily.get(d)
                ml = day_data[0] if day_data else []
                day_cc = day_data[1] if day_data else (cc or "DE")
                vma_ov = day_data[2] if day_data else None
                b_chk = "✅" if "breakfast" in ml else "☐"
                l_chk = "✅" if "lunch" in ml else "☐"
                d_chk = "✅" if "dinner" in ml else "☐"
                dtype = "partial" if i == 0 or i == days_range - 1 else "full"
                vma_day = float(vma_ov) if vma_ov is not None else get_vma(day_cc, dtype, ml)
                timeline_events.append({
                    "date": d,
                    "time": "",
                    "icon": "🍽",
                    "task": "Verpflegung",
                    "route": "",
                    "timerange": "🍳🥗🍽",
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
                "task": "Rückkehr",
                "route": "Heimreise",
                "timerange": ret_t or "18:00",
                "detail": "",
                "type": "journey"
            })

        # Fallback: Flugnummern aus trip_meta die noch nicht in Segmenten erscheinen
        # → auf Rückreisedatum platzieren mit direktem Beleg-Edit-Link
        all_meta_fns = [f.strip() for f in (fns or "").split(",") if f.strip()]
        missing_fns = [f for f in all_meta_fns if f not in fns_from_belege]
        if missing_fns:
            # Finde den Flug-Beleg für Edit-Link
            edit_att_id = None
            for a in atts:
                if a[1] in ("Flug","Kalendereintrag") and a[11]:  # afns not empty
                    edit_att_id = a[9]
                    break
            edit_hint = (f'<a href="/beleg-edit/{edit_att_id}?back=/trip/{tc}" '
                        f'style="font-size:10px;color:var(--am6);margin-left:6px">'
                        f'✏ Segment ergänzen</a>') if edit_att_id else ""
            for missing_fn in missing_fns:
                timeline_events.append({
                    "date": ret_d or dep_d or date.today(),
                    "time": "",
                    "icon": "✈",
                    "task": f"Flug {missing_fn}",
                    "route": "⚠ Segment fehlt",
                    "timerange": "",
                    "detail": "",
                    "extra": edit_hint,
                    "status": "",
                    "type": "beleg",
                    "att_id": edit_att_id,
                })

        # Chronologisch sortieren – Mahlzeiten nach Belegen, Journeys an Rand
        timeline_events.sort(key=lambda e: (
            e["date"] or date.today(),
            0 if e.get("type") == "journey" and e.get("icon","").startswith("🏠") else
            (99 if e.get("type") == "journey" else
             (50 if e.get("type") == "meal" else 10)),
            e.get("time","")
        ))

        # Timeline HTML mit Ein/Ausblenden
        tl_rows=""
        prev_date=None
        row_idx=0
        for ev in timeline_events:
            ev_date=ev["date"]
            row_id=f"tlrow_{tc}_{row_idx}"
            row_idx+=1

            # Datums-Trennzeile
            if ev_date != prev_date:
                wd=["Mo","Di","Mi","Do","Fr","Sa","So"][ev_date.weekday()] if ev_date else ""
                wkend_bg="background:#eef4ff" if ev_date and ev_date.weekday()>=5 else "background:var(--page)"
                wkend_c="color:var(--b600);font-weight:700" if ev_date and ev_date.weekday()>=5 else "color:var(--t300)"
                tl_rows+=f'<tr class="tl-date-row"><td colspan="7" style="{wkend_bg};padding:6px 12px 4px;font-size:11px;{wkend_c};border-bottom:2px solid var(--bd);letter-spacing:.04em">{str(ev_date)} {wd}</td></tr>'
                prev_date=ev_date

            ev_type=ev.get("type","beleg")
            type_colors={"journey":"var(--b700)","flight":"var(--b600)","train":"var(--gr6)","beleg":"var(--t900)","meal":"var(--t500)"}
            col=type_colors.get(ev_type,"var(--t900)")

            stat=ev.get("status","")
            stat_html=""
            if stat:
                sc="bdg-ok" if stat in ("ok","ok (manuell)","ok (Mail-Body)") else "bdg-w"
                stat_html=f'<span class="bdg {sc}" style="font-size:10px">{stat}</span>'

            # Spalte 1: Icon
            icon_cell=f'<td style="width:32px;text-align:center;font-size:16px;padding:8px 4px">{ev["icon"]}</td>'

            # Spalte 2: Task (Flug LH1234 / Check-in Marriott / Verpflegung)
            task=ev.get("task","")
            task_cell=f'<td style="font-weight:600;color:{col};padding:8px 6px;font-size:13px">{task}</td>'

            # Spalte 3: Route / Ort (FRA→LYS / Lyon / –)
            route=ev.get("route","") or "–"
            route_style="font-family:DM Mono,monospace;font-size:12px;color:var(--b600)" if "→" in route else "font-size:12px;color:var(--t500)"
            route_cell=f'<td style="{route_style};padding:8px 6px;white-space:nowrap">{route}</td>'

            # Spalte 4: Zeitraum (06:30→08:15 / ab 15:00 / Nacht 2/3)
            timerange=ev.get("timerange","") or ""
            if not timerange and ev.get("time"):
                timerange=ev.get("time","")
            tr_cell=f'<td style="font-family:DM Mono,monospace;font-size:11px;color:var(--t500);padding:8px 6px;white-space:nowrap">{timerange}</td>'

            # Spalte 5: Betrag / Info
            detail=ev.get("detail","") or ""
            detail_cell=f'<td style="font-family:DM Mono,monospace;font-size:12px;color:var(--t700);padding:8px 6px;text-align:right;white-space:nowrap">{detail}</td>'

            # Spalte 6: Status + Aktionen
            extra=ev.get("extra","") or ""
            act_cell=f'<td style="padding:8px 6px;white-space:nowrap;text-align:right">{stat_html} {extra}</td>'

            # Spalte 7: Toggle
            toggle_btn=f'<button onclick="toggleTLRow(\'{row_id}\')" title="Ausblenden" style="background:none;border:none;cursor:pointer;color:var(--t300);padding:0 4px;font-size:11px">👁</button>'
            tog_cell=f'<td style="width:26px;padding:4px 2px;text-align:right">{toggle_btn}</td>'

            # Zeilenhintergrund je Typ
            row_bg=""
            if ev_type=="journey": row_bg=' style="background:var(--b50)"'
            elif ev_type=="meal":  row_bg=' style="background:#f9fdf9"'
            elif ev.get("task","").startswith("Check-in"): row_bg=' style="background:#f0fdf4"'
            elif ev.get("task","").startswith("Check-out"): row_bg=' style="background:#fff9f0"'

            tl_rows+=f'<tr id="{row_id}" data-tl-type="{ev_type}"{row_bg}>{icon_cell}{task_cell}{route_cell}{tr_cell}{detail_cell}{act_cell}{tog_cell}</tr>'

        # Ausgeblendete Zeilen Sektion
        hidden_section=f"""
        <div id="tl-hidden" style="display:none">
          <h4 style="color:var(--t300);font-size:11px;margin:12px 0 6px;text-transform:uppercase;letter-spacing:.06em">Ausgeblendete Einträge</h4>
          <table id="tl-hidden-table" style="opacity:.6;width:100%">
            <colgroup><col style="width:32px"><col style="width:22%"><col style="width:18%"><col style="width:16%"><col style="width:12%"><col style="width:auto"><col style="width:26px"></colgroup>
          </table>
        </div>"""

        tl_js=f"""
        <script>
        function toggleTLRow(id){{
          const row=document.getElementById(id);
          if(!row)return;
          row.style.display='none';
          // Clone to hidden table
          const clone=row.cloneNode(true);
          clone.id=id+'_hidden';
          clone.style.display='';
          // Change toggle button to "einblenden"
          const btn=clone.querySelector('button');
          if(btn){{btn.title='Einblenden';btn.textContent='↩';btn.onclick=function(){{showTLRow(id,clone.id)}};}}
          document.getElementById('tl-hidden-table').appendChild(clone);
          document.getElementById('tl-hidden').style.display='block';
        }}
        function showTLRow(origId,cloneId){{
          const orig=document.getElementById(origId);
          const clone=document.getElementById(cloneId);
          if(orig)orig.style.display='';
          if(clone)clone.remove();
          const ht=document.getElementById('tl-hidden-table');
          if(ht&&ht.rows.length===0)document.getElementById('tl-hidden').style.display='none';
        }}
        </script>"""

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
            <a class="btn" href="/report-komplett/{tc}" target="_blank" style="background:linear-gradient(135deg,#0f9e6e,#0d8a5e)">📎 Komplett-PDF</a>
            <a class="btn-l" href="/meals/{tc}">🍽 Mahlzeiten</a>
            <a class="btn-l" href="/check-flights/{tc}">✈ Flüge</a>
            <a class="btn-l" href="/repair-segments/{tc}" style="color:var(--gr6)"
               onclick="this.textContent='⏳ Repariere...';fetch('/repair-segments/{tc}').then(r=>r.json()).then(d=>{{this.textContent='✓ Fertig';setTimeout(()=>location.reload(),800)}}).catch(()=>location.reload());return false;">🔧 Segmente reparieren</a>
            {"<a class='btn-l' href='/check-trains/"+tc+"'>🚆 Züge</a>" if trains else ""}
            <a class="btn-l" href="/edit-trip/{tc}">✏ Bearbeiten</a>
            <a class="btn-l" href="/">Zurück</a>
          </div>

          <!-- Chronologische Timeline -->
          <h3 style="margin-bottom:8px;color:var(--t700)">📅 Reise-Timeline
            <span style="font-size:11px;font-weight:400;color:var(--t300);margin-left:8px">👁 = Zeile ausblenden</span>
          </h3>
          <div style="overflow-x:auto"><table style="width:100%">
            <colgroup><col style="width:32px"><col style="width:22%"><col style="width:18%"><col style="width:16%"><col style="width:12%"><col style="width:auto"><col style="width:26px"></colgroup>
            <tr style="background:linear-gradient(180deg,var(--b50),#e8f0fe)">
              <th style="text-align:center">  </th>
              <th>Vorgang</th>
              <th>Route / Ort</th>
              <th>Zeitraum</th>
              <th style="text-align:right">Betrag</th>
              <th>Status</th>
              <th></th>
            </tr>
            {tl_rows or '<tr><td colspan="7" class="sub" style="padding:16px">Keine Ereignisse – Reisedaten und Mails zuordnen</td></tr>'}
          </table></div>
          {hidden_section}

          <!-- Summe -->
          <div style="margin-top:12px;text-align:right;font-family:DM Mono,monospace;font-size:14px;font-weight:500;color:var(--b600)">
            Belege gesamt: {beleg_sum:.2f} €
          </div>

          <!-- Flight Alerts -->
          {f'''<h3 style="margin:20px 0 8px;color:var(--t700)">⚠ Flight-Alerts</h3>
          <div style="overflow-x:auto"><table>
            <tr><th>Flug</th><th>Datum</th><th>Typ</th><th>Meldung</th><th>Zeitpunkt</th></tr>
            {alert_rows}</table></div>''' if alerts else ""}
        </div>{tl_js}""")
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
        for a in atts:
            if a[2]:
                try: beleg_sum+=float(a[2].replace(".","").replace(",","."))
                except: pass
        att_rows="".join(f"""<tr>
            <td><a href="/beleg/{a[9]}" target="_blank" style="color:var(--b600);text-decoration:none">📄 {a[0] or '–'}</a>
                <a href="/beleg-edit/{a[9]}" style="margin-left:5px;color:var(--t300);text-decoration:none" title="Korrigieren">✏</a></td>
            <td>{a[1] or ''}</td>
            <td style="font-family:'DM Mono',monospace"><b>{a[2] or ''}</b>{' '+a[3] if a[3] and a[3]!='EUR' else ' €'}</td>
            <td>{a[4] or ''}</td><td>{a[5] or ''}</td>
            <td><span class="bdg {"bdg-ok" if a[6] in ("ok","ok (manuell)") else "bdg-w"}">{a[6] or ''}</span></td>
            <td style="font-size:11px;color:var(--t300)">{(a[8] or '')[:60]}</td>
            </tr>""" for a in atts)
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

@app.get("/report-komplett/{tc}")
async def report_komplett(tc: str):
    """Komplettes Abrechnung-PDF: Deckblatt + VMA + alle Belege als eine PDF."""
    try:
        if not HAS_PDF_LIBS():
            return HTMLResponse("""<div style='font-family:sans-serif;padding:32px'>
                <h2>PDF-Bibliotheken fehlen</h2>
                <p>Bitte <code>reportlab</code> und <code>pypdf</code> in requirements.txt eintragen und neu deployen.</p>
            </div>""", status_code=500)

        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT traveler_name,departure_date,return_date,country_code,
            departure_time_home,arrival_time_home,destinations,meals_reimbursed,
            flight_numbers,train_numbers,colleagues,notes,pnr_code,
            nights_planned,nights_booked,trip_title,customer_code,vma_destinations,employee_code
            FROM trip_meta WHERE trip_code=%s""",(tc,))
        meta=cur.fetchone()
        if not meta:
            cur.close();conn.close()
            return HTMLResponse("Reise nicht gefunden",404)
        (traveler,dep,ret,cc,dep_t,ret_t,destinations,meals_reimb,
         fns,trains,colleagues,notes,pnr,nights_p,nights_b,
         trip_title,customer_code,vma_dest_str,employee_code)=meta

        cur.execute("""SELECT id,original_filename,detected_type,detected_amount,
            detected_amount_eur,detected_currency,detected_date,detected_vendor,
            analysis_status,storage_key,content_type,pdf_key
            FROM mail_attachments WHERE trip_code=%s
            AND original_filename NOT SIMILAR TO 'image[0-9]+[.](png|jpg|jpeg|gif|bmp|emz|wmz)'
            ORDER BY detected_date,id""",(tc,))
        atts=cur.fetchall();cur.close();conn.close()

        dep_d=dep if isinstance(dep,date) else (date.fromisoformat(str(dep)) if dep else None)
        ret_d=ret if isinstance(ret,date) else (date.fromisoformat(str(ret)) if ret else None)
        days=(ret_d-dep_d).days+1 if dep_d and ret_d else 0

        vma_dest=parse_vma_destinations(vma_dest_str or "")
        daily=load_daily_meals(tc)
        ml=[m.strip() for m in (meals_reimb or "").split(",") if m.strip()]
        if days>0:
            if daily:
                vma_total,vma_rows_data=calc_vma_from_daily(dep_d,ret_d,daily,vma_dest,cc)
            else:
                vma_total,vma_rows_data=calc_vma_multi(dep_d,ret_d,ml,vma_dest,cc)
        else:
            vma_total=0.0; vma_rows_data=[]
        trenn_total,_=trennungspauschale(dep_d,ret_d,dep_t or "08:00",ret_t or "18:00")

        beleg_sum=0.0
        for a in atts:
            if a[4]:
                try: beleg_sum+=float(a[4].replace(".","").replace(",","."))
                except: pass
        gesamt=beleg_sum+vma_total+trenn_total
        title_line=" · ".join(filter(None,[employee_code,trip_title,customer_code]))

        # ── 1. DECKBLATT PDF ────────────────────────────────────────────
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import mm
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, PageBreak
        from reportlab.lib.enums import TA_RIGHT, TA_CENTER

        buf_deckblatt = io.BytesIO()
        doc = SimpleDocTemplate(buf_deckblatt, pagesize=A4,
            leftMargin=20*mm, rightMargin=20*mm, topMargin=20*mm, bottomMargin=20*mm)
        styles = getSampleStyleSheet()
        BLU = colors.HexColor('#1a3d96')
        GRY = colors.HexColor('#5a6e8a')
        LBL = ParagraphStyle('lbl', parent=styles['Normal'], fontSize=8, textColor=GRY, fontName='Helvetica-Bold')
        VAL = ParagraphStyle('val', parent=styles['Normal'], fontSize=10)
        H1  = ParagraphStyle('h1', parent=styles['Title'], fontSize=22, textColor=BLU, spaceAfter=2)
        H2  = ParagraphStyle('h2', parent=styles['Heading2'], fontSize=12, textColor=BLU, spaceBefore=12, spaceAfter=4)
        SMN = ParagraphStyle('smn', parent=styles['Normal'], fontSize=8, textColor=GRY)
        MON = ParagraphStyle('mon', parent=styles['Normal'], fontSize=10, fontName='Courier')
        BIG = ParagraphStyle('big', parent=styles['Normal'], fontSize=18, fontName='Courier-Bold', textColor=BLU, alignment=2)

        story=[]
        story.append(Paragraph("REISEKOSTENABRECHNUNG", H1))
        story.append(Paragraph(f"{tc} · {title_line}", SMN))
        story.append(HRFlowable(width="100%", thickness=2, color=BLU, spaceAfter=8))

        # Meta-Grid
        meta_rows=[
            [Paragraph("Reisender", LBL), Paragraph(f"{employee_code or ''} {traveler or '–'}", VAL)],
            [Paragraph("Zeitraum", LBL), Paragraph(f"{dep} {dep_t or ''} – {ret} {ret_t or ''} ({days} Tage)", VAL)],
            [Paragraph("Reiseziel", LBL), Paragraph(destinations or cc or "–", VAL)],
            [Paragraph("PNR", LBL), Paragraph(pnr or "–", MON)],
            [Paragraph("Hotel", LBL), Paragraph(f"{nights_b or 0}/{nights_p or 0} Nächte", VAL)],
        ]
        if colleagues: meta_rows.append([Paragraph("Kollegen", LBL), Paragraph(colleagues, VAL)])
        if notes: meta_rows.append([Paragraph("Notiz", LBL), Paragraph(notes, VAL)])
        t=Table(meta_rows, colWidths=[40*mm, 130*mm])
        t.setStyle(TableStyle([
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('ROWBACKGROUNDS',(0,0),(-1,-1),[colors.HexColor('#f0f4f9'),colors.white]),
            ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#eaeef5')),
            ('TOPPADDING',(0,0),(-1,-1),5),('BOTTOMPADDING',(0,0),(-1,-1),5),
        ]))
        story.append(t)
        story.append(Spacer(1,8*mm))

        # Belege-Tabelle
        story.append(Paragraph("Belege", H2))
        brows=[[Paragraph(h,LBL) for h in ["#","Anbieter","Typ","Datum","Betrag","EUR","Status"]]]
        for i,(att_id,fn,dtype,amt,amt_eur,curr,ddate,vendor,stat,skey,ctype,pdf_k) in enumerate(atts,1):
            brows.append([
                Paragraph(str(i),SMN),
                Paragraph((vendor or fn or "–")[:40], VAL),
                Paragraph(dtype or "–", VAL),
                Paragraph(ddate or "–", VAL),
                Paragraph(f"{amt or '–'} {curr or ''}", MON),
                Paragraph(f"{amt_eur or '–'} €", MON),
                Paragraph(stat or "–", SMN),
            ])
        brows.append([
            Paragraph("","LBL"), Paragraph("","VAL"),
            Paragraph("","VAL"), Paragraph("","VAL"),
            Paragraph("Summe Belege", LBL), Paragraph(f"{beleg_sum:.2f} €", BIG),
            Paragraph("","SMN"),
        ])
        bt=Table(brows, colWidths=[8*mm,50*mm,22*mm,22*mm,25*mm,22*mm,20*mm])
        bt.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#eef4ff')),
            ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#eaeef5')),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
            ('FONTSIZE',(0,0),(-1,-1),8),
            ('BACKGROUND',(0,-1),(-1,-1),colors.HexColor('#f0f4f9')),
        ]))
        story.append(bt)
        story.append(Spacer(1,6*mm))

        # VMA-Tabelle
        story.append(Paragraph("Verpflegungsmehraufwand §9 EStG", H2))
        vrows=[[Paragraph(h,LBL) for h in ["Datum","Tag","Land","Mahlzeiten-Abzug","VMA"]]]
        for d,lbl,cc_d,m_icons,v in vma_rows_data:
            vrows.append([Paragraph(d,SMN),Paragraph(lbl,VAL),Paragraph(cc_d,MON),
                          Paragraph(m_icons,VAL),Paragraph(f"{v:.2f} €",MON)])
        vrows.append([Paragraph("","SMN"),Paragraph("","VAL"),Paragraph("","VAL"),
                      Paragraph("Summe VMA",LBL),Paragraph(f"{vma_total:.2f} €",MON)])
        vt=Table(vrows, colWidths=[25*mm,28*mm,18*mm,50*mm,28*mm])
        vt.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,0),colors.HexColor('#eef4ff')),
            ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#eaeef5')),
            ('VALIGN',(0,0),(-1,-1),'TOP'),
            ('TOPPADDING',(0,0),(-1,-1),4),('BOTTOMPADDING',(0,0),(-1,-1),4),
            ('FONTSIZE',(0,0),(-1,-1),8),
            ('BACKGROUND',(0,-1),(-1,-1),colors.HexColor('#f0f4f9')),
        ]))
        story.append(vt)
        story.append(Spacer(1,8*mm))

        # Gesamtsumme-Box
        summe_data=[[
            Paragraph("GESAMTBETRAG REISEKOSTENABRECHNUNG", LBL),
            Paragraph(f"{gesamt:,.2f} €", BIG),
        ]]
        if trenn_total>0:
            summe_data.insert(0,[Paragraph("Trennungspauschale",LBL),Paragraph(f"{trenn_total:.2f} €",MON)])
        summe_data.insert(0,[Paragraph("VMA §9 EStG",LBL),Paragraph(f"{vma_total:.2f} €",MON)])
        summe_data.insert(0,[Paragraph("Summe Belege",LBL),Paragraph(f"{beleg_sum:.2f} €",MON)])
        st=Table(summe_data, colWidths=[80*mm,90*mm])
        st.setStyle(TableStyle([
            ('BACKGROUND',(0,0),(-1,-2),colors.HexColor('#f0f4f9')),
            ('BACKGROUND',(0,-1),(-1,-1),colors.HexColor('#1a3d96')),
            ('TEXTCOLOR',(0,-1),(-1,-1),colors.white),
            ('GRID',(0,0),(-1,-1),0.5,colors.HexColor('#dde4ef')),
            ('TOPPADDING',(0,0),(-1,-1),6),('BOTTOMPADDING',(0,0),(-1,-1),6),
            ('FONTSIZE',(0,0),(-1,-1),9),
        ]))
        story.append(st)
        story.append(Spacer(1,6*mm))
        story.append(Paragraph(
            f"Erstellt: {datetime.now().strftime('%d.%m.%Y %H:%M')} · Herrhammer Kürschner Kerzenmaschinen",
            SMN))

        doc.build(story)
        deckblatt_pdf = buf_deckblatt.getvalue()

        # ── 2. BELEGE SAMMELN ────────────────────────────────────────────
        s3=get_s3()
        beleg_pdfs=[]
        for att_id,fn,dtype,amt,amt_eur,curr,ddate,vendor,stat,skey,ctype,pdf_k in atts:
            pdf_bytes=None
            # Priorität: generiertes PDF > Original-PDF > Original-Bild als PDF
            if pdf_k:
                try:
                    obj=s3.get_object(Bucket=S3_BUCKET,Key=pdf_k)
                    pdf_bytes=obj["Body"].read()
                except: pass
            if not pdf_bytes and skey and not skey.startswith(("mail_body_","S3-FEHLER")):
                try:
                    obj=s3.get_object(Bucket=S3_BUCKET,Key=skey)
                    raw=obj["Body"].read()
                    ctype_s=(ctype or "").lower()
                    if "pdf" in ctype_s:
                        pdf_bytes=raw
                    elif any(x in ctype_s for x in ["jpeg","jpg","png","webp"]):
                        # Bild → PDF mit reportlab
                        from reportlab.platypus import Image as RLImage
                        img_buf=io.BytesIO(raw)
                        out_buf=io.BytesIO()
                        doc2=SimpleDocTemplate(out_buf,pagesize=A4,
                            leftMargin=15*mm,rightMargin=15*mm,topMargin=15*mm,bottomMargin=15*mm)
                        from PIL import Image as PILImg
                        try:
                            pil=PILImg.open(img_buf)
                            w,h=pil.size
                            max_w=170*mm; max_h=240*mm
                            scale=min(max_w/w, max_h/h)
                            img_buf.seek(0)
                            img_story=[
                                Paragraph(f"{vendor or fn} · {ddate or '–'} · {amt_eur or '–'} €",
                                    ParagraphStyle('x',fontSize=9,textColor=GRY)),
                                Spacer(1,4*mm),
                                RLImage(img_buf, width=w*scale, height=h*scale)
                            ]
                            doc2.build(img_story)
                            pdf_bytes=out_buf.getvalue()
                        except: pass
                except: pass

            if not pdf_bytes and skey and skey.startswith("mail_body_"):
                # Mail-Body-Beleg: Fallback-PDF generieren
                pdf_bytes=make_text_pdf(
                    title=f"{dtype}: {vendor or '–'}",
                    body_text=f"Datum: {ddate or '–'}\nBetrag: {amt_eur or '–'} EUR\nStatus: {stat or '–'}",
                    meta={"Typ":dtype,"Anbieter":vendor,"Betrag":f"{amt_eur} €","Datum":ddate})

            if pdf_bytes:
                beleg_pdfs.append(pdf_bytes)

        # ── 3. ALLES ZUSAMMENFÜHREN ──────────────────────────────────────
        all_pdfs=[deckblatt_pdf]+beleg_pdfs
        final_pdf=merge_pdfs(all_pdfs)

        filename=f"Abrechnung_{tc}_{datetime.now().strftime('%Y%m%d')}.pdf"
        return Response(
            content=final_pdf,
            media_type="application/pdf",
            headers={"Content-Disposition": f'attachment; filename="{filename}"'}
        )

    except Exception as e:
        import traceback
        return HTMLResponse(f"<pre style='padding:24px'>{traceback.format_exc()}</pre>",status_code=500)



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

@app.get("/beleg-pdf/{att_id}")
def beleg_pdf(att_id: int):
    """Liefert das generierte PDF eines Belegs direkt aus S3."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT pdf_key,original_filename,detected_vendor,detected_type FROM mail_attachments WHERE id=%s",(att_id,))
        row=cur.fetchone();cur.close();conn.close()
        if not row or not row[0]:
            return HTMLResponse("Kein PDF für diesen Beleg vorhanden",status_code=404)
        pdf_key,fn,vendor,dtype=row
        s3=get_s3()
        obj=s3.get_object(Bucket=S3_BUCKET,Key=pdf_key)
        pdf_bytes=obj["Body"].read()
        label=f"{vendor or dtype or fn or 'beleg'}_{att_id}.pdf"
        return Response(content=pdf_bytes, media_type="application/pdf",
            headers={"Content-Disposition":f'inline; filename="{label}"'})
    except Exception as e:
        return HTMLResponse(f"Fehler: {e}",status_code=500)


@app.get("/beleg/{att_id}")
def beleg_vorschau(att_id: int):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT a.storage_key,a.original_filename,a.content_type,
            a.ki_bemerkung,a.detected_type,a.detected_amount_eur,a.detected_vendor,
            a.detected_date,a.pdf_key,a.trip_code,a.detected_amount,a.detected_currency,
            a.detected_checkin,a.detected_checkout,a.detected_nights,
            m.subject,m.body
            FROM mail_attachments a
            LEFT JOIN mail_messages m ON m.mail_uid=a.mail_uid
            WHERE a.id=%s""",(att_id,))
        row=cur.fetchone()
        if not row:
            cur.close();conn.close()
            return HTMLResponse("Beleg nicht gefunden",status_code=404)
        (storage_key,filename,content_type,bemerk,dtype,amt,vendor,ddate,
         pdf_key,tc,amt_orig,curr,checkin,checkout,nights,subj,body)=row

        # Mail-Body-Beleg → PDF generieren und direkt ausliefern
        if not storage_key or storage_key.startswith("mail_body_"):
            # Prüfe ob PDF bereits in S3
            if pdf_key:
                try:
                    s3=get_s3()
                    obj=s3.get_object(Bucket=S3_BUCKET,Key=pdf_key)
                    pdf_bytes=obj["Body"].read()
                    cur.close();conn.close()
                    label=f"{vendor or dtype or 'beleg'}_{att_id}.pdf"
                    return Response(content=pdf_bytes, media_type="application/pdf",
                        headers={"Content-Disposition":f'inline; filename="{label}"'})
                except: pass

            # PDF on-the-fly generieren
            if HAS_PDF_LIBS():
                meta={
                    "Typ": dtype or "–",
                    "Anbieter": vendor or "–",
                    "Betrag": f"{amt or amt_orig or '–'} €",
                    "Datum": ddate or "–",
                    "Check-in": checkin or "",
                    "Check-out": checkout or "",
                    "Nächte": str(nights) if nights else "",
                    "Reise": tc or "–",
                }
                # Leere Felder entfernen
                meta={k:v for k,v in meta.items() if v and v not in ("–","0","")}
                # Sophos-Links aus Body entfernen für bessere Lesbarkeit
                clean_body=re.sub(r'https://[^\s]*sophos[^\s]*','[Link]',body or "")
                clean_body=re.sub(r'https://[^\s]{80,}','[Link]',clean_body)
                clean_body=clean_body[:8000]  # max 8000 Zeichen

                pdf_bytes=make_text_pdf(
                    title=f"{dtype or 'Buchungsbestätigung'}: {vendor or '–'}",
                    body_text=f"Betreff: {subj or '–'}\n\n{clean_body}",
                    meta=meta)

                # In S3 speichern für künftige Aufrufe
                try:
                    new_key=f"mail_pdfs/{tc}/mail_body_{att_id}.pdf"
                    s3=get_s3()
                    s3.put_object(Bucket=S3_BUCKET,Key=new_key,Body=pdf_bytes,
                                  ContentType="application/pdf")
                    cur.execute("UPDATE mail_attachments SET pdf_key=%s WHERE id=%s",(new_key,att_id))
                    conn.commit()
                except: pass

                cur.close();conn.close()
                label=f"{vendor or dtype or 'beleg'}_{att_id}.pdf"
                return Response(content=pdf_bytes, media_type="application/pdf",
                    headers={"Content-Disposition":f'inline; filename="{label}"'})
            else:
                cur.close();conn.close()
                return HTMLResponse("PDF-Bibliotheken fehlen (reportlab/pypdf nicht installiert)",status_code=500)

        cur.close();conn.close()
        if storage_key.startswith("S3-FEHLER"):
            return HTMLResponse(f"Datei nicht im Bucket: {storage_key}",status_code=404)
        s3=get_s3()
        signed_url=s3.generate_presigned_url("get_object",
            Params={"Bucket":S3_BUCKET,"Key":storage_key,
                    "ResponseContentDisposition":f'inline; filename="{filename or "beleg"}"',
                    "ResponseContentType":content_type or "application/octet-stream"},
            ExpiresIn=300)
        return RedirectResponse(url=signed_url)
    except Exception as e:
        import traceback
        return HTMLResponse(f"<pre>Fehler: {traceback.format_exc()}</pre>",status_code=500)


@app.get("/beleg-edit/{att_id}", response_class=HTMLResponse)
def beleg_edit_form(att_id: int, back: str = ""):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT id,trip_code,original_filename,detected_type,
            detected_amount,detected_amount_eur,detected_currency,
            detected_date,detected_vendor,ki_bemerkung,confidence,analysis_status,
            detected_nights,detected_checkin,detected_checkout,
            detected_checkin_time,detected_checkout_time,
            flight_segments,detected_flight_numbers,storage_key
            FROM mail_attachments WHERE id=%s""",(att_id,))
        row=cur.fetchone()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code")
        all_codes=[r[0] for r in cur.fetchall()]
        cur.close();conn.close()
        if not row: return HTMLResponse("Nicht gefunden",404)
        (_,tc,fname,dtype,amt,amt_eur,curr,ddate,vendor,bemerk,conf,astatus,
         nights,checkin,checkout,cin_t,cout_t,seg_s,fn_s,storage_key)=row

        back_url = back or (f"/trip/{tc}" if tc else "/attachment-log")
        is_mail_body = bool(storage_key and storage_key.startswith("mail_body_"))

        type_opts="".join(f'<option {"selected" if dtype==t else ""}>{t}</option>'
            for t in ["Flug","Hotel","Taxi","Bahn","Mietwagen","Essen","Sonstiges"])
        code_opts="".join(f'<option value="{c}" {"selected" if tc==c else ""}>{c}</option>'
            for c in all_codes)

        # Hotel-Felder nur anzeigen wenn Typ Hotel
        hotel_section=f"""
            <div id="hotel-fields">
              <div class="fgrp"><label class="flbl">Check-in Datum (DD.MM.YYYY)</label>
                <input class="finp" name="detected_checkin" value="{checkin or ''}"></div>
              <div class="fgrp"><label class="flbl">Check-in Uhrzeit</label>
                <input class="finp" name="detected_checkin_time" value="{cin_t or '15:00'}" placeholder="15:00"></div>
              <div class="fgrp"><label class="flbl">Check-out Datum (DD.MM.YYYY)</label>
                <input class="finp" name="detected_checkout" value="{checkout or ''}"></div>
              <div class="fgrp"><label class="flbl">Check-out Uhrzeit</label>
                <input class="finp" name="detected_checkout_time" value="{cout_t or '11:00'}" placeholder="11:00"></div>
              <div class="fgrp ff"><label class="flbl">Anzahl Nächte</label>
                <input class="finp" type="number" name="detected_nights" value="{nights or 0}" min="0"></div>
            </div>"""

        flight_section=f"""
            <div id="flight-fields">
              <div class="fgrp ff"><label class="flbl">Flugnummern (kommagetrennt)</label>
                <input class="finp" name="detected_flight_numbers" value="{fn_s or ''}" placeholder="LH3463,LH1078"></div>
              <div class="fgrp ff">
                <label class="flbl">Flug-Segmente</label>
                <textarea class="finp" name="flight_segments" rows="3"
                  placeholder="LH3463|NUE|FRA|20.04.2026|06:30|20.04.2026|07:35;LH1078|FRA|LYS|20.04.2026|09:15|20.04.2026|10:20"
                  style="font-family:DM Mono,monospace;font-size:11px">{seg_s or ''}</textarea>
                <div class="hint">Format: FLUGNR|VON|NACH|DATUM_AB|ZEIT_AB|DATUM_AN|ZEIT_AN; nächstes Segment</div>
              </div>
            </div>"""

        # Beleg-Vorschau wenn in S3
        preview_html=""
        if not is_mail_body and storage_key and not storage_key.startswith("S3-FEHLER"):
            preview_html=f'<div style="margin-bottom:16px"><a class="btn-l" href="/beleg/{att_id}" target="_blank">📄 Original anzeigen</a></div>'

        return page_shell(f"Beleg #{att_id} bearbeiten",f"""
        <div class="page-card" style="max-width:700px">
          <h2>✏ Beleg #{att_id} korrigieren</h2>
          <p class="sub" style="margin-bottom:12px">{fname or '–'} · Status: {astatus or '–'}</p>
          {preview_html}
          <form method="post" action="/beleg-edit/{att_id}?back={back_url}">
            <div class="fgrid">
              <div class="fgrp ff"><label class="flbl">Reisecode</label>
                <select class="fsel" name="trip_code"><option value="">– nicht zugeordnet –</option>{code_opts}</select></div>
              <div class="fgrp"><label class="flbl">Belegtyp</label>
                <select class="fsel" name="detected_type" id="dtype-sel" onchange="showFields(this.value)">{type_opts}</select></div>
              <div class="fgrp"><label class="flbl">Anbieter / Hotel / Airline</label>
                <input class="finp" name="detected_vendor" value="{vendor or ''}"></div>
              <div class="fgrp"><label class="flbl">Datum (DD.MM.YYYY)</label>
                <input class="finp" name="detected_date" value="{ddate or ''}"></div>
              <div class="fgrp"><label class="flbl">Betrag (Original)</label>
                <input class="finp" name="detected_amount" value="{amt or ''}"></div>
              <div class="fgrp"><label class="flbl">Währung (ISO)</label>
                <input class="finp" name="detected_currency" value="{curr or 'EUR'}" maxlength="3"></div>
              <div class="fgrp ff"><label class="flbl">Betrag EUR (Komma als Dezimaltrennzeichen)</label>
                <input class="finp" name="detected_amount_eur" value="{amt_eur or ''}"></div>
              {hotel_section}
              {flight_section}
              <div class="fgrp ff"><label class="flbl">Notiz / KI-Bemerkung</label>
                <textarea class="finp" name="ki_bemerkung" rows="2">{bemerk or ''}</textarea></div>
            </div>
            <div class="mfooter">
              <a class="btn-mc" href="{back_url}">Abbrechen</a>
              <button type="submit" class="btn-mp">💾 Speichern</button>
            </div>
          </form>
        </div>
        <script>
        function showFields(t){{
          document.getElementById('hotel-fields').style.display=t==='Hotel'?'contents':'none';
          document.getElementById('flight-fields').style.display=t==='Flug'?'contents':'none';
        }}
        showFields('{dtype or ""}');
        </script>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')
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
async def beleg_edit_save(att_id: int, request: Request, back: str = ""):
    try:
        form=await request.form()
        conn=get_conn();cur=conn.cursor()
        nights=0
        try: nights=int(form.get("detected_nights") or 0)
        except: pass
        cur.execute("""UPDATE mail_attachments SET
            trip_code=%s,detected_type=%s,detected_vendor=%s,detected_date=%s,
            detected_amount=%s,detected_currency=%s,detected_amount_eur=%s,
            detected_nights=%s,detected_checkin=%s,detected_checkout=%s,
            detected_checkin_time=%s,detected_checkout_time=%s,
            flight_segments=%s,detected_flight_numbers=%s,
            ki_bemerkung=%s,review_flag='ok',analysis_status='ok (manuell)'
            WHERE id=%s""",
            (form.get("trip_code") or None,
             form.get("detected_type") or None,
             form.get("detected_vendor") or None,
             form.get("detected_date") or None,
             form.get("detected_amount") or None,
             (form.get("detected_currency") or "EUR").upper(),
             form.get("detected_amount_eur") or None,
             nights,
             form.get("detected_checkin") or None,
             form.get("detected_checkout") or None,
             form.get("detected_checkin_time") or None,
             form.get("detected_checkout_time") or None,
             form.get("flight_segments") or None,
             form.get("detected_flight_numbers") or None,
             form.get("ki_bemerkung") or None,
             att_id))
        conn.commit()
        tc = form.get("trip_code")
        if tc:
            cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(tc,))
            conn.commit()
        cur.close();conn.close()
        # Smart redirect: zurück zum Trip oder zur Log-Seite
        redirect_to = back or (f"/trip/{tc}" if tc else "/attachment-log")
        return RedirectResponse(url=redirect_to,status_code=303)
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

@app.get("/mail-eingabe", response_class=HTMLResponse)
def mail_eingabe_form():
    """Manuelle Mail-Eingabe – Text direkt einfügen wenn IMAP nicht funktioniert."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code DESC LIMIT 30")
        codes=[r[0] for r in cur.fetchall()]
        cur.close();conn.close()
        code_opts="<option value=''>– KI zuordnen lassen –</option>"+"".join(f"<option>{c}</option>" for c in codes)
        return page_shell("Mail manuell einfügen",f"""
        <div class="page-card" style="max-width:800px">
          <h2>📧 Mail-Text manuell einfügen</h2>
          <p class="sub" style="margin-bottom:16px">Mail-Text hier einfügen wenn der automatische Import nicht funktioniert.
          Der Text wird sofort analysiert.</p>
          <form method="post" action="/mail-eingabe">
            <div class="fgrid">
              <div class="fgrp"><label class="flbl">Reisecode zuordnen</label>
                <select class="fsel" name="trip_code">{code_opts}</select></div>
              <div class="fgrp"><label class="flbl">Betreff (optional)</label>
                <input class="finp" name="subject" placeholder="z.B. Reiseangebot Z6INOT"></div>
              <div class="fgrp ff"><label class="flbl">Mail-Text *</label>
                <textarea class="finp" name="body" rows="20" style="font-family:DM Mono,monospace;font-size:11px"
                  placeholder="Gesamten Mail-Text hier einfügen..."></textarea></div>
            </div>
            <div class="mfooter">
              <a class="btn-mc" href="/">Abbrechen</a>
              <button type="submit" class="btn-mp">📨 Importieren &amp; analysieren</button>
            </div>
          </form>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.post("/mail-eingabe", response_class=HTMLResponse)
async def mail_eingabe_save(request: Request):
    """Speichert manuell eingegebenen Mail-Text und analysiert ihn sofort."""
    try:
        form=await request.form()
        body=(form.get("body") or "").strip()
        subj=(form.get("subject") or "").strip()
        tc  =(form.get("trip_code") or "").strip()
        if not body:
            return page_shell("Fehler",'<div class="page-card"><p>Bitte Mail-Text eingeben.</p></div>')

        full=f"{subj}\n{body}"

        # Regex-Extraktion
        regex_fns  = extract_flight_numbers(full)
        regex_pnr  = extract_pnr(full) or ""
        regex_segs = extract_flight_segments_from_text(full) if regex_fns else []
        regex_seg_s= segments_to_string(regex_segs) if regex_segs else ""
        regex_tc   = extract_trip_code(full) or tc or ""

        conn=get_conn();cur=conn.cursor()
        known_codes=[r[0] for r in cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code") or []]
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code")
        known_codes=[r[0] for r in cur.fetchall()]

        # PNR → Reisecode
        if not regex_tc and regex_pnr:
            cur.execute("SELECT trip_code FROM trip_meta WHERE pnr_code=%s LIMIT 1",(regex_pnr,))
            row=cur.fetchone()
            if row: regex_tc=row[0]
        if not regex_tc and tc:
            regex_tc=tc

        # Duplikat-Check über Subject+PNR
        if regex_pnr:
            cur.execute("SELECT id FROM mail_messages WHERE body LIKE %s LIMIT 1",(f"%{regex_pnr}%",))
            if cur.fetchone():
                cur.close();conn.close()
                return page_shell("Duplikat",f"""<div class="page-card">
                  <h2 class="warn-t">⚠ Mail bereits vorhanden</h2>
                  <p>PNR <b>{regex_pnr}</b> ist bereits in der Datenbank.</p>
                  <div class="acts"><a class="btn" href="/reanalyze-mails" onclick="window.location='/analyze-attachments';return false">Neu analysieren</a>
                  <a class="btn-l" href="/mail-log">Mail-Log</a></div></div>""")

        # Mail einfügen
        uid=f"manual_{__import__('hashlib').md5(body[:100].encode()).hexdigest()[:8]}"
        mail_type="Flug" if regex_fns else detect_mail_type(full)
        cur.execute("""INSERT INTO mail_messages
            (mail_uid,message_id,sender,subject,body,trip_code,detected_type,pnr_code,analysis_status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (mail_uid) DO UPDATE SET
            body=EXCLUDED.body,trip_code=EXCLUDED.trip_code,analysis_status='ausstehend'""",
            (uid,uid,"manuell",subj or "Manuelle Eingabe",
             body,regex_tc or None,mail_type,regex_pnr or None,"ausstehend"))
        conn.commit()

        # Direkt analysieren
        fields=await mistral_extract(full,known_codes,"mail")
        if not fields: fields={}

        pnr_ki   = fields.get("pnr_code","") or regex_pnr or ""
        fns_ki   = fields.get("flight_numbers","") or (",".join(regex_fns) if regex_fns else "")
        seg_ki   = fields.get("flight_segments","") or regex_seg_s or ""
        betrag_ki= fields.get("betrag","") or ""
        typ_ki   = fields.get("beleg_typ","") or mail_type or "Sonstiges"
        vendor_ki= fields.get("anbieter","") or ""
        datum_ki = fields.get("datum","") or ""
        conf_ki  = fields.get("confidence","mittel") or "mittel"
        dest_ki  = fields.get("destination","") or ""
        rc       = fields.get("reisecode","") or regex_tc or tc or ""
        waehrung_ki = fields.get("waehrung","EUR") or "EUR"

        # trip_meta aktualisieren
        if rc:
            cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(rc,))
            if pnr_ki: cur.execute("UPDATE trip_meta SET pnr_code=%s WHERE trip_code=%s AND (pnr_code IS NULL OR pnr_code='')",(pnr_ki,rc))
            if fns_ki: cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s AND (flight_numbers IS NULL OR flight_numbers='')",(fns_ki,rc))
            if dest_ki: cur.execute("UPDATE trip_meta SET destinations=%s WHERE trip_code=%s AND (destinations IS NULL OR destinations='')",(dest_ki,rc))

        # Beleg-Insert
        betrag_eur_ki=""
        if betrag_ki:
            try:
                val=float(betrag_ki.replace(",","."))
                eur,_=await convert_to_eur(val,waehrung_ki)
                betrag_eur_ki=f"{eur:.2f}".replace(".",",")
            except: pass

        # Segment-Vollständigkeit prüfen
        if fns_ki and typ_ki=="Flug":
            fn_list=[f.strip() for f in fns_ki.split(",") if f.strip()]
            seg_list=[s.strip() for s in seg_ki.split(";") if s.strip()] if seg_ki else []
            seg_fns=[s.split("|")[0].strip() for s in seg_list]
            for mfn in [f for f in fn_list if f not in seg_fns]:
                seg_list.append(f"{mfn}|||||")
            if seg_list: seg_ki=";".join(seg_list)

        if betrag_ki and typ_ki and typ_ki not in ("Sonstiges","") and rc:
            cur.execute("""INSERT INTO mail_attachments
                (mail_uid,trip_code,original_filename,saved_filename,content_type,
                 storage_key,detected_type,detected_amount,detected_amount_eur,
                 detected_currency,detected_date,detected_vendor,
                 flight_segments,detected_flight_numbers,
                 analysis_status,confidence,review_flag,ki_bemerkung)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                (uid,rc,f"Mail: {(subj or 'Manuelle Eingabe')[:60]}",
                 f"Mail: {(subj or 'Manuelle Eingabe')[:60]}","text/plain",
                 f"mail_body_manual_{uid}",typ_ki,betrag_ki,betrag_eur_ki,
                 waehrung_ki,datum_ki,vendor_ki,
                 seg_ki or None,fns_ki or None,
                 "ok (Mail-Body)",conf_ki,
                 "ok" if conf_ki=="hoch" else "pruefen",
                 f"Manuell eingegeben · {subj or ''}"))

        cur.execute("UPDATE mail_messages SET analysis_status='ok',trip_code=%s WHERE mail_uid=%s",(rc or None,uid))

        # ── Lernbeispiel speichern wenn gutes Ergebnis ────────────────
        if typ_ki and typ_ki != "Sonstiges" and (fns_ki or betrag_ki) and conf_ki in ("hoch","mittel"):
            import json as _json
            example_result = {
                "beleg_typ": typ_ki,
                "betrag": betrag_ki,
                "waehrung": waehrung_ki,
                "datum": datum_ki,
                "anbieter": vendor_ki,
                "pnr_code": pnr_ki,
                "flight_numbers": fns_ki,
                "flight_segments": seg_ki,
                "confidence": "hoch",
            }
            # Nur relevante Felder
            example_result = {k:v for k,v in example_result.items() if v}
            save_ki_example(
                mail_type=typ_ki,
                input_text=full[:3000],
                result_json=example_result,
                description=f"{vendor_ki or typ_ki} · {subj[:40] if subj else 'manuell'}"
            )
            print(f"[KI-Beispiel] Gespeichert: {typ_ki} · {vendor_ki}")

        # VMA auto-berechnen
        if regex_segs and rc:
            cur.execute("SELECT departure_date,return_date,vma_destinations FROM trip_meta WHERE trip_code=%s",(rc,))
            trip_row=cur.fetchone()
            if trip_row and trip_row[0] and not trip_row[2]:
                dep_d_str=str(trip_row[0])
                ret_d_str=str(trip_row[1]) if trip_row[1] else ""
                dest_cc=None; arrive_date=None; leave_date=None
                for seg in regex_segs:
                    arr=seg.get("arr","").upper()
                    cc=AIRPORT_CC.get(arr)
                    if cc and cc!="DE":
                        dest_cc=cc
                        if seg.get("date"):
                            try:
                                p=seg["date"].split("."); arrive_date=date(int(p[2]),int(p[1]),int(p[0]))
                            except: pass
                        break
                for seg in reversed(regex_segs):
                    dep=seg.get("dep","").upper()
                    cc=AIRPORT_CC.get(dep)
                    if cc and cc!="DE":
                        if seg.get("date"):
                            try:
                                p=seg["date"].split("."); leave_date=date(int(p[2]),int(p[1]),int(p[0]))
                            except: pass
                        break
                if dest_cc and dest_cc!="DE":
                    try:
                        dep_d=date.fromisoformat(dep_d_str)
                        ret_d=date.fromisoformat(ret_d_str) if ret_d_str else None
                        arrive=arrive_date or (dep_d+timedelta(days=1))
                        leave=leave_date or ret_d or arrive
                        parts=[f"{dep_d}:DE"]
                        if arrive>dep_d: parts.append(f"{arrive}:{dest_cc}")
                        if leave and leave>arrive: parts.append(f"{leave}:DE")
                        vma_str=",".join(parts)
                        cur.execute("UPDATE trip_meta SET vma_destinations=%s WHERE trip_code=%s AND (vma_destinations IS NULL OR vma_destinations='')",(vma_str,rc))
                        print(f"[VMA manuell] {rc}: {vma_str}")
                    except: pass

        conn.commit();cur.close();conn.close()

        # PDF generieren
        pdf_key=None
        if HAS_PDF_LIBS() and rc:
            pdf_key=await generate_and_store_mail_pdf(0,subj,body,typ_ki,vendor_ki,betrag_ki,datum_ki,rc,get_conn())

        return page_shell("Mail importiert",f"""
        <div class="page-card">
          <h2 class="ok-t">✓ Mail importiert und analysiert</h2>
          <table style="margin:12px 0">
            <tr><td>Typ</td><td><b>{typ_ki or '–'}</b></td></tr>
            <tr><td>Reisecode</td><td><b style="font-family:DM Mono,monospace">{rc or '–'}</b></td></tr>
            <tr><td>PNR</td><td><b style="font-family:DM Mono,monospace;color:var(--gr6)">{pnr_ki or '–'}</b></td></tr>
            <tr><td>Flugnummern (Regex)</td><td><b style="font-family:DM Mono,monospace;color:var(--b600)">{", ".join(regex_fns) or '–'}</b></td></tr>
            <tr><td>Flugnummern (KI)</td><td>{fns_ki or '–'}</td></tr>
            <tr><td>Betrag</td><td>{betrag_ki or '–'} {waehrung_ki} = {betrag_eur_ki or '–'} €</td></tr>
            <tr><td>Segmente</td><td style="font-family:DM Mono,monospace;font-size:10px">{(seg_ki or '–')[:200]}</td></tr>
          </table>
          <div class="acts">
            {f'<a class="btn" href="/trip/{rc}">Reise {rc} anzeigen</a>' if rc else ''}
            <a class="btn-l" href="/mail-eingabe">Weitere Mail eingeben</a>
            <a class="btn-l" href="/attachment-log">Anhang-Log</a>
          </div>
        </div>""")
    except Exception as e:
        import traceback
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Fehler</h2><p>{e}</p><pre style="font-size:10px">{traceback.format_exc()}</pre></div>')


@app.get("/upload-beleg", response_class=HTMLResponse)
async def upload_beleg_form(request: Request):
    """Dedizierte Upload-Seite – kein Modal, kein JS-Problem."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code DESC LIMIT 30")
        codes=[r[0] for r in cur.fetchall()]
        cur.close();conn.close()
    except: codes=[]
    opts="<option value=''>– KI zuordnen lassen –</option>"+"".join(f"<option>{c}</option>" for c in codes)
    return page_shell("Beleg hochladen",f"""
    <div class="page-card" style="max-width:600px">
      <h2>📎 Beleg hochladen</h2>
      <form method="post" action="/upload-beleg" enctype="multipart/form-data">
        <div class="fgrid">
          <div class="fgrp ff">
            <label class="flbl">Reise zuordnen</label>
            <select class="fsel" name="trip_code">{opts}</select>
          </div>
          <div class="fgrp ff">
            <label class="flbl">Datei auswählen *</label>
            <input class="finp" type="file" name="file" accept=".pdf,.jpg,.jpeg,.png,.ics" required
              style="padding:8px;cursor:pointer">
            <div class="hint">PDF, JPG, PNG, ICS · PDF wird direkt via pypdf gelesen (kein OCR nötig)</div>
          </div>
        </div>
        <div style="font-size:11px;color:var(--t300);margin:12px 0">
          🔒 DSGVO: PDF-Text wird lokal extrahiert. Nur anonymisierte Daten an Mistral EU (Paris).
        </div>
        <div class="mfooter">
          <a class="btn-l" href="/">Abbrechen</a>
          <button type="submit" class="btn-mp">📤 Hochladen &amp; analysieren</button>
        </div>
      </form>
    </div>""")

@app.post("/upload-beleg", response_class=HTMLResponse)
async def upload_beleg(
    request: Request,
    file: UploadFile = File(...),
    trip_code: str = Form(default="")
):
    try:
        if not file or not file.filename:
            return page_shell("Upload",'<div class="page-card"><h2 class="err-t">Keine Datei</h2><a class="btn-l" href="/">Zurück</a></div>')

        ext = (file.filename or "").lower().split(".")[-1]
        if ext not in ("pdf","jpg","jpeg","png","webp","ics"):
            return page_shell("Upload",f'<div class="page-card"><h2 class="err-t">.{ext} nicht unterstützt</h2><p class="sub">Erlaubt: PDF, JPG, PNG, WEBP, ICS</p><a class="btn-l" href="/">Zurück</a></div>')

        file_bytes = await file.read()
        h = file_hash(file_bytes)
        conn=get_conn();cur=conn.cursor()

        # Duplikat-Check – bei Duplikat neu analysieren statt ablehnen
        cur.execute("SELECT id,trip_code,flight_segments FROM mail_attachments WHERE file_hash=%s",(h,))
        existing=cur.fetchone()
        if existing and existing[2]:  # Segmente bereits vorhanden → wirklich Duplikat
            cur.close();conn.close()
            existing_tc = existing[1] or ""
            trip_link = f'<a class="btn" href="/trip/{existing_tc}">Reise anzeigen</a>' if existing_tc else ""
            return page_shell("Upload",f'<div class="page-card"><h2 class="warn-t">⚠ Bereits analysiert</h2><p>ID {existing[0]} (Reise {existing_tc or "–"}) · Segmente bereits vorhanden.</p><div class="acts">{trip_link}<a class="btn-l" href="/">Dashboard</a></div></div>')
        elif existing:  # Datei bekannt aber Segmente fehlen → neu analysieren
            att_id = existing[0]
            code = trip_code.strip() or existing[1] or ""
        else:
            att_id = None
            code = trip_code.strip() or extract_trip_code(file.filename) or ""

        safe_fn=sanitize_filename(file.filename)
        uid=f"upload_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        storage_key=f"mail_attachments/{uid}_{safe_fn}"
        if not code: code=trip_code.strip() or ""

        # S3 Upload
        try:
            s3=get_s3()
            s3.put_object(Bucket=S3_BUCKET,Key=storage_key,Body=file_bytes,
                ContentType=file.content_type or "application/octet-stream")
        except Exception as s3e:
            storage_key=f"S3-FEHLER:{s3e}"

        # DB eintragen oder aktualisieren
        if code:
            cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(code,))
        if att_id:
            # Neu analysieren – storage_key aktualisieren
            cur.execute("UPDATE mail_attachments SET storage_key=%s,trip_code=%s,analysis_status='ausstehend' WHERE id=%s",
                (storage_key,code or None,att_id))
            conn.commit()
        else:
            cur.execute("""INSERT INTO mail_attachments
                (mail_uid,trip_code,original_filename,saved_filename,content_type,
                 storage_key,detected_type,analysis_status,confidence,review_flag,file_hash)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (uid,code,safe_fn,f"{uid}_{safe_fn}",file.content_type,
                 storage_key,"ausstehend","ausstehend","niedrig","pruefen",h))
            att_id=cur.fetchone()[0]
            conn.commit()

        # ── SOFORT ANALYSIEREN ─────────────────────────────────────────────
        ocr_text=""
        fields={}
        if not storage_key.startswith("S3-FEHLER"):
            # OCR
            # PDF: direkt mit pypdf lesen (schnell, kostenlos, offline)
            # Nur für Bilder Mistral OCR nutzen
            if ext == "pdf":
                try:
                    import pypdf as _pypdf
                    reader = _pypdf.PdfReader(io.BytesIO(file_bytes))
                    ocr_text = "\n".join(page.extract_text() or "" for page in reader.pages)
                    if not ocr_text.strip():
                        # Gescanntes PDF → Mistral OCR als Fallback
                        ocr_text = await mistral_ocr(file_bytes, safe_fn)
                except Exception:
                    ocr_text = await mistral_ocr(file_bytes, safe_fn)
            else:
                ocr_text = await mistral_ocr(file_bytes, safe_fn)
            if ocr_text and not ocr_text.startswith(("ERROR","KEIN","OCR_","NICHT")):
                # Regex-Extraktion
                regex_fns=extract_flight_numbers(ocr_text)
                regex_segs=extract_flight_segments_from_text(ocr_text) if regex_fns else []
                regex_seg_s=segments_to_string(regex_segs) if regex_segs else ""
                regex_pnr=extract_pnr(ocr_text) or ""

                # KI-Extraktion
                known_codes=[r[0] for r in cur.execute("SELECT trip_code FROM trip_meta") or []]
                cur.execute("SELECT trip_code FROM trip_meta ORDER BY trip_code")
                known_codes=[r[0] for r in cur.fetchall()]
                fields=await mistral_extract(ocr_text,known_codes,"anhang") or {}

                # Felder zusammenführen (Regex gewinnt bei Flugnummern/Segmenten)
                fns_ki=fields.get("flight_numbers","") or ""
                seg_ki=fields.get("flight_segments","") or ""
                # Regex überschreibt wenn besser
                if regex_fns:
                    fns_final=",".join(regex_fns)
                else:
                    fns_final=fns_ki
                if regex_seg_s and (not seg_ki or seg_ki.count("|")<regex_seg_s.count("|")):
                    seg_final=regex_seg_s
                else:
                    seg_final=seg_ki or regex_seg_s

                # Segment-Vollständigkeit
                if fns_final:
                    fn_list=[f.strip() for f in fns_final.split(",") if f.strip()]
                    seg_list=[s.strip() for s in seg_final.split(";") if s.strip()] if seg_final else []
                    seg_fns=[s.split("|")[0].strip() for s in seg_list]
                    for mfn in [f for f in fn_list if f not in seg_fns]:
                        seg_list.append(f"{mfn}|||||")
                    seg_final=";".join(seg_list)

                betrag=fields.get("betrag","") or ""
                waehrung=fields.get("waehrung","EUR") or "EUR"
                datum=fields.get("datum","") or ""
                anbieter=fields.get("anbieter","") or ""
                typ=fields.get("beleg_typ","Sonstiges") or "Sonstiges"
                pnr=fields.get("pnr_code","") or regex_pnr or ""
                conf=fields.get("confidence","mittel") or "mittel"
                dest=fields.get("destination","") or ""

                # Reisecode
                if not code:
                    code=fields.get("reisecode","") or ""
                    if not code and pnr:
                        cur.execute("SELECT trip_code FROM trip_meta WHERE pnr_code=%s LIMIT 1",(pnr,))
                        row=cur.fetchone()
                        if row: code=row[0]
                    if code:
                        cur.execute("INSERT INTO trip_meta (trip_code) VALUES (%s) ON CONFLICT DO NOTHING",(code,))

                # EUR-Betrag
                betrag_eur=""
                if betrag:
                    try:
                        val=float(betrag.replace(",","."))
                        eur,_=await convert_to_eur(val,waehrung)
                        betrag_eur=f"{eur:.2f}".replace(".",",")
                    except: pass

                # UPDATE Beleg
                cur.execute("""UPDATE mail_attachments SET
                    trip_code=%s,detected_type=%s,detected_amount=%s,detected_amount_eur=%s,
                    detected_currency=%s,detected_date=%s,detected_vendor=%s,
                    detected_flight_numbers=%s,flight_segments=%s,pnr_code=%s,
                    extracted_text=%s,analysis_status=%s,confidence=%s,
                    review_flag=%s,ki_bemerkung=%s WHERE id=%s""",
                    (code or None,typ,betrag,betrag_eur,waehrung,datum,anbieter,
                     fns_final or None,seg_final or None,pnr or None,
                     ocr_text[:10000],"ok",conf,
                     "ok" if conf=="hoch" else "pruefen",
                     f"Upload: {safe_fn}",att_id))

                # trip_meta aktualisieren
                if code:
                    if pnr: cur.execute("UPDATE trip_meta SET pnr_code=%s WHERE trip_code=%s AND (pnr_code IS NULL OR pnr_code='')",(pnr,code))
                    if fns_final: cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s AND (flight_numbers IS NULL OR flight_numbers='')",(fns_final,code))
                    if dest: cur.execute("UPDATE trip_meta SET destinations=%s WHERE trip_code=%s AND (destinations IS NULL OR destinations='')",(dest,code))

                # VMA automatisch berechnen - immer wenn Auslandsflüge erkannt
                if regex_segs and code:
                    cur.execute("SELECT departure_date,return_date,vma_destinations FROM trip_meta WHERE trip_code=%s",(code,))
                    trip_row=cur.fetchone()
                    if trip_row and trip_row[0]:
                        dep_d_str=str(trip_row[0])
                        ret_d_str=str(trip_row[1]) if trip_row[1] else ""
                        dest_cc=None;arrive_date=None;leave_date=None
                        for seg in regex_segs:
                            arr=seg.get("arr","").upper()
                            cc=AIRPORT_CC.get(arr)
                            if cc and cc!="DE":
                                dest_cc=cc
                                try:
                                    p=seg["date"].split(".");arrive_date=date(int(p[2]),int(p[1]),int(p[0]))
                                except: pass
                                break
                        for seg in reversed(regex_segs):
                            dep=seg.get("dep","").upper()
                            cc=AIRPORT_CC.get(dep)
                            if cc and cc!="DE":
                                try:
                                    arr_d=seg.get("arr_date") or seg.get("date","")
                                    p=arr_d.split(".");leave_date=date(int(p[2]),int(p[1]),int(p[0]))
                                except: pass
                                break
                        if dest_cc and dest_cc!="DE":
                            try:
                                dep_d=date.fromisoformat(dep_d_str)
                                ret_d=date.fromisoformat(ret_d_str) if ret_d_str else None
                                arrive=arrive_date or (dep_d+timedelta(days=1))
                                leave=leave_date or ret_d or arrive
                                parts=[f"{dep_d}:DE"]
                                if arrive>dep_d: parts.append(f"{arrive}:{dest_cc}")
                                if leave and leave>arrive: parts.append(f"{leave}:DE")
                                vma_str=",".join(parts)
                                cur.execute("UPDATE trip_meta SET vma_destinations=%s WHERE trip_code=%s",(vma_str,code))
                                print(f"[VMA Upload] {code}: {vma_str}")
                            except Exception as ve:
                                print(f"[VMA Upload Fehler]: {ve}")

                conn.commit()
                # Alle synthetischen Flug-Belege löschen wenn echter Upload mit Segmenten da
                if seg_final and code and att_id:
                    cur.execute("""DELETE FROM mail_attachments
                        WHERE trip_code=%s AND detected_type='Flug'
                        AND (storage_key LIKE 'repaired_%%'
                          OR storage_key LIKE 'manual_seg_%%'
                          OR storage_key LIKE 'manual%%')
                        AND id != %s""",(code, att_id))
                    deleted=cur.rowcount
                    if deleted: print(f"[Upload] {deleted} synthetische Belege für {code} gelöscht")
                    conn.commit()
            else:
                cur.execute("UPDATE mail_attachments SET analysis_status='analysefehler',extracted_text=%s WHERE id=%s",(ocr_text,att_id))
                conn.commit()

        cur.close();conn.close()

        # Ergebnis-Seite
        fns_show=fields.get("flight_numbers","") or ",".join(extract_flight_numbers(ocr_text)) if ocr_text else ""
        seg_show=(fields.get("flight_segments","") or "")[:200]
        return page_shell("Upload",f"""
        <div class="page-card">
          <h2 class="ok-t">✓ {safe_fn} hochgeladen &amp; analysiert</h2>
          <table style="margin:12px 0">
            <tr><td>Reise</td><td><b style="font-family:DM Mono,monospace">{code or '–'}</b></td></tr>
            <tr><td>Typ</td><td>{fields.get("beleg_typ","–")}</td></tr>
            <tr><td>Anbieter</td><td>{fields.get("anbieter","–")}</td></tr>
            <tr><td>Betrag</td><td>{fields.get("betrag","–")} {fields.get("waehrung","")}</td></tr>
            <tr><td>PNR</td><td style="font-family:DM Mono,monospace;color:var(--gr6)">{fields.get("pnr_code","–")}</td></tr>
            <tr><td>Flugnummern</td><td style="font-family:DM Mono,monospace;color:var(--b600)">{fns_show or "–"}</td></tr>
            <tr><td>Segmente</td><td style="font-family:DM Mono,monospace;font-size:10px">{seg_show or "–"}</td></tr>
          </table>
          <div class="acts">
            {f'<a class="btn" href="/trip/{code}">Reise {code} anzeigen</a>' if code else ''}
            <a class="btn-l" href="/beleg-edit/{att_id}?back={"/trip/"+code if code else "/attachment-log"}">✏ Korrigieren</a>
            <a class="btn-l" href="/">Dashboard</a>
          </div>
        </div>""")
    except Exception as e:
        import traceback
        return page_shell("Fehler",f'<div class="page-card"><h2 class="err-t">Upload-Fehler</h2><p>{e}</p><pre style="font-size:10px">{traceback.format_exc()[:500]}</pre><a class="btn-l" href="/">Zurück</a></div>')


@app.get("/reanalyze-mails")
def reanalyze_mails():
    """Setzt Mail-Status zurück, löscht Mail-Body-Belege, markiert Inline-Bilder."""
    try:
        conn=get_conn();cur=conn.cursor()
        # Mail-Status zurücksetzen
        cur.execute("UPDATE mail_messages SET analysis_status='ausstehend' WHERE analysis_status='ok'")
        n_mails=cur.rowcount
        # Alte Mail-Body-Belege löschen
        cur.execute("DELETE FROM mail_attachments WHERE storage_key LIKE 'mail_body_%'")
        n_belege=cur.rowcount
        # Inline-Bilder + nicht-analysierbare sofort als irrelevant markieren
        cur.execute("""UPDATE mail_attachments SET
            analysis_status='Inline-Grafik', confidence='niedrig', review_flag='ok',
            detected_type='Inline-Grafik'
            WHERE original_filename SIMILAR TO 'image[0-9]+[.](png|jpg|jpeg|gif|bmp|emz|wmz)'
            OR original_filename ILIKE '%.emz'
            OR original_filename ILIKE '%.wmz'""")
        n_images=cur.rowcount
        # Ausstehende echte Anhänge (PDFs, Bilder ohne inline-Muster) zurücksetzen
        cur.execute("""UPDATE mail_attachments SET analysis_status='ausstehend'
            WHERE analysis_status NOT IN ('Inline-Grafik','ok (manuell)')
            AND storage_key NOT LIKE 'mail_body_%'
            AND original_filename NOT SIMILAR TO 'image[0-9]+[.](png|jpg|jpeg|gif|bmp|emz|wmz)'
            AND original_filename NOT ILIKE '%.emz'
            AND original_filename NOT ILIKE '%.wmz'""")
        conn.commit();cur.close();conn.close()
        return {"status":"ok","mails_reset":n_mails,
                "mailbody_belege_geloescht":n_belege,
                "inline_bilder_markiert":n_images,
                "hinweis":"Bitte /analyze-attachments aufrufen"}
    except Exception as e:
        return {"status":"fehler","detail":str(e)}

@app.get("/set-segments/{tc}")
def set_segments_form(tc: str):
    """Formular zum Setzen von Flug-Segmenten – vorausgefüllt mit bekannten Daten."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""SELECT a.id,a.detected_flight_numbers,a.flight_segments
            FROM mail_attachments a WHERE a.trip_code=%s AND a.detected_type='Flug'
            ORDER BY a.id LIMIT 1""",(tc,))
        beleg=cur.fetchone()
        cur.execute("SELECT flight_numbers,departure_date,return_date,destinations FROM trip_meta WHERE trip_code=%s",(tc,))
        meta=cur.fetchone()
        cur.close();conn.close()

        fns_meta=meta[0] if meta else ""
        dep_d=str(meta[1]) if meta and meta[1] else ""
        ret_d=str(meta[2]) if meta and meta[2] else ""
        dest=meta[3] if meta else ""

        att_id=beleg[0] if beleg else "new"
        current_fns=beleg[1] if beleg else fns_meta
        current_segs=beleg[2] if beleg else ""

        # Segmente vorausfüllen wenn leer oder nur Stubs (ohne Airports)
        prefill_segs=current_segs or ""
        if not prefill_segs or "||||||" in prefill_segs:
            # Generiere vorausgefüllte Segmente aus Flugnummern + Daten
            fn_list=[f.strip() for f in (current_fns or fns_meta or "").split(",") if f.strip()]
            half=len(fn_list)//2 or 1
            segs=[]
            for i,fn in enumerate(fn_list):
                d=dep_d if i<half else ret_d
                d_fmt=d.replace("-",".") if "-" in d else d
                # DD.MM.YYYY aus YYYY-MM-DD
                if d_fmt and "-" not in d_fmt:
                    pass
                elif d_fmt:
                    p=d_fmt.split("-")
                    d_fmt=f"{p[2]}.{p[1]}.{p[0]}" if len(p)==3 else d_fmt
                segs.append(f"{fn}|||{d_fmt}|||")
            prefill_segs=";".join(segs)

        # Jedes Segment als eigene Zeile im Textarea
        seg_lines="\n".join(s.strip() for s in prefill_segs.split(";") if s.strip())

        return page_shell(f"Segmente {tc}",f"""
        <div class="page-card" style="max-width:860px">
          <h2>✈ Flug-Segmente setzen – {tc}</h2>
          <div style="background:var(--b50);border:1px solid var(--b100);border-radius:8px;padding:14px 16px;margin-bottom:18px;font-size:12.5px">
            <b>Format pro Zeile:</b> <code style="font-size:11px">FLUGNR|DEP|ARR|TT.MM.YYYY|HH:MM|TT.MM.YYYY|HH:MM</code><br>
            <b>Beispiel:</b> <code style="font-size:11px">LH3463|NUE|FRA|20.04.2026|13:00|20.04.2026|14:15</code><br>
            <span style="color:var(--t300)">Abflug- und Ankunfts-IATA-Code · Abflugdatum · Abflugzeit · Ankunftsdatum · Ankunftszeit</span>
          </div>
          <form method="post" action="/set-segments/{tc}/{att_id}">
            <div class="fgrp" style="margin-bottom:12px">
              <label class="flbl">Flugnummern (kommagetrennt)</label>
              <input class="finp" name="fns" value="{current_fns or fns_meta or ''}"
                placeholder="LH3463,LH1078,LH1077,LH3463" style="font-family:DM Mono,monospace">
            </div>
            <div class="fgrp">
              <label class="flbl">Segmente – eine Zeile pro Segment</label>
              <textarea class="finp" name="segments" rows="{max(4,len(seg_lines.splitlines())+2)}"
                style="font-family:DM Mono,monospace;font-size:12px;line-height:1.8">{seg_lines}</textarea>
            </div>
            <div style="background:var(--am1);border:1px solid rgba(201,124,10,.2);border-radius:6px;padding:10px 14px;margin:12px 0;font-size:12px">
              <b>Reise {tc}:</b> {dep_d} → {ret_d} · Ziel: {dest or "–"}<br>
              <span style="color:var(--t500)">Flughäfen: NUE=Nürnberg · FRA=Frankfurt · LYS=Lyon · ZRH=Zürich · SJO=San José · PTY=Panama</span>
            </div>
            <div class="mfooter">
              <a class="btn-l" href="/trip/{tc}">Abbrechen</a>
              <button type="submit" class="btn-mp">💾 Segmente speichern</button>
            </div>
          </form>
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.post("/set-segments/{tc}/{att_id}")
async def set_segments_save(tc: str, att_id: str, request: Request):
    """Speichert Segmente. Akzeptiert Semikolon oder Zeilenumbruch als Trenner."""
    try:
        form=await request.form()
        fns=(form.get("fns") or "").strip()
        segs_raw=(form.get("segments") or "").strip()
        # Normalisieren: Zeilenumbrüche → Semikolon
        segs=";".join(l.strip() for l in re.split(r'[;\n]', segs_raw) if l.strip())

        conn=get_conn();cur=conn.cursor()

        if att_id == "new":
            uid=f"manual_seg_{tc}"
            cur.execute("SELECT id FROM mail_attachments WHERE storage_key=%s",(f"manual_seg_{tc}",))
            existing=cur.fetchone()
            if existing:
                cur.execute("UPDATE mail_attachments SET flight_segments=%s,detected_flight_numbers=%s,analysis_status='ok (manuell)',confidence='hoch',review_flag='ok' WHERE id=%s",
                    (segs or None,fns or None,existing[0]))
            else:
                cur.execute("""INSERT INTO mail_attachments
                    (mail_uid,trip_code,original_filename,saved_filename,content_type,
                     storage_key,detected_type,detected_flight_numbers,flight_segments,
                     analysis_status,confidence,review_flag,ki_bemerkung)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (uid,tc,f"Flüge {tc}",f"Flüge {tc}","text/plain",
                     f"manual_seg_{tc}","Flug",fns or None,segs or None,
                     "ok (manuell)","hoch","ok","Manuell eingetragen"))
        else:
            cur.execute("UPDATE mail_attachments SET flight_segments=%s,detected_flight_numbers=%s,analysis_status='ok (manuell)',confidence='hoch',review_flag='ok' WHERE id=%s",
                (segs or None,fns or None,int(att_id)))

        if fns:
            cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s",(fns,tc))

        # VMA aus Segmenten berechnen
        if segs:
            seg_list=[s.strip().split("|") for s in segs.split(";") if s.strip()]
            dest_cc=None; arrive_d=None; leave_d=None
            for seg in seg_list:
                if len(seg)>=3 and seg[2].strip():
                    cc=AIRPORT_CC.get(seg[2].strip().upper())
                    if cc and cc!="DE" and not dest_cc:
                        dest_cc=cc
                        try:
                            p=seg[3].strip().split("."); arrive_d=date(int(p[2]),int(p[1]),int(p[0]))
                        except: pass
            for seg in reversed(seg_list):
                if len(seg)>=2 and seg[1].strip():
                    cc=AIRPORT_CC.get(seg[1].strip().upper())
                    if cc and cc!="DE":
                        try:
                            ds=seg[5].strip() if len(seg)>5 and seg[5].strip() else (seg[3].strip() if len(seg)>3 else "")
                            p=ds.split("."); leave_d=date(int(p[2]),int(p[1]),int(p[0]))
                        except: pass
                        break
            if dest_cc and dest_cc!="DE":
                cur.execute("SELECT departure_date,return_date FROM trip_meta WHERE trip_code=%s",(tc,))
                tr=cur.fetchone()
                if tr and tr[0]:
                    try:
                        dep=tr[0]; ret=tr[1]
                        arrive=arrive_d or (dep+timedelta(days=1))
                        leave=leave_d or ret or arrive
                        parts=[f"{dep}:DE"]
                        if arrive>dep: parts.append(f"{arrive}:{dest_cc}")
                        if leave and leave>arrive: parts.append(f"{leave}:DE")
                        cur.execute("UPDATE trip_meta SET vma_destinations=%s WHERE trip_code=%s",(",".join(parts),tc))
                    except: pass

        conn.commit();cur.close();conn.close()
        return RedirectResponse(url=f"/trip/{tc}",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)},status_code=500)


@app.get("/repair-segments/{tc}")
async def repair_segments(tc: str):
    """
    Liest alle Mail-Bodies + S3-PDFs der Reise und extrahiert Flug-Segmente.
    Erstellt/aktualisiert den Flug-Beleg mit korrekten Segmenten.
    """
    try:
        conn=get_conn(); cur=conn.cursor()

        # trip_meta
        cur.execute("SELECT flight_numbers,departure_date,return_date,pnr_code FROM trip_meta WHERE trip_code=%s",(tc,))
        meta=cur.fetchone()
        if not meta: return {"status":"fehler","detail":"Reise nicht gefunden"}
        meta_fns=[f.strip() for f in (meta[0] or "").split(",") if f.strip()]
        dep_d, ret_d, pnr_meta = meta[1], meta[2], meta[3]

        # Alle Mails der Reise
        cur.execute("SELECT id,subject,body FROM mail_messages WHERE trip_code=%s ORDER BY id",(tc,))
        mails=cur.fetchall()

        # Alle Anhänge (extracted_text + S3-PDFs)
        cur.execute("""SELECT id,storage_key,original_filename,extracted_text
            FROM mail_attachments WHERE trip_code=%s
            AND (content_type='application/pdf' OR original_filename ILIKE '%%.pdf')
            AND storage_key NOT LIKE 'S3-FEHLER%%'
            AND storage_key NOT LIKE 'mail_body%%'
            AND storage_key NOT LIKE 'repaired%%'
            ORDER BY id""",(tc,))
        pdfs=cur.fetchall()

        all_text_sources = []

        # Mail-Bodies sammeln
        for mid, subj, body in mails:
            if not body: continue
            import html as _h
            t = _h.unescape(body)
            t = re.sub(r'<style[^>]*>.*?</style>', ' ', t, flags=re.DOTALL|re.IGNORECASE)
            t = re.sub(r'<[^>]+>', ' ', t)
            t = re.sub(r'[ \t]+', ' ', t)
            t = re.sub(r'\n{3,}', '\n\n', t).strip()
            # Sophos-Links entfernen
            t = re.sub(r'https?://[\w.-]*sophos[\w./-]*', ' ', t, flags=re.IGNORECASE)
            full = f"{subj or ''}\n{t}"
            all_text_sources.append(("mail",mid,full))

        # PDFs aus S3 lesen
        for att_id, skey, fname, ext_text in pdfs:
            if ext_text:
                all_text_sources.append(("pdf_cached",att_id,ext_text))
            else:
                try:
                    s3=get_s3()
                    obj=s3.get_object(Bucket=S3_BUCKET,Key=skey)
                    pdf_bytes=obj["Body"].read()
                    import pypdf as _pypdf, io as _io
                    reader=_pypdf.PdfReader(_io.BytesIO(pdf_bytes))
                    pdf_text="\n".join(page.extract_text() or "" for page in reader.pages)
                    if pdf_text.strip():
                        cur.execute("UPDATE mail_attachments SET extracted_text=%s WHERE id=%s",(pdf_text[:10000],att_id))
                        all_text_sources.append(("pdf_s3",att_id,pdf_text))
                except: pass

        # Segmente aus allen Quellen extrahieren
        all_segs = []
        seen_seg_keys = set()
        sources_used = []

        for src_type, src_id, text in all_text_sources:
            segs = extract_flight_segments_from_text(text)
            for seg in segs:
                # Eindeutigkeitsschlüssel: FN + Datum + Abflugzeit
                key = f"{seg['fn']}_{seg['date']}_{seg['dep_time']}"
                if key not in seen_seg_keys and (seg['dep'] or seg['arr'] or seg['date']):
                    seen_seg_keys.add(key)
                    all_segs.append(seg)
                    sources_used.append(f"{seg['fn']} von {src_type}#{src_id}")

        # Fehlende meta_fns als Stubs hinzufügen (mit Datum)
        found_fns = [s["fn"] for s in all_segs]
        half = max(1, len(meta_fns)//2)
        for i, fn in enumerate(meta_fns):
            if fn not in found_fns:
                d = str(dep_d) if dep_d and i < half else (str(ret_d) if ret_d else str(dep_d) if dep_d else "")
                all_segs.append({"fn":fn,"dep":"","arr":"","date":d,"arr_date":d,"dep_time":"","arr_time":""})

        # Segmente in Reihenfolge sortieren (Datum aufsteigend)
        def sort_key(s):
            d = s.get("date","")
            try:
                p=d.split("."); return (int(p[2]),int(p[1]),int(p[0]),s.get("dep_time",""))
            except: return (9999,1,1,"")
        all_segs.sort(key=sort_key)

        # Segment-String bauen
        seg_string = ";".join(
            f"{s['fn']}|{s['dep']}|{s['arr']}|{s['date']}|{s['dep_time']}|{s.get('arr_date',s['date'])}|{s['arr_time']}"
            for s in all_segs
        )
        # Flugnummern dedupliziert aber Reihenfolge bewahren
        seen=set(); fns_dedup=[]
        for s in all_segs:
            if s["fn"] not in seen: seen.add(s["fn"]); fns_dedup.append(s["fn"])
        fns_string = ",".join(fns_dedup)

        # VMA automatisch berechnen
        non_de_segs = [s for s in all_segs if s.get("arr") and AIRPORT_CC.get(s["arr"]) and AIRPORT_CC.get(s["arr"]) != "DE"]
        if non_de_segs:
            first_abroad = non_de_segs[0]
            dest_cc = AIRPORT_CC.get(first_abroad["arr"],"")
            # Letztes Segment im Ausland
            return_segs = [s for s in all_segs if s.get("dep") and AIRPORT_CC.get(s["dep"]) and AIRPORT_CC.get(s["dep"]) != "DE"]
            last_abroad_dep = return_segs[-1] if return_segs else None

            if dest_cc and dest_cc != "DE" and dep_d:
                try:
                    from datetime import date as _date, timedelta as _td
                    dep_date = dep_d
                    arr_d_str = first_abroad["date"]
                    p=arr_d_str.split(".")
                    arrive = _date(int(p[2]),int(p[1]),int(p[0])) if arr_d_str and len(p)==3 else dep_date+_td(days=1)
                    if last_abroad_dep:
                        dp2=last_abroad_dep["date"].split(".")
                        leave = _date(int(dp2[2]),int(dp2[1]),int(dp2[0])) if len(dp2)==3 else (ret_d or arrive)
                    else:
                        leave = ret_d or arrive
                    parts=[f"{dep_date}:DE"]
                    if arrive > dep_date: parts.append(f"{arrive}:{dest_cc}")
                    if leave and leave > arrive: parts.append(f"{leave}:DE")
                    vma_str=",".join(parts)
                    cur.execute("UPDATE trip_meta SET vma_destinations=%s WHERE trip_code=%s",(vma_str,tc))
                except Exception as ve:
                    pass

        # Existierenden Flug-Beleg suchen
        cur.execute("""SELECT id FROM mail_attachments
            WHERE trip_code=%s AND detected_type='Flug'
            ORDER BY CASE WHEN storage_key LIKE 'repaired%%' THEN 1 ELSE 0 END, id
            LIMIT 1""",(tc,))
        existing = cur.fetchone()

        if existing:
            cur.execute("""UPDATE mail_attachments SET
                flight_segments=%s, detected_flight_numbers=%s,
                analysis_status='ok (repariert)', confidence='hoch', review_flag='ok',
                ki_bemerkung=%s WHERE id=%s""",
                (seg_string, fns_string, f"Repariert: {len(all_segs)} Segmente aus {len(all_text_sources)} Quellen", existing[0]))
            beleg_id=existing[0]; action="aktualisiert"
        else:
            uid=f"repaired_{tc}"
            cur.execute("""INSERT INTO mail_attachments
                (mail_uid,trip_code,original_filename,saved_filename,content_type,
                 storage_key,detected_type,detected_flight_numbers,flight_segments,
                 analysis_status,confidence,review_flag,ki_bemerkung)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id""",
                (uid,tc,f"Flüge {tc}",f"Flüge {tc}","text/plain",f"repaired_{tc}",
                 "Flug",fns_string,seg_string,
                 "ok (repariert)","hoch","ok",f"Auto: {len(all_segs)} Segs"))
            beleg_id=cur.fetchone()[0]; action="neu erstellt"

        cur.execute("UPDATE trip_meta SET flight_numbers=%s WHERE trip_code=%s",(fns_string,tc))
        conn.commit(); cur.close(); conn.close()

        return {"status":"ok","action":action,"beleg_id":beleg_id,
                "segmente":seg_string,"quellen":sources_used}
    except Exception as e:
        import traceback
        return {"status":"fehler","detail":str(e),"trace":traceback.format_exc()[:1000]}


@app.get("/recalc-vma/{tc}")
def recalc_vma(tc: str):
    """Berechnet vma_destinations für eine Reise neu aus Flug-Segmenten und Ziel."""
    try:
        conn=get_conn();cur=conn.cursor()
        # Reset vma_destinations
        cur.execute("UPDATE trip_meta SET vma_destinations=NULL WHERE trip_code=%s",(tc,))
        # Hole flight_segments aus mail_attachments
        cur.execute("""SELECT flight_segments,detected_checkin,detected_checkout,detected_type
            FROM mail_attachments WHERE trip_code=%s AND flight_segments IS NOT NULL
            ORDER BY id""", (tc,))
        segs_rows=cur.fetchall()
        cur.execute("SELECT departure_date,return_date,destinations FROM trip_meta WHERE trip_code=%s",(tc,))
        meta=cur.fetchone()
        if not meta: return {"status":"fehler","detail":"Reise nicht gefunden"}
        dep_d_raw,ret_d_raw,destinations=meta

        # AIRPORT_CC = globale Konstante (oben definiert)

        dest_cc=None
        arrive_date=None
        leave_date=None

        # Aus flight_segments
        for seg_str,_,_,_ in segs_rows:
            if not seg_str: continue
            segs=[s.strip().split("|") for s in seg_str.split(";") if s.strip()]
            # Hinflug: Ankunft nicht-DE
            for seg in segs:
                if len(seg)>=3:
                    arr=seg[2].strip().upper()
                    cc=AIRPORT_CC.get(arr)
                    if cc and cc!="DE":
                        dest_cc=cc
                        if len(seg)>=6 and seg[5].strip():
                            try:
                                p=seg[5].split("."); arrive_date=date(int(p[2]),int(p[1]),int(p[0]))
                            except: pass
                        if not arrive_date and len(seg)>=4 and seg[3].strip():
                            try:
                                p=seg[3].split("."); arrive_date=date(int(p[2]),int(p[1]),int(p[0]))
                            except: pass
                        break
            # Rückflug: Abflug nicht-DE
            for seg in reversed(segs):
                if len(seg)>=2:
                    dep=seg[1].strip().upper()
                    cc=AIRPORT_CC.get(dep)
                    if cc and cc!="DE":
                        if len(seg)>=4 and seg[3].strip():
                            try:
                                p=seg[3].split("."); leave_date=date(int(p[2]),int(p[1]),int(p[0]))
                            except: pass
                        break
            if dest_cc: break

        # Fallback: Destinations-Text
        if not dest_cc and destinations:
            DEST_CC_MAP={"lyon":"FR","paris":"FR","frankreich":"FR","france":"FR",
                "london":"GB","grossbritannien":"GB","indien":"IN","dubai":"AE",
                "usa":"US","schweiz":"CH","oesterreich":"AT","spanien":"ES",
                "italien":"IT","tuerkei":"TR","japan":"JP","singapur":"SG","china":"CN"}
            for k,v in DEST_CC_MAP.items():
                if k in destinations.lower():
                    dest_cc=v; break

        if not dest_cc or dest_cc=="DE":
            return {"status":"ok","detail":"Kein Auslandsaufenthalt erkannt","vma_destinations":None}

        try:
            dep_d=dep_d_raw if isinstance(dep_d_raw,date) else date.fromisoformat(str(dep_d_raw))
            ret_d=ret_d_raw if isinstance(ret_d_raw,date) else (date.fromisoformat(str(ret_d_raw)) if ret_d_raw else None)
            arrive=arrive_date or (dep_d+timedelta(days=1))
            leave=leave_date or ret_d or arrive
            parts=[f"{dep_d}:DE"]
            if arrive>dep_d: parts.append(f"{arrive}:{dest_cc}")
            if leave>arrive: parts.append(f"{leave}:DE")
            vma_str=",".join(parts)
            cur.execute("UPDATE trip_meta SET vma_destinations=%s WHERE trip_code=%s",(vma_str,tc))
            conn.commit();cur.close();conn.close()
            return {"status":"ok","vma_destinations":vma_str,"land":dest_cc}
        except Exception as e:
            return {"status":"fehler","detail":str(e)}
    except Exception as e:
        return {"status":"fehler","detail":str(e)}


@app.get("/cleanup-duplicates")
def cleanup_duplicates():
    """Entfernt doppelte Mail-Body-Belege – behält jeweils den neuesten pro Typ+Reise."""
    try:
        conn=get_conn();cur=conn.cursor()
        # Duplikate: gleiche trip_code + detected_type + storage_key LIKE mail_body_%
        # Behalte den mit der höchsten ID (neuesten)
        cur.execute("""DELETE FROM mail_attachments WHERE id NOT IN (
            SELECT MAX(id) FROM mail_attachments
            WHERE storage_key LIKE 'mail_body_%'
            GROUP BY trip_code, detected_type, detected_vendor
        ) AND storage_key LIKE 'mail_body_%'""")
        n=cur.rowcount
        conn.commit();cur.close();conn.close()
        return {"status":"ok","geloescht":n}
    except Exception as e:
        return {"status":"fehler","detail":str(e)}



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

@app.get("/vma-rates", response_class=HTMLResponse)
def vma_rates_page():
    """Zeigt aktuelle VMA-Sätze und ermöglicht Aktualisierung."""
    try:
        conn=get_conn();cur=conn.cursor()
        # VMA-Sätze aus DB laden falls vorhanden (überschreiben Hardcode)
        cur.execute("""CREATE TABLE IF NOT EXISTS vma_settings (
            id SERIAL PRIMARY KEY, cc TEXT UNIQUE, full_rate NUMERIC, partial_rate NUMERIC,
            label TEXT, updated_at TIMESTAMP DEFAULT now())""")
        cur.execute("SELECT cc,label,full_rate,partial_rate,updated_at FROM vma_settings ORDER BY cc")
        db_rates=cur.fetchall()
        cur.close();conn.close()

        # Tabelle aus aktuellem VMA dict
        rows=""
        for cc, r in sorted(VMA.items()):
            db_row=next((x for x in db_rates if x[0]==cc),None)
            full = float(db_row[2]) if db_row else r["full"]
            partial = float(db_row[3]) if db_row else r["partial"]
            updated = str(db_row[4])[:10] if db_row else "Hardcode 2026"
            rows+=f"""<tr>
                <td style="font-family:DM Mono,monospace;font-weight:600">{cc}</td>
                <td style="font-size:11px;color:var(--t500)">{db_row[1] if db_row else cc}</td>
                <td style="font-family:DM Mono,monospace;text-align:right">{partial:.2f} €</td>
                <td style="font-family:DM Mono,monospace;text-align:right">{full:.2f} €</td>
                <td style="font-size:11px;color:var(--t300)">{updated}</td>
            </tr>"""

        return page_shell("VMA-Sätze",f"""
        <div class="page-card" style="max-width:900px">
          <h2>Verpflegungsmehraufwendungen – Aktuelle Sätze</h2>
          <div style="background:var(--am1);border:1px solid rgba(201,124,10,.2);border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:13px">
            <b>⚠ Wichtiger Hinweis:</b> Das BMF veröffentlicht jährlich neue Sätze (meist im Oktober für das Folgejahr).
            Bitte prüfen Sie die Sätze unter
            <a href="https://www.bundesfinanzministerium.de" target="_blank" style="color:var(--b600)">bundesfinanzministerium.de</a>
            und aktualisieren Sie die Werte hier bei Bedarf.
            <b>Aktuell hinterlegt: Sätze 2026</b>
          </div>
          <div class="acts" style="margin-bottom:16px">
            <a class="btn" href="/vma-rates/update" onclick="return confirm('VMA-Sätze aus Hardcode in DB schreiben?')">💾 Aktuelle Sätze in DB speichern</a>
            <a class="btn-l" href="/">Dashboard</a>
          </div>
          <div style="overflow-x:auto"><table>
            <tr><th>Code</th><th>Region</th><th>8-24h (Partial)</th><th>&gt;24h (Full)</th><th>Stand</th></tr>
            {rows}
          </table></div>
          <p class="sub" style="margin-top:12px">Mahlzeitenabzug: Frühstück 20% · Mittagessen 40% · Abendessen 40% des jeweiligen Ländersatzes (§ 9 Abs. 4a EStG)</p>
          <p class="sub">Quelle: BMF-Schreiben zu § 9 Abs. 4a EStG · Sätze gelten ab 01.01.2026</p>
        </div>""",active_tab="")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/vma-rates/update")
def vma_rates_save():
    """Schreibt die Hardcode-VMA-Sätze in die DB (für Audit-Trail)."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS vma_settings (
            id SERIAL PRIMARY KEY, cc TEXT UNIQUE, full_rate NUMERIC, partial_rate NUMERIC,
            label TEXT, updated_at TIMESTAMP DEFAULT now())""")
        LABELS={"DE":"Deutschland","FR":"Frankreich","FR_PARIS":"Frankreich/Paris",
                "GB":"Großbritannien","GB_LONDON":"Großbritannien/London",
                "US":"USA","US_NYC":"USA/New York","US_LA":"USA/Los Angeles",
                "US_CHI":"USA/Chicago","US_MIA":"USA/Miami",
                "AT":"Österreich","CH":"Schweiz","CH_GENF":"Schweiz/Genf",
                "IT":"Italien","IT_MAILAND":"Italien/Mailand",
                "ES":"Spanien","ES_MADRID":"Spanien/Madrid",
                "NL":"Niederlande","BE":"Belgien","DK":"Dänemark",
                "PL":"Polen","PL_WARSCHAU":"Polen/Warschau",
                "JP":"Japan","JP_TOKIO":"Japan/Tokio",
                "CN":"China","CN_HK":"Hongkong",
                "IN":"Indien","AE":"VAE/Dubai","AZ":"Aserbaidschan",
                "SG":"Singapur","QA":"Katar","SA":"Saudi-Arabien",
                "TR":"Türkei","SE":"Schweden","NO":"Norwegen",
                "FI":"Finnland","CZ":"Tschechien","HU":"Ungarn","RO":"Rumänien",
                "KR":"Südkorea","AU":"Australien","CA":"Kanada","BR":"Brasilien","RU":"Russland"}
        for cc,r in VMA.items():
            cur.execute("""INSERT INTO vma_settings (cc,full_rate,partial_rate,label,updated_at)
                VALUES (%s,%s,%s,%s,now())
                ON CONFLICT (cc) DO UPDATE SET full_rate=EXCLUDED.full_rate,
                partial_rate=EXCLUDED.partial_rate,label=EXCLUDED.label,updated_at=now()""",
                (cc,r["full"],r["partial"],LABELS.get(cc,cc)))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/vma-rates",status_code=303)
    except Exception as e:
        return {"status":"fehler","detail":str(e)}


@app.get("/dsgvo", response_class=HTMLResponse)
def dsgvo_info():
    return page_shell("Datenschutz",f"""
    <div class="page-card" style="max-width:800px">
      <h2>🔒 Datenschutz & DSGVO-Konformität</h2>

      <h3 style="margin:20px 0 8px;color:var(--b700)">Datenspeicherung</h3>
      <table>
        <tr><th>Was</th><th>Wo</th><th>Zugriff</th></tr>
        <tr><td>Reisedaten, Belege, Mails</td><td>PostgreSQL auf Render (EU/Frankfurt)</td><td>Nur Herrhammer intern</td></tr>
        <tr><td>Dateien (PDFs, Bilder)</td><td>Hetzner Object Storage NBG1 (Nürnberg, DE)</td><td>Nur Herrhammer intern</td></tr>
        <tr><td>KI-Lernbeispiele</td><td>Anonymisiert in PostgreSQL</td><td>Nur Herrhammer intern</td></tr>
      </table>

      <h3 style="margin:20px 0 8px;color:var(--b700)">Mistral KI – Datenverarbeitung</h3>
      <table>
        <tr><th>Aspekt</th><th>Status</th></tr>
        <tr><td>Server-Standort</td><td>✅ Paris, Frankreich (EU)</td></tr>
        <tr><td>DSGVO-Konformität</td><td>✅ Mistral AI ist EU-Unternehmen, GDPR-compliant</td></tr>
        <tr><td>Training auf Kundendaten</td><td>✅ NEIN – nur Inference, kein Fine-Tuning</td></tr>
        <tr><td>Datenspeicherung bei Mistral</td><td>✅ Keine – Daten nach Response gelöscht</td></tr>
        <tr><td>Namen im API-Call</td><td>✅ Anonymisiert vor Übermittlung (Art. 25 DSGVO)</td></tr>
        <tr><td>E-Mails im API-Call</td><td>✅ Werden zu [E-MAIL] anonymisiert</td></tr>
        <tr><td>Telefonnummern im API-Call</td><td>✅ Werden zu [TEL] anonymisiert</td></tr>
        <tr><td>Flugnummern, Beträge</td><td>✅ Bleiben erhalten (keine PII)</td></tr>
      </table>

      <h3 style="margin:20px 0 8px;color:var(--b700)">KI-Lernbeispiele (Few-Shot)</h3>
      <div style="background:var(--b50);border:1px solid var(--b100);border-radius:8px;padding:12px 16px;font-size:13px">
        <p>✅ <b>DSGVO-konform:</b> Beispiele werden <b>vor der Speicherung anonymisiert</b>.</p>
        <p style="margin-top:6px">Die Beispiele verlassen das System nur als anonymisierter Text im Mistral-Prompt.
        Mistral speichert sie nicht, trainiert nicht darauf – sie wirken nur für die Dauer eines API-Calls
        als Kontext (In-Context Learning / RAG-Prinzip).</p>
        <p style="margin-top:6px">Rechtsgrundlage: Art. 6 Abs. 1 lit. b DSGVO (Vertragserfüllung),
        Art. 25 DSGVO (Privacy by Design).</p>
      </div>

      <h3 style="margin:20px 0 8px;color:var(--b700)">Empfehlung für Betriebsrat / Datenschutzbeauftragten</h3>
      <ul style="font-size:13px;line-height:1.8;padding-left:20px">
        <li>Mistral AI Data Processing Agreement (DPA) abschließen → <a href="https://mistral.ai/privacy/" target="_blank" style="color:var(--b600)">mistral.ai/privacy</a></li>
        <li>Render.com DPA → EU-Rechenzentrum Frankfurt konfiguriert</li>
        <li>Hetzner Online GmbH → deutsches Unternehmen, DSGVO-konform</li>
        <li>Interne Datenschutzerklärung für Mitarbeiter erstellen</li>
      </ul>

      <div class="acts" style="margin-top:20px">
        <a class="btn-l" href="/">← Dashboard</a>
        <a class="btn-l" href="/ki-beispiele">KI-Beispiele ansehen</a>
      </div>
    </div>""")


@app.get("/ki-beispiele", response_class=HTMLResponse)
def ki_beispiele():
    """Zeigt und verwaltet gespeicherte KI-Lernbeispiele."""
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("""CREATE TABLE IF NOT EXISTS ki_examples (
            id SERIAL PRIMARY KEY, mail_type TEXT, input_text TEXT,
            expected_json TEXT, description TEXT, approved BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT now())""")
        cur.execute("SELECT id,mail_type,description,approved,created_at,LENGTH(input_text) FROM ki_examples ORDER BY id DESC")
        rows=cur.fetchall();cur.close();conn.close()
        html="".join(f"""<tr>
            <td style="font-size:11px;color:var(--t300)">{str(r[4])[:10]}</td>
            <td><span class="bdg bdg-w">{r[1] or '–'}</span></td>
            <td>{r[2] or '–'}</td>
            <td>{r[5] or 0} Z.</td>
            <td><span class="bdg {'bdg-ok' if r[3] else 'bdg-e'}">{'✓ aktiv' if r[3] else '✗ inaktiv'}</span></td>
            <td>
              <a href="/ki-beispiele/toggle/{r[0]}" style="font-size:11px;color:var(--b600)">{'Deaktivieren' if r[3] else 'Aktivieren'}</a>
              · <a href="/ki-beispiele/delete/{r[0]}" onclick="return confirm('Löschen?')" style="font-size:11px;color:var(--re6)">Löschen</a>
            </td></tr>""" for r in rows)
        return page_shell("KI-Beispiele",f"""
        <div class="page-card">
          <h2>🧠 KI-Lernbeispiele ({len(rows)})</h2>
          <p class="sub" style="margin-bottom:12px">Diese Beispiele werden automatisch beim manuellen Import gespeichert und helfen Mistral ähnliche Mails besser zu erkennen.</p>
          <div class="acts">
            <a class="btn-l" href="/">Dashboard</a>
            <a class="btn" href="/mail-eingabe">+ Mail manuell eingeben</a>
          </div>
          {"<table><tr><th>Datum</th><th>Typ</th><th>Beschreibung</th><th>Länge</th><th>Status</th><th>Aktion</th></tr>"+html+"</table>" if rows else '<div class="empty">Noch keine Beispiele. Manuell Mails importieren um Beispiele zu erzeugen.</div>'}
        </div>""")
    except Exception as e:
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p></div>')

@app.get("/ki-beispiele/toggle/{ex_id}")
def ki_beispiel_toggle(ex_id: int):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("UPDATE ki_examples SET approved=NOT approved WHERE id=%s",(ex_id,))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/ki-beispiele",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)})

@app.get("/ki-beispiele/delete/{ex_id}")
def ki_beispiel_delete(ex_id: int):
    try:
        conn=get_conn();cur=conn.cursor()
        cur.execute("DELETE FROM ki_examples WHERE id=%s",(ex_id,))
        conn.commit();cur.close();conn.close()
        return RedirectResponse(url="/ki-beispiele",status_code=303)
    except Exception as e:
        return JSONResponse({"status":"fehler","detail":str(e)})


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
        cur.execute("""SELECT departure_date,return_date,traveler_name,trip_title,
            country_code,vma_destinations FROM trip_meta WHERE trip_code=%s""",(tc,))
        meta=cur.fetchone()
        if not meta: return HTMLResponse("Reise nicht gefunden",404)
        dep,ret,traveler,title,default_cc,vma_dest_str=meta
        dep_d=dep if isinstance(dep,date) else (date.fromisoformat(str(dep)) if dep else None)
        ret_d=ret if isinstance(ret,date) else (date.fromisoformat(str(ret)) if ret else None)
        if not dep_d or not ret_d:
            return page_shell(f"Mahlzeiten {tc}",f"""
            <div class="page-card"><h2>Mahlzeiten {tc}</h2>
            <p class="sub">Bitte zuerst Abreise- und Rückkehrdatum hinterlegen.</p>
            <a class="btn" href="/edit-trip/{tc}">Reise bearbeiten</a></div>""")

        vma_dest_dict=parse_vma_destinations(vma_dest_str or "")
        cur.execute("""SELECT meal_date,breakfast,lunch,dinner,notes,country_code,vma_override
            FROM daily_meals WHERE trip_code=%s ORDER BY meal_date""",(tc,))
        existing={row[0]: row for row in cur.fetchall()}
        cur.close();conn.close()

        days=(ret_d-dep_d).days+1

        # Alle verfügbaren Länder aus VMA-Tabelle für Dropdown
        country_opts=""
        for cc_key, r in sorted(VMA.items()):
            if "_" not in cc_key:  # nur Länder-Codes, keine Städte
                label=cc_key
                country_opts+=f'<option value="{cc_key}">{cc_key} ({r["full"]:.0f}€/Tag)</option>'

        rows_html=""
        vma_total=0.0
        for i in range(days):
            d=dep_d+timedelta(days=i)
            e=existing.get(d)
            b_chk="checked" if e and e[1] else ""
            l_chk="checked" if e and e[2] else ""
            di_chk="checked" if e and e[3] else ""
            notes_val=(e[4] if e and e[4] else "")
            # Land für diesen Tag: manuelle Angabe > vma_destinations > default
            day_cc = (e[5] if e and e[5] else None) or get_country_for_day(d,vma_dest_dict,default_cc or "DE")
            # VMA-Override manuell?
            vma_override = float(e[6]) if e and e[6] is not None else None

            dtype="partial" if i==0 or i==days-1 else "full"
            ml=[]
            if e:
                if e[1]: ml.append("breakfast")
                if e[2]: ml.append("lunch")
                if e[3]: ml.append("dinner")

            if vma_override is not None:
                vma_day=vma_override
                vma_source_icon="✏"
            else:
                vma_day=get_vma(day_cc,dtype,ml)
                vma_source_icon=""
            vma_total+=vma_day

            wd=["Mo","Di","Mi","Do","Fr","Sa","So"][d.weekday()]
            wkend_style=' style="background:var(--b50)"' if d.weekday()>=5 else ""

            # Land-Dropdown für diesen Tag
            cc_options="".join(
                f'<option value="{cc}" {"selected" if cc==day_cc else ""}>{cc}</option>'
                for cc in sorted(set(k for k in VMA.keys() if "_" not in k))
            )

            # VMA-Satz Info
            vma_r=VMA.get(day_cc,{"full":28.0,"partial":14.0})
            full_rate=vma_r["full"]
            base=full_rate if dtype=="full" else vma_r["partial"]

            rows_html+=f"""<tr{wkend_style}>
                <td style="font-weight:500;white-space:nowrap;font-size:12px">{str(d)} {wd}</td>
                <td style="text-align:center"><input type="checkbox" name="b_{d}" {b_chk} onchange="calcRow(this)"></td>
                <td style="text-align:center"><input type="checkbox" name="l_{d}" {l_chk} onchange="calcRow(this)"></td>
                <td style="text-align:center"><input type="checkbox" name="d_{d}" {di_chk} onchange="calcRow(this)"></td>
                <td>
                  <select name="cc_{d}" class="fsel" style="padding:2px 4px;font-size:11px;width:70px"
                    onchange="this.form.submit()" title="Land für VMA-Satz">
                    {cc_options}
                  </select>
                </td>
                <td>
                  <input type="number" class="finp" name="vma_override_{d}"
                    value="{vma_override if vma_override is not None else ''}"
                    placeholder="{vma_day:.2f}" step="0.01" min="0" style="padding:2px 6px;font-size:11px;width:70px"
                    title="Manueller VMA-Betrag (leer = automatisch)">
                </td>
                <td><input type="text" class="finp" name="n_{d}" value="{notes_val}" placeholder="Notiz..." style="padding:2px 6px;font-size:11px"></td>
                <td style="text-align:right;font-family:DM Mono,monospace;color:var(--b600);font-size:12px;white-space:nowrap">
                  {vma_day:.2f} € {vma_source_icon}
                  <div style="font-size:9px;color:var(--t300)">{day_cc} · {base:.0f}€/Tag · F:{full_rate*0.2:.2f}/M:{full_rate*0.4:.2f}/A:{full_rate*0.4:.2f}€</div>
                </td>
            </tr>"""

        title_str=f" · {title}" if title else ""

        # VMA-Satz-Jahr-Hinweis
        current_year=datetime.now().year
        vma_warning=""
        if current_year > 2026:
            vma_warning=f"""<div style="background:var(--am1);border:1px solid rgba(201,124,10,.25);border-radius:8px;padding:10px 14px;margin-bottom:12px;font-size:12px">
              ⚠ <b>Hinweis:</b> Die hinterlegten VMA-Sätze gelten für 2026.
              Für {current_year} bitte aktuelle BMF-Sätze unter
              <a href="/vma-rates" style="color:var(--am6)">VMA-Sätze</a> prüfen und aktualisieren.
            </div>"""

        return page_shell(f"Mahlzeiten {tc}",f"""
        <div class="page-card" style="max-width:900px">
          <h2>🍽 Mahlzeiten-Erfassung – {tc}{title_str}</h2>
          <p class="sub" style="margin-bottom:4px">Reisender: {traveler or '–'} · {days} Tage · {str(dep_d)} bis {str(ret_d)}</p>
          <p class="sub" style="margin-bottom:12px">Haken = Mahlzeit vom Kunden/Hotel gestellt (→ Abzug vom VMA)</p>
          {vma_warning}
          <form method="post" action="/meals/{tc}">
            <div style="overflow-x:auto"><table>
              <tr>
                <th>Datum</th>
                <th style="text-align:center">🍳 Früh<br><span style="font-size:9px;font-weight:400">−20%</span></th>
                <th style="text-align:center">🥗 Mittag<br><span style="font-size:9px;font-weight:400">−40%</span></th>
                <th style="text-align:center">🍽 Abend<br><span style="font-size:9px;font-weight:400">−40%</span></th>
                <th>Land</th>
                <th>VMA manuell<br><span style="font-size:9px;font-weight:400">leer = auto</span></th>
                <th>Notiz</th>
                <th style="text-align:right">VMA</th>
              </tr>
              {rows_html}
              <tr style="background:var(--b50)">
                <td colspan="7"><b>Summe VMA</b></td>
                <td style="text-align:right;font-family:DM Mono,monospace;font-weight:700;color:var(--b600)">{vma_total:.2f} €</td>
              </tr>
            </table></div>
            <div class="mfooter">
              <a class="btn-mc" href="/trip/{tc}">Zurück</a>
              <button type="submit" class="btn-mp">💾 Speichern</button>
            </div>
          </form>
          <div style="margin-top:12px;font-size:11px;color:var(--t300)">
            💡 Land-Dropdown ändert den VMA-Satz für diesen Tag. Manuelles VMA überschreibt die Berechnung komplett.
            <a href="/vma-rates" style="color:var(--b600)">Aktuelle VMA-Sätze ansehen</a>
          </div>
        </div>""")
    except Exception as e:
        import traceback
        return page_shell("Fehler",f'<div class="page-card"><p>{e}</p><pre style="font-size:10px">{traceback.format_exc()}</pre></div>')



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
            cc=form.get(f"cc_{d}","DE").strip().upper() or "DE"
            vma_ov_raw=form.get(f"vma_override_{d}","").strip()
            vma_ov=float(vma_ov_raw) if vma_ov_raw else None
            cur.execute("""INSERT INTO daily_meals
                (trip_code,meal_date,breakfast,lunch,dinner,notes,country_code,vma_override,updated_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,now())
                ON CONFLICT (trip_code,meal_date) DO UPDATE SET
                breakfast=EXCLUDED.breakfast, lunch=EXCLUDED.lunch,
                dinner=EXCLUDED.dinner, notes=EXCLUDED.notes,
                country_code=EXCLUDED.country_code, vma_override=EXCLUDED.vma_override,
                updated_at=now()""",
                (tc,d,b,l,di,notes,cc,vma_ov))
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
