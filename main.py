"""
# v2.0-e – httpx hinzugefügt, OpenAI Test
Herrhammer Reisekosten – Schritt a)
Mitarbeiter- und Reiseverwaltung

Läuft auf Render (PostgreSQL) und lokal (SQLite).
Datenbank wird automatisch erkannt via DATABASE_URL.
"""
from __future__ import annotations
import os, re, json
from datetime import date, datetime, timedelta
from typing import Optional

# ── Web-Framework ──────────────────────────────────────────────────────────────
from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ── Datenbank ──────────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")

def get_db():
    """
    Gibt eine DB-Verbindung zurück.
    PostgreSQL wenn DATABASE_URL gesetzt, sonst SQLite lokal.
    """
    if DATABASE_URL:
        import psycopg2
        return psycopg2.connect(DATABASE_URL)
    else:
        import sqlite3
        conn = sqlite3.connect("reisekosten.db", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

def is_postgres() -> bool:
    return bool(DATABASE_URL)

def ph() -> str:
    """Placeholder: %s für PostgreSQL, ? für SQLite."""
    return "%s" if is_postgres() else "?"

# ── VMA-Tagessätze 2026 (§ 9 Abs. 4a EStG) ────────────────────────────────────
# Quelle: BMF-Schreiben Auslandsreisekosten 2024 (gilt weiter für 2026)
VMA_SAETZE: dict[str, dict] = {
    "DE": {"name": "Deutschland",        "voll": 28.00,  "halb": 14.00},
    "FR": {"name": "Frankreich",         "voll": 53.00,  "halb": 26.50},
    "CH": {"name": "Schweiz",            "voll": 82.00,  "halb": 41.00},
    "AT": {"name": "Österreich",         "voll": 50.00,  "halb": 25.00},
    "GB": {"name": "Großbritannien",     "voll": 53.00,  "halb": 26.50},
    "IT": {"name": "Italien",            "voll": 48.00,  "halb": 24.00},
    "ES": {"name": "Spanien",            "voll": 45.00,  "halb": 22.50},
    "NL": {"name": "Niederlande",        "voll": 48.00,  "halb": 24.00},
    "BE": {"name": "Belgien",            "voll": 48.00,  "halb": 24.00},
    "PL": {"name": "Polen",              "voll": 45.00,  "halb": 22.50},
    "CZ": {"name": "Tschechien",         "voll": 45.00,  "halb": 22.50},
    "SE": {"name": "Schweden",           "voll": 55.00,  "halb": 27.50},
    "NO": {"name": "Norwegen",           "voll": 72.00,  "halb": 36.00},
    "DK": {"name": "Dänemark",           "voll": 58.00,  "halb": 29.00},
    "FI": {"name": "Finnland",           "voll": 53.00,  "halb": 26.50},
    "PT": {"name": "Portugal",           "voll": 45.00,  "halb": 22.50},
    "GR": {"name": "Griechenland",       "voll": 45.00,  "halb": 22.50},
    "TR": {"name": "Türkei",             "voll": 45.00,  "halb": 22.50},
    "US": {"name": "USA",                "voll": 59.00,  "halb": 29.50},
    "CA": {"name": "Kanada",             "voll": 55.00,  "halb": 27.50},
    "JP": {"name": "Japan",              "voll": 73.00,  "halb": 36.50},
    "CN": {"name": "China",              "voll": 53.00,  "halb": 26.50},
    "SG": {"name": "Singapur",           "voll": 60.00,  "halb": 30.00},
    "IN": {"name": "Indien",             "voll": 40.00,  "halb": 20.00},
    "AE": {"name": "VAE / Dubai",        "voll": 53.00,  "halb": 26.50},
    "QA": {"name": "Katar",              "voll": 50.00,  "halb": 25.00},
    "AU": {"name": "Australien",         "voll": 65.00,  "halb": 32.50},
    "BR": {"name": "Brasilien",          "voll": 46.00,  "halb": 23.00},
    "MX": {"name": "Mexiko",             "voll": 46.00,  "halb": 23.00},
    "AR": {"name": "Argentinien",        "voll": 45.00,  "halb": 22.50},
    "ZA": {"name": "Südafrika",          "voll": 40.00,  "halb": 20.00},
    "CR": {"name": "Costa Rica",         "voll": 40.00,  "halb": 20.00},
    "PA": {"name": "Panama",             "voll": 45.00,  "halb": 22.50},
    "CO": {"name": "Kolumbien",          "voll": 40.00,  "halb": 20.00},
    "CL": {"name": "Chile",              "voll": 45.00,  "halb": 22.50},
    "KR": {"name": "Südkorea",           "voll": 55.00,  "halb": 27.50},
    "TH": {"name": "Thailand",           "voll": 40.00,  "halb": 20.00},
    "ID": {"name": "Indonesien",         "voll": 40.00,  "halb": 20.00},
    "MY": {"name": "Malaysia",           "voll": 40.00,  "halb": 20.00},
    "HK": {"name": "Hongkong",           "voll": 67.00,  "halb": 33.50},
    "IL": {"name": "Israel",             "voll": 55.00,  "halb": 27.50},
    "RU": {"name": "Russland",           "voll": 45.00,  "halb": 22.50},
    "UA": {"name": "Ukraine",            "voll": 35.00,  "halb": 17.50},
    "HU": {"name": "Ungarn",             "voll": 40.00,  "halb": 20.00},
    "RO": {"name": "Rumänien",           "voll": 35.00,  "halb": 17.50},
    "HR": {"name": "Kroatien",           "voll": 45.00,  "halb": 22.50},
    "SK": {"name": "Slowakei",           "voll": 40.00,  "halb": 20.00},
    "SI": {"name": "Slowenien",          "voll": 45.00,  "halb": 22.50},
    "BG": {"name": "Bulgarien",          "voll": 35.00,  "halb": 17.50},
    "RS": {"name": "Serbien",            "voll": 35.00,  "halb": 17.50},
    "EG": {"name": "Ägypten",            "voll": 35.00,  "halb": 17.50},
    "MA": {"name": "Marokko",            "voll": 35.00,  "halb": 17.50},
    "NG": {"name": "Nigeria",            "voll": 40.00,  "halb": 20.00},
    "KE": {"name": "Kenia",              "voll": 35.00,  "halb": 17.50},
    "PH": {"name": "Philippinen",        "voll": 37.00,  "halb": 18.50},
    "VN": {"name": "Vietnam",            "voll": 35.00,  "halb": 17.50},
    "NZ": {"name": "Neuseeland",         "voll": 55.00,  "halb": 27.50},
}

# IATA → ISO-Ländercode (wichtigste Flughäfen)
IATA_TO_LAND: dict[str, str] = {
    # Deutschland
    "FRA":"DE","MUC":"DE","NUE":"DE","BER":"DE","HAM":"DE",
    "STR":"DE","DUS":"DE","CGN":"DE","LEJ":"DE","HAJ":"DE",
    # Europa
    "LYS":"FR","CDG":"FR","ORY":"FR","NCE":"FR","MRS":"FR","BOD":"FR",
    "LHR":"GB","LGW":"GB","MAN":"GB","EDI":"GB","BHX":"GB",
    "ZRH":"CH","GVA":"CH","BSL":"CH",
    "VIE":"AT","SZG":"AT","INN":"AT",
    "FCO":"IT","MXP":"IT","LIN":"IT","VCE":"IT","NAP":"IT","PMO":"IT",
    "MAD":"ES","BCN":"ES","AGP":"ES","PMI":"ES","VLC":"ES","SVQ":"ES",
    "AMS":"NL","RTM":"NL","EIN":"NL",
    "BRU":"BE","CRL":"BE",
    "LIS":"PT","OPO":"PT","FAO":"PT",
    "ATH":"GR","SKG":"GR","HER":"GR","RHO":"GR","CFU":"GR",
    "OSL":"NO","BGO":"NO","TRD":"NO",
    "ARN":"SE","GOT":"SE","MMX":"SE",
    "CPH":"DK","AAL":"DK","BLL":"DK",
    "HEL":"FI","TMP":"FI","TKU":"FI",
    "WAW":"PL","KRK":"PL","WRO":"PL","GDN":"PL","KTW":"PL",
    "PRG":"CZ","BRQ":"CZ",
    "BUD":"HU","DEB":"HU",
    "OTP":"RO","CLJ":"RO",
    "SOF":"BG",
    "ZAG":"HR","SPU":"HR","DBV":"HR",
    "BEG":"RS",
    "LJU":"SI",
    "BTS":"SK","KSC":"SK",
    "IST":"TR","SAW":"TR","AYT":"TR","ADB":"TR","ESB":"TR",
    "DUB":"IE","SNN":"IE",
    "KEF":"IS",
    # Nordamerika
    "JFK":"US","LGA":"US","EWR":"US","ORD":"US","MDW":"US",
    "LAX":"US","SFO":"US","SJC":"US","OAK":"US","SEA":"US",
    "MIA":"US","FLL":"US","MCO":"US","TPA":"US","ATL":"US",
    "DFW":"US","IAH":"US","HOU":"US","DEN":"US","PHX":"US",
    "LAS":"US","BOS":"US","IAD":"US","DCA":"US","BWI":"US",
    "YYZ":"CA","YUL":"CA","YVR":"CA","YYC":"CA","YEG":"CA",
    # Mittelamerika / Karibik
    "SJO":"CR",  # San José Costa Rica
    "PTY":"PA",  # Panama City
    "GUA":"GT","SAL":"SV","TGU":"HN","MGA":"NI",
    "CUN":"MX","MEX":"MX","GDL":"MX","MTY":"MX","TLC":"MX",
    "HAV":"CU","MBJ":"JM","NAS":"BS","PUJ":"DO","SDQ":"DO",
    # Südamerika
    "GRU":"BR","GIG":"BR","BSB":"BR","SSA":"BR","REC":"BR","FOR":"BR",
    "EZE":"AR","AEP":"AR","COR":"AR","MDZ":"AR",
    "SCL":"CL","PMC":"CL",
    "LIM":"PE","CUZ":"PE",
    "BOG":"CO","MDE":"CO","CLO":"CO","CTG":"CO",
    "UIO":"EC","GYE":"EC",
    "CCS":"VE","MAR":"VE",
    "ASU":"PY","MVD":"UY",
    # Asien
    "NRT":"JP","HND":"JP","KIX":"JP","NGO":"JP","CTS":"JP",
    "PEK":"CN","PKX":"CN","PVG":"CN","SHA":"CN","CAN":"CN",
    "HKG":"HK","MFM":"MO",
    "ICN":"KR","GMP":"KR","PUS":"KR",
    "TPE":"TW","KHH":"TW",
    "SIN":"SG",
    "KUL":"MY","PEN":"MY","BKI":"MY",
    "BKK":"TH","HKT":"TH","CNX":"TH",
    "CGK":"ID","DPS":"ID","SUB":"ID",
    "MNL":"PH","CEB":"PH",
    "SGN":"VN","HAN":"VN","DAD":"VN",
    "DEL":"IN","BOM":"IN","MAA":"IN","BLR":"IN","CCU":"IN","HYD":"IN",
    "DAC":"BD","CMB":"LK",
    "KTM":"NP","RGN":"MM",
    "DXB":"AE","AUH":"AE","SHJ":"AE","DWC":"AE",
    "DOH":"QA","BAH":"BH","KWI":"KW","MCT":"OM","RUH":"SA","JED":"SA",
    "TLV":"IL","AMM":"JO","BEY":"LB",
    "IST":"TR","ESB":"TR",
    # Afrika
    "CAI":"EG","HRG":"EG","SSH":"EG","LXR":"EG",
    "CMN":"MA","RAK":"MA","AGA":"MA","FEZ":"MA",
    "TUN":"TN","DJE":"TN",
    "ALG":"DZ",
    "NBO":"KE","MBA":"KE",
    "ADD":"ET",
    "JNB":"ZA","CPT":"ZA","DUR":"ZA",
    "LOS":"NG","ABV":"NG",
    "ACC":"GH","ABJ":"CI","DKR":"SN",
    "DAR":"TZ","ZNZ":"TZ",
    # Ozeanien
    "SYD":"AU","MEL":"AU","BNE":"AU","PER":"AU","ADL":"AU","CBR":"AU",
    "AKL":"NZ","CHC":"NZ","WLG":"NZ","ZQN":"NZ",
    "NAN":"FJ",
    # Russland / Zentralasien
    "SVO":"RU","DME":"RU","VKO":"RU","LED":"RU",
    "IEV":"UA","KBP":"UA","ODS":"UA","LWO":"UA",
    "GYD":"AZ","TBS":"GE","EVN":"AM",
    "ALA":"KZ","TSE":"KZ",
    "TAS":"UZ",
}

# Länder-Dropdown für Formular
LAENDER_LISTE = [
    ("DE","Deutschland"), ("FR","Frankreich"), ("CH","Schweiz"),
    ("AT","Österreich"), ("GB","Großbritannien"), ("IT","Italien"),
    ("ES","Spanien"), ("NL","Niederlande"), ("BE","Belgien"),
    ("PL","Polen"), ("CZ","Tschechien"), ("SE","Schweden"),
    ("NO","Norwegen"), ("DK","Dänemark"), ("FI","Finnland"),
    ("PT","Portugal"), ("GR","Griechenland"), ("TR","Türkei"),
    ("US","USA"), ("CA","Kanada"), ("JP","Japan"), ("CN","China"),
    ("SG","Singapur"), ("IN","Indien"), ("AE","VAE / Dubai"),
    ("QA","Katar"), ("AU","Australien"), ("BR","Brasilien"),
    ("MX","Mexiko"), ("AR","Argentinien"), ("ZA","Südafrika"),
    ("CR","Costa Rica"), ("PA","Panama"), ("CO","Kolumbien"),
    ("CL","Chile"), ("KR","Südkorea"), ("TH","Thailand"),
    ("ID","Indonesien"), ("MY","Malaysia"), ("HK","Hongkong"),
    ("IL","Israel"), ("HU","Ungarn"), ("RO","Rumänien"),
    ("HR","Kroatien"), ("BG","Bulgarien"), ("EG","Ägypten"),
    ("MA","Marokko"), ("NG","Nigeria"), ("KE","Kenia"),
    ("PH","Philippinen"), ("VN","Vietnam"), ("NZ","Neuseeland"),
]

# ── Datenbank Schema ────────────────────────────────────────────────────────────
def get_schema() -> list[str]:
    """
    Gibt SQL-Statements für Schema-Erstellung zurück.
    Kompatibel mit PostgreSQL und SQLite.
    """
    if is_postgres():
        return [
            """CREATE TABLE IF NOT EXISTS mitarbeiter (
                kuerzel     TEXT PRIMARY KEY,
                klarname    TEXT NOT NULL,
                aktiv       BOOLEAN DEFAULT TRUE,
                erstellt    TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS reisen (
                code        TEXT PRIMARY KEY,
                titel       TEXT NOT NULL,
                abreise     DATE NOT NULL,
                rueckkehr   DATE NOT NULL,
                notiz       TEXT,
                erstellt    TIMESTAMP DEFAULT NOW()
            )""",
            """CREATE TABLE IF NOT EXISTS reise_mitarbeiter (
                reise_code  TEXT NOT NULL,
                kuerzel     TEXT NOT NULL,
                PRIMARY KEY (reise_code, kuerzel),
                CONSTRAINT fk_rm_reise FOREIGN KEY (reise_code)
                    REFERENCES reisen(code) ON DELETE CASCADE,
                CONSTRAINT fk_rm_ma FOREIGN KEY (kuerzel)
                    REFERENCES mitarbeiter(kuerzel) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS reise_laender (
                id          SERIAL PRIMARY KEY,
                reise_code  TEXT NOT NULL,
                datum_von   DATE NOT NULL,
                datum_bis   DATE NOT NULL,
                land_code   TEXT NOT NULL,
                land_name   TEXT NOT NULL,
                vma_voll    NUMERIC(6,2),
                vma_halb    NUMERIC(6,2),
                CONSTRAINT fk_rl_reise FOREIGN KEY (reise_code)
                    REFERENCES reisen(code) ON DELETE CASCADE
            )""",
        ]
    else:
        return [
            """CREATE TABLE IF NOT EXISTS mitarbeiter (
                kuerzel     TEXT PRIMARY KEY,
                klarname    TEXT NOT NULL,
                aktiv       INTEGER DEFAULT 1,
                erstellt    TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS reisen (
                code        TEXT PRIMARY KEY,
                titel       TEXT NOT NULL,
                abreise     TEXT NOT NULL,
                rueckkehr   TEXT NOT NULL,
                notiz       TEXT,
                erstellt    TEXT DEFAULT (datetime('now'))
            )""",
            """CREATE TABLE IF NOT EXISTS reise_mitarbeiter (
                reise_code  TEXT REFERENCES reisen(code) ON DELETE CASCADE,
                kuerzel     TEXT REFERENCES mitarbeiter(kuerzel) ON DELETE CASCADE,
                PRIMARY KEY (reise_code, kuerzel)
            )""",
            """CREATE TABLE IF NOT EXISTS reise_laender (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                reise_code  TEXT REFERENCES reisen(code) ON DELETE CASCADE,
                datum_von   TEXT NOT NULL,
                datum_bis   TEXT NOT NULL,
                land_code   TEXT NOT NULL,
                land_name   TEXT NOT NULL,
                vma_voll    REAL,
                vma_halb    REAL
            )""",
        ]

# ── Hilfsfunktionen ────────────────────────────────────────────────────────────
def fmt_date(d) -> str:
    if not d: return "–"
    if isinstance(d, date): return d.strftime("%d.%m.%Y")
    s = str(d)[:10]
    try:
        return date.fromisoformat(s).strftime("%d.%m.%Y")
    except:
        return s

def next_reise_code(cur) -> str:
    """Generiert nächsten Reisecode YY-NNN."""
    year = str(date.today().year)[-2:]
    if is_postgres():
        cur.execute("SELECT code FROM reisen WHERE code LIKE %s ORDER BY code DESC LIMIT 1",
                    (f"{year}-%",))
    else:
        cur.execute("SELECT code FROM reisen WHERE code LIKE ? ORDER BY code DESC LIMIT 1",
                    (f"{year}-%",))
    row = cur.fetchone()
    if row:
        last = row[0] if isinstance(row, tuple) else row["code"]
        m = re.match(r"\d{2}-(\d{3})", last)
        num = int(m.group(1)) + 1 if m else 1
    else:
        num = 1
    return f"{year}-{num:03d}"

def vma_fuer_land(land_code: str) -> tuple[float, float]:
    """Gibt (voll, halb) VMA-Satz für Ländercode zurück."""
    s = VMA_SAETZE.get(land_code.upper(), {"voll": 28.00, "halb": 14.00})
    return s["voll"], s["halb"]

# ── CSS + HTML Shell ───────────────────────────────────────────────────────────
CSS = """
:root {
    --bg: #f8fafc; --white: #ffffff; --border: #e2e8f0;
    --text: #0f172a; --muted: #64748b; --light: #94a3b8;
    --blue: #2563eb; --blue-d: #1d4ed8; --blue-l: #eff6ff;
    --green: #059669; --green-l: #ecfdf5;
    --amber: #d97706; --amber-l: #fffbeb;
    --red: #dc2626; --red-l: #fef2f2;
    --radius: 8px; --radius-s: 6px;
    --shadow: 0 1px 3px rgba(0,0,0,.08), 0 1px 2px rgba(0,0,0,.04);
    --shadow-md: 0 4px 6px rgba(0,0,0,.07), 0 2px 4px rgba(0,0,0,.04);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
       background: var(--bg); color: var(--text); font-size: 14px; line-height: 1.5; }

/* Navigation */
nav {
    background: #1e293b; padding: 0 24px;
    display: flex; align-items: center; gap: 0;
    position: sticky; top: 0; z-index: 100;
    box-shadow: 0 2px 8px rgba(0,0,0,.2);
    height: 52px;
}
.nav-brand {
    color: #f1f5f9; font-weight: 700; font-size: 15px;
    margin-right: 24px; white-space: nowrap;
    text-decoration: none;
}
.nav-link {
    color: #94a3b8; text-decoration: none; font-size: 13px; font-weight: 500;
    padding: 16px 12px; border-bottom: 2px solid transparent;
    transition: color .15s, border-color .15s; white-space: nowrap;
}
.nav-link:hover { color: #f1f5f9; }
.nav-link.active { color: #f1f5f9; border-bottom-color: #3b82f6; }
.nav-right { margin-left: auto; font-size: 11px; color: #475569; }

/* Layout */
main { padding: 28px 24px; max-width: 1100px; margin: 0 auto; }
.page-title { font-size: 22px; font-weight: 700; color: var(--text); margin-bottom: 20px; }

/* Karten */
.card {
    background: var(--white); border: 1px solid var(--border);
    border-radius: var(--radius); box-shadow: var(--shadow);
    margin-bottom: 16px;
}
.card-header {
    padding: 14px 20px; border-bottom: 1px solid var(--border);
    display: flex; align-items: center; justify-content: space-between;
}
.card-title { font-size: 15px; font-weight: 600; }
.card-body { padding: 20px; }

/* Buttons */
.btn {
    display: inline-flex; align-items: center; gap: 6px;
    padding: 8px 16px; border-radius: var(--radius-s);
    font-size: 13px; font-weight: 600; cursor: pointer;
    text-decoration: none; border: none; transition: all .15s;
    white-space: nowrap;
}
.btn-primary { background: var(--blue); color: white; }
.btn-primary:hover { background: var(--blue-d); }
.btn-success { background: var(--green); color: white; }
.btn-success:hover { background: #047857; }
.btn-secondary {
    background: white; color: #374151;
    border: 1px solid var(--border);
}
.btn-secondary:hover { background: #f9fafb; border-color: #9ca3af; }
.btn-danger { background: var(--red); color: white; }
.btn-danger:hover { background: #b91c1c; }
.btn-sm { padding: 5px 10px; font-size: 12px; }

/* Formulare */
.form-grid { display: grid; gap: 16px; }
.form-grid-2 { grid-template-columns: 1fr 1fr; }
.form-grid-3 { grid-template-columns: 1fr 1fr 1fr; }
.form-group { display: flex; flex-direction: column; gap: 4px; }
.form-group.full { grid-column: 1 / -1; }
label { font-size: 12px; font-weight: 600; color: #374151; }
.required { color: var(--red); margin-left: 2px; }
input[type="text"], input[type="date"], input[type="email"],
input[type="number"], select, textarea {
    width: 100%; padding: 8px 12px;
    border: 1px solid var(--border); border-radius: var(--radius-s);
    font-size: 13px; background: white; color: var(--text);
    transition: border-color .15s, box-shadow .15s;
}
input:focus, select:focus, textarea:focus {
    outline: none; border-color: var(--blue);
    box-shadow: 0 0 0 3px rgba(37,99,235,.1);
}
.form-hint { font-size: 11px; color: var(--muted); margin-top: 2px; }
.form-actions {
    display: flex; gap: 8px; padding-top: 16px;
    border-top: 1px solid var(--border); margin-top: 20px;
}

/* Tabellen */
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; }
th {
    text-align: left; padding: 10px 14px;
    font-size: 11px; font-weight: 700; color: var(--muted);
    text-transform: uppercase; letter-spacing: .05em;
    border-bottom: 1px solid var(--border);
    background: #f8fafc; white-space: nowrap;
}
td {
    padding: 11px 14px; font-size: 13px;
    border-bottom: 1px solid #f1f5f9; vertical-align: middle;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: #fafafa; }
.td-mono { font-family: "SF Mono", "Fira Code", monospace; font-size: 12px; }

/* Badges */
.badge {
    display: inline-block; padding: 2px 8px; border-radius: 4px;
    font-size: 11px; font-weight: 700;
}
.badge-blue { background: var(--blue-l); color: var(--blue); }
.badge-green { background: var(--green-l); color: var(--green); }
.badge-amber { background: var(--amber-l); color: var(--amber); }
.badge-red { background: var(--red-l); color: var(--red); }
.badge-gray { background: #f1f5f9; color: var(--muted); }

/* Alerts */
.alert { padding: 12px 16px; border-radius: var(--radius); font-size: 13px; margin-bottom: 16px; }
.alert-ok { background: var(--green-l); border: 1px solid #6ee7b7; color: #065f46; }
.alert-warn { background: var(--amber-l); border: 1px solid #fcd34d; color: #92400e; }
.alert-err { background: var(--red-l); border: 1px solid #fca5a5; color: #991b1b; }

/* Leerer Zustand */
.empty-state {
    text-align: center; padding: 48px 20px; color: var(--light);
}
.empty-state p { margin-top: 8px; font-size: 13px; }

/* VMA-Tabelle Farben */
.vma-row-de { background: #f0fdf4; }
.vma-row-eu { background: #eff6ff; }
.vma-row-int { background: #fafafa; }

@media (max-width: 640px) {
    .form-grid-2, .form-grid-3 { grid-template-columns: 1fr; }
    main { padding: 16px; }
}
"""

APP_VERSION = "2.0-e"

def shell(title: str, content: str, page: str = "") -> str:
    def nav(p, label, url):
        cls = "nav-link active" if page == p else "nav-link"
        return f'<a href="{url}" class="{cls}">{label}</a>'
    return f"""<!DOCTYPE html>
<html lang="de">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title} – Herrhammer Reisekosten</title>
<style>{CSS}</style>
</head>
<body>
<nav>
  <a href="/" class="nav-brand">✈ Reisekosten</a>
  {nav("start", "Dashboard", "/")}
  {nav("mitarbeiter", "Mitarbeiter", "/mitarbeiter")}
  {nav("reisen", "Reisen", "/reisen")}
  {nav("vma", "VMA-Sätze", "/vma")}
  <div class="nav-right">v{APP_VERSION}</div>
</nav>
<main>
{content}
</main>
</body>
</html>"""

# ── FastAPI App ────────────────────────────────────────────────────────────────
app = FastAPI(title="Herrhammer Reisekosten", version=APP_VERSION)

if not os.path.exists("static"):
    os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── System-Routen ──────────────────────────────────────────────────────────────
@app.get("/test-openai")
async def test_openai():
    """Testet die OpenAI API-Verbindung."""
    import httpx, os
    key = os.getenv("OPENAI_API_KEY", "")
    if not key:
        return {"status": "fehler", "detail": "OPENAI_API_KEY nicht gesetzt"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                "https://api.openai.com/v1/chat/completions",
                headers={"Authorization": f"Bearer {key}",
                         "Content-Type": "application/json"},
                json={"model": "gpt-4o-mini",
                      "messages": [{"role": "user",
                                    "content": "Antworte nur mit: OK"}],
                      "max_tokens": 5})
            if resp.status_code == 200:
                antwort = resp.json()["choices"][0]["message"]["content"]
                return {"status": "ok", "antwort": antwort,
                        "modell": "gpt-4o-mini"}
            else:
                return {"status": "fehler", "http": resp.status_code,
                        "detail": resp.text[:200]}
    except Exception as e:
        import traceback
        return {"status": "fehler", "detail": str(e),
                "trace": traceback.format_exc()[:500]}


@app.get("/init")
def init():
    """Legt Tabellen an. Bestehende Tabellen werden NICHT gelöscht."""
    try:
        db = get_db(); cur = db.cursor()
        for sql in get_schema():
            cur.execute(sql)
        db.commit(); cur.close(); db.close()
        return {"status": "ok", "version": APP_VERSION,
                "db": "postgresql" if is_postgres() else "sqlite"}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}

@app.get("/init-reset")
def init_reset(confirm: str = ""):
    """
    Löscht ALLE Tabellen und legt sie neu an.
    Nur aufrufen mit ?confirm=ja
    """
    if confirm != "ja":
        return {"status": "warten",
                "hinweis": "Aufruf mit ?confirm=ja um alle Daten zu löschen und neu anzulegen"}
    try:
        db = get_db(); cur = db.cursor()
        # Tabellen in richtiger Reihenfolge löschen (Foreign Keys beachten)
        for tbl in ["reise_laender", "reise_mitarbeiter", "reisen", "mitarbeiter"]:
            try:
                cur.execute(f"DROP TABLE IF EXISTS {tbl} CASCADE")
            except: pass
        db.commit()
        # Neu anlegen
        for sql in get_schema():
            cur.execute(sql)
        db.commit(); cur.close(); db.close()
        return {"status": "ok", "aktion": "reset+init", "version": APP_VERSION,
                "db": "postgresql" if is_postgres() else "sqlite"}
    except Exception as e:
        return {"status": "fehler", "detail": str(e)}

@app.get("/version")
def version():
    return {"version": APP_VERSION,
            "db": "postgresql" if is_postgres() else "sqlite"}

# ── Dashboard ──────────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def dashboard():
    try:
        db = get_db(); cur = db.cursor()
        P = ph()

        cur.execute("SELECT COUNT(*) FROM mitarbeiter WHERE aktiv = TRUE" if is_postgres()
                    else "SELECT COUNT(*) FROM mitarbeiter WHERE aktiv = 1")
        ma_count = cur.fetchone()[0]

        cur.execute("SELECT COUNT(*) FROM reisen")
        r_count = cur.fetchone()[0]

        today = date.today()
        if is_postgres():
            cur.execute("SELECT COUNT(*) FROM reisen WHERE abreise <= %s AND rueckkehr >= %s",
                        (today, today))
        else:
            cur.execute("SELECT COUNT(*) FROM reisen WHERE abreise <= ? AND rueckkehr >= ?",
                        (str(today), str(today)))
        aktiv_count = cur.fetchone()[0]

        # Aktuelle und kommende Reisen
        if is_postgres():
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                           STRING_AGG(rm.kuerzel, ', ' ORDER BY rm.kuerzel) as ma
                           FROM reisen r
                           LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                           WHERE r.rueckkehr >= %s
                           GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                           ORDER BY r.abreise
                           LIMIT 10""", (today,))
        else:
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                           GROUP_CONCAT(rm.kuerzel, ', ') as ma
                           FROM reisen r
                           LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                           WHERE r.rueckkehr >= ?
                           GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                           ORDER BY r.abreise
                           LIMIT 10""", (str(today),))
        rows = cur.fetchall()
        cur.close(); db.close()

        def status_badge(ab, zu):
            if isinstance(ab, str): ab = date.fromisoformat(ab)
            if isinstance(zu, str): zu = date.fromisoformat(zu)
            if today < ab:
                tage = (ab - today).days
                return f'<span class="badge badge-blue">In {tage} Tag{"en" if tage!=1 else ""}</span>'
            elif today <= zu:
                return '<span class="badge badge-green">● Aktiv</span>'
            else:
                return '<span class="badge badge-gray">Fertig</span>'

        reise_rows = ""
        for r in rows:
            code, titel, ab, zu, ma = (r if isinstance(r, tuple)
                                        else (r["code"],r["titel"],r["abreise"],r["rueckkehr"],r["ma"]))
            reise_rows += f"""<tr>
                <td><a href="/reise/{code}" class="td-mono" style="color:var(--blue)">{code}</a></td>
                <td style="font-weight:500"><a href="/reise/{code}" style="color:inherit;text-decoration:none">{titel}</a></td>
                <td>{fmt_date(ab)}</td>
                <td>{fmt_date(zu)}</td>
                <td style="color:var(--muted)">{ma or "–"}</td>
                <td>{status_badge(ab, zu)}</td>
            </tr>"""

        content = f"""
        <h1 class="page-title">Dashboard</h1>

        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:24px">
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:36px;font-weight:700;color:var(--blue)">{ma_count}</div>
            <div style="color:var(--muted);font-size:12px;margin-top:4px">Aktive Mitarbeiter</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:36px;font-weight:700;color:var(--green)">{aktiv_count}</div>
            <div style="color:var(--muted);font-size:12px;margin-top:4px">Laufende Reisen</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:36px;font-weight:700;color:var(--text)">{r_count}</div>
            <div style="color:var(--muted);font-size:12px;margin-top:4px">Reisen gesamt</div>
          </div></div>
        </div>

        <div class="card">
          <div class="card-header">
            <span class="card-title">Aktuelle & kommende Reisen</span>
            <a href="/reisen/neu" class="btn btn-primary btn-sm">+ Neue Reise</a>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr>
                <th>Code</th><th>Titel</th><th>Abreise</th>
                <th>Rückkehr</th><th>Mitarbeiter</th><th>Status</th>
              </tr></thead>
              <tbody>
                {reise_rows or '<tr><td colspan="6"><div class="empty-state">Keine Reisen – <a href="/reisen/neu">Erste Reise anlegen</a></div></td></tr>'}
              </tbody>
            </table>
          </div>
        </div>"""
        return HTMLResponse(shell("Dashboard", content, "start"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler", f"""
        <div class="alert alert-warn">
            <b>Datenbank nicht initialisiert?</b><br>
            Bitte <a href="/init">/init aufrufen</a> um Tabellen anzulegen.<br>
            Fehler: {e}
        </div>
        <pre style="font-size:11px;color:var(--muted)">{traceback.format_exc()[:500]}</pre>
        """))

# ── Mitarbeiter ────────────────────────────────────────────────────────────────
@app.get("/mitarbeiter", response_class=HTMLResponse)
def mitarbeiter_liste():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT m.kuerzel, m.klarname, m.aktiv,
                       COUNT(rm.reise_code) as reise_count
                       FROM mitarbeiter m
                       LEFT JOIN reise_mitarbeiter rm ON rm.kuerzel = m.kuerzel
                       GROUP BY m.kuerzel, m.klarname, m.aktiv
                       ORDER BY m.klarname""")
        rows = cur.fetchall()
        cur.close(); db.close()

        def get(r, key, idx):
            return r[key] if hasattr(r, 'keys') else r[idx]

        zeilen = ""
        for r in rows:
            kuerzel = get(r,"kuerzel",0)
            klarname = get(r,"klarname",1)
            aktiv = get(r,"aktiv",2)
            rcnt = get(r,"reise_count",3)
            badge = ('<span class="badge badge-green">Aktiv</span>' if aktiv
                     else '<span class="badge badge-gray">Inaktiv</span>')
            zeilen += f"""<tr>
                <td class="td-mono" style="font-weight:700">{kuerzel}</td>
                <td style="font-weight:500">{klarname}</td>
                <td>{badge}</td>
                <td style="color:var(--muted)">{rcnt}</td>
                <td>
                  <a href="/mitarbeiter/{kuerzel}/bearbeiten"
                     class="btn btn-secondary btn-sm">✏ Bearbeiten</a>
                </td>
            </tr>"""

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
          <h1 class="page-title" style="margin:0">Mitarbeiter</h1>
          <a href="/mitarbeiter/neu" class="btn btn-primary">+ Neu anlegen</a>
        </div>
        <div class="card">
          <div class="table-wrap">
            <table>
              <thead><tr>
                <th>Kürzel</th><th>Name</th><th>Status</th><th>Reisen</th><th></th>
              </tr></thead>
              <tbody>
                {zeilen or '<tr><td colspan="5"><div class="empty-state">Noch keine Mitarbeiter – <a href="/mitarbeiter/neu">Jetzt anlegen</a></div></td></tr>'}
              </tbody>
            </table>
          </div>
        </div>"""
        return HTMLResponse(shell("Mitarbeiter", content, "mitarbeiter"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/mitarbeiter/neu", response_class=HTMLResponse)
def mitarbeiter_neu_form():
    content = """
    <h1 class="page-title">Mitarbeiter anlegen</h1>
    <div class="card" style="max-width:480px">
      <div class="card-body">
        <form method="post" action="/mitarbeiter/neu">
          <div class="form-grid">
            <div class="form-group">
              <label>Kürzel <span class="required">*</span></label>
              <input type="text" name="kuerzel" maxlength="5" required
                     placeholder="z.B. RD" style="text-transform:uppercase"
                     autofocus>
              <div class="form-hint">2–5 Buchstaben, eindeutig pro Mitarbeiter</div>
            </div>
            <div class="form-group">
              <label>Klarname <span class="required">*</span></label>
              <input type="text" name="klarname" required
                     placeholder="z.B. Ralf Diesslin">
            </div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary">Anlegen</button>
            <a href="/mitarbeiter" class="btn btn-secondary">Abbrechen</a>
          </div>
        </form>
      </div>
    </div>"""
    return HTMLResponse(shell("Mitarbeiter anlegen", content, "mitarbeiter"))

@app.post("/mitarbeiter/neu")
async def mitarbeiter_neu(request: Request):
    form = await request.form()
    kuerzel = (form.get("kuerzel") or "").strip().upper()
    klarname = (form.get("klarname") or "").strip()
    if not kuerzel or not klarname:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Kürzel und Name sind Pflichtfelder.</div>'
            '<a href="/mitarbeiter/neu" class="btn btn-secondary">Zurück</a>'))
    if not re.match(r'^[A-Z]{1,5}$', kuerzel):
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Kürzel: nur Buchstaben, 1–5 Zeichen.</div>'
            '<a href="/mitarbeiter/neu" class="btn btn-secondary">Zurück</a>'))
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"INSERT INTO mitarbeiter (kuerzel, klarname) VALUES ({P},{P})",
                    (kuerzel, klarname))
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/mitarbeiter", status_code=303)
    except Exception as e:
        err = str(e)
        if "unique" in err.lower() or "duplicate" in err.lower():
            msg = f'Kürzel "{kuerzel}" existiert bereits.'
        else:
            msg = err
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{msg}</div>'
            '<a href="/mitarbeiter/neu" class="btn btn-secondary">Zurück</a>'))

@app.get("/mitarbeiter/{kuerzel}/bearbeiten", response_class=HTMLResponse)
def mitarbeiter_bearbeiten_form(kuerzel: str):
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"SELECT kuerzel, klarname, aktiv FROM mitarbeiter WHERE kuerzel={P}",
                    (kuerzel.upper(),))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler",
                '<div class="alert alert-err">Mitarbeiter nicht gefunden.</div>'))
        k = r[0] if isinstance(r, tuple) else r["kuerzel"]
        n = r[1] if isinstance(r, tuple) else r["klarname"]
        a = r[2] if isinstance(r, tuple) else r["aktiv"]
        aktiv_check = "checked" if a else ""
        content = f"""
        <h1 class="page-title">Mitarbeiter bearbeiten</h1>
        <div class="card" style="max-width:480px">
          <div class="card-body">
            <form method="post" action="/mitarbeiter/{k}/bearbeiten">
              <div class="form-grid">
                <div class="form-group">
                  <label>Kürzel</label>
                  <input type="text" value="{k}" disabled
                         style="background:#f8fafc;color:var(--muted)">
                </div>
                <div class="form-group">
                  <label>Klarname <span class="required">*</span></label>
                  <input type="text" name="klarname" value="{n}" required autofocus>
                </div>
                <div class="form-group full">
                  <label style="display:flex;align-items:center;gap:8px;cursor:pointer">
                    <input type="checkbox" name="aktiv" value="1" {aktiv_check}
                           style="width:auto;margin:0">
                    Mitarbeiter aktiv
                  </label>
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/mitarbeiter" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell(f"MA {k} bearbeiten", content, "mitarbeiter"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/mitarbeiter/{kuerzel}/bearbeiten")
async def mitarbeiter_bearbeiten(kuerzel: str, request: Request):
    form = await request.form()
    klarname = (form.get("klarname") or "").strip()
    aktiv = bool(form.get("aktiv"))
    if not klarname:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Name darf nicht leer sein.</div>'))
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        aktiv_val = True if is_postgres() else 1
        inaktiv_val = False if is_postgres() else 0
        cur.execute(f"UPDATE mitarbeiter SET klarname={P}, aktiv={P} WHERE kuerzel={P}",
                    (klarname, aktiv_val if aktiv else inaktiv_val, kuerzel.upper()))
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/mitarbeiter", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

# ── Reisen ─────────────────────────────────────────────────────────────────────
@app.get("/reisen", response_class=HTMLResponse)
def reisen_liste():
    try:
        db = get_db(); cur = db.cursor()
        today = date.today()
        if is_postgres():
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                           STRING_AGG(rm.kuerzel, ', ' ORDER BY rm.kuerzel) as ma,
                           COUNT(DISTINCT rl.id) as laender_count
                           FROM reisen r
                           LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                           LEFT JOIN reise_laender rl ON rl.reise_code = r.code
                           GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                           ORDER BY r.abreise DESC""")
        else:
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                           GROUP_CONCAT(rm.kuerzel, ', ') as ma,
                           COUNT(DISTINCT rl.id) as laender_count
                           FROM reisen r
                           LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                           LEFT JOIN reise_laender rl ON rl.reise_code = r.code
                           GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                           ORDER BY r.abreise DESC""")
        rows = cur.fetchall()
        cur.close(); db.close()

        def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

        def status(ab, zu):
            if isinstance(ab, str): ab = date.fromisoformat(ab)
            if isinstance(zu, str): zu = date.fromisoformat(zu)
            if today < ab: return f'<span class="badge badge-blue">Geplant</span>'
            elif today <= zu: return '<span class="badge badge-green">● Aktiv</span>'
            else: return '<span class="badge badge-gray">Abgeschlossen</span>'

        zeilen = ""
        for r in rows:
            code = get(r,"code",0); titel = get(r,"titel",1)
            ab = get(r,"abreise",2); zu = get(r,"rueckkehr",3)
            ma = get(r,"ma",4); lc = get(r,"laender_count",5)
            vma_ok = "✓" if lc and lc > 0 else '<span style="color:var(--amber)">–</span>'
            zeilen += f"""<tr>
                <td class="td-mono" style="font-weight:700">
                  <a href="/reise/{code}" style="color:var(--blue)">{code}</a></td>
                <td style="font-weight:500">
                  <a href="/reise/{code}" style="color:inherit;text-decoration:none">{titel}</a></td>
                <td>{fmt_date(ab)}</td><td>{fmt_date(zu)}</td>
                <td style="color:var(--muted)">{ma or "–"}</td>
                <td style="text-align:center">{vma_ok}</td>
                <td>{status(ab,zu)}</td>
                <td>
                  <a href="/reise/{code}" class="btn btn-secondary btn-sm">Detail</a>
                </td>
            </tr>"""

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
          <h1 class="page-title" style="margin:0">Reisen</h1>
          <a href="/reisen/neu" class="btn btn-primary">+ Neue Reise</a>
        </div>
        <div class="card">
          <div class="table-wrap"><table>
            <thead><tr>
              <th>Code</th><th>Titel</th><th>Abreise</th><th>Rückkehr</th>
              <th>Mitarbeiter</th><th>VMA</th><th>Status</th><th></th>
            </tr></thead>
            <tbody>
              {zeilen or '<tr><td colspan="8"><div class="empty-state">Keine Reisen – <a href="/reisen/neu">Erste Reise anlegen</a></div></td></tr>'}
            </tbody>
          </table></div>
        </div>"""
        return HTMLResponse(shell("Reisen", content, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reisen/neu", response_class=HTMLResponse)
def reise_neu_form():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv = TRUE"
                    if is_postgres()
                    else "SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv = 1"
                    " ORDER BY klarname")
        ma_rows = cur.fetchall()
        cur.close(); db.close()
    except: ma_rows = []

    def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

    ma_opts = "".join(
        f'<option value="{get(r,"kuerzel",0)}">'
        f'{get(r,"kuerzel",0)} – {get(r,"klarname",1)}</option>'
        for r in ma_rows)

    land_opts = "".join(
        f'<option value="{code}">{name} ({code})</option>'
        for code, name in LAENDER_LISTE)

    # Vorschau-Code
    try:
        db = get_db(); cur = db.cursor()
        code_vorschau = next_reise_code(cur)
        cur.close(); db.close()
    except: code_vorschau = "–"

    content = f"""
    <h1 class="page-title">Neue Reise anlegen</h1>
    <div class="card" style="max-width:800px">
      <div class="card-body">
        <form method="post" action="/reisen/neu">

          <div style="background:var(--blue-l);border:1px solid #bfdbfe;border-radius:var(--radius);
                      padding:12px 16px;margin-bottom:20px;display:flex;align-items:center;gap:12px">
            <span style="font-size:22px;font-family:monospace;font-weight:700;color:var(--blue)">{code_vorschau}</span>
            <span style="font-size:12px;color:#3b82f6">Reisecode (wird automatisch vergeben)</span>
          </div>

          <div class="form-grid form-grid-2">
            <div class="form-group full">
              <label>Titel / Beschreibung <span class="required">*</span></label>
              <input type="text" name="titel" required autofocus
                     placeholder="z.B. ECMA Lyon oder Costa Rica Kundenbesuch">
            </div>
            <div class="form-group">
              <label>Abreise <span class="required">*</span></label>
              <input type="date" name="abreise" required
                     onchange="updateRueckkehr(this.value)">
            </div>
            <div class="form-group">
              <label>Rückkehr <span class="required">*</span></label>
              <input type="date" name="rueckkehr" required id="inp-rueckkehr">
            </div>
            <div class="form-group full">
              <label>Mitarbeiter <span class="required">*</span></label>
              <select name="mitarbeiter" multiple required size="4"
                      style="height:auto">
                {ma_opts or '<option disabled>Erst Mitarbeiter anlegen</option>'}
              </select>
              <div class="form-hint">Mehrfachauswahl: Strg+Klick (Windows) oder Cmd+Klick (Mac)</div>
            </div>
            <div class="form-group full">
              <label>Notiz (optional)</label>
              <textarea name="notiz" rows="2"
                        placeholder="z.B. Kundenprojekt, Messe, internes Meeting"></textarea>
            </div>
          </div>

          <hr style="border:none;border-top:1px solid var(--border);margin:24px 0">

          <h2 style="font-size:15px;font-weight:600;margin-bottom:16px">
            🌍 Länder & VMA-Sätze
          </h2>
          <div class="alert alert-warn" style="margin-bottom:16px">
            Die Länder-Timeline wird für die automatische VMA-Berechnung genutzt.
            Trage alle Länder mit den jeweiligen Aufenthalts-Zeiträumen ein.
          </div>

          <div id="laender-container">
            <div class="laender-zeile" style="display:grid;grid-template-columns:1fr 1fr 1fr auto;
                 gap:8px;margin-bottom:8px;align-items:end">
              <div class="form-group" style="margin:0">
                <label>Land</label>
                <select name="land_code[]" onchange="updateVMA(this)">
                  {land_opts}
                </select>
              </div>
              <div class="form-group" style="margin:0">
                <label>Von (Datum)</label>
                <input type="date" name="land_von[]">
              </div>
              <div class="form-group" style="margin:0">
                <label>Bis (Datum)</label>
                <input type="date" name="land_bis[]">
              </div>
              <div style="padding-bottom:1px">
                <button type="button" onclick="removeLand(this)"
                        class="btn btn-secondary btn-sm">✕</button>
              </div>
            </div>
          </div>

          <button type="button" onclick="addLand()" class="btn btn-secondary btn-sm"
                  style="margin-bottom:20px">+ Land hinzufügen</button>

          <div class="form-actions">
            <button type="submit" class="btn btn-primary">Reise anlegen</button>
            <a href="/reisen" class="btn btn-secondary">Abbrechen</a>
          </div>

        </form>
      </div>
    </div>

    <script>
    const VMA = {json.dumps({k: v for k, v in VMA_SAETZE.items()})};
    const LAND_OPTS = `{land_opts}`;

    function updateRueckkehr(v) {{
        if (!v) return;
        const r = document.getElementById('inp-rueckkehr');
        if (r && !r.value) {{
            const d = new Date(v);
            d.setDate(d.getDate() + 3);
            r.value = d.toISOString().split('T')[0];
        }}
    }}

    function updateVMA(sel) {{
        const code = sel.value;
        const info = VMA[code];
        if (info) {{
            const row = sel.closest('.laender-zeile');
            let hint = row.querySelector('.vma-hint');
            if (!hint) {{
                hint = document.createElement('div');
                hint.className = 'vma-hint';
                hint.style.cssText = 'grid-column:1/-1;font-size:11px;color:#059669;margin-top:-4px;margin-bottom:4px';
                row.after(hint);
            }}
            hint.textContent = info.name + ': ' + info.voll + ' EUR/Tag (voll) · ' + info.halb + ' EUR/Tag (halber Satz)';
        }}
    }}

    function addLand() {{
        const container = document.getElementById('laender-container');
        const div = document.createElement('div');
        div.className = 'laender-zeile';
        div.style.cssText = 'display:grid;grid-template-columns:1fr 1fr 1fr auto;gap:8px;margin-bottom:8px;align-items:end';
        div.innerHTML = `
          <div class="form-group" style="margin:0">
            <label>Land</label>
            <select name="land_code[]" onchange="updateVMA(this)">${{LAND_OPTS}}</select>
          </div>
          <div class="form-group" style="margin:0">
            <label>Von (Datum)</label>
            <input type="date" name="land_von[]">
          </div>
          <div class="form-group" style="margin:0">
            <label>Bis (Datum)</label>
            <input type="date" name="land_bis[]">
          </div>
          <div style="padding-bottom:1px">
            <button type="button" onclick="removeLand(this)" class="btn btn-secondary btn-sm">✕</button>
          </div>`;
        container.appendChild(div);
    }}

    function removeLand(btn) {{
        const row = btn.closest('.laender-zeile');
        const hint = row.nextElementSibling;
        if (hint && hint.classList.contains('vma-hint')) hint.remove();
        row.remove();
    }}

    // Erste Zeile: VMA-Info anzeigen
    document.querySelectorAll('select[name="land_code[]"]').forEach(updateVMA);
    </script>
    """
    return HTMLResponse(shell("Neue Reise", content, "reisen"))

@app.post("/reisen/neu")
async def reise_neu(request: Request):
    form = await request.form()
    titel = (form.get("titel") or "").strip()
    abreise = (form.get("abreise") or "").strip()
    rueckkehr = (form.get("rueckkehr") or "").strip()
    notiz = (form.get("notiz") or "").strip()
    mitarbeiter = form.getlist("mitarbeiter")
    land_codes = form.getlist("land_code[]")
    land_vons = form.getlist("land_von[]")
    land_bis_list = form.getlist("land_bis[]")

    if not all([titel, abreise, rueckkehr, mitarbeiter]):
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Titel, Zeitraum und mindestens ein Mitarbeiter sind Pflicht.</div>'
            '<a href="/reisen/neu" class="btn btn-secondary">Zurück</a>'))
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        code = next_reise_code(cur)

        cur.execute(
            f"INSERT INTO reisen (code,titel,abreise,rueckkehr,notiz) VALUES ({P},{P},{P},{P},{P})",
            (code, titel, abreise, rueckkehr, notiz or None))

        for ma in mitarbeiter:
            cur.execute(f"INSERT INTO reise_mitarbeiter (reise_code,kuerzel) VALUES ({P},{P})",
                        (code, ma))

        # Länder
        for i, lcode in enumerate(land_codes):
            if not lcode: continue
            lvon = land_vons[i] if i < len(land_vons) else ""
            lbis = land_bis_list[i] if i < len(land_bis_list) else ""
            if not lvon or not lbis: continue
            lname = VMA_SAETZE.get(lcode, {}).get("name", lcode)
            vvoll, vhalb = vma_fuer_land(lcode)
            cur.execute(
                f"INSERT INTO reise_laender (reise_code,datum_von,datum_bis,land_code,land_name,vma_voll,vma_halb) "
                f"VALUES ({P},{P},{P},{P},{P},{P},{P})",
                (code, lvon, lbis, lcode, lname, vvoll, vhalb))

        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{code}", status_code=303)
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:400]}</pre>'
            '<a href="/reisen/neu" class="btn btn-secondary">Zurück</a>'))

# ── Reise Detail ───────────────────────────────────────────────────────────────
@app.get("/reise/{code}", response_class=HTMLResponse)
def reise_detail(code: str):
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"SELECT code,titel,abreise,rueckkehr,notiz FROM reisen WHERE code={P}",
                    (code.upper(),))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Nicht gefunden",
                '<div class="alert alert-err">Reise nicht gefunden.</div>'))

        def get(row, k, i): return row[k] if hasattr(row,'keys') else row[i]
        rcode = get(r,"code",0); titel = get(r,"titel",1)
        ab = get(r,"abreise",2); zu = get(r,"rueckkehr",3); notiz = get(r,"notiz",4)

        # Mitarbeiter
        cur.execute(f"""SELECT m.kuerzel, m.klarname FROM mitarbeiter m
                        JOIN reise_mitarbeiter rm ON rm.kuerzel = m.kuerzel
                        WHERE rm.reise_code = {P} ORDER BY m.klarname""", (rcode,))
        ma_rows = cur.fetchall()

        # Länder / VMA
        cur.execute(f"""SELECT id, datum_von, datum_bis, land_code, land_name,
                        vma_voll, vma_halb FROM reise_laender
                        WHERE reise_code = {P} ORDER BY datum_von""", (rcode,))
        land_rows = cur.fetchall()
        cur.close(); db.close()

        today = date.today()
        ab_d = date.fromisoformat(str(ab)[:10]) if ab else None
        zu_d = date.fromisoformat(str(zu)[:10]) if zu else None

        if not ab_d: status_html = '<span class="badge badge-gray">Kein Datum</span>'
        elif today < ab_d:
            tage = (ab_d - today).days
            status_html = f'<span class="badge badge-blue">In {tage} Tag{"en" if tage!=1 else ""}</span>'
        elif zu_d and today <= zu_d:
            status_html = '<span class="badge badge-green">● Aktiv</span>'
        else:
            status_html = '<span class="badge badge-gray">Abgeschlossen</span>'

        # VMA-Berechnung Übersicht
        vma_total = 0.0
        vma_zeilen = ""
        if land_rows:
            for lr in land_rows:
                lid = get(lr,"id",0)
                lvon = get(lr,"datum_von",1)
                lbis = get(lr,"datum_bis",2)
                lcode_l = get(lr,"land_code",3)
                lname_l = get(lr,"land_name",4)
                vvoll = get(lr,"vma_voll",5) or 0
                vhalb = get(lr,"vma_halb",6) or 0

                # Tage berechnen
                try:
                    # Datum aus PostgreSQL (date-Objekt) oder String
                    def to_date(v):
                        if isinstance(v, date): return v
                        return date.fromisoformat(str(v)[:10])
                    d_von = to_date(lvon)
                    d_bis = to_date(lbis)
                    tage = (d_bis - d_von).days + 1
                    # Steuerrecht: Erster + letzter Tag = halber Satz
                    # Bei 1 Tag (Hin- und Rückreise selber Tag) = halber Satz
                    if tage <= 0:
                        betrag = 0.0
                    elif tage == 1:
                        betrag = float(vhalb)
                    elif tage == 2:
                        betrag = float(vhalb) * 2
                    else:
                        betrag = float(vhalb) + (float(vvoll) * (tage - 2)) + float(vhalb)
                    vma_total += betrag
                    tage_txt = f"{tage} Tag{'e' if tage!=1 else ''}"
                    betrag_txt = f"{betrag:.2f} EUR"
                except Exception as ve:
                    tage_txt = f"Fehler: {ve}"; betrag_txt = "–"

                vma_zeilen += f"""<tr>
                    <td><span class="badge badge-blue">{lcode_l}</span> {lname_l}</td>
                    <td>{fmt_date(lvon)}</td><td>{fmt_date(lbis)}</td>
                    <td style="text-align:right">{vvoll:.2f} EUR</td>
                    <td style="text-align:right">{vhalb:.2f} EUR</td>
                    <td>{tage_txt}</td>
                    <td style="font-weight:600;text-align:right">{betrag_txt}</td>
                    <td>
                      <a href="/reise/{rcode}/land/{lid}/bearbeiten"
                         class="btn btn-secondary btn-sm">✏</a>
                    </td>
                </tr>"""

        ma_html = " ".join(
            f'<span class="badge badge-green">{get(m,"kuerzel",0)} – {get(m,"klarname",1)}</span>'
            for m in ma_rows) or "–"

        content = f"""
        <div style="display:flex;align-items:flex-start;gap:16px;margin-bottom:20px;flex-wrap:wrap">
          <div style="flex:1">
            <div style="font-family:monospace;font-size:13px;color:var(--muted);margin-bottom:4px">{rcode}</div>
            <h1 class="page-title" style="margin:0">{titel}</h1>
            <div style="margin-top:8px;display:flex;gap:8px;align-items:center;flex-wrap:wrap">
              {status_html}
              <span style="color:var(--muted);font-size:13px">
                📅 {fmt_date(ab)} – {fmt_date(zu)}
              </span>
              <span style="color:var(--muted);font-size:13px">👤 {ma_html}</span>
            </div>
            {f'<div style="margin-top:8px;font-size:13px;color:var(--muted)">{notiz}</div>' if notiz else ''}
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <a href="/reise/{rcode}/bearbeiten" class="btn btn-secondary">✏ Bearbeiten</a>
          </div>
        </div>

        <div class="card">
          <div class="card-header">
            <span class="card-title">🌍 Länder & VMA-Sätze</span>
            <a href="/reise/{rcode}/land/neu" class="btn btn-secondary btn-sm">+ Land hinzufügen</a>
          </div>
          {'<div class="table-wrap"><table><thead><tr><th>Land</th><th>Von</th><th>Bis</th><th style="text-align:right">VMA Voll</th><th style="text-align:right">VMA Halb</th><th>Tage</th><th style="text-align:right">Gesamt</th><th></th></tr></thead><tbody>' + vma_zeilen + f'</tbody><tfoot><tr><td colspan="6" style="text-align:right;font-weight:600;padding:10px 14px;border-top:2px solid var(--border)">VMA Gesamt:</td><td style="font-weight:700;font-size:15px;color:var(--green);text-align:right;padding:10px 14px;border-top:2px solid var(--border)">{vma_total:.2f} EUR</td><td style="border-top:2px solid var(--border)"></td></tr></tfoot></table></div>' if land_rows else '<div class="card-body"><div class="empty-state"><b>Noch keine Länder hinterlegt</b><p>Füge Länder hinzu für die automatische VMA-Berechnung</p><a href="/reise/{rcode}/land/neu" class="btn btn-primary" style="margin-top:12px">+ Land hinzufügen</a></div></div>'}
        </div>

        <div style="margin-top:12px">
          <a href="/reisen" class="btn btn-secondary">← Zurück</a>
        </div>"""
        return HTMLResponse(shell(f"Reise {rcode}", content, "reisen"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:400]}</pre>'))

@app.get("/reise/{code}/bearbeiten", response_class=HTMLResponse)
def reise_bearbeiten_form(code: str):
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"SELECT code,titel,abreise,rueckkehr,notiz FROM reisen WHERE code={P}",
                    (code.upper(),))
        r = cur.fetchone()
        if not r:
            return HTMLResponse(shell("Fehler",'<div class="alert alert-err">Nicht gefunden.</div>'))
        def get(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        rcode = get(r,"code",0); titel = get(r,"titel",1)
        ab = get(r,"abreise",2); zu = get(r,"rueckkehr",3); notiz = get(r,"notiz",4)

        cur.execute("SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv = TRUE"
                    if is_postgres()
                    else "SELECT kuerzel, klarname FROM mitarbeiter WHERE aktiv = 1"
                    " ORDER BY klarname")
        all_ma = cur.fetchall()
        cur.execute(f"SELECT kuerzel FROM reise_mitarbeiter WHERE reise_code={P}", (rcode,))
        assigned = {get(x,"kuerzel",0) for x in cur.fetchall()}
        cur.close(); db.close()

        ma_opts = "".join(
            f'<option value="{get(m,"kuerzel",0)}"'
            f'{" selected" if get(m,"kuerzel",0) in assigned else ""}>'
            f'{get(m,"kuerzel",0)} – {get(m,"klarname",1)}</option>'
            for m in all_ma)

        ab_s = str(ab)[:10] if ab else ""; zu_s = str(zu)[:10] if zu else ""
        content = f"""
        <h1 class="page-title">Reise {rcode} bearbeiten</h1>
        <div class="card" style="max-width:600px">
          <div class="card-body">
            <form method="post" action="/reise/{rcode}/bearbeiten">
              <div class="form-grid form-grid-2">
                <div class="form-group full">
                  <label>Titel <span class="required">*</span></label>
                  <input type="text" name="titel" value="{titel}" required>
                </div>
                <div class="form-group">
                  <label>Abreise <span class="required">*</span></label>
                  <input type="date" name="abreise" value="{ab_s}" required>
                </div>
                <div class="form-group">
                  <label>Rückkehr <span class="required">*</span></label>
                  <input type="date" name="rueckkehr" value="{zu_s}" required>
                </div>
                <div class="form-group full">
                  <label>Mitarbeiter</label>
                  <select name="mitarbeiter" multiple size="4">{ma_opts}</select>
                  <div class="form-hint">Strg+Klick für Mehrfachauswahl</div>
                </div>
                <div class="form-group full">
                  <label>Notiz</label>
                  <textarea name="notiz" rows="2">{notiz or ''}</textarea>
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell(f"Reise {rcode} bearbeiten", content, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/reise/{code}/bearbeiten")
async def reise_bearbeiten(code: str, request: Request):
    form = await request.form()
    titel = (form.get("titel") or "").strip()
    abreise = (form.get("abreise") or "").strip()
    rueckkehr = (form.get("rueckkehr") or "").strip()
    notiz = (form.get("notiz") or "").strip()
    mitarbeiter = form.getlist("mitarbeiter")
    rcode = code.upper()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(
            f"UPDATE reisen SET titel={P},abreise={P},rueckkehr={P},notiz={P} WHERE code={P}",
            (titel, abreise, rueckkehr, notiz or None, rcode))
        cur.execute(f"DELETE FROM reise_mitarbeiter WHERE reise_code={P}", (rcode,))
        for ma in mitarbeiter:
            cur.execute(f"INSERT INTO reise_mitarbeiter (reise_code,kuerzel) VALUES ({P},{P})",
                        (rcode, ma))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

# ── Land hinzufügen ────────────────────────────────────────────────────────────
@app.get("/reise/{code}/land/neu", response_class=HTMLResponse)
def land_neu_form(code: str):
    rcode = code.upper()
    land_opts = "".join(
        f'<option value="{lc}">{name} ({lc})</option>'
        for lc, name in LAENDER_LISTE)
    content = f"""
    <h1 class="page-title">Land hinzufügen – {rcode}</h1>
    <div class="card" style="max-width:500px">
      <div class="card-body">
        <form method="post" action="/reise/{rcode}/land/neu">
          <div class="form-grid form-grid-2">
            <div class="form-group full">
              <label>Land <span class="required">*</span></label>
              <select name="land_code" required onchange="showVMA(this.value)">
                {land_opts}
              </select>
              <div id="vma-info" class="form-hint" style="color:var(--green)"></div>
            </div>
            <div class="form-group">
              <label>Von (Datum) <span class="required">*</span></label>
              <input type="date" name="datum_von" required>
            </div>
            <div class="form-group">
              <label>Bis (Datum) <span class="required">*</span></label>
              <input type="date" name="datum_bis" required>
            </div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary">Hinzufügen</button>
            <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
          </div>
        </form>
      </div>
    </div>
    <script>
    const VMA = {json.dumps(VMA_SAETZE)};
    function showVMA(code) {{
        const info = VMA[code];
        const el = document.getElementById('vma-info');
        if (info) el.textContent = info.name + ': ' + info.voll + ' EUR/Tag · ' + info.halb + ' EUR halber Satz';
    }}
    showVMA(document.querySelector('select[name="land_code"]').value);
    </script>"""
    return HTMLResponse(shell(f"Land – {rcode}", content, "reisen"))

@app.post("/reise/{code}/land/neu")
async def land_neu(code: str, request: Request):
    rcode = code.upper()
    form = await request.form()
    land_code = (form.get("land_code") or "").strip().upper()
    datum_von = (form.get("datum_von") or "").strip()
    datum_bis = (form.get("datum_bis") or "").strip()
    if not all([land_code, datum_von, datum_bis]):
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Alle Felder sind Pflicht.</div>'
            f'<a href="/reise/{rcode}/land/neu" class="btn btn-secondary">Zurück</a>'))
    try:
        P = ph()
        land_name = VMA_SAETZE.get(land_code, {}).get("name", land_code)
        vvoll, vhalb = vma_fuer_land(land_code)
        db = get_db(); cur = db.cursor()
        cur.execute(
            f"INSERT INTO reise_laender (reise_code,datum_von,datum_bis,land_code,land_name,vma_voll,vma_halb) "
            f"VALUES ({P},{P},{P},{P},{P},{P},{P})",
            (rcode, datum_von, datum_bis, land_code, land_name, vvoll, vhalb))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reise/{code}/land/{lid}/bearbeiten", response_class=HTMLResponse)
def land_bearbeiten_form(code: str, lid: int):
    rcode = code.upper()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(
            f"SELECT id,datum_von,datum_bis,land_code,vma_voll,vma_halb FROM reise_laender WHERE id={P}",
            (lid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r: return HTMLResponse(shell("Fehler",'<div class="alert alert-err">Nicht gefunden.</div>'))
        def get(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        dvon = str(get(r,"datum_von",1))[:10]; dbis = str(get(r,"datum_bis",2))[:10]
        lcode = get(r,"land_code",3)
        vvoll = get(r,"vma_voll",4) or 0; vhalb = get(r,"vma_halb",5) or 0

        land_opts = "".join(
            f'<option value="{lc}"{" selected" if lc==lcode else ""}>{name} ({lc})</option>'
            for lc, name in LAENDER_LISTE)

        content = f"""
        <h1 class="page-title">Land bearbeiten – {rcode}</h1>
        <div class="card" style="max-width:500px">
          <div class="card-body">
            <form method="post" action="/reise/{rcode}/land/{lid}/bearbeiten">
              <div class="form-grid form-grid-2">
                <div class="form-group full">
                  <label>Land</label>
                  <select name="land_code" onchange="showVMA(this.value)">{land_opts}</select>
                </div>
                <div class="form-group">
                  <label>Von</label>
                  <input type="date" name="datum_von" value="{dvon}" required>
                </div>
                <div class="form-group">
                  <label>Bis</label>
                  <input type="date" name="datum_bis" value="{dbis}" required>
                </div>
                <div class="form-group">
                  <label>VMA Voll (EUR/Tag)</label>
                  <input type="number" step="0.01" name="vma_voll" value="{vvoll}">
                </div>
                <div class="form-group">
                  <label>VMA Halb (EUR/Tag)</label>
                  <input type="number" step="0.01" name="vma_halb" value="{vhalb}">
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/reise/{rcode}/land/{lid}/loeschen"
                   onclick="return confirm('Land löschen?')"
                   class="btn btn-danger">Löschen</a>
                <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>
        <script>
        const VMA = {json.dumps(VMA_SAETZE)};
        function showVMA(code) {{
            const info = VMA[code];
            if (info) {{
                document.querySelector('input[name="vma_voll"]').value = info.voll;
                document.querySelector('input[name="vma_halb"]').value = info.halb;
            }}
        }}
        </script>"""
        return HTMLResponse(shell(f"Land bearbeiten", content, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/reise/{code}/land/{lid}/bearbeiten")
async def land_bearbeiten(code: str, lid: int, request: Request):
    rcode = code.upper()
    form = await request.form()
    lcode = (form.get("land_code") or "").strip().upper()
    dvon = (form.get("datum_von") or "").strip()
    dbis = (form.get("datum_bis") or "").strip()
    vvoll = float(form.get("vma_voll") or 0)
    vhalb = float(form.get("vma_halb") or 0)
    lname = VMA_SAETZE.get(lcode, {}).get("name", lcode)
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(
            f"UPDATE reise_laender SET land_code={P},land_name={P},datum_von={P},"
            f"datum_bis={P},vma_voll={P},vma_halb={P} WHERE id={P}",
            (lcode, lname, dvon, dbis, vvoll, vhalb, lid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reise/{code}/land/{lid}/loeschen")
def land_loeschen(code: str, lid: int):
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"DELETE FROM reise_laender WHERE id={P}", (lid,))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── VMA-Tabelle Übersicht ──────────────────────────────────────────────────────
@app.get("/vma", response_class=HTMLResponse)
def vma_uebersicht():
    zeilen = ""
    for code, info in sorted(VMA_SAETZE.items(), key=lambda x: x[1]["name"]):
        region = ("🇩🇪" if code == "DE"
                  else "🇪🇺" if code in ("FR","CH","AT","GB","IT","ES","NL","BE","PL",
                                          "CZ","SE","NO","DK","FI","PT","GR","TR","HU",
                                          "RO","HR","BG","SK","SI","RS")
                  else "🌍")
        zeilen += f"""<tr>
            <td class="td-mono">{code}</td>
            <td>{region} {info["name"]}</td>
            <td style="text-align:right;font-weight:600">{info["voll"]:.2f} EUR</td>
            <td style="text-align:right">{info["halb"]:.2f} EUR</td>
        </tr>"""

    content = f"""
    <h1 class="page-title">VMA-Tagessätze 2026</h1>
    <div class="alert alert-warn" style="margin-bottom:20px">
      Quelle: BMF-Schreiben Auslandsreisekosten 2024 (§ 9 Abs. 4a EStG).
      Stand: Januar 2026. Bei Änderungen bitte Buchhalter kontaktieren.
    </div>
    <div class="card">
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>ISO</th><th>Land</th>
            <th style="text-align:right">Voller Satz/Tag</th>
            <th style="text-align:right">Halber Satz/Tag</th>
          </tr></thead>
          <tbody>{zeilen}</tbody>
        </table>
      </div>
    </div>
    <div class="alert alert-ok" style="margin-top:16px">
      <b>Regel:</b> Erster und letzter Reisetag → halber Satz. Volle Tage dazwischen → voller Satz.
      Bei Aufenthalt in mehreren Ländern gilt der Satz des Landes, in dem der Reisende
      um 24:00 Uhr Ortszeit war.
    </div>"""
    return HTMLResponse(shell("VMA-Sätze 2026", content, "vma"))
