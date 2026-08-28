"""
mod_vma.py – VMA-Sätze 2026, IATA-Codes, Länderlisten
"""
from __future__ import annotations
import httpx
from datetime import date as _date

PAUSCHBETRAG_URL = "https://cdn.jsdelivr.net/npm/pauschbetrag-api@1/ALL.json"

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

# Sicherheitsnetz: falls die KI bei einem Flug-/Bahnsegment keinen IATA-Code,
# sondern nur den Städtenamen erfasst hat (z.B. weil auf dem Beleg kein Code
# gedruckt war). Deckt gängige Geschäftsreiseziele ab, keine Vollständigkeit
# nötig – der Analyse-Prompt wurde zusätzlich geschärft, damit die KI künftig
# selbst den IATA-Code aus eigenem Wissen ableitet.
STADT_ZU_LAND: dict[str, str] = {
    "mexico city": "MX", "méxico city": "MX", "ciudad de mexico": "MX",
    "new york": "US", "los angeles": "US", "san francisco": "US", "chicago": "US",
    "miami": "US", "boston": "US", "washington": "US", "atlanta": "US", "seattle": "US",
    "london": "GB", "paris": "FR", "madrid": "ES", "barcelona": "ES", "rome": "IT",
    "rom": "IT", "milan": "IT", "mailand": "IT", "amsterdam": "NL", "brussels": "BE",
    "brüssel": "BE", "zurich": "CH", "zürich": "CH", "geneva": "CH", "genf": "CH",
    "vienna": "AT", "wien": "AT", "warsaw": "PL", "warschau": "PL", "prague": "CZ",
    "prag": "CZ", "budapest": "HU", "istanbul": "TR", "moscow": "RU", "moskau": "RU",
    "dubai": "AE", "abu dhabi": "AE", "doha": "QA", "riyadh": "SA", "cairo": "EG",
    "kairo": "EG", "johannesburg": "ZA", "cape town": "ZA", "kapstadt": "ZA",
    "beijing": "CN", "peking": "CN", "shanghai": "CN", "hong kong": "HK",
    "tokyo": "JP", "tokio": "JP", "osaka": "JP", "seoul": "KR", "singapore": "SG",
    "singapur": "SG", "bangkok": "TH", "kuala lumpur": "MY", "jakarta": "ID",
    "manila": "PH", "mumbai": "IN", "delhi": "IN", "new delhi": "IN", "bangalore": "IN",
    "sydney": "AU", "melbourne": "AU", "auckland": "NZ", "toronto": "CA",
    "vancouver": "CA", "montreal": "CA", "sao paulo": "BR", "são paulo": "BR",
    "rio de janeiro": "BR", "buenos aires": "AR", "santiago": "CL", "lima": "PE",
    "bogota": "CO", "bogotá": "CO", "cancun": "MX", "cancún": "MX",
    "stockholm": "SE", "oslo": "NO", "copenhagen": "DK", "kopenhagen": "DK",
    "helsinki": "FI", "dublin": "IE", "lisbon": "PT", "lissabon": "PT",
    "athens": "GR", "athen": "GR",
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
            """CREATE TABLE IF NOT EXISTS vma_tage (
                id              SERIAL PRIMARY KEY,
                reise_code      TEXT NOT NULL REFERENCES reisen(code) ON DELETE CASCADE,
                datum           DATE NOT NULL,
                land_code       TEXT NOT NULL,
                land_name       TEXT NOT NULL,
                vma_satz_voll   NUMERIC(6,2) NOT NULL,
                vma_satz_halb   NUMERIC(6,2) NOT NULL,
                ist_halber_satz BOOLEAN DEFAULT FALSE,
                fruehstueck     BOOLEAN DEFAULT FALSE,
                mittagessen     BOOLEAN DEFAULT FALSE,
                abendessen      BOOLEAN DEFAULT FALSE,
                vma_brutto      NUMERIC(6,2),
                vma_netto       NUMERIC(6,2),
                quelle          TEXT DEFAULT 'auto',
                notiz           TEXT,
                UNIQUE(reise_code, datum)
            )""",
            """CREATE TABLE IF NOT EXISTS belege (
                id                    SERIAL PRIMARY KEY,
                reise_code            TEXT REFERENCES reisen(code) ON DELETE SET NULL,
                dateiname             TEXT,
                s3_original           TEXT,
                s3_anon               TEXT,
                s3_analyse            TEXT,
                rohtext               TEXT,
                anon_text             TEXT,
                ki_json               TEXT,
                pflichtfelder_ok      BOOLEAN DEFAULT FALSE,
                fehlende_felder       TEXT,
                belegdatum            DATE,
                belegart              TEXT,
                transportart          TEXT,
                transportart_freitext TEXT,
                anbieter              TEXT,
                rechnungsnummer       TEXT,
                buchungscode          TEXT,
                reisender             TEXT,
                land_beleg            TEXT,
                betrag_brutto         NUMERIC(10,2),
                betrag_netto          NUMERIC(10,2),
                betrag_mwst           NUMERIC(10,2),
                waehrung              TEXT DEFAULT 'EUR',
                event_datum_von       DATE,
                event_datum_bis       DATE,
                event_ort_von         TEXT,
                event_ort_bis         TEXT,
                hotel_name            TEXT,
                hotel_checkin_datum   DATE,
                hotel_checkin_zeit    TEXT,
                hotel_checkout_datum  DATE,
                hotel_checkout_zeit   TEXT,
                hotel_naechte         INTEGER,
                tanken_kraftstoff     TEXT,
                tanken_menge          NUMERIC(8,3),
                tanken_einheit        TEXT,
                tanken_preis_einheit  NUMERIC(8,3),
                tanken_tankstelle     TEXT,
                tanken_kennzeichen    TEXT,
                status                TEXT DEFAULT 'neu',
                fehler                TEXT,
                erstellt              TIMESTAMP DEFAULT NOW()
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


def _land_name_de(iso_code: str) -> str:
    """Deutscher Ländername per Babel, Fallback: eigene Liste, dann Code."""
    try:
        from babel import Locale
        name = Locale("de").territories.get(iso_code.upper())
        if name:
            return name
    except Exception:
        pass
    return VMA_SAETZE.get(iso_code.upper(), {}).get("name", iso_code.upper())


def importiere_aktuelle_saetze() -> dict:
    """
    Lädt die aktuellen BMF-Auslandstagegeld-Sätze (inkl. Städte-Sonderfälle
    wie Los Angeles, New York etc.) von der öffentlichen pauschbetrag-api
    (https://github.com/david-loe/pauschbetrag-api, offizielle BMF-Quelle)
    und speichert sie in der Tabelle vma_saetze.
    """
    from mod_db import get_db, ph, is_postgres

    try:
        resp = httpx.get(PAUSCHBETRAG_URL, timeout=30.0)
        resp.raise_for_status()
        perioden = resp.json()
    except Exception as e:
        return {"fehler": f"Abruf fehlgeschlagen: {e}"}

    if not perioden:
        return {"fehler": "Keine Daten erhalten"}

    # Aktuell gültige Periode: validUntil ist null, sonst die mit dem
    # spätesten validFrom nehmen
    aktuelle = next((p for p in perioden if p.get("validUntil") is None), None)
    if not aktuelle:
        aktuelle = sorted(perioden, key=lambda p: p.get("validFrom", ""))[-1]

    gueltig_ab = aktuelle.get("validFrom")
    gueltig_bis = aktuelle.get("validUntil")
    laender = aktuelle.get("data", [])

    db = get_db(); cur = db.cursor()
    P = ph()
    anzahl_laender = 0
    anzahl_staedte = 0

    for land in laender:
        code = (land.get("countryCode") or "").upper()
        if not code:
            continue
        name = _land_name_de(code)
        voll = land.get("catering24")
        halb = land.get("catering8")
        uebernacht = land.get("overnight")
        if voll is None or halb is None:
            continue

        if is_postgres():
            cur.execute(f"""
                INSERT INTO vma_saetze (land_code, ort, land_name, vma_voll, vma_halb,
                    uebernachtung, gueltig_ab, gueltig_bis, quelle, aktualisiert)
                VALUES ({P},NULL,{P},{P},{P},{P},{P},{P},'pauschbetrag-api',NOW())
                ON CONFLICT (land_code, ort) DO UPDATE SET
                    land_name={P}, vma_voll={P}, vma_halb={P}, uebernachtung={P},
                    gueltig_ab={P}, gueltig_bis={P}, quelle='pauschbetrag-api', aktualisiert=NOW()
            """, (code, name, voll, halb, uebernacht, gueltig_ab, gueltig_bis,
                  name, voll, halb, uebernacht, gueltig_ab, gueltig_bis))
        else:
            cur.execute(f"""
                INSERT INTO vma_saetze (land_code, ort, land_name, vma_voll, vma_halb,
                    uebernachtung, gueltig_ab, gueltig_bis, quelle)
                VALUES ({P},NULL,{P},{P},{P},{P},{P},{P},'pauschbetrag-api')
                ON CONFLICT (land_code, ort) DO UPDATE SET
                    land_name=excluded.land_name, vma_voll=excluded.vma_voll,
                    vma_halb=excluded.vma_halb, uebernachtung=excluded.uebernachtung,
                    gueltig_ab=excluded.gueltig_ab, gueltig_bis=excluded.gueltig_bis
            """, (code, name, voll, halb, uebernacht, gueltig_ab, gueltig_bis))
        anzahl_laender += 1

        for special in (land.get("specials") or []):
            stadt = (special.get("city") or "").strip()
            if not stadt:
                continue
            s_voll = special.get("catering24")
            s_halb = special.get("catering8")
            s_uebernacht = special.get("overnight")
            if s_voll is None or s_halb is None:
                continue
            stadt_name = f"{name} – {stadt}"
            if is_postgres():
                cur.execute(f"""
                    INSERT INTO vma_saetze (land_code, ort, land_name, vma_voll, vma_halb,
                        uebernachtung, gueltig_ab, gueltig_bis, quelle, aktualisiert)
                    VALUES ({P},{P},{P},{P},{P},{P},{P},{P},'pauschbetrag-api',NOW())
                    ON CONFLICT (land_code, ort) DO UPDATE SET
                        land_name={P}, vma_voll={P}, vma_halb={P}, uebernachtung={P},
                        gueltig_ab={P}, gueltig_bis={P}, quelle='pauschbetrag-api', aktualisiert=NOW()
                """, (code, stadt, stadt_name, s_voll, s_halb, s_uebernacht, gueltig_ab, gueltig_bis,
                      stadt_name, s_voll, s_halb, s_uebernacht, gueltig_ab, gueltig_bis))
            else:
                cur.execute(f"""
                    INSERT INTO vma_saetze (land_code, ort, land_name, vma_voll, vma_halb,
                        uebernachtung, gueltig_ab, gueltig_bis, quelle)
                    VALUES ({P},{P},{P},{P},{P},{P},{P},{P},'pauschbetrag-api')
                    ON CONFLICT (land_code, ort) DO UPDATE SET
                        land_name=excluded.land_name, vma_voll=excluded.vma_voll,
                        vma_halb=excluded.vma_halb, uebernachtung=excluded.uebernachtung,
                        gueltig_ab=excluded.gueltig_ab, gueltig_bis=excluded.gueltig_bis
                """, (code, stadt, stadt_name, s_voll, s_halb, s_uebernacht, gueltig_ab, gueltig_bis))
            anzahl_staedte += 1

    db.commit(); cur.close(); db.close()

    return {
        "ok": True,
        "laender": anzahl_laender,
        "staedte": anzahl_staedte,
        "gueltig_ab": gueltig_ab,
    }


def vma_fuer_land_erweitert(cur, land_code: str, ort: str | None = None) -> dict:
    """
    Liefert den VMA-Satz für ein Land, optional mit Städte-Sonderfall.
    Reihenfolge: 1) importierter Städte-Satz  2) importierter Länder-Satz
    3) fest hinterlegter Satz im Code (Fallback, falls noch nicht importiert).
    """
    from mod_db import ph
    P = ph()
    land_code = (land_code or "DE").upper()

    if ort:
        cur.execute(f"SELECT land_name, vma_voll, vma_halb FROM vma_saetze "
                    f"WHERE land_code={P} AND ort={P}", (land_code, ort))
        row = cur.fetchone()
        if row:
            n = row[0] if isinstance(row, tuple) else row["land_name"]
            v = row[1] if isinstance(row, tuple) else row["vma_voll"]
            h = row[2] if isinstance(row, tuple) else row["vma_halb"]
            return {"land_name": n, "voll": float(v), "halb": float(h), "quelle": "importiert (Stadt)"}

    cur.execute(f"SELECT land_name, vma_voll, vma_halb FROM vma_saetze "
                f"WHERE land_code={P} AND ort IS NULL", (land_code,))
    row = cur.fetchone()
    if row:
        n = row[0] if isinstance(row, tuple) else row["land_name"]
        v = row[1] if isinstance(row, tuple) else row["vma_voll"]
        h = row[2] if isinstance(row, tuple) else row["vma_halb"]
        return {"land_name": n, "voll": float(v), "halb": float(h), "quelle": "importiert"}

    s = VMA_SAETZE.get(land_code, {"name": land_code, "voll": 28.00, "halb": 14.00})
    return {"land_name": s["name"], "voll": s["voll"], "halb": s["halb"], "quelle": "code (nicht importiert)"}


def staedte_fuer_land(cur, land_code: str) -> list[str]:
    """Liste der importierten Städte-Sonderfälle für ein Land (z.B. für USA: Los Angeles, New York, ...)."""
    from mod_db import ph
    P = ph()
    cur.execute(f"SELECT ort FROM vma_saetze WHERE land_code={P} AND ort IS NOT NULL ORDER BY ort",
                (land_code.upper(),))
    rows = cur.fetchall()
    return [r[0] if isinstance(r, tuple) else r["ort"] for r in rows]

