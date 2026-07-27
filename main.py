"""
# v2.2-f – Dashboard Alarm, Organisatoren fix, Anonymisierung E-Mails
Modulare Struktur – Einstiegspunkt
"""
# v2.2-a – Modularisierung komplett
from __future__ import annotations
import os, re, json, io, base64
import httpx
from datetime import date, datetime, timedelta
from typing import Optional

from fastapi import FastAPI, Request, Form, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# ── Module importieren ────────────────────────────────────────────────────────
from mod_db import get_db, is_postgres, ph, fmt_date, next_reise_code, get_schema, get_migrations, repair_legacy_columns
from mod_vma import (VMA_SAETZE, IATA_TO_LAND, LAENDER_LISTE, vma_fuer_land,
                      importiere_aktuelle_saetze, vma_fuer_land_erweitert)
from mod_anon import anonymisieren
from mod_beleg import (beleg_verarbeiten, gpt_analyse, gpt_analyse_bild,
                        lade_ma_daten, get_s3, s3_upload, s3_download,
                        bild_zu_pdf, text_zu_pdf, pdf_text_lesen,
                        beleg_neu_anonymisieren,
                        OPENAI_KEY, OPENAI_MODEL, OPENAI_URL,
                        S3_ENDPOINT, S3_BUCKET)
from mod_mail import fetch_mails
from mod_vma_tage import (vma_berechnen, land_fuer_tag,
                           fruehstueck_aus_beleg, vma_tage_generieren)

# ── Konfiguration ─────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")
IMAP_HOST    = os.getenv("IMAP_HOST", "")
IMAP_USER    = os.getenv("IMAP_USER", "")
IMAP_PASS    = os.getenv("IMAP_PASS", "")
APP_VERSION  = "2.3-e"

# ── CSS + HTML Shell ──────────────────────────────────────────────────────────
# ── CSS + HTML Shell ───────────────────────────────────────────────────────────
CSS = """
:root {
    --bg: #f0f4f8;
    --white: #ffffff;
    --border: #dde3ea;
    --border-strong: #c4cdd8;
    --text: #0d1b2a;
    --muted: #5a6a7a;
    --light: #8fa0b0;
    --blue: #1a56db;
    --blue-d: #1344b8;
    --blue-l: #eff4ff;
    --blue-nav: #0f2d6e;
    --green: #047857;
    --green-l: #ecfdf5;
    --amber: #b45309;
    --amber-l: #fffbeb;
    --red: #c81e1e;
    --red-l: #fef2f2;
    --radius: 10px;
    --radius-s: 6px;
    --shadow: 0 1px 3px rgba(0,0,0,.06), 0 1px 2px rgba(0,0,0,.04);
    --shadow-md: 0 4px 12px rgba(0,0,0,.08);
}
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    background: var(--bg);
    color: var(--text);
    font-size: 14px;
    line-height: 1.5;
}

/* Navigation */
nav {
    background: linear-gradient(135deg, #0f2d6e 0%, #1a56db 100%);
    padding: 0 28px;
    display: flex;
    align-items: center;
    gap: 0;
    position: sticky;
    top: 0;
    z-index: 100;
    box-shadow: 0 2px 12px rgba(15,45,110,.3);
    height: 60px;
}
.nav-brand {
    display: flex;
    align-items: center;
    margin-right: 32px;
    padding: 4px 0;
    text-decoration: none;
    flex-shrink: 0;
}
.nav-brand img {
    height: 42px;
    width: auto;
    filter: brightness(0) invert(1);
    opacity: 0.95;
}
.nav-link {
    color: rgba(255,255,255,0.7);
    text-decoration: none;
    font-size: 13px;
    font-weight: 500;
    padding: 20px 14px;
    border-bottom: 3px solid transparent;
    transition: color .15s, border-color .15s;
    white-space: nowrap;
    letter-spacing: .01em;
}
.nav-link:hover { color: white; border-bottom-color: rgba(255,255,255,0.4); }
.nav-link.active { color: white; border-bottom-color: #60a5fa; }
.nav-right { margin-left: auto; font-size: 11px; color: rgba(255,255,255,0.4);
             font-family: monospace; letter-spacing:.05em; }

/* Layout */
main { padding: 28px 28px; max-width: 1200px; margin: 0 auto; }
.page-title { font-size: 22px; font-weight: 700; color: var(--text); margin-bottom: 20px;
              letter-spacing: -.02em; }

/* Karten */
.card {
    background: var(--white);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--shadow);
    margin-bottom: 16px;
}
.card-header {
    padding: 14px 20px;
    border-bottom: 1px solid var(--border);
    display: flex;
    align-items: center;
    justify-content: space-between;
    background: #fafbfc;
    border-radius: var(--radius) var(--radius) 0 0;
}
.card-title { font-size: 14px; font-weight: 600; color: var(--text); }
.card-body { padding: 20px; }

/* Stat-Karten */
.stat-card {
    background: var(--white);
    border: 1px solid var(--border);
    border-radius: var(--radius);
    box-shadow: var(--shadow);
    padding: 20px;
    text-align: center;
    transition: box-shadow .15s, transform .15s;
}
.stat-card:hover { box-shadow: var(--shadow-md); transform: translateY(-1px); }
.stat-num { font-size: 32px; font-weight: 700; letter-spacing: -.03em; }
.stat-label { font-size: 12px; color: var(--muted); margin-top: 4px; font-weight: 500; }

/* Buttons */
.btn {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 8px 16px;
    border-radius: var(--radius-s);
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    text-decoration: none;
    border: none;
    transition: all .15s;
    white-space: nowrap;
    letter-spacing: .01em;
}
.btn-primary { background: var(--blue); color: white; }
.btn-primary:hover { background: var(--blue-d); box-shadow: 0 2px 8px rgba(26,86,219,.3); }
.btn-success { background: var(--green); color: white; }
.btn-success:hover { background: #036545; }
.btn-secondary { background: white; color: #374151; border: 1px solid var(--border); }
.btn-secondary:hover { background: #f9fafb; border-color: var(--border-strong); }
.btn-danger { background: var(--red); color: white; }
.btn-danger:hover { background: #a31717; }
.btn-sm { padding: 5px 10px; font-size: 12px; }

/* Formulare */
.form-grid { display: grid; gap: 16px; }
.form-grid-2 { grid-template-columns: 1fr 1fr; }
.form-grid-3 { grid-template-columns: 1fr 1fr 1fr; }
.form-group { display: flex; flex-direction: column; gap: 4px; }
.form-group.full { grid-column: 1 / -1; }
label { font-size: 12px; font-weight: 600; color: #374151; letter-spacing: .01em; }
.required { color: var(--red); margin-left: 2px; }
input[type="text"], input[type="date"], input[type="email"],
input[type="number"], select, textarea {
    width: 100%;
    padding: 8px 12px;
    border: 1px solid var(--border);
    border-radius: var(--radius-s);
    font-size: 13px;
    background: white;
    color: var(--text);
    transition: border-color .15s, box-shadow .15s;
}
input:focus, select:focus, textarea:focus {
    outline: none;
    border-color: var(--blue);
    box-shadow: 0 0 0 3px rgba(26,86,219,.1);
}
.form-hint { font-size: 11px; color: var(--muted); margin-top: 2px; }
.form-actions {
    display: flex;
    gap: 8px;
    padding-top: 16px;
    border-top: 1px solid var(--border);
    margin-top: 20px;
}

/* Tabellen */
.table-wrap { overflow-x: auto; border-radius: 0 0 var(--radius) var(--radius); }
table { width: 100%; border-collapse: collapse; }
th {
    text-align: left;
    padding: 9px 14px;
    font-size: 11px;
    font-weight: 700;
    color: var(--muted);
    text-transform: uppercase;
    letter-spacing: .06em;
    border-bottom: 1px solid var(--border);
    background: #fafbfc;
    white-space: nowrap;
}
td {
    padding: 11px 14px;
    font-size: 13px;
    border-bottom: 1px solid #f3f4f6;
    vertical-align: middle;
}
tr:last-child td { border-bottom: none; }
tr:hover td { background: #fafbfe; }
.td-mono { font-family: "SF Mono", "Fira Code", monospace; font-size: 12px; }

/* Badges */
.badge {
    display: inline-flex;
    align-items: center;
    gap: 3px;
    padding: 2px 8px;
    border-radius: 20px;
    font-size: 11px;
    font-weight: 600;
    letter-spacing: .02em;
}
.badge-blue { background: #dbeafe; color: #1e40af; }
.badge-green { background: #d1fae5; color: #065f46; }
.badge-amber { background: #fef3c7; color: #92400e; }
.badge-red { background: var(--red-l); color: #991b1b; }
.badge-gray { background: #f3f4f6; color: #6b7280; }
.badge-purple { background: #ede9fe; color: #5b21b6; }

/* Alerts */
.alert {
    padding: 12px 16px;
    border-radius: var(--radius-s);
    font-size: 13px;
    margin-bottom: 16px;
    border-left: 4px solid;
}
.alert-ok { background: var(--green-l); border-color: #34d399; color: #065f46; }
.alert-warn { background: var(--amber-l); border-color: #fbbf24; color: #92400e; }
.alert-err { background: var(--red-l); border-color: #f87171; color: #991b1b; }
.alert-info { background: var(--blue-l); border-color: #93c5fd; color: #1e40af; }

/* Leerer Zustand */
.empty-state { text-align: center; padding: 40px 20px; color: var(--light); }
.empty-state p { margin-top: 8px; font-size: 13px; }

/* Reise-Sektion Header */
.sektion-header {
    display: flex;
    align-items: center;
    gap: 10px;
    margin: 24px 0 12px;
    padding-bottom: 8px;
    border-bottom: 2px solid var(--border);
}
.sektion-titel {
    font-size: 16px;
    font-weight: 700;
    color: var(--text);
}
.sektion-count {
    background: var(--blue-l);
    color: var(--blue);
    padding: 2px 10px;
    border-radius: 20px;
    font-size: 12px;
    font-weight: 700;
}

@media (max-width: 640px) {
    .form-grid-2, .form-grid-3 { grid-template-columns: 1fr; }
    main { padding: 16px; }
    nav { padding: 0 16px; }
}
"""

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
  <a href="/" class="nav-brand" style="display:flex;align-items:center;gap:10px;padding:6px 0">
    <img src="/static/logo3.png" alt="Herrhammer" style="height:32px;width:auto">
  </a>
  {nav("start", "Dashboard", "/")}
  {nav("mitarbeiter", "Mitarbeiter", "/mitarbeiter")}
  {nav("reisen", "Reisen", "/reisen")}
  {nav("belege", "Belege", "/belege")}
  {nav("mails", "📬 Mails", "/mails-abrufen")}
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

@app.on_event("startup")
async def startup():
    print(f"[Startup] Herrhammer Reisekosten {APP_VERSION} gestartet")

if not os.path.exists("static"):
    os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── System-Routen ──────────────────────────────────────────────────────────────
# ═══════════════════════════════════════════════════════════════════════════════
# SCHRITT B) – BELEGE VERARBEITEN
# ═══════════════════════════════════════════════════════════════════════════════



# ── App ───────────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup():
    print(f"[Startup] Herrhammer Reisekosten {APP_VERSION} gestartet")

if not os.path.exists("static"):
    os.makedirs("static", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

# ── Beleg-Routen ──────────────────────────────────────────────────────────────
@app.get("/beleg/upload", response_class=HTMLResponse)
def beleg_upload_form():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("SELECT code, titel, abreise FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.execute("SELECT COUNT(*) FROM belege WHERE reise_code IS NULL")
        unzugeordnet = cur.fetchone()[0]
        cur.close(); db.close()
    except: reisen = []

    def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

    opts = '<option value="">– Reise wählen (optional) –</option>'
    for r in reisen:
        code = get(r,"code",0); titel = get(r,"titel",1); ab = get(r,"abreise",2)
        opts += f'<option value="{code}">{code} – {titel} ({fmt_date(ab)})</option>'

    content = f"""
    <h1 class="page-title">Beleg hochladen</h1>
    <div class="card" style="max-width:560px">
      <div class="card-body">
        <div class="alert alert-warn" style="margin-bottom:20px">
          Der Beleg wird automatisch:<br>
          1. Zu PDF konvertiert (bei Foto/Bild)<br>
          2. Anonymisiert (Namen, E-Mails, Herrhammer)<br>
          3. Von GPT-4o analysiert (Typ, Betrag, Datum, Segmente)
        </div>
        <form method="post" action="/beleg/upload" enctype="multipart/form-data">
          <div class="form-grid">
            <div class="form-group">
              <label>Reise zuordnen</label>
              <select name="reise_code" class="sel">{opts}</select>
              <div class="form-hint">Oder leer lassen und später zuordnen</div>
            </div>
            <div class="form-group">
              <label>Datei <span class="required">*</span></label>
              <input type="file" name="datei" required
                     accept=".pdf,.jpg,.jpeg,.png,.heic,.webp"
                     style="width:100%;padding:8px;border:1px solid var(--border);
                            border-radius:var(--radius-s);background:white">
              <div class="form-hint">PDF, JPG, PNG, HEIC, WebP</div>
            </div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary">
              Hochladen & Analysieren
            </button>
            <a href="/belege" class="btn btn-secondary">Abbrechen</a>
          </div>
        </form>
      </div>
    </div>"""
    return HTMLResponse(shell("Beleg hochladen", content))

@app.post("/beleg/upload")
async def beleg_upload(request: Request,
                       datei: UploadFile = File(...),
                       reise_code: str = Form("")):
    try:
        datei_bytes = await datei.read()
        ct = datei.content_type or "application/octet-stream"
        rc = reise_code.strip() or None

        result = await beleg_verarbeiten(datei_bytes, datei.filename or "upload", rc, ct)
        return RedirectResponse(f"/beleg/{result['beleg_id']}", status_code=303)
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err"><b>Fehler:</b> {e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'
            '<a href="/beleg/upload" class="btn btn-secondary">Zurück</a>'))

# ── Beleg Detailseite ──────────────────────────────────────────────────────────
@app.get("/beleg/{bid}", response_class=HTMLResponse)
def beleg_detail(bid: int):
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"""SELECT id, reise_code, transportart, dateiname,
            s3_original, s3_anon, s3_analyse, rohtext, anon_text, ki_json,
            pflichtfelder_ok, fehlende_felder,
            belegdatum, belegart, anbieter, rechnungsnummer, buchungscode,
            reisender, land_beleg,
            betrag_brutto, betrag_netto, betrag_mwst, waehrung,
            event_datum_von, event_datum_bis, event_ort_von, event_ort_bis,
            hotel_name, hotel_checkin_datum, hotel_checkin_zeit,
            hotel_checkout_datum, hotel_checkout_zeit, hotel_naechte,
            tanken_kraftstoff, tanken_menge, tanken_einheit,
            tanken_preis_einheit, tanken_tankstelle, tanken_kennzeichen,
            status, fehler, erstellt,
            kurs_eur, betrag_eur, kurs_datum, kurs_quelle
            FROM belege WHERE id={P}""", (bid,))
        r = cur.fetchone()
        # Reisen für Zuordnung
        cur.execute("SELECT code,titel FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler",'<div class="alert alert-err">Beleg nicht gefunden.</div>'))

        def get(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        bid2=get(r,"id",0); rcode=get(r,"reise_code",1); typ=get(r,"transportart",2)
        dateiname=get(r,"dateiname",3); s3o=get(r,"s3_original",4)
        s3a=get(r,"s3_anon",5); s3an=get(r,"s3_analyse",6)
        rohtext=get(r,"rohtext",7); anon_text=get(r,"anon_text",8)
        ki_json_str=get(r,"ki_json",9)
        pf_ok=get(r,"pflichtfelder_ok",10); fehlend=get(r,"fehlende_felder",11)
        belegdatum=get(r,"belegdatum",12); belegart=get(r,"belegart",13)
        vendor=get(r,"anbieter",14); rechnr=get(r,"rechnungsnummer",15)
        buchungscode=get(r,"buchungscode",16); reisender=get(r,"reisender",17)
        land=get(r,"land_beleg",18)
        betrag_brutto=get(r,"betrag_brutto",19); betrag_netto=get(r,"betrag_netto",20)
        betrag_mwst=get(r,"betrag_mwst",21); waehrung=get(r,"waehrung",22)
        ev_von=get(r,"event_datum_von",23); ev_bis=get(r,"event_datum_bis",24)
        ev_ort_von=get(r,"event_ort_von",25); ev_ort_bis=get(r,"event_ort_bis",26)
        hotel_name=get(r,"hotel_name",27)
        hotel_ci_dat=get(r,"hotel_checkin_datum",28); hotel_ci_zeit=get(r,"hotel_checkin_zeit",29)
        hotel_co_dat=get(r,"hotel_checkout_datum",30); hotel_co_zeit=get(r,"hotel_checkout_zeit",31)
        hotel_naechte=get(r,"hotel_naechte",32)
        tank_kraft=get(r,"tanken_kraftstoff",33); tank_menge=get(r,"tanken_menge",34)
        tank_einh=get(r,"tanken_einheit",35); tank_preis=get(r,"tanken_preis_einheit",36)
        tank_stelle=get(r,"tanken_tankstelle",37); tank_kfz=get(r,"tanken_kennzeichen",38)
        status=get(r,"status",39); fehler=get(r,"fehler",40); erstellt=get(r,"erstellt",41)
        kurs_eur=get(r,"kurs_eur",42); betrag_eur=get(r,"betrag_eur",43)
        kurs_datum=get(r,"kurs_datum",44); kurs_quelle=get(r,"kurs_quelle",45)
        zusammenfassung = f"{typ}: {vendor} – {betrag_brutto} {waehrung}" if vendor else ""

        # KI-JSON parsen
        ki = {}
        try: ki = json.loads(ki_json_str or "{}")
        except: pass

        segmente = ki.get("segmente") or []

        # Typ-Badge
        typ_farben = {
            "Flug":"#dbeafe:#1e40af","Hotel":"#dcfce7:#166534",
            "Bahn":"#e0e7ff:#3730a3","Taxi":"#fef3c7:#92400e",
            "Mietwagen":"#fce7f3:#9d174d","Bewirtung":"#fff7ed:#9a3412",
            "Tanken":"#f0fdf4:#14532d","Sonstiges":"#f1f5f9:#475569"
        }
        tc = typ_farben.get(typ or "Sonstiges","#f1f5f9:#475569").split(":")
        typ_badge = (f'<span style="background:{tc[0]};color:{tc[1]};'
                     f'padding:3px 10px;border-radius:4px;font-size:12px;'
                     f'font-weight:700">{typ}</span>')

        # Segmente Tabelle
        seg_html = ""
        if segmente:
            rows = ""
            for s in segmente:
                ab_tz = s.get("abreise_zeitzone","") or ""
                an_tz = s.get("ankunft_zeitzone","") or ""
                von_iata = s.get("von_iata","") or ""
                von_ort  = s.get("von_ort","") or ""
                nach_iata= s.get("nach_iata","") or ""
                nach_ort = s.get("nach_ort","") or ""
                t_name   = s.get("transport_name","") or ""
                t_nr     = s.get("transport_nummer","") or ""
                ab_dat   = s.get("abreise_datum","") or ""
                ab_zeit  = s.get("abreise_zeit","") or ""
                an_dat   = s.get("ankunft_datum","") or ab_dat
                an_zeit  = s.get("ankunft_zeit","") or ""
                klasse   = s.get("klasse","") or ""
                hinweis  = s.get("hinweis","") or ""
                rows += (f'<tr>'
                    f'<td style="text-align:center;color:var(--muted);font-size:12px">{s.get("nr","")}</td>'
                    f'<td style="font-weight:700;color:var(--blue);font-family:monospace;white-space:nowrap">'
                    f'{t_name} {t_nr}</td>'
                    f'<td><b>{von_iata}</b>'
                    f'<div style="font-size:11px;color:var(--muted)">{von_ort}</div></td>'
                    f'<td style="color:var(--muted)">→</td>'
                    f'<td><b>{nach_iata}</b>'
                    f'<div style="font-size:11px;color:var(--muted)">{nach_ort}</div></td>'
                    f'<td style="font-family:monospace;font-size:12px;white-space:nowrap">'
                    f'<b>{ab_dat}</b><br>'
                    f'<span style="color:var(--blue)">{ab_zeit} {ab_tz}</span></td>'
                    f'<td style="font-family:monospace;font-size:12px;white-space:nowrap">'
                    f'<b>{an_dat}</b><br>'
                    f'<span style="color:var(--green)">{an_zeit} {an_tz}</span></td>'
                    f'<td style="font-size:11px;color:var(--muted)">{klasse}</td>'
                    f'<td style="font-size:11px;color:var(--muted);max-width:120px">'
                    f'{hinweis}</td>'
                    f'</tr>')
            seg_html = (f'<div class="card" style="margin-top:16px">'
                f'<div class="card-header"><span class="card-title">'
                f'✈ Reisesegmente ({len(segmente)})</span></div>'
                f'<div class="table-wrap"><table>'
                f'<thead><tr><th>#</th><th>Transport</th><th>Von</th><th></th><th>Nach</th>'
                f'<th>Abflug</th><th>Ankunft</th><th>Klasse</th><th>Hinweis</th></tr></thead>'
                f'<tbody>{rows}</tbody></table></div></div>')

        # Reise-Dropdown
        r_opts = '<option value="">– Keine –</option>'
        for rv in reisen:
            rc2 = rv[0] if isinstance(rv,tuple) else rv["code"]
            rt2 = rv[1] if isinstance(rv,tuple) else rv["titel"]
            sel = ' selected' if rc2==rcode else ""
            r_opts += f'<option value="{rc2}"{sel}>{rc2} – {rt2}</option>'

        status_badge = ('<span class="badge badge-green">OK</span>' if status=="ok"
                        else '<span class="badge badge-red">Fehler</span>' if status=="fehler"
                        else '<span class="badge badge-amber">Ausstehend</span>')

        content = f"""
        <div style="display:flex;align-items:center;gap:12px;margin-bottom:20px">
          <a href="/belege" class="btn btn-secondary">← Belege</a>
          <h1 class="page-title" style="margin:0">Beleg #{bid2}</h1>
          {typ_badge}
          {status_badge}
        </div>

        <div style="display:grid;grid-template-columns:1fr 1fr;gap:16px">
          <div class="card">
            <div class="card-header"><span class="card-title">📊 KI-Analyse</span></div>
            <div class="card-body">
              <dl style="display:grid;grid-template-columns:160px 1fr;gap:4px 12px">
                <dt style="color:var(--muted);font-size:12px">Datei</dt>
                <dd style="font-size:12px;color:var(--muted)">{dateiname}</dd>
                <dt style="color:var(--muted);font-size:12px">Transportart</dt>
                <dd>{typ_badge}{f' – {ki.get("transportart_freitext")}' if ki.get("transportart_freitext") else ""}</dd>
                <dt style="color:var(--muted);font-size:12px">Belegart</dt>
                <dd>
                  <form method="post" action="/beleg/{bid2}/belegart" style="display:inline-block">
                    <select name="belegart" onchange="this.form.submit()"
                            style="padding:4px 8px;font-size:12px;border:1px solid var(--border);
                                   border-radius:6px;background:white">
                      <option value=""{' selected' if not belegart else ''}>– wählen –</option>
                      <option value="Rechnung"{' selected' if belegart=='Rechnung' else ''}>Rechnung</option>
                      <option value="Quittung"{' selected' if belegart=='Quittung' else ''}>Quittung</option>
                      <option value="Buchungsbestaetigung"{' selected' if belegart=='Buchungsbestaetigung' else ''}>Buchungsbestätigung</option>
                      <option value="Tankbeleg"{' selected' if belegart=='Tankbeleg' else ''}>Tankbeleg (Rechnung)</option>
                      <option value="Hotel"{' selected' if belegart=='Hotel' else ''}>Hotel</option>
                      <option value="Taxi"{' selected' if belegart=='Taxi' else ''}>Taxi</option>
                      <option value="Bewirtung"{' selected' if belegart=='Bewirtung' else ''}>Bewirtung</option>
                      <option value="Sonstige Kosten"{' selected' if belegart=='Sonstige Kosten' else ''}>Sonstige Kosten (z.B. Extragepäck)</option>
                      <option value="Sonstiges"{' selected' if belegart=='Sonstiges' else ''}>Sonstiges</option>
                    </select>
                  </form>
                </dd>
                <dt style="color:var(--muted);font-size:12px">Anbieter</dt>
                <dd style="font-weight:600">{vendor or "–"}</dd>
                <dt style="color:var(--muted);font-size:12px">Reisender</dt>
                <dd>{reisender or "–"}</dd>
                <dt style="color:var(--muted);font-size:12px">Land</dt>
                <dd>{land or "–"}</dd>
                <dt style="color:var(--muted);font-size:12px">Betrag brutto</dt>
                <dd style="font-weight:700;color:var(--green);font-size:15px">
                  {f"{float(betrag_brutto):.2f}" if betrag_brutto else "–"} {waehrung}</dd>
                {f'<dt style="color:var(--muted);font-size:12px">Netto</dt><dd>{float(betrag_netto):.2f} {waehrung}</dd>' if betrag_netto else ""}
                {f'<dt style="color:var(--muted);font-size:12px">{"MwSt." if land == "DE" else "VAT"}</dt><dd>{float(betrag_mwst):.2f} {waehrung}</dd>' if betrag_mwst else ""}
                <dt style="color:var(--muted);font-size:12px">Belegdatum</dt>
                <dd>{fmt_date(belegdatum)}</dd>
                <dt style="color:var(--muted);font-size:12px">Event</dt>
                <dd>{fmt_date(ev_von)}{f" – {fmt_date(ev_bis)}" if ev_bis else ""}</dd>
                <dt style="color:var(--muted);font-size:12px">Strecke</dt>
                <dd>{ev_ort_von or "–"}{f" → {ev_ort_bis}" if ev_ort_bis else ""}</dd>
                <dt style="color:var(--muted);font-size:12px">Buchungscode</dt>
                <dd style="font-family:monospace">{buchungscode or "–"}</dd>
                <dt style="color:var(--muted);font-size:12px">Rechnungsnr.</dt>
                <dd style="font-family:monospace">{rechnr or "–"}</dd>
                {f'<dt style="color:var(--muted);font-size:12px">EUR-Betrag</dt><dd style="font-weight:600;color:var(--green)">{float(betrag_eur):.2f} EUR</dd>' if betrag_eur else ('<dt style="color:var(--amber);font-size:12px">EUR-Betrag</dt><dd style="color:var(--amber);font-size:12px">⚠ Kurs fehlt – bitte nachtragen</dd>' if waehrung and waehrung != "EUR" else "")}
                {f'<dt style="color:var(--muted);font-size:12px">Kurs</dt><dd style="font-family:monospace">{kurs_eur} ({kurs_datum or ""} {kurs_quelle or ""})</dd>' if kurs_eur else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Hotel</dt><dd style="font-weight:600">{hotel_name}</dd>' if hotel_name else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Check-in</dt><dd>{fmt_date(hotel_ci_dat)} {hotel_ci_zeit or ""}</dd>' if hotel_ci_dat else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Check-out</dt><dd>{fmt_date(hotel_co_dat)} {hotel_co_zeit or ""}</dd>' if hotel_co_dat else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Nächte</dt><dd>{hotel_naechte}</dd>' if hotel_naechte else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Kraftstoff</dt><dd>{tank_kraft}</dd>' if tank_kraft else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Menge</dt><dd>{tank_menge} {tank_einh or ""}</dd>' if tank_menge else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Preis/Einheit</dt><dd>{tank_preis} {waehrung}</dd>' if tank_preis else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Tankstelle</dt><dd>{tank_stelle}</dd>' if tank_stelle else ""}
                {f'<dt style="color:var(--muted);font-size:12px">Kennzeichen</dt><dd style="font-family:monospace">{tank_kfz}</dd>' if tank_kfz else ""}
              </dl>
              {f'<div class="alert alert-err" style="margin-top:12px"><b>Fehlende Pflichtfelder:</b> {fehlend}</div>' if not pf_ok else ""}
              {f'<div class="alert alert-err" style="margin-top:8px">{fehler}</div>' if fehler else ""}
            </div>
          </div>

          <div class="card">
            <div class="card-header"><span class="card-title">📎 Dokumente</span></div>
            <div class="card-body">
              <div style="display:flex;flex-direction:column;gap:8px">
                <a href="/beleg/{bid2}/pdf/original" target="_blank"
                   class="btn btn-secondary">📄 Original-PDF öffnen</a>
                <a href="/beleg/{bid2}/pdf/anon" target="_blank"
                   class="btn btn-secondary">🔒 Anonymisiert öffnen</a>
                <a href="/beleg/{bid2}/pdf/analyse" target="_blank"
                   class="btn btn-secondary">🔍 Analyse-PDF öffnen</a>
              </div>
              <form method="post" action="/beleg/{bid2}/neu-anonymisieren" style="margin-top:8px">
                <button type="submit" class="btn btn-secondary" style="width:100%">
                  🔄 Neu anonymisieren
                </button>
              </form>
              <a href="/beleg/{bid2}/loeschen" class="btn btn-secondary"
                 style="display:block;margin-top:8px;text-align:center;color:#b91c1c;border-color:#fca5a5"
                 onclick="return confirm('Diesen Beleg unwiderruflich löschen?')">
                🗑 Beleg löschen
              </a>
              <hr style="border:none;border-top:1px solid var(--border);margin:16px 0">
              <form method="post" action="/beleg/{bid2}/zuordnen">
                <div class="form-group">
                  <label>Reise zuordnen</label>
                  <select name="reise_code">{r_opts}</select>
                </div>
                <button type="submit" class="btn btn-primary" style="margin-top:8px;width:100%">
                  Speichern
                </button>
              </form>
              {"" if waehrung == "EUR" else f'''
              <hr style="border:none;border-top:1px solid var(--border);margin:16px 0">
              <div style="font-size:12px;font-weight:600;color:var(--muted);margin-bottom:8px">
                💱 Wechselkurs ({waehrung} → EUR)
              </div>
              <form method="post" action="/beleg/{bid2}/kurs">
                <div class="form-grid form-grid-2" style="gap:8px">
                  <div class="form-group" style="margin:0">
                    <label>Kurs (1 {waehrung} = ? EUR)</label>
                    <input type="number" step="0.0001" name="kurs_eur"
                           value="{kurs_eur or ""}" placeholder="z.B. 0.9200"
                           class="inp" style="font-family:monospace">
                  </div>
                  <div class="form-group" style="margin:0">
                    <label>Kursdatum</label>
                    <input type="date" name="kurs_datum" class="inp"
                           value="{str(kurs_datum)[:10] if kurs_datum else ""}">
                  </div>
                  <div class="form-group full" style="margin:0">
                    <label>Quelle (z.B. EZB, Bank)</label>
                    <input type="text" name="kurs_quelle" class="inp"
                           value="{kurs_quelle or ""}" placeholder="EZB / Kreditkarte">
                  </div>
                </div>
                <div style="margin-top:8px;padding:8px;background:var(--blue-l);
                            border-radius:var(--radius-s);font-size:12px;color:var(--blue)">
                  EUR-Betrag: <b>{f"{float(betrag_brutto)*float(kurs_eur):.2f} EUR" if betrag_brutto and kurs_eur else "wird berechnet"}</b>
                </div>
                <button type="submit" class="btn btn-success" style="margin-top:8px;width:100%">
                  💱 Kurs speichern
                </button>
              </form>'''}
            </div>
          </div>
        </div>

        {seg_html}

        <div class="card" style="margin-top:16px">
          <div class="card-header"><span class="card-title">📄 Rohtext (original)</span></div>
          <div class="card-body">
            <pre style="font-size:11px;white-space:pre-wrap;color:var(--muted);
                        max-height:200px;overflow-y:auto;background:var(--bg);
                        padding:12px;border-radius:var(--radius-s)">{(rohtext or "").replace("<","&lt;")[:3000]}</pre>
          </div>
        </div>"""
        return HTMLResponse(shell(f"Beleg #{bid2}", content))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:400]}</pre>'))

@app.get("/beleg/{bid}/loeschen")
def beleg_loeschen(bid: int):
    """Löscht einen Beleg unwiderruflich aus der Datenbank (Dateien in S3 bleiben)."""
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"DELETE FROM belege WHERE id={P}", (bid,))
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/belege", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/beleg/{bid}/neu-anonymisieren")
async def beleg_neu_anonymisieren_route(bid: int):
    """Führt die Anonymisierung für diesen Beleg erneut aus (z.B. nach Bugfix)."""
    result = await beleg_neu_anonymisieren(bid)
    if result.get("fehler"):
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{result["fehler"]}</div>'
            f'<a href="/beleg/{bid}" class="btn btn-secondary">Zurück</a>'))
    return RedirectResponse(f"/beleg/{bid}", status_code=303)

@app.post("/beleg/{bid}/belegart")
async def beleg_belegart_speichern(bid: int, request: Request):
    """Speichert manuell gewählte Belegart (Rechnung/Quittung/etc.)."""
    form = await request.form()
    belegart = (form.get("belegart") or "").strip() or None
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        cur.execute(f"UPDATE belege SET belegart={P} WHERE id={P}", (belegart, bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/beleg/{bid}/zuordnen")
async def beleg_zuordnen(bid: int, request: Request):
    form = await request.form()
    rcode = (form.get("reise_code") or "").strip() or None
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"UPDATE belege SET reise_code={P} WHERE id={P}", (rcode, bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/beleg/{bid}/pdf/{typ}")
def beleg_pdf(bid: int, typ: str):
    """Liefert Original-, Anon- oder Analyse-PDF aus S3."""
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT s3_original,s3_anon,s3_analyse FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r: return JSONResponse({"fehler": "Nicht gefunden"}, status_code=404)
        def get(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        keys = {"original": get(r,"s3_original",0),
                "anon": get(r,"s3_anon",1),
                "analyse": get(r,"s3_analyse",2)}
        key = keys.get(typ)
        if not key: return JSONResponse({"fehler": "Ungültiger Typ"}, status_code=400)
        from fastapi.responses import Response
        data = s3_download(key)
        return Response(content=data, media_type="application/pdf",
                        headers={"Content-Disposition": f"inline; filename=beleg_{bid}_{typ}.pdf"})
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.get("/unzugeordnet", response_class=HTMLResponse)
def belege_unzugeordnet():
    """Alle Belege ohne Reisezuordnung – müssen zugeordnet werden."""
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT id, transportart, transportart_freitext,
            dateiname, anbieter, betrag_brutto, waehrung,
            belegdatum, status, erstellt
            FROM belege WHERE reise_code IS NULL
            ORDER BY erstellt DESC""")
        rows = cur.fetchall()
        cur.execute("SELECT code, titel, abreise FROM reisen ORDER BY abreise DESC")
        reisen = cur.fetchall()
        cur.close(); db.close()

        def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

        if not rows:
            return HTMLResponse(shell("Alle Belege zugeordnet", """
            <div style="text-align:center;padding:60px 20px">
              <div style="font-size:48px;margin-bottom:16px">✅</div>
              <h1 style="font-size:20px;font-weight:700;margin-bottom:8px">
                Alle Belege zugeordnet</h1>
              <p style="color:#64748b;margin-bottom:20px">
                Es gibt keine offenen Belege.</p>
              <a href="/" class="btn btn-secondary">← Dashboard</a>
            </div>"""))

        # Reisen-Optionen für Dropdown
        r_opts = '<option value="">– Reise wählen –</option>'
        for rv in reisen:
            rc = get(rv,"code",0); rt = get(rv,"titel",1); ab = get(rv,"abreise",2)
            r_opts += f'<option value="{rc}">{rc} – {rt} ({fmt_date(ab)})</option>'

        typ_farben = {
            "Flug":"#dbeafe:#1e40af","Hotel":"#dcfce7:#166534",
            "Bahn":"#e0e7ff:#3730a3","Taxi":"#fef3c7:#92400e",
            "Mietwagen":"#fce7f3:#9d174d","Bewirtung":"#fff7ed:#9a3412",
            "Tanken":"#f0fdf4:#14532d","Sonstiges":"#f1f5f9:#475569"
        }

        karten = ""
        for r in rows:
            bid=get(r,"id",0); typ=get(r,"transportart",1)
            freitext=get(r,"transportart_freitext",2) or ""
            datei=get(r,"dateiname",3); vendor=get(r,"anbieter",4)
            betrag=get(r,"betrag_brutto",5); waehrung=get(r,"waehrung",6)
            bd=get(r,"belegdatum",7); zusamm=get(r,"status",8)

            tc = typ_farben.get(typ or "Sonstiges","#f1f5f9:#475569").split(":")
            typ_badge = (f'<span style="background:{tc[0]};color:{tc[1]};'
                        f'padding:2px 8px;border-radius:4px;font-size:11px;'
                        f'font-weight:700">{typ or "?"}</span>')
            bet_s = f"{float(betrag):.2f} {waehrung}" if betrag else "–"

            karten += f"""
            <div class="card" style="border-left:4px solid #ef4444">
              <div class="card-body">
                <div style="display:flex;justify-content:space-between;
                            align-items:flex-start;gap:16px;flex-wrap:wrap">
                  <div style="flex:1;min-width:200px">
                    <div style="display:flex;gap:8px;align-items:center;margin-bottom:8px">
                      {typ_badge}
                      <span style="font-size:12px;color:#64748b">{datei[:40]}</span>
                    </div>
                    <div style="font-weight:700;font-size:15px;margin-bottom:4px">
                      {vendor or "Unbekannter Anbieter"}</div>
                    <div style="display:flex;gap:16px;flex-wrap:wrap">
                      <span style="font-weight:700;color:#059669">{bet_s}</span>
                      <span style="color:#64748b">{fmt_date(bd)}</span>
                    </div>
                    {f'<div style="font-size:12px;color:#94a3b8;margin-top:4px">{zusamm}</div>' if zusamm else ''}
                  </div>
                  <div style="display:flex;flex-direction:column;gap:8px;min-width:280px">
                    <form method="post" action="/beleg/{bid}/zuordnen"
                          style="display:flex;gap:8px">
                      <select name="reise_code" style="flex:1;padding:7px 10px;
                              border:1px solid #d1d5db;border-radius:6px;font-size:13px">
                        {r_opts}
                      </select>
                      <button type="submit" class="btn btn-success btn-sm"
                              style="white-space:nowrap">✓ Zuordnen</button>
                    </form>
                    <a href="/beleg/{bid}" class="btn btn-secondary btn-sm"
                       style="text-align:center">Detail ansehen</a>
                  </div>
                </div>
              </div>
            </div>"""

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;
                    margin-bottom:20px">
          <div>
            <h1 class="page-title" style="margin:0">⚠ Unzugeordnete Belege</h1>
            <p style="color:#64748b;margin-top:4px;font-size:13px">
              {len(rows)} Beleg{"e" if len(rows)!=1 else ""} ohne Reisezuordnung.
              Bitte jeden Beleg einer Reise zuordnen.
            </p>
          </div>
          <a href="/" class="btn btn-secondary">← Dashboard</a>
        </div>
        {karten}"""
        return HTMLResponse(shell("Unzugeordnete Belege", content))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:400]}</pre>'))


@app.get("/belege", response_class=HTMLResponse)
def belege_liste():
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT b.id, b.reise_code, b.transportart, b.anbieter,
            b.betrag_brutto, b.waehrung, b.belegdatum, b.status,
            b.dateiname, b.pflichtfelder_ok, b.fehlende_felder
            FROM belege b ORDER BY b.erstellt DESC LIMIT 100""")
        rows = cur.fetchall()
        cur.close(); db.close()

        def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

        typ_farben = {
            "Flug":"badge-blue","Hotel":"badge-green","Bahn":"badge-blue",
            "Taxi":"badge-amber","Mietwagen":"badge-red","Tanken":"badge-green",
            "Verpflegung":"badge-amber","Bewirtung":"badge-amber","Sonstiges":"badge-gray"
        }
        zeilen = ""
        for r in rows:
            bid=get(r,"id",0); rcode=get(r,"reise_code",1); typ=get(r,"transportart",2)
            vendor=get(r,"anbieter",3); betrag=get(r,"betrag_brutto",4)
            waehrung=get(r,"waehrung",5); bd=get(r,"belegdatum",6)
            status=get(r,"status",7); datei=get(r,"dateiname",8)
            pf_ok=get(r,"pflichtfelder_ok",9)
            bc = typ_farben.get(typ or "","badge-gray")
            bet_s = f"{float(betrag):.2f} {waehrung}" if betrag else "–"
            stat_b = ('<span class="badge badge-green">✓</span>' if status=="ok"
                      else '<span class="badge badge-red">✗</span>' if status=="fehler"
                      else '<span class="badge badge-amber">…</span>')
            zeilen += (f'<tr>'
                f'<td><a href="/beleg/{bid}" style="color:var(--blue);font-weight:600">#{bid}</a></td>'
                f'<td><span class="badge {bc}">{typ or "?"}</span></td>'
                f'<td style="font-weight:500">{vendor or datei[:30]}</td>'
                f'<td style="font-weight:600;color:var(--green)">{bet_s}</td>'
                f'<td>{fmt_date(bd)}</td>'
                f'<td style="font-family:monospace;font-size:12px;color:var(--blue)">{rcode or "–"}</td>'
                f'<td>{stat_b}</td>'
                f'<td><a href="/beleg/{bid}" class="btn btn-secondary btn-sm">Detail</a></td>'
                f'</tr>')

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:20px">
          <h1 class="page-title" style="margin:0">Belege ({len(rows)})</h1>
          <div style="display:flex;gap:8px">
            <a href="/mails-abrufen" class="btn btn-success">📬 Mails abrufen</a>
            <a href="/beleg/upload" class="btn btn-primary">+ Beleg hochladen</a>
          </div>
        </div>
        <div class="card">
          <div class="table-wrap"><table>
            <thead><tr>
              <th>#</th><th>Typ</th><th>Anbieter</th><th>Betrag</th>
              <th>Datum</th><th>Reise</th><th>Status</th><th></th>
            </tr></thead>
            <tbody>
              {zeilen or '<tr><td colspan="8"><div class="empty-state">Noch keine Belege – <a href="/beleg/upload">Ersten Beleg hochladen</a></div></td></tr>'}
            </tbody>
          </table></div>
        </div>"""
        return HTMLResponse(shell("Belege", content))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/debug-anon")
def debug_anon():
    """Zeigt welche Namen für Anonymisierung geladen werden."""
    namen, mails = lade_ma_daten()
    return {"namen": namen, "mails": mails,
            "anzahl": len(namen),
            "hinweis": "Wenn leer: Mitarbeiter neu anlegen unter /mitarbeiter/neu"}


# ═══════════════════════════════════════════════════════════════════════════════
# MAIL-IMPORT
# ═══════════════════════════════════════════════════════════════════════════════

import imaplib, email as _email_mod
from email.header import decode_header as _decode_header


# ── Mail-Route ────────────────────────────────────────────────────────────────
@app.get("/mails-abrufen", response_class=HTMLResponse)
async def mails_abrufen():
    """Holt ungelesene Mails und verarbeitet sie als Belege."""
    result = await fetch_mails()

    if "fehler" in result and "importiert" not in result:
        body = f'''
        <div class="alert alert-err">
          <b>Fehler:</b> {result["fehler"]}
        </div>
        <a href="/belege" class="btn btn-secondary">← Zurück</a>'''
    else:
        fehler_html = ""
        if result.get("fehler_details"):
            items = "".join(f"<li>{d}</li>" for d in result["fehler_details"])
            fehler_html = f'<div class="alert alert-warn" style="margin-top:12px"><b>Fehlerdetails:</b><ul>{items}</ul></div>'

        body = f'''
        <h1 class="page-title">📬 Mails abgerufen</h1>
        <div class="alert alert-ok" style="margin-bottom:16px">
          ✓ {result.get("importiert",0)} Mails verarbeitet &nbsp;·&nbsp;
          {result.get("belege_erstellt",0)} Belege erstellt &nbsp;·&nbsp;
          {result.get("duplikate",0)} Duplikate &nbsp;·&nbsp;
          {result.get("fehler",0)} Fehler
        </div>
        {fehler_html}
        <div style="display:flex;gap:8px;margin-top:16px">
          <a href="/belege" class="btn btn-primary">📋 Belege ansehen</a>
          <a href="/unzugeordnet" class="btn btn-secondary">📬 Posteingang</a>
          <a href="/" class="btn btn-secondary">← Dashboard</a>
        </div>'''

    return HTMLResponse(shell("Mails abrufen", body))


# ═══════════════════════════════════════════════════════════════════════════════
# VMA-TAGE LOGIK
# ═══════════════════════════════════════════════════════════════════════════════


# ── VMA-Routen ────────────────────────────────────────────────────────────────
@app.get("/reise/{code}/vma-generieren")
async def vma_generieren(code: str):
    """Generiert VMA-Tage aus Belegen und Länder-Einträgen."""
    try:
        db = get_db()
        n = vma_tage_generieren(code.upper(), db)
        db.close()
        return RedirectResponse(f"/reise/{code.upper()}/uebersicht", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.post("/reise/{code}/vma/{vid}/speichern")
async def vma_tag_speichern(code: str, vid: int, request: Request):
    """Speichert manuell geänderten VMA-Tag."""
    form = await request.form()
    frueh   = bool(form.get("fruehstueck"))
    mittag  = bool(form.get("mittagessen"))
    abend   = bool(form.get("abendessen"))
    lcode   = (form.get("land_code") or "DE").strip().upper()
    ist_halb= bool(form.get("ist_halber_satz"))
    notiz   = (form.get("notiz") or "").strip()

    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        # Sätze aus DB oder VMA-Tabelle
        satz = VMA_SAETZE.get(lcode, VMA_SAETZE["DE"])
        voll = satz["voll"]; halb = satz["halb"]
        lname = satz.get("name", lcode)
        brutto, netto = vma_berechnen(voll, halb, ist_halb, frueh, mittag, abend)
        cur.execute(f"""UPDATE vma_tage SET
            land_code={P}, land_name={P}, vma_satz_voll={P}, vma_satz_halb={P},
            ist_halber_satz={P}, fruehstueck={P}, mittagessen={P}, abendessen={P},
            vma_brutto={P}, vma_netto={P}, quelle='manuell', notiz={P}
            WHERE id={P}""",
            (lcode, lname, voll, halb, ist_halb, frueh, mittag, abend,
             brutto, netto, notiz or None, vid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{code.upper()}/uebersicht", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)

@app.get("/reise/{code}/uebersicht", response_class=HTMLResponse)
def reise_uebersicht(code: str):
    """Reise-Übersicht: Timeline mit VMA pro Tag + Belegen."""
    rcode = code.upper()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()

        # Reise
        cur.execute(f"SELECT code,titel,abreise,rueckkehr FROM reisen WHERE code={P}", (rcode,))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Nicht gefunden</div>'))
        def g(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        titel=g(r,"titel",1); ab=g(r,"abreise",2); zu=g(r,"rueckkehr",3)

        # VMA-Tage
        cur.execute(f"""SELECT id,datum,land_code,land_name,vma_satz_voll,vma_satz_halb,
            ist_halber_satz,fruehstueck,mittagessen,abendessen,
            vma_brutto,vma_netto,quelle,notiz
            FROM vma_tage WHERE reise_code={P} ORDER BY datum""", (rcode,))
        vma_rows = cur.fetchall()
        vma_by_date = {}
        for vr in vma_rows:
            d = g(vr,"datum",1)
            if isinstance(d, str): d = date.fromisoformat(d[:10])
            vma_by_date[d] = vr

        # Belege
        cur.execute(f"""SELECT id,transportart,transportart_freitext,anbieter,
            betrag_brutto,waehrung,event_datum_von,event_datum_bis,
            event_ort_von,event_ort_bis,hotel_name,hotel_checkin_datum,
            hotel_checkin_zeit,hotel_checkout_datum,hotel_checkout_zeit,
            hotel_naechte,s3_original,s3_anon,s3_analyse,ki_json,belegdatum
            FROM belege WHERE reise_code={P}
            ORDER BY COALESCE(event_datum_von,belegdatum)""", (rcode,))
        belege = cur.fetchall()
        cur.close(); db.close()

        # Reisedaten ermitteln
        if isinstance(ab, str): ab = date.fromisoformat(ab[:10])
        if isinstance(zu, str): zu = date.fromisoformat(zu[:10])
        tage_gesamt = (zu - ab).days + 1

        # Belege nach Tag gruppieren
        def beleg_datum(b):
            ev = g(b,"event_datum_von",6)
            bd = g(b,"belegdatum",20)
            for v in [ev, bd]:
                if v:
                    if isinstance(v, date): return v
                    try: return date.fromisoformat(str(v)[:10])
                    except: pass
            return None

        belege_by_date = {}
        for b in belege:
            bd = beleg_datum(b)
            if bd:
                belege_by_date.setdefault(bd, []).append(b)
            else:
                belege_by_date.setdefault(None, []).append(b)

        # Farben pro Transportart
        BADGE = {
            "Flug": '<span style="background:#dbeafe;color:#1e40af;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">✈ Flug</span>',
            "Hotel": '<span style="background:#dcfce7;color:#166534;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">🏨 Hotel</span>',
            "Mietwagen": '<span style="background:#ede9fe;color:#5b21b6;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">🚗 Mietwagen</span>',
            "Taxi": '<span style="background:#fef3c7;color:#92400e;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">🚕 Taxi</span>',
            "Bahn": '<span style="background:#e0e7ff;color:#3730a3;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">🚆 Bahn</span>',
            "Tanken": '<span style="background:#f0fdf4;color:#14532d;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">⛽ Tanken</span>',
            "Verpflegung": '<span style="background:#fff7ed;color:#9a3412;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">🍽 Verpflegung</span>',
            "Bewirtung": '<span style="background:#fff7ed;color:#9a3412;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">🍽 Bewirtung</span>',
            "Sonstiges": '<span style="background:#f1f5f9;color:#475569;padding:2px 8px;border-radius:12px;font-size:11px;font-weight:500">📄 Sonstiges</span>',
        }

        # VMA Gesamtsumme
        vma_total = sum(
            float(g(vr,"vma_netto",11) or 0) for vr in vma_rows)
        kosten_total = sum(
            float(g(b,"betrag_brutto",4) or 0) for b in belege)

        # HTML generieren
        rows_html = ""
        wochentage = ["Mo","Di","Mi","Do","Fr","Sa","So"]
        monate = ["Jan","Feb","Mär","Apr","Mai","Jun",
                  "Jul","Aug","Sep","Okt","Nov","Dez"]

        def cb(rcode, vid, lcode, ist_halb, name, checked, label, abzug_pct):
            ch = "checked" if checked else ""
            return (f'<label style="display:inline-flex;align-items:center;gap:4px;'
                    f'cursor:pointer;font-size:12px;color:#374151;margin-right:10px">'
                    f'<input type="checkbox" name="{name}" value="1" {ch} '
                    f'onchange="this.form.submit()" style="width:auto;margin:0">'
                    f'{label} <span style="color:#c81e1e;font-size:10px">-{abzug_pct}%</span>'
                    f'</label>')

        def beleg_zeile(b):
            bid = g(b,"id",0); typ = g(b,"transportart",1) or "Sonstiges"
            freitext = g(b,"transportart_freitext",2) or ""
            anbieter = g(b,"anbieter",3) or "–"
            betrag = g(b,"betrag_brutto",4)
            waehrung = g(b,"waehrung",5) or "EUR"
            ort_von = g(b,"event_ort_von",8) or ""
            ort_bis = g(b,"event_ort_bis",9) or ""
            hotel_name = g(b,"hotel_name",10) or ""
            ci_dat = g(b,"hotel_checkin_datum",11)
            ci_zeit = g(b,"hotel_checkin_zeit",12) or ""
            co_dat = g(b,"hotel_checkout_datum",13)
            co_zeit = g(b,"hotel_checkout_zeit",14) or ""
            naechte = g(b,"hotel_naechte",15) or ""

            seg_info = ""
            ki_str = g(b,"ki_json",19) or ""
            if ki_str:
                try:
                    ki = json.loads(ki_str)
                    segs = ki.get("segmente") or []
                    if segs:
                        seg_parts = []
                        for s in segs:
                            fn = s.get("transport_nummer","") or ""
                            tn = s.get("transport_name","") or ""
                            vi = s.get("von_iata","") or ""
                            ni = s.get("nach_iata","") or ""
                            ab_z = s.get("abreise_zeit","") or ""
                            an_z = s.get("ankunft_zeit","") or ""
                            tz_ab = s.get("abreise_zeitzone","") or ""
                            tz_an = s.get("ankunft_zeitzone","") or ""
                            hin = s.get("hinweis","") or ""
                            p = f"{tn} {fn}: {vi}→{ni} {ab_z}{' '+tz_ab if tz_ab else ''}–{an_z}{' '+tz_an if tz_an else ''}"
                            if hin: p += f" ({hin})"
                            seg_parts.append(p)
                        seg_info = " · ".join(seg_parts)
                except: pass

            if not seg_info:
                if hotel_name:
                    ci_s = fmt_date(ci_dat) + (" " + ci_zeit if ci_zeit else "")
                    co_s = fmt_date(co_dat) + (" " + co_zeit if co_zeit else "")
                    seg_info = f"{hotel_name} · Check-in {ci_s} · Check-out {co_s}"
                    if naechte: seg_info += f" · {naechte} Nächte"
                elif ort_von or ort_bis:
                    seg_info = f"{ort_von}" + (f" → {ort_bis}" if ort_bis else "")

            badge = BADGE.get(typ, BADGE["Sonstiges"])
            if freitext: badge = badge.replace("Sonstiges", f"Sonstiges – {freitext}")
            bet_s = f"{float(betrag):.2f} {waehrung}" if betrag else "–"

            return (
                f'<div style="display:flex;align-items:flex-start;justify-content:space-between;'
                f'gap:8px;margin-bottom:6px">'
                f'<div style="min-width:0">{badge}<br>'
                f'<span style="font-size:12px;font-weight:600">{anbieter}</span>'
                + (f'<br><span style="font-size:11px;color:#64748b">{seg_info}</span>' if seg_info else "")
                + f'</div>'
                f'<div style="text-align:right;white-space:nowrap">'
                f'<div style="font-size:12px;font-weight:600">{bet_s}</div>'
                f'<a href="/beleg/{bid}" style="font-size:11px;color:#2563eb;text-decoration:none">Detail</a>'
                f'</div></div>')

        QUELLE_LABEL = {
            "Flug-Segment": "auto · Flug",
            "Hotel-Beleg": "auto · Hotel",
            "Manuell": "auto · Reise-Land",
            "Standard": "auto · Standard",
            "manuell": "✎ manuell",
        }

        for i in range(tage_gesamt):
            tag = ab + timedelta(days=i)
            wt = wochentage[tag.weekday()]
            datum_s = f"{wt} {tag.day:02d}.{tag.month:02d}."
            zeile_bg = "#fafbfe" if i % 2 == 1 else "#ffffff"

            # Belege für diesen Tag
            tages_belege = belege_by_date.get(tag, [])
            tages_summe = sum(float(g(b,"betrag_brutto",4) or 0) for b in tages_belege)
            belege_col = "".join(beleg_zeile(b) for b in tages_belege) or \
                '<span style="font-size:11px;color:#94a3b8;font-style:italic">keine Belege</span>'

            vr = vma_by_date.get(tag)
            if vr:
                vid = g(vr,"id",0)
                lcode = g(vr,"land_code",2) or "DE"
                lname = g(vr,"land_name",3) or "Deutschland"
                voll = float(g(vr,"vma_satz_voll",4) or 0)
                halb = float(g(vr,"vma_satz_halb",5) or 0)
                ist_halb = bool(g(vr,"ist_halber_satz",6))
                frueh = bool(g(vr,"fruehstueck",7))
                mittag = bool(g(vr,"mittagessen",8))
                abend = bool(g(vr,"abendessen",9))
                vma_netto = float(g(vr,"vma_netto",11) or 0)
                quelle = g(vr,"quelle",12) or "Standard"

                basis_txt = f"{halb:.2f} €" if ist_halb else f"{voll:.2f} €"
                halb_label = "½ Anreisetag" if i == 0 else ("½ Abreisetag" if i == tage_gesamt-1 else "½ Satz")
                halb_badge = f'<span style="font-size:10px;background:#fef3c7;color:#92400e;padding:1px 7px;border-radius:10px">{halb_label}</span>' if ist_halb else ""
                quelle_txt = QUELLE_LABEL.get(quelle, quelle)
                quelle_color = "#7c3aed" if quelle == "manuell" else "#94a3b8"

                zeile = (
                    f'<tr style="background:{zeile_bg};border-bottom:1px solid #f3f4f6">'
                    f'<td style="padding:10px 12px;vertical-align:top;white-space:nowrap">'
                    f'<div style="font-weight:600;color:#0d1b2a">{datum_s}</div>{halb_badge}</td>'
                    f'<td style="padding:10px 12px;vertical-align:top">'
                    f'<div style="font-weight:500">🌍 {lname} ({lcode})</div>'
                    f'<div style="font-size:11px;color:{quelle_color}">{quelle_txt}</div></td>'
                    f'<td style="padding:10px 12px;vertical-align:top">'
                    f'<form method="post" action="/reise/{rcode}/vma/{vid}/speichern">'
                    f'<input type="hidden" name="land_code" value="{lcode}">'
                    f'<input type="hidden" name="ist_halber_satz" value="{"1" if ist_halb else ""}">'
                    f'{cb(rcode, vid, lcode, ist_halb, "fruehstueck", frueh, "Frühstück", 20)}'
                    f'{cb(rcode, vid, lcode, ist_halb, "mittagessen", mittag, "Mittag", 40)}'
                    f'{cb(rcode, vid, lcode, ist_halb, "abendessen", abend, "Abend", 40)}'
                    f'</form></td>'
                    f'<td style="padding:10px 12px;text-align:right;vertical-align:top;white-space:nowrap">'
                    f'<div style="text-decoration:line-through;color:#94a3b8;font-size:11px">{basis_txt}</div>'
                    f'<div style="font-weight:700;color:#047857">{vma_netto:.2f} €</div></td>'
                    f'<td style="padding:10px 12px;vertical-align:top">{belege_col}'
                    + (f'<div style="font-size:11px;color:#5a6a7a;margin-top:2px">Summe: {tages_summe:.2f} €</div>' if tages_belege else "")
                    + '</td></tr>'
                )
            else:
                zeile = (
                    f'<tr style="background:{zeile_bg};border-bottom:1px solid #f3f4f6">'
                    f'<td style="padding:10px 12px;vertical-align:top;white-space:nowrap">'
                    f'<div style="font-weight:600;color:#0d1b2a">{datum_s}</div></td>'
                    f'<td colspan="3" style="padding:10px 12px;font-size:12px;color:#94a3b8">'
                    f'VMA nicht berechnet – '
                    f'<a href="/reise/{rcode}/vma-generieren" style="color:#2563eb">Generieren</a></td>'
                    f'<td style="padding:10px 12px;vertical-align:top">{belege_col}</td></tr>'
                )

            rows_html += zeile

        # Belege außerhalb des Reisezeitraums (z.B. Datum nach Rückkehr)
        tage_im_bereich = {ab + timedelta(days=i) for i in range(tage_gesamt)}
        ausserhalb = {d: bs for d, bs in belege_by_date.items()
                      if d is not None and d not in tage_im_bereich}
        if ausserhalb:
            anzahl_ausserhalb = sum(len(bs) for bs in ausserhalb.values())
            rows_html += (
                f'<tr><td colspan="5" style="padding:10px 12px;font-size:12px;'
                f'color:#b45309;font-style:italic">'
                f'⚠ Belege außerhalb des Reisezeitraums ({anzahl_ausserhalb})</td></tr>'
            )
            for d in sorted(ausserhalb.keys()):
                tag_html = "".join(beleg_zeile(b) for b in ausserhalb[d])
                rows_html += (
                    f'<tr><td style="padding:10px 12px;vertical-align:top;white-space:nowrap">'
                    f'<div style="font-weight:600;color:#0d1b2a">{fmt_date(d)}</div></td>'
                    f'<td colspan="3" style="padding:10px 12px;font-size:12px;color:#94a3b8">'
                    f'liegt nicht im Reisezeitraum ({fmt_date(ab)} – {fmt_date(zu)})</td>'
                    f'<td style="padding:10px 12px;vertical-align:top">{tag_html}</td></tr>'
                )

        # Belege ohne Datum
        undated = belege_by_date.get(None, [])
        if undated:
            undated_html = "".join(beleg_zeile(b) for b in undated)
            rows_html += (
                f'<tr><td colspan="5" style="padding:10px 12px;font-size:12px;'
                f'color:#94a3b8;font-style:italic">Belege ohne Datum ({len(undated)})</td></tr>'
                f'<tr><td colspan="5" style="padding:10px 12px">{undated_html}</td></tr>'
            )

        content = f"""
        <div style="display:flex;align-items:flex-start;justify-content:space-between;
                    margin-bottom:20px;flex-wrap:wrap;gap:12px">
          <div>
            <div style="font-family:monospace;font-size:12px;color:#64748b">{rcode}</div>
            <h1 class="page-title" style="margin:4px 0">{titel}</h1>
            <div style="font-size:13px;color:#64748b">
              📅 {fmt_date(ab)} – {fmt_date(zu)} &nbsp;·&nbsp;
              {tage_gesamt} Tage
            </div>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <a href="/reise/{rcode}/vma-generieren" class="btn btn-success">
              🔄 VMA neu berechnen</a>
            <a href="/reise/{rcode}" class="btn btn-secondary">← Reise</a>
          </div>
        </div>

        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:20px">
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:500;color:#059669">{vma_total:.2f} €</div>
            <div style="font-size:12px;color:#64748b">VMA gesamt (netto)</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:500">{kosten_total:.2f} €</div>
            <div style="font-size:12px;color:#64748b">Kosten aus Belegen</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:500">{len(belege)}</div>
            <div style="font-size:12px;color:#64748b">Belege</div>
          </div></div>
        </div>

        <div class="card" style="padding:0;overflow:hidden">
          <table style="width:100%;border-collapse:collapse">
            <thead>
              <tr style="background:#f8fafc;border-bottom:1px solid #e2e8f0">
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:left;width:120px">Tag</th>
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:left;width:170px">Land / Satz</th>
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:left;width:220px">Mahlzeiten</th>
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:right;width:100px">VMA netto</th>
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:left">Belege</th>
              </tr>
            </thead>
            <tbody>
              {rows_html}
            </tbody>
          </table>
        </div>"""

        return HTMLResponse(shell(f"Übersicht {rcode}", content, "reisen"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'))



# ── System-Routen ─────────────────────────────────────────────────────────────
@app.post("/beleg/{bid}/kurs")
async def beleg_kurs_speichern(bid: int, request: Request):
    """Speichert Wechselkurs für Auslandsbeleg."""
    form = await request.form()
    try:
        kurs = float(form.get("kurs_eur") or 0) or None
        kurs_datum = (form.get("kurs_datum") or "").strip() or None
        kurs_quelle = (form.get("kurs_quelle") or "").strip() or None
        P = ph()
        db = get_db(); cur = db.cursor()
        # Betrag_eur berechnen
        cur.execute(f"SELECT betrag_brutto FROM belege WHERE id={P}", (bid,))
        r = cur.fetchone()
        brutto = float(r[0] if isinstance(r,tuple) else r["betrag_brutto"]) if r else 0
        betrag_eur = round(brutto * kurs, 2) if kurs and brutto else None
        cur.execute(f"""UPDATE belege SET
            kurs_eur={P}, betrag_eur={P}, kurs_datum={P}, kurs_quelle={P}
            WHERE id={P}""",
            (kurs, betrag_eur, kurs_datum, kurs_quelle, bid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/beleg/{bid}", status_code=303)
    except Exception as e:
        return JSONResponse({"fehler": str(e)}, status_code=500)


@app.get("/reise/{code}/abschluss", response_class=HTMLResponse)
def reise_abschluss(code: str):
    """
    Abschluss-Übersicht einer Reise:
    - VMA-Tabelle komplett
    - Kostenaufstellung (nur Rechnungen/Quittungen)
    - Hinweise bei fehlenden Rechnungen
    - Wechselkurs-Status
    """
    rcode = code.upper()
    try:
        db = get_db(); cur = db.cursor()
        P = ph()

        cur.execute(f"""SELECT code,titel,abreise,rueckkehr,notiz
            FROM reisen WHERE code={P}""", (rcode,))
        r = cur.fetchone()
        if not r:
            cur.close(); db.close()
            return HTMLResponse(shell("Fehler",
                '<div class="alert alert-err">Reise nicht gefunden</div>'))
        def g(row,k,i): return row[k] if hasattr(row,'keys') else row[i]
        titel=g(r,"titel",1); ab=g(r,"abreise",2); zu=g(r,"rueckkehr",3)

        # Mitarbeiter
        cur.execute(f"""SELECT m.kuerzel, m.klarname FROM mitarbeiter m
            JOIN reise_mitarbeiter rm ON rm.kuerzel=m.kuerzel
            WHERE rm.reise_code={P} ORDER BY m.klarname""", (rcode,))
        ma_rows = cur.fetchall()

        # VMA-Tage
        cur.execute(f"""SELECT datum,land_code,land_name,vma_satz_voll,vma_satz_halb,
            ist_halber_satz,fruehstueck,mittagessen,abendessen,vma_brutto,vma_netto
            FROM vma_tage WHERE reise_code={P} ORDER BY datum""", (rcode,))
        vma_rows = cur.fetchall()

        # Alle Belege
        cur.execute(f"""SELECT id,belegart,transportart,transportart_freitext,
            anbieter,rechnungsnummer,belegdatum,
            betrag_brutto,betrag_netto,betrag_mwst,waehrung,
            land_beleg,betrag_eur,kurs_eur,kurs_datum,kurs_quelle,
            s3_original,status
            FROM belege WHERE reise_code={P}
            ORDER BY belegdatum NULLS LAST, id""", (rcode,))
        belege = cur.fetchall()
        cur.close(); db.close()

        # VMA berechnen
        vma_total_netto = sum(float(g(v,"vma_netto",10) or 0) for v in vma_rows)

        # Belege kategorisieren
        rechnungen = []      # Rechnung/Quittung/Receipt
        bestaetigung = []    # Nur Buchungsbestätigung
        kurs_fehlt = []      # Auslandsbelege ohne Kurs

        RECHNUNG_ARTEN = {"rechnung","quittung","receipt"}
        for b in belege:
            art = (g(b,"belegart",1) or "").lower()
            is_rechnung = any(r in art for r in RECHNUNG_ARTEN)
            waehrung = g(b,"waehrung",10) or "EUR"
            if is_rechnung:
                rechnungen.append(b)
                if waehrung != "EUR" and not g(b,"kurs_eur",13):
                    kurs_fehlt.append(b)
            else:
                bestaetigung.append(b)

        kosten_eur = sum(
            float(g(b,"betrag_eur",12) or g(b,"betrag_brutto",7) or 0)
            for b in rechnungen
            if (g(b,"waehrung",10) or "EUR") == "EUR" or g(b,"betrag_eur",12))

        # Wochentage
        wt = ["Mo","Di","Mi","Do","Fr","Sa","So"]
        mo = ["Jan","Feb","Mär","Apr","Mai","Jun",
              "Jul","Aug","Sep","Okt","Nov","Dez"]

        def fdat(d):
            if not d: return "–"
            if isinstance(d, date): return d.strftime("%d.%m.%Y")
            try: return date.fromisoformat(str(d)[:10]).strftime("%d.%m.%Y")
            except: return str(d)[:10]

        # ── VMA-Tabelle ───────────────────────────────────────────────────────
        vma_html = ""
        for v in vma_rows:
            dat = g(v,"datum",0)
            if isinstance(dat,str): dat = date.fromisoformat(dat[:10])
            lcode=g(v,"land_code",1); lname=g(v,"land_name",2)
            voll=float(g(v,"vma_satz_voll",3) or 0)
            halb=float(g(v,"vma_satz_halb",4) or 0)
            ist_halb=bool(g(v,"ist_halber_satz",5))
            frueh=bool(g(v,"fruehstueck",6))
            mitt=bool(g(v,"mittagessen",7))
            abend=bool(g(v,"abendessen",8))
            netto=float(g(v,"vma_netto",10) or 0)

            abzuege = []
            if frueh: abzuege.append("Frühstück")
            if mitt: abzuege.append("Mittagessen")
            if abend: abzuege.append("Abendessen")

            vma_html += f"""<tr>
                <td>{wt[dat.weekday()]} {dat.strftime("%d.%m.%Y")}</td>
                <td><span style="font-family:monospace;font-size:11px;
                    background:#f1f5f9;padding:1px 6px;border-radius:4px">{lcode}</span>
                    {lname}</td>
                <td style="text-align:right;font-family:monospace">
                    {"½ " if ist_halb else ""}{halb if ist_halb else voll:.2f} €</td>
                <td style="font-size:12px;color:#64748b">
                    {", ".join(abzuege) if abzuege else "–"}</td>
                <td style="text-align:right;font-weight:600;color:#059669;
                    font-family:monospace">{netto:.2f} €</td>
            </tr>"""

        # ── Kosten-Tabelle ────────────────────────────────────────────────────
        kosten_html = ""
        summen = {}  # pro Transportart
        for b in rechnungen:
            bid2=g(b,"id",0); art=g(b,"belegart",1) or "–"
            typ=g(b,"transportart",2) or "Sonstiges"
            freitext=g(b,"transportart_freitext",3) or ""
            anbieter=g(b,"anbieter",4) or "–"
            rechnr=g(b,"rechnungsnummer",5) or "–"
            bd=g(b,"belegdatum",6)
            brutto=g(b,"betrag_brutto",7); netto_b=g(b,"betrag_netto",8)
            mwst=g(b,"betrag_mwst",9); waehrung=g(b,"waehrung",10) or "EUR"
            land=g(b,"land_beleg",11) or ""
            betrag_eur_b=g(b,"betrag_eur",12); kurs=g(b,"kurs_eur",13)

            typ_label = typ + (f" – {freitext}" if freitext else "")

            # Betrag-Spalte
            if waehrung == "EUR":
                bet_s = f"{float(brutto):.2f} EUR" if brutto else "–"
                mwst_s = (f"MwSt: {float(mwst):.2f} EUR"
                          if mwst and land == "DE" else
                          "VAT: nicht abzugsfähig (Ausland)" if mwst and land != "DE"
                          else "")
                eur_val = float(brutto) if brutto else 0
            else:
                bet_s = f"{float(brutto):.2f} {waehrung}" if brutto else "–"
                if betrag_eur_b:
                    bet_s += f" = {float(betrag_eur_b):.2f} EUR (Kurs: {kurs})"
                    eur_val = float(betrag_eur_b)
                else:
                    bet_s += f' <span style="color:#ef4444">⚠ Kurs fehlt</span>'
                    eur_val = 0
                mwst_s = "Auslandsbeleg – Vorsteuer nicht abzugsfähig"

            summen[typ] = summen.get(typ, 0) + eur_val

            kosten_html += f"""<tr>
                <td>{fdat(bd)}</td>
                <td><span style="font-size:11px;background:#f1f5f9;padding:1px 6px;
                    border-radius:4px">{typ_label}</span></td>
                <td><b>{anbieter}</b></td>
                <td style="font-family:monospace;font-size:11px;color:#64748b">{rechnr}</td>
                <td style="text-align:right;font-family:monospace">
                    {bet_s}</td>
                <td style="font-size:11px;color:#64748b">{mwst_s}</td>
                <td>
                  <a href="/beleg/{bid2}/pdf/original" target="_blank"
                     class="btn btn-secondary btn-sm">PDF</a>
                </td>
            </tr>"""

        # Summen pro Kategorie
        summen_html = ""
        for typ, summe in sorted(summen.items()):
            summen_html += f"""<tr style="background:#f8fafc">
                <td colspan="4" style="text-align:right;font-size:12px;
                    color:#64748b;padding:6px 14px">Summe {typ}</td>
                <td style="text-align:right;font-family:monospace;
                    font-weight:600;padding:6px 14px">{summe:.2f} EUR</td>
                <td colspan="2"></td>
            </tr>"""

        # Buchungsbestätigungen (Hinweis)
        best_html = ""
        if bestaetigung:
            items = ""
            for b in bestaetigung:
                typ=g(b,"transportart",2) or "?"
                anbieter=g(b,"anbieter",4) or "–"
                bid2=g(b,"id",0)
                items += (f'<li style="margin:4px 0">{typ}: {anbieter} – ' 
                          f'<a href="/beleg/{bid2}" style="color:var(--blue)">'
                          f'Beleg #{bid2}</a> ' 
                          f'<span style="color:#ef4444;font-size:11px">'
                          f'⚠ Keine Rechnung vorhanden</span></li>')
            best_html = f"""
            <div class="alert alert-warn" style="margin-top:16px">
              <b>⚠ Nur Buchungsbestätigung vorhanden – keine Rechnung:</b>
              <ul style="margin-top:8px;padding-left:16px">{items}</ul>
            </div>"""

        # Fehlende Kurse
        kurs_html = ""
        if kurs_fehlt:
            items = ""
            for b in kurs_fehlt:
                bid2=g(b,"id",0); w=g(b,"waehrung",10); brutto=g(b,"betrag_brutto",7)
                anbieter=g(b,"anbieter",4) or "–"
                items += (f'<li style="margin:4px 0">' 
                          f'<a href="/beleg/{bid2}" style="color:var(--blue)">' 
                          f'Beleg #{bid2}</a>: {anbieter} – {brutto} {w} ' 
                          f'<span style="color:#ef4444">⚠ Kurs fehlt</span></li>')
            kurs_html = f"""
            <div class="alert alert-err" style="margin-top:8px">
              <b>❌ Wechselkurs fehlt – bitte nachtragen:</b>
              <ul style="margin-top:8px;padding-left:16px">{items}</ul>
            </div>"""

        ma_str = " · ".join(f"{g(m,'kuerzel',0)} – {g(m,'klarname',1)}"
                             for m in ma_rows)

        content = f"""
        <div style="display:flex;align-items:flex-start;justify-content:space-between;
                    margin-bottom:20px;flex-wrap:wrap;gap:12px">
          <div>
            <div style="font-family:monospace;font-size:12px;color:#64748b">{rcode}</div>
            <h1 class="page-title" style="margin:4px 0">{titel}</h1>
            <div style="font-size:13px;color:#64748b">
              📅 {fdat(ab)} – {fdat(zu)} &nbsp;·&nbsp; 👤 {ma_str}
            </div>
          </div>
          <div style="display:flex;gap:8px;flex-wrap:wrap">
            <a href="/reise/{rcode}/abschluss/pdf" class="btn btn-primary">
              📄 PDF-Export
            </a>
            <a href="/reise/{rcode}/uebersicht" class="btn btn-secondary">
              📋 Übersicht
            </a>
            <a href="/reise/{rcode}" class="btn btn-secondary">← Reise</a>
          </div>
        </div>

        <!-- Zusammenfassung -->
        <div style="display:grid;grid-template-columns:repeat(3,1fr);gap:12px;margin-bottom:20px">
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:600;color:#059669">{vma_total_netto:.2f} €</div>
            <div style="font-size:12px;color:#64748b">VMA gesamt (netto)</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:600">{kosten_eur:.2f} €</div>
            <div style="font-size:12px;color:#64748b">Kosten (Rechnungen)</div>
          </div></div>
          <div class="card"><div class="card-body" style="text-align:center">
            <div style="font-size:24px;font-weight:600;color:#2563eb">
              {vma_total_netto + kosten_eur:.2f} €</div>
            <div style="font-size:12px;color:#64748b">Gesamt zur Abrechnung</div>
          </div></div>
        </div>

        {kurs_html}
        {best_html}

        <!-- VMA-Tabelle -->
        <div class="card" style="margin-bottom:16px">
          <div class="card-header">
            <span class="card-title">🌍 Verpflegungsmehraufwand</span>
            <span style="font-size:13px;font-weight:600;color:#059669">
              {vma_total_netto:.2f} EUR</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr>
                <th>Datum</th><th>Land</th><th style="text-align:right">Satz</th>
                <th>Abzüge</th><th style="text-align:right">Netto</th>
              </tr></thead>
              <tbody>
                {vma_html or '<tr><td colspan="5" class="empty-state">Keine VMA-Daten – bitte Übersicht aufrufen</td></tr>'}
              </tbody>
              <tfoot><tr style="border-top:2px solid var(--border)">
                <td colspan="4" style="text-align:right;font-weight:600;padding:10px 14px">
                  Gesamt VMA:</td>
                <td style="text-align:right;font-weight:700;font-size:15px;
                    color:#059669;padding:10px 14px">{vma_total_netto:.2f} EUR</td>
              </tr></tfoot>
            </table>
          </div>
        </div>

        <!-- Kosten-Tabelle -->
        <div class="card">
          <div class="card-header">
            <span class="card-title">🧾 Belege (Rechnungen & Quittungen)</span>
            <span style="font-size:13px;font-weight:600">{kosten_eur:.2f} EUR</span>
          </div>
          <div class="table-wrap">
            <table>
              <thead><tr>
                <th>Datum</th><th>Art</th><th>Anbieter</th>
                <th>Rechnungsnr.</th><th style="text-align:right">Betrag</th>
                <th>MwSt-Hinweis</th><th>PDF</th>
              </tr></thead>
              <tbody>
                {kosten_html or '<tr><td colspan="7" class="empty-state">Keine Rechnungen vorhanden</td></tr>'}
                {summen_html}
              </tbody>
              <tfoot><tr style="border-top:2px solid var(--border);background:#f0fdf4">
                <td colspan="4" style="text-align:right;font-weight:600;padding:10px 14px">
                  Gesamt Kosten:</td>
                <td style="text-align:right;font-weight:700;font-size:15px;
                    color:#059669;padding:10px 14px">{kosten_eur:.2f} EUR</td>
                <td colspan="2"></td>
              </tr></tfoot>
            </table>
          </div>
        </div>"""

        return HTMLResponse(shell(f"Abschluss {rcode}", content, "reisen"))
    except Exception as e:
        import traceback
        return HTMLResponse(shell("Fehler",
            f'<div class="alert alert-err">{e}</div>'
            f'<pre style="font-size:11px">{traceback.format_exc()[:500]}</pre>'))


@app.get("/test-openai")
async def test_openai():
    """Testet die OpenAI API-Verbindung."""
    import httpx, os
    if not OPENAI_KEY:
        return {"status": "fehler", "detail": "OPENAI_API_KEY nicht gesetzt"}
    try:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.post(
                OPENAI_URL,
                headers={"Authorization": f"Bearer {OPENAI_KEY}",
                         "Content-Type": "application/json"},
                json={"model": OPENAI_MODEL,
                      "messages": [{"role": "user",
                                    "content": "Antworte nur mit: OK"}],
                      "max_tokens": 5})
            if resp.status_code == 200:
                antwort = resp.json()["choices"][0]["message"]["content"]
                return {"status": "ok", "antwort": antwort, "modell": OPENAI_MODEL}
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
        db.commit()
        for sql in get_migrations():
            try:
                cur.execute(sql)
                db.commit()
            except Exception:
                db.rollback()
        cur.close(); db.close()
        repair_legacy_columns()
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
        for tbl in ["belege", "reise_laender", "reise_mitarbeiter", "reisen", "mitarbeiter"]:
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

# ── Dashboard + Mitarbeiter ───────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
def dashboard():
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        today = date.today()

        # Mitarbeiter
        cur.execute("SELECT COUNT(*) FROM mitarbeiter WHERE aktiv = TRUE"
                    if is_postgres()
                    else "SELECT COUNT(*) FROM mitarbeiter WHERE aktiv = 1")
        ma_count = cur.fetchone()[0]

        # Alle Reisen mit Mitarbeitern
        if is_postgres():
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                STRING_AGG(rm.kuerzel, ', ' ORDER BY rm.kuerzel) as ma
                FROM reisen r
                LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                ORDER BY r.abreise DESC""")
        else:
            cur.execute("""SELECT r.code, r.titel, r.abreise, r.rueckkehr,
                GROUP_CONCAT(rm.kuerzel, ', ') as ma
                FROM reisen r
                LEFT JOIN reise_mitarbeiter rm ON rm.reise_code = r.code
                GROUP BY r.code, r.titel, r.abreise, r.rueckkehr
                ORDER BY r.abreise DESC""")
        alle_reisen = cur.fetchall()

        # Unzugeordnete Belege
        try:
            cur.execute("SELECT COUNT(*) FROM belege WHERE reise_code IS NULL")
            unzugeordnet = cur.fetchone()[0]
        except: unzugeordnet = 0
        cur.close(); db.close()

        def to_date(v):
            if isinstance(v, date): return v
            try: return date.fromisoformat(str(v)[:10])
            except: return None

        def get(r,k,i): return r[k] if hasattr(r,'keys') else r[i]

        # In 3 Gruppen aufteilen
        aktiv = []; geplant = []; abgeschlossen = []
        for r in alle_reisen:
            ab = to_date(get(r,"abreise",2))
            zu = to_date(get(r,"rueckkehr",3))
            if not ab or not zu:
                geplant.append(r)
            elif today < ab:
                geplant.append(r)
            elif today <= zu:
                aktiv.append(r)
            else:
                abgeschlossen.append(r)

        def reise_zeile(r, typ=""):
            code=get(r,"code",0); titel=get(r,"titel",1)
            ab=to_date(get(r,"abreise",2)); zu=to_date(get(r,"rueckkehr",3))
            ma=get(r,"ma",4) or "–"
            tage = (zu-ab).days+1 if ab and zu else "?"
            if typ=="aktiv":
                badge = '<span class="badge badge-green">● Aktiv</span>'
                link_extra = f'<a href="/reise/{code}/uebersicht" style="font-size:11px;color:var(--muted);margin-left:8px">Übersicht</a>'
            elif typ=="geplant":
                delta = (ab-today).days if ab else 0
                badge = f'<span class="badge badge-blue">in {delta} Tagen</span>'
                link_extra = f'<a href="/reise/{code}/uebersicht" style="font-size:11px;color:var(--muted);margin-left:8px">Übersicht</a>'
            else:
                badge = '<span class="badge badge-gray">Abgeschlossen</span>'
                link_extra = f'<a href="/reise/{code}/abschluss" style="font-size:11px;color:var(--muted);margin-left:8px">Abschluss</a>'
            return f"""<tr>
                <td><a href="/reise/{code}" class="td-mono"
                    style="color:var(--blue);font-weight:600">{code}</a>{link_extra}</td>
                <td style="font-weight:500">
                  <a href="/reise/{code}" style="color:inherit;text-decoration:none">{titel}</a>
                </td>
                <td>{fmt_date(ab)}</td>
                <td>{fmt_date(zu)}</td>
                <td style="color:var(--muted)">{ma}</td>
                <td style="text-align:center;color:var(--muted)">{tage}</td>
                <td>{badge}</td>
            </tr>"""

        def sektion(titel_s, emoji, rows, typ, limit=None):
            if not rows:
                return f"""<div class="card" style="margin-bottom:16px">
                  <div class="card-header">
                    <span class="card-title">{emoji} {titel_s} (0)</span>
                    <a href="/reisen/neu" class="btn btn-primary btn-sm">+ Neue Reise</a>
                  </div>
                  <div class="empty-state"><p>Keine {titel_s.lower()}</p></div>
                </div>"""
            anzeige = rows[:limit] if limit else rows
            mehr = len(rows) - len(anzeige)
            zeilen = "".join(reise_zeile(r, typ) for r in anzeige)
            mehr_link = (f'<tr><td colspan="7" style="text-align:center;padding:10px;'
                         f'font-size:12px;color:var(--muted)">'
                         f'<a href="/reisen" style="color:var(--blue)">'
                         f'+ {mehr} weitere anzeigen →</a></td></tr>') if mehr else ""
            return f"""<div class="sektion-header">
              <span class="sektion-titel">{emoji} {titel_s}</span>
              <span class="sektion-count">{len(rows)}</span>
              <div style="margin-left:auto">
                <a href="/reisen/neu" class="btn btn-primary btn-sm">+ Neue Reise</a>
              </div>
            </div>
            <div class="card" style="margin-bottom:16px">
              <div class="table-wrap"><table>
                <thead><tr>
                  <th>Code</th><th>Titel</th><th>Abreise</th>
                  <th>Rückkehr</th><th>Mitarbeiter</th><th style="text-align:center">Tage</th>
                  <th>Status</th>
                </tr></thead>
                <tbody>{zeilen}{mehr_link}</tbody>
              </table></div>
            </div>"""

        # Weitere Alarme prüfen
        try:
            cur2 = db.cursor() if not db.closed else get_db().cursor()
        except:
            db2 = get_db(); cur2 = db2.cursor()
        
        alarme = []
        if unzugeordnet > 0:
            alarme.append({
                "url": "/unzugeordnet",
                "text": f'⚠ {unzugeordnet} Beleg{"e" if unzugeordnet!=1 else ""} ohne Reisezuordnung',
                "sub": "Jetzt zuordnen →"
            })
        try:
            cur2.execute(
                "SELECT COUNT(*) FROM belege WHERE pflichtfelder_ok = FALSE"
                if is_postgres() else
                "SELECT COUNT(*) FROM belege WHERE pflichtfelder_ok = 0")
            n_fehler = cur2.fetchone()[0]
            if n_fehler > 0:
                alarme.append({
                    "url": "/belege",
                    "text": f'⚠ {n_fehler} Beleg{"e" if n_fehler!=1 else ""} mit fehlenden Pflichtfeldern',
                    "sub": "Zur Belegliste →"
                })
        except: pass
        try:
            cur2.execute(
                """SELECT COUNT(*) FROM belege
                   WHERE waehrung != 'EUR' AND (kurs_eur IS NULL OR kurs_eur = 0)""")
            n_kurs = cur2.fetchone()[0]
            if n_kurs > 0:
                alarme.append({
                    "url": "/belege",
                    "text": f'💱 {n_kurs} Auslandsbeleg{"e" if n_kurs!=1 else ""} ohne Wechselkurs',
                    "sub": "Kurs nachtragen →"
                })
        except: pass
        try: cur2.close()
        except: pass

        if alarme:
            warn_items = "".join(
                f'<a href="{a["url"]}" style="display:flex;align-items:center;'
                f'justify-content:space-between;padding:10px 16px;'
                f'text-decoration:none;color:#991b1b;border-bottom:1px solid #fecaca">'
                f'<span style="font-weight:600">{a["text"]}</span>'
                f'<span style="font-size:12px;color:#ef4444">{a["sub"]}</span>'
                f'</a>'
                for a in alarme)
            warn_html = (
                f'<div style="background:#fef2f2;border:1px solid #fca5a5;'
                f'border-radius:var(--radius);margin-bottom:20px;overflow:hidden">'
                f'<div style="padding:8px 16px;background:#fecaca;font-size:11px;'
                f'font-weight:700;color:#991b1b;text-transform:uppercase;letter-spacing:.05em">'
                f'🔔 Aktionen erforderlich ({len(alarme)})</div>'
                f'{warn_items}</div>')
        else:
            warn_html = (
                f'<div style="background:var(--green-l);border:1px solid #6ee7b7;'
                f'border-radius:var(--radius);padding:10px 16px;margin-bottom:20px;'
                f'font-size:13px;color:#065f46;font-weight:500">'
                f'✅ Alles in Ordnung – keine offenen Aktionen</div>')

        content = f"""
        <div style="display:flex;align-items:center;justify-content:space-between;
                    margin-bottom:20px">
          <h1 class="page-title" style="margin:0">Dashboard</h1>
          <div style="display:flex;gap:8px">
            <a href="/mails-abrufen" class="btn btn-success">📬 Mails abrufen</a>
            <a href="/beleg/upload" class="btn btn-secondary">📎 Beleg hochladen</a>
          </div>
        </div>

        {warn_html}

        <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:28px">
          <div class="stat-card" style="border-top:3px solid var(--green)">
            <div class="stat-num" style="color:var(--green)">{len(aktiv)}</div>
            <div class="stat-label">🟢 Aktive Reisen</div>
          </div>
          <div class="stat-card" style="border-top:3px solid var(--blue)">
            <div class="stat-num" style="color:var(--blue)">{len(geplant)}</div>
            <div class="stat-label">📋 In Planung</div>
          </div>
          <div class="stat-card" style="border-top:3px solid var(--border-strong)">
            <div class="stat-num" style="color:var(--muted)">{len(abgeschlossen)}</div>
            <div class="stat-label">✓ Abgeschlossen</div>
          </div>
          <div class="stat-card" style="border-top:3px solid #8b5cf6">
            <div class="stat-num" style="color:#5b21b6">{ma_count}</div>
            <div class="stat-label">👤 Mitarbeiter</div>
          </div>
        </div>

        {sektion("Aktuelle Reisen", "🟢", aktiv, "aktiv")}
        {sektion("In Planung", "📋", geplant, "geplant")}
        {sektion("Abgeschlossen", "✓", abgeschlossen, "abgeschlossen", limit=5)}
        """
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
        cur.execute("""SELECT m.kuerzel, m.klarname, m.email, m.email2, m.email3, m.rolle, m.aktiv,
                       COUNT(rm.reise_code) as reise_count
                       FROM mitarbeiter m
                       LEFT JOIN reise_mitarbeiter rm ON rm.kuerzel = m.kuerzel
                       GROUP BY m.kuerzel, m.klarname, m.email, m.email2, m.email3, m.rolle, m.aktiv
                       ORDER BY m.klarname""")
        rows = cur.fetchall()
        cur.close(); db.close()

        def get(r, key, idx):
            return r[key] if hasattr(r, 'keys') else r[idx]

        ROLLEN = {
            "reisender": '<span class="badge badge-blue">✈ Reisender</span>',
            "organisator": '<span class="badge badge-purple">📋 Organisator</span>',
        }

        zeilen = ""
        for r in rows:
            kuerzel = get(r,"kuerzel",0)
            klarname = get(r,"klarname",1)
            email_liste = [get(r,"email",2), get(r,"email2",3), get(r,"email3",4)]
            email   = "<br>".join(e for e in email_liste if e) or "–"
            rolle   = get(r,"rolle",5) or "reisender"
            aktiv   = get(r,"aktiv",6)
            rcnt    = get(r,"reise_count",7)
            aktiv_badge = ('<span class="badge badge-green">Aktiv</span>' if aktiv
                           else '<span class="badge badge-gray">Inaktiv</span>')
            rolle_badge = ROLLEN.get(rolle,
                f'<span class="badge badge-gray">{rolle}</span>')
            zeilen += f"""<tr>
                <td class="td-mono" style="font-weight:700">{kuerzel}</td>
                <td style="font-weight:500">{klarname}</td>
                <td style="font-size:12px;color:var(--muted)">{email}</td>
                <td>{rolle_badge}</td>
                <td>{aktiv_badge}</td>
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
                <th>Kürzel</th><th>Name</th><th>E-Mail</th><th>Rolle</th>
                <th>Status</th><th>Reisen</th><th></th>
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
          <div class="form-grid form-grid-2">
            <div class="form-group">
              <label>Kürzel <span class="required">*</span></label>
              <input type="text" name="kuerzel" maxlength="5" required
                     placeholder="z.B. RD" style="text-transform:uppercase"
                     autofocus>
              <div class="form-hint">2–5 Buchstaben, eindeutig</div>
            </div>
            <div class="form-group">
              <label>Klarname <span class="required">*</span></label>
              <input type="text" name="klarname" required
                     placeholder="z.B. Ralf Diesslin">
            </div>
            <div class="form-group">
              <label>E-Mail-Adresse 1</label>
              <input type="email" name="email"
                     placeholder="rdiesslin@herrhammer.de">
              <div class="form-hint">Für automatische Beleg-Erkennung</div>
            </div>
            <div class="form-group">
              <label>E-Mail-Adresse 2</label>
              <input type="email" name="email2" placeholder="optional">
            </div>
            <div class="form-group">
              <label>E-Mail-Adresse 3</label>
              <input type="email" name="email3" placeholder="optional">
            </div>
            <div class="form-group">
              <label>Rolle <span class="required">*</span></label>
              <select name="rolle">
                <option value="reisender">✈ Reisender</option>
                <option value="organisator">📋 Organisator</option>
              </select>
              <div class="form-hint">Reisender = fährt selbst · Organisator = bucht für andere</div>
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
    email   = (form.get("email") or "").strip() or None
    email2  = (form.get("email2") or "").strip() or None
    email3  = (form.get("email3") or "").strip() or None
    rolle   = (form.get("rolle") or "reisender").strip()
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
        cur.execute(f"INSERT INTO mitarbeiter (kuerzel, klarname, email, email2, email3, rolle) VALUES ({P},{P},{P},{P},{P},{P})",
                    (kuerzel, klarname, email, email2, email3, rolle))
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
        cur.execute(f"SELECT kuerzel, klarname, email, rolle, aktiv, email2, email3 FROM mitarbeiter WHERE kuerzel={P}",
                    (kuerzel.upper(),))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler",
                '<div class="alert alert-err">Mitarbeiter nicht gefunden.</div>'))
        k = r[0] if isinstance(r, tuple) else r["kuerzel"]
        n = r[1] if isinstance(r, tuple) else r["klarname"]
        em = (r[2] if isinstance(r, tuple) else r.get("email","")) or ""
        ro = (r[3] if isinstance(r, tuple) else r.get("rolle","reisender")) or "reisender"
        a = r[4] if isinstance(r, tuple) else r["aktiv"]
        em2 = (r[5] if isinstance(r, tuple) else r.get("email2","")) or ""
        em3 = (r[6] if isinstance(r, tuple) else r.get("email3","")) or ""
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
                <div class="form-group">
                  <label>E-Mail-Adresse 1</label>
                  <input type="email" name="email" value="{em}"
                         placeholder="rdiesslin@herrhammer.de">
                </div>
                <div class="form-group">
                  <label>E-Mail-Adresse 2</label>
                  <input type="email" name="email2" value="{em2}" placeholder="optional">
                </div>
                <div class="form-group">
                  <label>E-Mail-Adresse 3</label>
                  <input type="email" name="email3" value="{em3}" placeholder="optional">
                </div>
                <div class="form-group">
                  <label>Rolle</label>
                  <select name="rolle">
                    <option value="reisender" {"selected" if ro=="reisender" else ""}>
                      ✈ Reisender</option>
                    <option value="organisator" {"selected" if ro=="organisator" else ""}>
                      📋 Organisator</option>
                  </select>
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
    email    = (form.get("email") or "").strip() or None
    email2   = (form.get("email2") or "").strip() or None
    email3   = (form.get("email3") or "").strip() or None
    rolle    = (form.get("rolle") or "reisender").strip()
    aktiv    = bool(form.get("aktiv"))
    if not klarname:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Name darf nicht leer sein.</div>'))
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        aktiv_val = True if is_postgres() else 1
        inaktiv_val = False if is_postgres() else 0
        cur.execute(f"UPDATE mitarbeiter SET klarname={P}, email={P}, email2={P}, email3={P}, rolle={P}, aktiv={P} WHERE kuerzel={P}",
                    (klarname, email, email2, email3, rolle, aktiv_val if aktiv else inaktiv_val, kuerzel.upper()))
        db.commit(); cur.close(); db.close()
        return RedirectResponse("/mitarbeiter", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

# ── Reisen ─────────────────────────────────────────────────────────────────────

# ── Reisen + Länder + VMA-Tabelle ─────────────────────────────────────────────
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

        # VMA je Tag
        cur.execute(f"""SELECT id, datum, land_code, land_name, ist_halber_satz,
                        fruehstueck, mittagessen, abendessen, vma_netto, vma_satz_voll, vma_satz_halb
                        FROM vma_tage WHERE reise_code = {P} ORDER BY datum""", (rcode,))
        vma_tage_rows = cur.fetchall()

        # Belege für den Tagesverlauf
        cur.execute(f"""SELECT id, transportart, transportart_freitext, anbieter,
                        betrag_brutto, waehrung, belegdatum, hotel_checkin_zeit, ki_json,
                        event_datum_von, event_datum_bis, hotel_checkin_datum,
                        hotel_checkout_datum, hotel_checkout_zeit
                        FROM belege WHERE reise_code = {P} ORDER BY belegdatum""", (rcode,))
        beleg_rows_tag = cur.fetchall()

        # Manuelle Termine für den Tagesverlauf
        cur.execute(f"""SELECT id, datum, uhrzeit_von, uhrzeit_bis, titel, typ
                        FROM termine WHERE reise_code = {P} ORDER BY datum, uhrzeit_von""", (rcode,))
        termin_rows = cur.fetchall()
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

        # Länder – kompakte Zeile statt großer Tabelle (VMA je Tag steht unten beim Tagesverlauf)
        laender_kompakt = " · ".join(
            f'{get(lr,"land_code",3)} {get(lr,"land_name",4)} '
            f'({fmt_date(get(lr,"datum_von",1))}–{fmt_date(get(lr,"datum_bis",2))}) '
            f'<a href="/reise/{rcode}/land/{get(lr,"id",0)}/bearbeiten" style="color:var(--muted)">✏</a>'
            for lr in land_rows)

        ma_html = " ".join(
            f'<span class="badge badge-green">{get(m,"kuerzel",0)} – {get(m,"klarname",1)}</span>'
            for m in ma_rows) or "–"

        # Belege und Termine nach Datum gruppieren
        TERMIN_ICON = {
            "termin": "🤝", "fahrt": "🚕", "mietwagen": "🚗",
            "hotel": "🏨", "sonstiges": "📌",
        }
        BADGE_DETAIL = {
            "Flug": ("✈", "#dbeafe", "#1e40af"), "Hotel": ("🏨", "#dcfce7", "#166534"),
            "Mietwagen": ("🚗", "#ede9fe", "#5b21b6"), "Taxi": ("🚕", "#fef3c7", "#92400e"),
            "Bahn": ("🚆", "#e0e7ff", "#3730a3"), "Tanken": ("⛽", "#fee2e2", "#991b1b"),
            "Bewirtung": ("🍽", "#fff7ed", "#9a3412"), "Sonstiges": ("📄", "#f1f5f9", "#475569"),
        }

        def zeit_aus_beleg(b):
            zeit = get(b, "hotel_checkin_zeit", 7)
            if zeit: return zeit
            ki_str = get(b, "ki_json", 8) or ""
            try:
                segs = json.loads(ki_str).get("segmente") or []
                if segs: return segs[0].get("abreise_zeit", "") or ""
            except: pass
            return ""

        def to_d(v):
            if not v: return None
            if isinstance(v, str): return date.fromisoformat(v[:10])
            return v

        # Zweite Zeit für "Ende"-Ereignisse (Mietwagen-Abgabe, Hotel-Checkout)
        ENDE_LABEL = {"Mietwagen": ("abgeben", "🚗"), "Hotel": ("auschecken", "🏨")}

        belege_je_tag: dict = {}
        for b in beleg_rows_tag:
            # Leistungsdatum (wann die Leistung tatsächlich stattfand) hat Vorrang
            # vor dem Belegdatum (z.B. Ausstellungsdatum einer Flugrechnung, das
            # Wochen vor dem eigentlichen Flug liegen kann).
            typ = get(b, "transportart", 1) or "Sonstiges"
            start = to_d(get(b, "event_datum_von", 9)) or to_d(get(b, "belegdatum", 6))
            if typ == "Hotel":
                ende = to_d(get(b, "hotel_checkout_datum", 12)) or to_d(get(b, "event_datum_bis", 10))
                start = to_d(get(b, "hotel_checkin_datum", 11)) or start
            else:
                ende = to_d(get(b, "event_datum_bis", 10))
            if not start: continue
            belege_je_tag.setdefault(start, []).append((b, "start"))
            if ende and ende != start and typ in ENDE_LABEL:
                belege_je_tag.setdefault(ende, []).append((b, "ende"))

        termine_je_tag: dict = {}
        for t in termin_rows:
            td = get(t, "datum", 1)
            if isinstance(td, str): td = date.fromisoformat(td[:10])
            termine_je_tag.setdefault(td, []).append(t)

        def tagesablauf_html(tag_datum, rcode):
            eintraege = []
            tages_summe = 0.0
            for b, rolle in belege_je_tag.get(tag_datum, []):
                bid = get(b,"id",0); typ = get(b,"transportart",1) or "Sonstiges"
                freitext = get(b,"transportart_freitext",2) or ""
                anbieter = get(b,"anbieter",3) or "–"
                betrag = get(b,"betrag_brutto",4); waehrung = get(b,"waehrung",5) or "EUR"
                icon, bg, fg = BADGE_DETAIL.get(typ, BADGE_DETAIL["Sonstiges"])

                if rolle == "start":
                    if typ == "Hotel":
                        zeit = get(b,"hotel_checkin_zeit",7) or ""
                        titel_txt = f"{icon} Hotel einchecken · {anbieter}"
                    elif typ == "Mietwagen":
                        zeit = zeit_aus_beleg(b)
                        titel_txt = f"{icon} Mietwagen abholen · {anbieter}"
                    else:
                        zeit = zeit_aus_beleg(b)
                        titel_txt = f"{icon} {typ}" + (f" – {freitext}" if freitext else "") + f" · {anbieter}"
                    bet_s = f"{float(betrag):.2f} {waehrung}" if betrag else None
                    if bet_s: tages_summe += float(betrag)
                else:  # ende – nur Hinweis, Betrag zählt nicht doppelt
                    label, ic = ENDE_LABEL.get(typ, ("Ende", icon))
                    zeit = get(b,"hotel_checkout_zeit",13) or "" if typ == "Hotel" else ""
                    titel_txt = f"{ic} {typ} {label} · {anbieter}"
                    bet_s = None

                eintraege.append((zeit or "99:99", "beleg", bid, titel_txt, bet_s, bg, fg))
            for t in termine_je_tag.get(tag_datum, []):
                tid = get(t,"id",0); von = get(t,"uhrzeit_von",2) or ""; bis = get(t,"uhrzeit_bis",3) or ""
                titel_t = get(t,"titel",4); typ_t = get(t,"typ",5) or "termin"
                icon = TERMIN_ICON.get(typ_t, "📌")
                zeit_txt = f"{von}–{bis}" if von and bis else (von or "")
                eintraege.append((von or "99:99", "termin", tid, f"{icon} {titel_t}", zeit_txt, None, None))

            eintraege.sort(key=lambda x: x[0])
            if not eintraege:
                return '<div style="padding:12px 16px;font-size:12px;color:var(--muted);font-style:italic">Keine Termine oder Belege an diesem Tag.</div>'

            rows = ""
            for zeit, art, eid, titel_txt, rechts, bg, fg in eintraege:
                zeit_disp = "" if zeit == "99:99" else zeit
                if art == "beleg":
                    rechts_html = (f'<span style="background:{bg};color:{fg};padding:2px 8px;'
                                    f'border-radius:12px;font-size:11px;font-weight:500">{rechts}</span>'
                                    if rechts else '<span style="font-size:11px;color:var(--muted);font-style:italic">kein Betrag</span>')
                    link = f'<a href="/beleg/{eid}" style="font-size:11px;color:#2563eb;text-decoration:none">Detail</a>'
                else:
                    rechts_html = f'<span style="font-size:11px;color:#7c3aed">✎ manuell · {rechts}</span>' if rechts else '<span style="font-size:11px;color:#7c3aed">✎ manuell</span>'
                    link = (f'<a href="/reise/{rcode}/termin/{eid}/bearbeiten" style="font-size:11px;color:var(--muted);text-decoration:none">✎</a> '
                            f'<a href="/reise/{rcode}/termin/{eid}/loeschen" style="font-size:11px;color:var(--muted);text-decoration:none" '
                            f'onclick="return confirm(\'Termin löschen?\')">🗑</a>')
                rows += f"""<div style="display:flex;gap:12px;padding:8px 16px;border-bottom:1px solid var(--border);align-items:flex-start">
                    <div style="width:44px;flex-shrink:0;font-size:12px;color:var(--muted);font-weight:600;padding-top:1px">{zeit_disp}</div>
                    <div style="flex:1;font-size:13px">{titel_txt}</div>
                    <div style="text-align:right;white-space:nowrap;display:flex;gap:8px;align-items:center">{rechts_html}{link}</div>
                </div>"""
            if tages_summe > 0:
                rows += (f'<div style="display:flex;justify-content:flex-end;padding:8px 16px;'
                          f'background:var(--bg)"><span style="font-size:12px;font-weight:600;'
                          f'color:var(--text)">Summe Belege: {tages_summe:.2f} EUR</span></div>')
            return rows

        wochentage = ["Mo","Di","Mi","Do","Fr","Sa","So"]
        vma_tage_summe = 0.0
        tage_blocks = ""

        def cb(rcode, vid, name, checked, label, abzug_pct):
            ch = "checked" if checked else ""
            return (f'<label style="display:inline-flex;align-items:center;gap:4px;'
                    f'cursor:pointer;font-size:12px;color:var(--muted);margin-right:10px">'
                    f'<input type="checkbox" name="{name}" value="1" {ch} '
                    f'onchange="this.form.submit()" style="width:auto;margin:0">'
                    f'{label} <span style="color:#c81e1e;font-size:10px">-{abzug_pct}%</span>'
                    f'</label>')

        for i, vt in enumerate(vma_tage_rows):
            vid = get(vt,"id",0)
            vd = get(vt,"datum",1)
            if isinstance(vd, str): vd = date.fromisoformat(vd[:10])
            lcode_t = get(vt,"land_code",2) or "DE"
            lname_t = get(vt,"land_name",3) or "Deutschland"
            ist_halb = bool(get(vt,"ist_halber_satz",4))
            frueh = bool(get(vt,"fruehstueck",5))
            mittag = bool(get(vt,"mittagessen",6))
            abend = bool(get(vt,"abendessen",7))
            netto = float(get(vt,"vma_netto",8) or 0)
            vma_tage_summe += netto

            wt = wochentage[vd.weekday()] if vd else "–"
            datum_txt = f"{wt} {vd.day:02d}.{vd.month:02d}.{vd.year}" if vd else "–"
            halb_badge = ('<span style="font-size:10px;background:#fef3c7;color:#92400e;'
                           'padding:1px 7px;border-radius:10px">½ Satz</span>') if ist_halb else ""

            tage_blocks += f"""<div style="border-bottom:1px solid var(--border)">
              <div style="padding:12px 16px;background:var(--bg)">
                <div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">
                  <div>
                    <div style="font-weight:700;color:var(--text);font-size:14px">{datum_txt} {halb_badge}</div>
                    <div style="font-size:11px;color:var(--muted);margin-top:2px">🌍 {lname_t} ({lcode_t})</div>
                  </div>
                  <div style="text-align:right">
                    <div style="font-size:16px;font-weight:700;color:var(--green)">{netto:.2f} EUR VMA netto</div>
                  </div>
                  <a href="/reise/{rcode}/termin/neu?datum={vd.isoformat() if vd else ''}"
                     style="font-size:11px;color:#2563eb;text-decoration:none;border:0.5px solid #bfdbfe;
                            border-radius:6px;padding:4px 10px">+ Termin</a>
                </div>
                <form method="post" action="/reise/{rcode}/vma/{vid}/speichern" style="margin-top:8px">
                  <input type="hidden" name="land_code" value="{lcode_t}">
                  <input type="hidden" name="ist_halber_satz" value="{'1' if ist_halb else ''}">
                  {cb(rcode, vid, "fruehstueck", frueh, "Frühstück", 20)}
                  {cb(rcode, vid, "mittagessen", mittag, "Mittag", 40)}
                  {cb(rcode, vid, "abendessen", abend, "Abend", 40)}
                </form>
              </div>
              {tagesablauf_html(vd, rcode)}
            </div>"""

        # Belege mit Datum außerhalb des berechneten Reisezeitraums – sonst verschwinden sie
        tage_im_bereich = {get(vt,"datum",1) if not isinstance(get(vt,"datum",1), str)
                            else date.fromisoformat(get(vt,"datum",1)[:10]) for vt in vma_tage_rows}
        ausserhalb_tage = sorted(d for d in belege_je_tag.keys() if d not in tage_im_bereich)
        if ausserhalb_tage:
            tage_blocks += ('<div style="padding:10px 16px;font-size:12px;color:#b45309;'
                             'font-style:italic;background:#fffbeb">⚠ Belege außerhalb des '
                             'Reisezeitraums (Datum passt nicht zur Reisedauer)</div>')
            for d in ausserhalb_tage:
                wt = wochentage[d.weekday()]
                datum_txt = f"{wt} {d.day:02d}.{d.month:02d}.{d.year}"
                tage_blocks += f"""<div style="border-bottom:1px solid var(--border)">
                  <div style="padding:10px 16px;background:var(--bg)">
                    <div style="font-weight:700;color:var(--text);font-size:14px">{datum_txt}</div>
                  </div>
                  {tagesablauf_html(d, rcode)}
                </div>"""

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
            <a href="/reise/{rcode}/abschluss" class="btn btn-primary">🧾 Abschluss</a>
            <a href="/reise/{rcode}/uebersicht" class="btn btn-secondary">📋 Übersicht</a>
            <a href="/reise/{rcode}/bearbeiten" class="btn btn-secondary">✏ Bearbeiten</a>
            <a href="/reise/{rcode}/land/neu" class="btn btn-secondary">🌍 + Land</a>
          </div>
        </div>

        {'<div style="font-size:12px;color:var(--muted);margin-bottom:16px">🌍 ' + laender_kompakt + '</div>' if laender_kompakt else ''}

        <div class="card">
          <div class="card-header">
            <span class="card-title">📅 Tagesverlauf & VMA</span>
          </div>
          {tage_blocks if tage_blocks else '<div class="card-body"><div class="empty-state"><b>Noch keine VMA-Tage berechnet</b><p>Erst Länder hinterlegen, dann VMA generieren</p><a href="/reise/' + rcode + '/vma-generieren" class="btn btn-primary" style="margin-top:12px">🔄 VMA berechnen</a></div></div>'}
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
              <select name="land_code" id="land_code" required onchange="ladeVMA(this.value)">
                {land_opts}
              </select>
            </div>
            <div class="form-group full">
              <label>Ort / Region</label>
              <select name="ort" id="ort_select" onchange="zeigeSatz()">
                <option value="">Standard (ganzes Land)</option>
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
    let aktuelleInfo = null;
    async function ladeVMA(code) {{
        const res = await fetch('/vma-saetze/info/' + code);
        const data = await res.json();
        aktuelleInfo = data;
        const sel = document.getElementById('ort_select');
        sel.innerHTML = '<option value="">Standard (ganzes Land)</option>';
        (data.staedte || []).forEach(s => {{
            const opt = document.createElement('option');
            opt.value = s.ort;
            opt.textContent = s.ort + ' (' + s.voll.toFixed(2) + ' EUR / ' + s.halb.toFixed(2) + ' EUR)';
            sel.appendChild(opt);
        }});
        zeigeSatz();
    }}
    function zeigeSatz() {{
        const el = document.getElementById('vma-info');
        const ort = document.getElementById('ort_select').value;
        if (!aktuelleInfo) {{ el.textContent = ''; return; }}
        if (ort) {{
            const s = (aktuelleInfo.staedte || []).find(x => x.ort === ort);
            if (s) el.textContent = s.name + ': ' + s.voll.toFixed(2) + ' EUR/Tag · ' + s.halb.toFixed(2) + ' EUR halber Satz';
        }} else if (aktuelleInfo.standard) {{
            const s = aktuelleInfo.standard;
            el.textContent = s.land_name + ' (' + s.quelle + '): ' + s.voll.toFixed(2) + ' EUR/Tag · ' + s.halb.toFixed(2) + ' EUR halber Satz';
        }}
    }}
    ladeVMA(document.getElementById('land_code').value);
    </script>"""
    return HTMLResponse(shell(f"Land – {rcode}", content, "reisen"))

@app.post("/reise/{code}/land/neu")
async def land_neu(code: str, request: Request):
    rcode = code.upper()
    form = await request.form()
    land_code = (form.get("land_code") or "").strip().upper()
    ort = (form.get("ort") or "").strip() or None
    datum_von = (form.get("datum_von") or "").strip()
    datum_bis = (form.get("datum_bis") or "").strip()
    if not all([land_code, datum_von, datum_bis]):
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Alle Felder sind Pflicht.</div>'
            f'<a href="/reise/{rcode}/land/neu" class="btn btn-secondary">Zurück</a>'))
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        info = vma_fuer_land_erweitert(cur, land_code, ort)
        land_name = info["land_name"]
        vvoll, vhalb = info["voll"], info["halb"]
        cur.execute(
            f"INSERT INTO reise_laender (reise_code,datum_von,datum_bis,land_code,land_name,vma_voll,vma_halb,ort) "
            f"VALUES ({P},{P},{P},{P},{P},{P},{P},{P})",
            (rcode, datum_von, datum_bis, land_code, land_name, vvoll, vhalb, ort))
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

# ── Manuelle Termine ────────────────────────────────────────────────────────────
TERMIN_TYPEN = [
    ("termin", "🤝 Termin"), ("fahrt", "🚕 Fahrt"), ("mietwagen", "🚗 Mietwagen"),
    ("hotel", "🏨 Hotel"), ("sonstiges", "📌 Sonstiges"),
]

@app.get("/reise/{code}/termin/neu", response_class=HTMLResponse)
def termin_neu_form(code: str, datum: str = ""):
    rcode = code.upper()
    typ_opts = "".join(f'<option value="{v}">{l}</option>' for v, l in TERMIN_TYPEN)
    content = f"""
    <h1 class="page-title">Termin hinzufügen – {rcode}</h1>
    <div class="card" style="max-width:500px">
      <div class="card-body">
        <form method="post" action="/reise/{rcode}/termin/neu">
          <div class="form-grid form-grid-2">
            <div class="form-group full">
              <label>Titel <span class="required">*</span></label>
              <input type="text" name="titel" required placeholder="z.B. Kundentermin">
            </div>
            <div class="form-group">
              <label>Typ</label>
              <select name="typ">{typ_opts}</select>
            </div>
            <div class="form-group">
              <label>Datum <span class="required">*</span></label>
              <input type="date" name="datum" value="{datum}" required>
            </div>
            <div class="form-group">
              <label>Uhrzeit von</label>
              <input type="time" name="uhrzeit_von">
            </div>
            <div class="form-group">
              <label>Uhrzeit bis</label>
              <input type="time" name="uhrzeit_bis">
            </div>
            <div class="form-group full">
              <label>Notiz</label>
              <input type="text" name="notiz" placeholder="optional">
            </div>
          </div>
          <div class="form-actions">
            <button type="submit" class="btn btn-primary">Hinzufügen</button>
            <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
          </div>
        </form>
      </div>
    </div>"""
    return HTMLResponse(shell(f"Termin – {rcode}", content, "reisen"))

@app.post("/reise/{code}/termin/neu")
async def termin_neu(code: str, request: Request):
    rcode = code.upper()
    form = await request.form()
    titel = (form.get("titel") or "").strip()
    typ = (form.get("typ") or "termin").strip()
    datum = (form.get("datum") or "").strip()
    von = (form.get("uhrzeit_von") or "").strip() or None
    bis = (form.get("uhrzeit_bis") or "").strip() or None
    notiz = (form.get("notiz") or "").strip() or None
    if not titel or not datum:
        return HTMLResponse(shell("Fehler",
            '<div class="alert alert-err">Titel und Datum sind Pflicht.</div>'
            f'<a href="/reise/{rcode}/termin/neu" class="btn btn-secondary">Zurück</a>'))
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(
            f"INSERT INTO termine (reise_code,datum,uhrzeit_von,uhrzeit_bis,titel,typ,notiz) "
            f"VALUES ({P},{P},{P},{P},{P},{P},{P})",
            (rcode, datum, von, bis, titel, typ, notiz))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reise/{code}/termin/{tid}/bearbeiten", response_class=HTMLResponse)
def termin_bearbeiten_form(code: str, tid: int):
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"SELECT id,datum,uhrzeit_von,uhrzeit_bis,titel,typ,notiz FROM termine WHERE id={P}", (tid,))
        r = cur.fetchone()
        cur.close(); db.close()
        if not r:
            return HTMLResponse(shell("Fehler", '<div class="alert alert-err">Termin nicht gefunden.</div>'))
        g = lambda k, i: r[k] if hasattr(r, "keys") else r[i]
        datum_v = str(g("datum",1))[:10]; von_v = g("uhrzeit_von",2) or ""; bis_v = g("uhrzeit_bis",3) or ""
        titel_v = g("titel",4); typ_v = g("typ",5) or "termin"; notiz_v = g("notiz",6) or ""
        typ_opts = "".join(
            f'<option value="{v}"{" selected" if v==typ_v else ""}>{l}</option>' for v, l in TERMIN_TYPEN)
        content = f"""
        <h1 class="page-title">Termin bearbeiten – {rcode}</h1>
        <div class="card" style="max-width:500px">
          <div class="card-body">
            <form method="post" action="/reise/{rcode}/termin/{tid}/bearbeiten">
              <div class="form-grid form-grid-2">
                <div class="form-group full">
                  <label>Titel <span class="required">*</span></label>
                  <input type="text" name="titel" value="{titel_v}" required>
                </div>
                <div class="form-group">
                  <label>Typ</label>
                  <select name="typ">{typ_opts}</select>
                </div>
                <div class="form-group">
                  <label>Datum <span class="required">*</span></label>
                  <input type="date" name="datum" value="{datum_v}" required>
                </div>
                <div class="form-group">
                  <label>Uhrzeit von</label>
                  <input type="time" name="uhrzeit_von" value="{von_v}">
                </div>
                <div class="form-group">
                  <label>Uhrzeit bis</label>
                  <input type="time" name="uhrzeit_bis" value="{bis_v}">
                </div>
                <div class="form-group full">
                  <label>Notiz</label>
                  <input type="text" name="notiz" value="{notiz_v}">
                </div>
              </div>
              <div class="form-actions">
                <button type="submit" class="btn btn-primary">Speichern</button>
                <a href="/reise/{rcode}" class="btn btn-secondary">Abbrechen</a>
              </div>
            </form>
          </div>
        </div>"""
        return HTMLResponse(shell(f"Termin – {rcode}", content, "reisen"))
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.post("/reise/{code}/termin/{tid}/bearbeiten")
async def termin_bearbeiten(code: str, tid: int, request: Request):
    rcode = code.upper()
    form = await request.form()
    titel = (form.get("titel") or "").strip()
    typ = (form.get("typ") or "termin").strip()
    datum = (form.get("datum") or "").strip()
    von = (form.get("uhrzeit_von") or "").strip() or None
    bis = (form.get("uhrzeit_bis") or "").strip() or None
    notiz = (form.get("notiz") or "").strip() or None
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(
            f"UPDATE termine SET titel={P},typ={P},datum={P},uhrzeit_von={P},uhrzeit_bis={P},notiz={P} WHERE id={P}",
            (titel, typ, datum, von, bis, notiz, tid))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return HTMLResponse(shell("Fehler", f'<div class="alert alert-err">{e}</div>'))

@app.get("/reise/{code}/termin/{tid}/loeschen")
def termin_loeschen(code: str, tid: int):
    rcode = code.upper()
    try:
        P = ph()
        db = get_db(); cur = db.cursor()
        cur.execute(f"DELETE FROM termine WHERE id={P}", (tid,))
        db.commit(); cur.close(); db.close()
        return RedirectResponse(f"/reise/{rcode}", status_code=303)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=500)

# ── VMA-Tabelle Übersicht ──────────────────────────────────────────────────────
@app.post("/vma-saetze/import")
def vma_saetze_import():
    result = importiere_aktuelle_saetze()
    if result.get("fehler"):
        msg = f"fehler={result['fehler']}"
    else:
        msg = f"ok=1&laender={result['laender']}&staedte={result['staedte']}&ab={result.get('gueltig_ab','')}"
    return RedirectResponse(f"/vma?{msg}", status_code=303)

@app.get("/vma-saetze/info/{land_code}")
def vma_saetze_info(land_code: str):
    """JSON: Standard-Satz + Städte-Sonderfälle für ein Land (für das Land-Formular)."""
    try:
        db = get_db(); cur = db.cursor()
        P = ph()
        standard = vma_fuer_land_erweitert(cur, land_code, None)
        cur.execute(f"SELECT ort, land_name, vma_voll, vma_halb FROM vma_saetze "
                    f"WHERE land_code={P} AND ort IS NOT NULL ORDER BY ort", (land_code.upper(),))
        rows = cur.fetchall()
        cur.close(); db.close()
        staedte = [{"ort": (r[0] if isinstance(r, tuple) else r["ort"]),
                    "name": (r[1] if isinstance(r, tuple) else r["land_name"]),
                    "voll": float(r[2] if isinstance(r, tuple) else r["vma_voll"]),
                    "halb": float(r[3] if isinstance(r, tuple) else r["vma_halb"])} for r in rows]
        return JSONResponse({"standard": standard, "staedte": staedte})
    except Exception as e:
        return JSONResponse({"standard": None, "staedte": [], "fehler": str(e)})

@app.get("/vma", response_class=HTMLResponse)
def vma_uebersicht(ok: str = "", fehler: str = "", laender: str = "", staedte: str = "", ab: str = ""):
    try:
        db = get_db(); cur = db.cursor()
        cur.execute("""SELECT land_code, ort, land_name, vma_voll, vma_halb, gueltig_ab, aktualisiert
                       FROM vma_saetze ORDER BY land_name, ort""")
        rows = cur.fetchall()
        cur.close(); db.close()
    except Exception:
        rows = []

    def g(r, key, idx):
        return r[key] if hasattr(r, "keys") else r[idx]

    importiert_am = None
    laender_map: dict[str, dict] = {}
    for r in rows:
        lc = g(r,"land_code",0); ort = g(r,"ort",1); ln = g(r,"land_name",2)
        voll = float(g(r,"vma_voll",3)); halb = float(g(r,"vma_halb",4))
        importiert_am = g(r,"aktualisiert",6) or importiert_am
        eintrag = laender_map.setdefault(lc, {"name": None, "voll": None, "halb": None, "staedte": []})
        if ort:
            eintrag["staedte"].append({"name": ln, "voll": voll, "halb": halb})
        else:
            eintrag["name"] = ln; eintrag["voll"] = voll; eintrag["halb"] = halb

    zeilen = ""
    if laender_map:
        # Importierte Sätze aus der DB anzeigen
        for lc, info in sorted(laender_map.items(), key=lambda x: x[1]["name"] or x[0]):
            if info["voll"] is None:
                continue
            zeilen += f"""<tr>
                <td class="td-mono">{lc}</td>
                <td>🌍 {info["name"]}</td>
                <td style="text-align:right;font-weight:600">{info["voll"]:.2f} EUR</td>
                <td style="text-align:right">{info["halb"]:.2f} EUR</td>
            </tr>"""
            for st in sorted(info["staedte"], key=lambda s: s["name"]):
                zeilen += f"""<tr style="background:#fafbfe">
                    <td class="td-mono" style="color:var(--muted)">↳</td>
                    <td style="padding-left:24px">📍 {st["name"]}</td>
                    <td style="text-align:right;font-weight:600">{st["voll"]:.2f} EUR</td>
                    <td style="text-align:right">{st["halb"]:.2f} EUR</td>
                </tr>"""
    else:
        # Fallback: fest hinterlegte Sätze im Code
        for code, info in sorted(VMA_SAETZE.items(), key=lambda x: x[1]["name"]):
            zeilen += f"""<tr>
                <td class="td-mono">{code}</td>
                <td>🌍 {info["name"]}</td>
                <td style="text-align:right;font-weight:600">{info["voll"]:.2f} EUR</td>
                <td style="text-align:right">{info["halb"]:.2f} EUR</td>
            </tr>"""

    banner = ""
    if ok:
        banner = (f'<div class="alert alert-ok" style="margin-bottom:16px">'
                  f'✓ Import erfolgreich: {laender} Länder, {staedte} Städte-Sonderfälle '
                  f'(gültig ab {ab}).</div>')
    elif fehler:
        banner = f'<div class="alert alert-err" style="margin-bottom:16px">Import fehlgeschlagen: {fehler}</div>'
    elif not laender_map:
        banner = ('<div class="alert alert-warn" style="margin-bottom:20px">'
                   'Es wurden noch keine aktuellen Sätze importiert – unten stehen die im Code '
                   'hinterlegten Werte (Stand 2024/2026, ohne Städte-Sonderfälle).</div>')

    content = f"""
    <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:12px">
      <h1 class="page-title" style="margin:0">VMA-Tagessätze</h1>
      <form method="post" action="/vma-saetze/import">
        <button type="submit" class="btn btn-primary">🔄 Aktuelle Sätze importieren</button>
      </form>
    </div>
    {banner}
    <div class="card">
      <div class="table-wrap">
        <table>
          <thead><tr>
            <th>ISO</th><th>Land / Ort</th>
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
      um 24:00 Uhr Ortszeit war. Städte-Sonderfälle (z.B. Los Angeles) werden beim
      "Land hinzufügen" in der Reise automatisch zur Auswahl angeboten, sobald importiert.
      Quelle: <a href="https://github.com/david-loe/pauschbetrag-api" target="_blank" style="color:var(--muted)">pauschbetrag-api</a> (offizielle BMF-Daten).
    </div>"""
    return HTMLResponse(shell("VMA-Sätze", content, "vma"))

