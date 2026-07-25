"""
main.py – Herrhammer Reisekosten v2.2
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
from mod_db import get_db, is_postgres, ph, fmt_date, next_reise_code, get_schema
from mod_vma import VMA_SAETZE, IATA_TO_LAND, LAENDER_LISTE, vma_fuer_land
from mod_anon import anonymisieren
from mod_beleg import (beleg_verarbeiten, gpt_analyse, gpt_analyse_bild,
                        lade_ma_daten, get_s3, s3_upload, s3_download,
                        bild_zu_pdf, text_zu_pdf, pdf_text_lesen,
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
APP_VERSION  = "2.2-a"

# ── CSS + HTML Shell ──────────────────────────────────────────────────────────
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

APP_VERSION = "2.1-q"

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
app = FastAPI(title="Herrhammer Reisekosten", version=APP_VERSION)

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
            status, fehler, erstellt
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
                <dd>{belegart or "–"}</dd>
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

        for i in range(tage_gesamt):
            tag = ab + timedelta(days=i)
            wt = wochentage[tag.weekday()]
            datum_s = f"{wt} {tag.day:02d}. {monate[tag.month-1]} {tag.year}"
            datum_iso = tag.isoformat()

            # VMA-Zeile für diesen Tag
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
                vma_brutto = float(g(vr,"vma_brutto",10) or 0)
                vma_netto = float(g(vr,"vma_netto",11) or 0)
                quelle = g(vr,"quelle",12) or "auto"

                basis_txt = f"{halb:.2f} €" if ist_halb else f"{voll:.2f} €"
                halb_badge = '<span style="font-size:10px;background:#fef3c7;color:#92400e;padding:1px 6px;border-radius:10px;margin-left:4px">½ Satz</span>' if ist_halb else ""
                auto_badge = '<span style="font-size:10px;color:#94a3b8;margin-left:4px">auto</span>' if quelle=="auto" else '<span style="font-size:10px;color:#7c3aed;margin-left:4px">✎ manuell</span>'

                def cb(name, checked, label, abzug_pct):
                    ch = "checked" if checked else ""
                    return (f'<label style="display:inline-flex;align-items:center;gap:4px;'
                            f'cursor:pointer;font-size:12px;color:var(--text-secondary)">'
                            f'<input type="checkbox" name="{name}" value="1" {ch} '
                            f'onchange="this.form.submit()" style="width:auto;margin:0">'
                            f'{label} <span style="color:#ef4444;font-size:11px">-{abzug_pct}%</span>'
                            f'</label>')

                vma_row = (
                    f'<tr style="background:#f0fdf4">'
                    f'<td style="padding:8px 12px;font-size:12px;color:#64748b;white-space:nowrap">'
                    f'<b style="color:#059669">VMA</b></td>'
                    f'<td style="padding:8px 12px">'
                    f'<span style="font-size:12px;font-weight:500;color:#059669">'
                    f'🌍 {lname} ({lcode})</span>{halb_badge}{auto_badge}</td>'
                    f'<td style="padding:8px 12px">'
                    f'<form method="post" action="/reise/{rcode}/vma/{vid}/speichern" '
                    f'style="display:inline-flex;gap:12px;align-items:center;flex-wrap:wrap">'
                    f'<input type="hidden" name="land_code" value="{lcode}">'
                    f'<input type="hidden" name="ist_halber_satz" value="{"1" if ist_halb else ""}">'
                    f'{cb("fruehstueck", frueh, "Frühstück", 20)}'
                    f'{cb("mittagessen", mittag, "Mittagessen", 40)}'
                    f'{cb("abendessen", abend, "Abendessen", 40)}'
                    f'</form></td>'
                    f'<td style="padding:8px 12px;text-align:right;font-weight:600;'
                    f'color:#059669;white-space:nowrap">'
                    f'<span style="text-decoration:line-through;color:#94a3b8;font-weight:400;font-size:11px">{basis_txt}</span> '
                    f'{vma_netto:.2f} €</td>'
                    f'<td style="padding:8px 12px"></td>'
                    f'</tr>')
            else:
                vma_row = (
                    f'<tr style="background:#fafafa">'
                    f'<td colspan="5" style="padding:6px 12px;font-size:11px;color:#94a3b8">'
                    f'VMA für diesen Tag nicht berechnet – '
                    f'<a href="/reise/{rcode}/vma-generieren" style="color:#2563eb">Generieren</a>'
                    f'</td></tr>')

            rows_html += vma_row

            # Beleg-Zeilen für diesen Tag
            tages_belege = belege_by_date.get(tag, [])
            for b in tages_belege:
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

                # Details aus KI-JSON (Segmente)
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

                rows_html += (
                    f'<tr>'
                    f'<td style="padding:8px 12px;font-size:12px;color:#64748b">'
                    f'&nbsp;</td>'
                    f'<td style="padding:8px 12px">{badge}</td>'
                    f'<td style="padding:8px 12px;font-size:13px">'
                    f"<b>{anbieter}</b>"
                    + (f'<br><small style="color:#64748b;font-size:11px">{seg_info}</small>' if seg_info else "")
                    + f'</td>'
                    + f'<td style="padding:8px 12px;text-align:right;font-weight:600;white-space:nowrap">{bet_s}</td>'
                    + f'<td style="padding:8px 12px;white-space:nowrap">'
                    + f'<a href="/beleg/{bid}/pdf/original" target="_blank" style="font-size:11px;color:#2563eb;border:0.5px solid #bfdbfe;border-radius:4px;padding:2px 6px;text-decoration:none;margin-right:4px">Orig</a>'
                    + f'<a href="/beleg/{bid}/pdf/anon" target="_blank" style="font-size:11px;color:#2563eb;border:0.5px solid #bfdbfe;border-radius:4px;padding:2px 6px;text-decoration:none;margin-right:4px">Anon</a>'
                    + f'<a href="/beleg/{bid}/pdf/analyse" target="_blank" style="font-size:11px;color:#2563eb;border:0.5px solid #bfdbfe;border-radius:4px;padding:2px 6px;text-decoration:none">KI</a>'
                    + '</td></tr>')

            # Tages-Trennlinie
            rows_html += (
                f'<tr><td colspan="5" style="padding:0;height:2px;'
                f'background:var(--border)"></td></tr>')

        # Belege ohne Datum
        undated = belege_by_date.get(None, [])
        if undated:
            rows_html += (f'<tr><td colspan="5" style="padding:8px 12px;'
                         f'font-size:12px;color:#94a3b8;font-style:italic">'
                         f'Belege ohne Datum ({len(undated)})</td></tr>')
            for b in undated:
                bid = g(b,"id",0); typ = g(b,"transportart",1) or "Sonstiges"
                anbieter = g(b,"anbieter",3) or "–"
                betrag = g(b,"betrag_brutto",4)
                waehrung = g(b,"waehrung",5) or "EUR"
                bet_s = f"{float(betrag):.2f} {waehrung}" if betrag else "–"
                badge = BADGE.get(typ, BADGE["Sonstiges"])
                rows_html += (f'<tr><td style="padding:8px 12px"></td>'
                    f'<td style="padding:8px 12px">{badge}</td>'
                    f'<td style="padding:8px 12px;font-size:13px">{anbieter}</td>'
                    f'<td style="padding:8px 12px;text-align:right;font-weight:600">{bet_s}</td>'
                    f'<td style="padding:8px 12px">'
                    f'<a href="/beleg/{bid}" style="font-size:11px;color:#2563eb">Detail</a>'
                    f'</td></tr>')

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
                           text-align:left;width:80px">Datum</th>
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:left;width:140px">Art</th>
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:left">Details / Mahlzeiten</th>
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:right;width:110px">Kosten</th>
                <th style="padding:10px 12px;font-size:11px;color:#64748b;
                           text-align:left;width:150px">Belege</th>
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
        # Unzugeordnete Belege zaehlen
        try:
            cur.execute("SELECT COUNT(*) FROM belege WHERE reise_code IS NULL")
            unzugeordnet = cur.fetchone()[0]
        except:
            unzugeordnet = 0
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
        {f'<a href="/unzugeordnet" style="display:inline-flex;align-items:center;gap:8px;'
          f'background:#fef2f2;border:1px solid #fca5a5;color:#991b1b;'
          f'padding:10px 16px;border-radius:8px;text-decoration:none;font-weight:600;'
          f'margin-bottom:20px;font-size:13px">'
          f'⚠ {unzugeordnet} Beleg{"e" if unzugeordnet!=1 else ""} ohne Reisezuordnung → Jetzt zuordnen'
          f'</a>' if unzugeordnet > 0 else ''}

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
            <a href="/reise/{rcode}/uebersicht" class="btn btn-primary">📋 Übersicht</a>
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

