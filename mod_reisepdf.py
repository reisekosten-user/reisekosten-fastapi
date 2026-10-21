"""
mod_reisepdf.py – PDF-Dokumente für die Reisemappe (v3.41)

In den Einstellungen lädt der Organisator PDFs hoch, die jeder Reisende in seiner Reisemappe braucht
(z. B. die Kopie der Krankenversicherung). Jedes Dokument gilt entweder für ALLE Mitarbeiter oder nur für
einen bestimmten Mitarbeiter (z. B. seine persönliche Versicherungskarte).

Die Reisemappe ist das Reiseplan-PDF. Dieses Modul hängt die passenden Dokumente an und setzt vorne eine
Seite „Anlagen“ mit Inhaltsverzeichnis. Zusätzlich bekommen die PDF-Leser Lesezeichen.
Dateien liegen im S3-Speicher (wie die Belege), die Liste in der Tabelle reise_pdfs.
"""
from __future__ import annotations
import io
import uuid
from datetime import datetime
from xml.sax.saxutils import escape as _xml

from mod_db import get_db, ph, is_postgres

TABELLE = "reise_pdfs"
MAX_MB = 15
MAX_SEITEN = 60
ENDUNGEN_BILD = (".jpg", ".jpeg", ".png", ".tif", ".tiff", ".webp", ".heic", ".heif")


def _g(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


# ── Tabelle ───────────────────────────────────────────────────────

def tabelle_sichern() -> None:
    """Legt die Tabelle an, falls sie fehlt (läuft in Postgres und SQLite gleich)."""
    try:
        db = get_db(); cur = db.cursor()
        key = "SERIAL PRIMARY KEY" if is_postgres() else "INTEGER PRIMARY KEY AUTOINCREMENT"
        cur.execute(f"CREATE TABLE IF NOT EXISTS {TABELLE} ("
                    f"id {key}, titel TEXT, kuerzel TEXT, dateiname TEXT, s3_key TEXT, "
                    f"seiten INTEGER, groesse INTEGER, aktiv INTEGER DEFAULT 1, erstellt TEXT)")
        db.commit(); cur.close(); db.close()
    except Exception as ex:
        print(f"[ReisePDF] Tabelle {TABELLE} nicht angelegt: {ex}")


_SPALTEN = ("id", "titel", "kuerzel", "dateiname", "s3_key", "seiten", "groesse", "aktiv", "erstellt")


def _zeile(r) -> dict:
    d = {k: _g(r, k, i) for i, k in enumerate(_SPALTEN)}
    d["aktiv"] = bool(d["aktiv"]) if d["aktiv"] is not None else True
    d["kuerzel"] = d["kuerzel"] or ""
    return d


def liste(nur_aktive: bool = False) -> list:
    """Alle Dokumente: zuerst die für alle Mitarbeiter, dann die persönlichen (je nach Anlage-Reihenfolge)."""
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT {', '.join(_SPALTEN)} FROM {TABELLE} ORDER BY id")
        rows = [_zeile(r) for r in cur.fetchall()]
    except Exception:
        try: db.rollback()
        except Exception: pass
        rows = []
    finally:
        cur.close(); db.close()
    if nur_aktive:
        rows = [r for r in rows if r["aktiv"]]
    return sorted(rows, key=lambda r: (1 if r["kuerzel"] else 0, r["id"]))


def holen(pid: int) -> dict | None:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT {', '.join(_SPALTEN)} FROM {TABELLE} WHERE id={P}", (pid,))
        r = cur.fetchone()
    finally:
        cur.close(); db.close()
    return _zeile(r) if r else None


# ── Hochladen / Ändern ────────────────────────────────────────────

def pruefen(daten: bytes, dateiname: str, content_type: str = "") -> tuple:
    """Macht aus dem Upload ein lesbares PDF. Rückgabe: (pdf_bytes, seiten) oder wirft ValueError mit klarem Text."""
    if not daten:
        raise ValueError("Die Datei ist leer.")
    if len(daten) > MAX_MB * 1024 * 1024:
        raise ValueError(f"Die Datei ist größer als {MAX_MB} MB.")
    name = (dateiname or "").lower()
    ct = (content_type or "").lower()
    if daten[:4] != b"%PDF":
        if ct.startswith("image/") or name.endswith(ENDUNGEN_BILD):
            try:
                from mod_beleg import bild_zu_pdf
                daten = bild_zu_pdf(daten, dateiname or "bild")
            except Exception as ex:
                raise ValueError(f"Das Bild konnte nicht in ein PDF umgewandelt werden ({str(ex)[:80]}).")
        else:
            raise ValueError("Bitte ein PDF oder ein Foto (JPG/PNG) hochladen.")
    try:
        from pypdf import PdfReader
        r = PdfReader(io.BytesIO(daten))
        if r.is_encrypted:
            if not r.decrypt(""):
                raise ValueError("Das PDF ist passwortgeschützt. Bitte ohne Passwort speichern und neu hochladen.")
        seiten = len(r.pages)
    except ValueError:
        raise
    except Exception as ex:
        raise ValueError(f"Das PDF ist nicht lesbar ({str(ex)[:80]}).")
    if seiten < 1:
        raise ValueError("Das PDF hat keine Seiten.")
    if seiten > MAX_SEITEN:
        raise ValueError(f"Das PDF hat {seiten} Seiten (erlaubt sind höchstens {MAX_SEITEN}).")
    return daten, seiten


def hinzufuegen(titel: str, kuerzel: str, daten: bytes, dateiname: str, content_type: str = "") -> int:
    """Speichert ein Dokument (S3 + Liste). kuerzel leer = für alle Mitarbeiter. Gibt die ID zurück."""
    titel = (titel or "").strip()[:120]
    if not titel:
        raise ValueError("Bitte einen Titel eingeben.")
    pdf, seiten = pruefen(daten, dateiname, content_type)
    from mod_beleg import s3_upload
    key = f"reise_pdfs/{uuid.uuid4().hex}.pdf"
    s3_upload(key, pdf)
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        sql = (f"INSERT INTO {TABELLE} (titel, kuerzel, dateiname, s3_key, seiten, groesse, aktiv, erstellt) "
               f"VALUES ({P},{P},{P},{P},{P},{P},{P},{P})")
        vals = (titel, (kuerzel or "").strip() or None, (dateiname or "")[:200], key, seiten, len(pdf), 1,
                datetime.now().strftime("%Y-%m-%d %H:%M"))
        if is_postgres():
            cur.execute(sql + " RETURNING id", vals)
            neu = cur.fetchone()
            pid = neu[0] if not hasattr(neu, "keys") else neu["id"]
        else:
            cur.execute(sql, vals)
            pid = cur.lastrowid
        db.commit()
    finally:
        cur.close(); db.close()
    return pid


def loeschen(pid: int) -> None:
    """Entfernt den Eintrag aus der Liste. Die Datei im Speicher bleibt als Sicherung liegen."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"DELETE FROM {TABELLE} WHERE id={P}", (pid,))
        db.commit()
    finally:
        cur.close(); db.close()


def aktiv_setzen(pid: int, aktiv: bool) -> None:
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"UPDATE {TABELLE} SET aktiv={P} WHERE id={P}", (1 if aktiv else 0, pid))
        db.commit()
    finally:
        cur.close(); db.close()


def pdf_bytes(pid: int) -> bytes | None:
    d = holen(pid)
    if not d or not d["s3_key"]:
        return None
    from mod_beleg import s3_download
    return s3_download(d["s3_key"])


# ── Anhänge für die Reisemappe ────────────────────────────────────

def anhaenge_fuer(kuerzel: str | None) -> list:
    """Dokumente für diese Reisemappe: alle aktiven „für alle Mitarbeiter“ plus die persönlichen des Mitarbeiters.
    Ohne Kürzel (Reise mit mehreren Reisenden, kein Mitarbeiter gewählt) nur die Dokumente für alle.
    Rückgabe: [{titel, bytes, quelle}] – nicht ladbare Dateien werden übersprungen und in 'fehler' gemeldet."""
    erg = []
    for d in liste(nur_aktive=True):
        if d["kuerzel"] and d["kuerzel"] != (kuerzel or ""):
            continue
        try:
            daten = pdf_bytes(d["id"])
        except Exception as ex:
            erg.append({"titel": d["titel"], "bytes": None, "quelle": "Einstellungen", "fehler": str(ex)[:80]})
            continue
        erg.append({"titel": d["titel"], "bytes": daten, "quelle": "Einstellungen", "fehler": None})
    return erg


def a1_anlage(beleg_id: int, titel: str) -> dict:
    """Die A1-Bescheinigung (Beleg) als Anlage der Reisemappe."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT s3_original FROM belege WHERE id={P}", (beleg_id,))
        r = cur.fetchone()
    finally:
        cur.close(); db.close()
    key = _g(r, "s3_original", 0) if r else None
    if not key:
        return {"titel": titel, "bytes": None, "quelle": "A1", "fehler": "Datei fehlt"}
    try:
        from mod_beleg import s3_download
        return {"titel": titel, "bytes": s3_download(key), "quelle": "A1", "fehler": None}
    except Exception as ex:
        return {"titel": titel, "bytes": None, "quelle": "A1", "fehler": str(ex)[:80]}


def _index_pdf(titel: str, zeilen: list, probleme: list) -> bytes:
    """Eine Seite „Anlagen“: Nummer, Titel, Seite im Gesamt-PDF."""
    from reportlab.lib.pagesizes import A4
    from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
    from reportlab.lib.styles import getSampleStyleSheet
    from reportlab.lib.units import mm
    from reportlab.lib import colors
    st = getSampleStyleSheet()
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4, leftMargin=20 * mm, rightMargin=20 * mm, topMargin=20 * mm,
                            bottomMargin=20 * mm, title="Anlagen")
    story = [Paragraph("Anlagen zur Reisemappe", st["Title"]), Paragraph(_xml(titel), st["Normal"]), Spacer(1, 8 * mm)]
    daten = [["Nr.", "Dokument", "Seite"]]
    for i, (t, seite, n) in enumerate(zeilen, 1):
        daten.append([str(i), Paragraph(_xml(t), st["Normal"]), f"{seite}" if n == 1 else f"{seite}–{seite + n - 1}"])
    tab = Table(daten, colWidths=[14 * mm, 126 * mm, 30 * mm])
    tab.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#1e3a5f")), ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"), ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.HexColor("#cbd5e1")), ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, colors.HexColor("#f1f5f9")]),
    ]))
    story.append(tab)
    if probleme:
        story += [Spacer(1, 6 * mm), Paragraph("<b>Nicht angehängt:</b>", st["Normal"])]
        for t, grund in probleme:
            story.append(Paragraph(f"• {_xml(t)} – {_xml(grund)}", st["Normal"]))
    doc.build(story)
    return buf.getvalue()


def mappe_zusammenbauen(basis_pdf: bytes, anlagen: list, kopf: str = "") -> tuple:
    """
    Hängt die Anlagen hinter das Reiseplan-PDF (zuerst eine Seite „Anlagen“ mit Inhaltsverzeichnis).
    anlagen: [{titel, bytes, fehler}]. Rückgabe: (pdf_bytes, probleme) – probleme = [(titel, Grund)].
    Ist nichts anzuhängen, kommt das Reiseplan-PDF unverändert zurück.
    """
    from pypdf import PdfReader, PdfWriter
    probleme, gut = [], []
    for a in anlagen:
        if not a.get("bytes"):
            probleme.append((a["titel"], a.get("fehler") or "Datei nicht lesbar"))
            continue
        try:
            r = PdfReader(io.BytesIO(a["bytes"]))
            if r.is_encrypted and not r.decrypt(""):
                raise ValueError("passwortgeschützt")
            gut.append((a["titel"], r, len(r.pages)))
        except Exception as ex:
            probleme.append((a["titel"], str(ex)[:80] or "nicht lesbar"))
    if not gut:
        return basis_pdf, probleme
    basis = PdfReader(io.BytesIO(basis_pdf))
    n0 = len(basis.pages)
    n_index = 1
    for _ in range(2):                              # Inhaltsverzeichnis länger als eine Seite: Seitenzahlen neu rechnen
        seite = n0 + n_index + 1
        zeilen = []
        for t, _r, n in gut:
            zeilen.append((t, seite, n))
            seite += n
        index = PdfReader(io.BytesIO(_index_pdf(kopf, zeilen, probleme)))
        if len(index.pages) == n_index:
            break
        n_index = len(index.pages)
    w = PdfWriter()
    for p in basis.pages:
        w.add_page(p)
    for p in index.pages:
        w.add_page(p)
    for _t, r, _n in gut:
        for p in r.pages:
            w.add_page(p)
    try:
        w.add_outline_item("Reiseplan", 0)
        w.add_outline_item("Anlagen (Übersicht)", n0)
        for t, seite, _n in zeilen:
            w.add_outline_item(t, seite - 1)
    except Exception:
        pass                                         # Lesezeichen sind nur Komfort
    out = io.BytesIO()
    w.write(out)
    return out.getvalue(), probleme
