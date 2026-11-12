"""
mod_aufraeumen.py – Alte Kalender-Antworten als Belege aufräumen (v3.57)

Vor Version 3.52 wurden Antworten auf Outlook-Einladungen („Accepted: …“, „Declined: …“) als Beleg abgelegt, Dateiname
„Mail: Accepted: …“. Diese Seite listet sie auf und löscht die ausgewählten. Gelöscht wird nur, was sicher kein echter Beleg ist:
kein Betrag, nicht geprüft, nicht an Habel gesendet, keine Prüf-Belegart, nicht Teil einer Bewirtung oder Bargeld-Buchung.
"""
from __future__ import annotations
from html import escape as e

from mod_db import get_db, ph
from mod_mail import _KAL_BETREFF

PRUEF_ARTEN = ("Rechnung", "Quittung", "Bewirtungsbeleg", "Vorschussquittung", "Rueckgabequittung", "Bargeldbeschaffung")


def _g(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


def ist_kalender_antwort(dateiname) -> bool:
    """True bei „Mail: Accepted: …“, „Mail: Declined: …“ und den Entsprechungen anderer Sprachen."""
    name = str(dateiname or "")
    if not name.lower().startswith("mail:"):
        return False
    return bool(_KAL_BETREFF.match(name[5:]))


_SPALTEN = ("id", "dateiname", "reise_code", "belegdatum", "erstellt", "betrag_brutto", "geprueft", "dms_versendet_am", "belegart")


def _grund(r: dict, sperre) -> str:
    """Leer = sicher löschbar; sonst der Grund, warum der Beleg stehen bleibt."""
    if float(r["betrag_brutto"] or 0) != 0:
        return "hat einen Betrag"
    if r["geprueft"]:
        return "ist als geprüft markiert"
    if r["dms_versendet_am"]:
        return "wurde an Habel gesendet"
    if (r["belegart"] or "") in PRUEF_ARTEN:
        return "ist als %s eingestuft" % r["belegart"]
    if sperre:
        try:
            s = sperre(r["id"])
        except Exception:
            s = ""
        if s:
            return "gehört zu einer Bewirtung oder Bargeld-Buchung"
    return ""


def kandidaten(sperre=None) -> list:
    """Alle Belege mit Kalender-Antwort-Dateiname; je Beleg 'grund' (leer = löschbar)."""
    db = get_db(); cur = db.cursor()
    try:
        cur.execute("SELECT " + ", ".join(_SPALTEN) + " FROM belege WHERE LOWER(dateiname) LIKE " + ph() + " ORDER BY id",
                    ("mail:%",))
        out = []
        for row in cur.fetchall():
            r = {k: _g(row, k, i) for i, k in enumerate(_SPALTEN)}
            if not ist_kalender_antwort(r["dateiname"]):
                continue
            r["geprueft"] = bool(r["geprueft"])
            r["grund"] = _grund(r, sperre)
            out.append(r)
        return out
    finally:
        cur.close(); db.close()


def loeschen(ids: list, sperre=None) -> dict:
    """Löscht die gewählten Belege, sofern sie weiterhin Kalender-Antworten und sicher löschbar sind."""
    gewuenscht = set()
    for i in ids:
        try:
            gewuenscht.add(int(i))
        except (TypeError, ValueError):
            continue
    erlaubt = {r["id"]: r for r in kandidaten(sperre)}
    geloescht, uebersprungen, reisen = 0, [], set()
    db = get_db(); cur = db.cursor()
    try:
        for bid in sorted(gewuenscht):
            r = erlaubt.get(bid)
            if r is None:
                uebersprungen.append((bid, "ist keine Kalender-Antwort"))
            elif r["grund"]:
                uebersprungen.append((bid, r["grund"]))
            else:
                cur.execute("DELETE FROM belege WHERE id=" + ph(), (bid,))
                geloescht += 1
                if r["reise_code"]:
                    reisen.add(r["reise_code"])
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        cur.close(); db.close()
    try:
        from mod_vma_tage import vma_auto_aktualisieren
        for rc in reisen:
            vma_auto_aktualisieren(rc)
    except Exception as ex:
        print(f"[Aufräumen] VMA nicht aktualisiert: {ex}")
    return {"geloescht": geloescht, "uebersprungen": uebersprungen}


def seite_html(sperre=None, ergebnis: dict | None = None) -> str:
    liste = kandidaten(sperre)
    loeschbar = [r for r in liste if not r["grund"]]
    meldung = ""
    if ergebnis is not None:
        meldung = '<div class="alert alert-ok" style="margin-bottom:16px">%d Beleg(e) gelöscht.</div>' % ergebnis["geloescht"]
        if ergebnis["uebersprungen"]:
            meldung += '<div class="alert alert-warn" style="margin-bottom:16px"><b>Nicht gelöscht:</b>%s</div>' % "".join(
                "<div>• #%s %s</div>" % (i, e(g)) for i, g in ergebnis["uebersprungen"])
    zeilen = ""
    for r in liste:
        name = str(r["dateiname"] or "")
        if r["grund"]:
            box = '<span title="%s" style="color:var(--muted)">—</span>' % e(r["grund"])
            hinweis = '<span style="font-size:12px;color:#92400e">bleibt: %s</span>' % e(r["grund"])
        else:
            box = '<input type="checkbox" name="ids" value="%s" checked>' % r["id"]
            hinweis = '<span style="font-size:12px;color:var(--muted)">kann gelöscht werden</span>'
        zeilen += ('<tr><td style="width:36px">%s</td><td><a href="/beleg/%s">#%s</a></td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>'
                   % (box, r["id"], r["id"], e(name[:90]), e(r["reise_code"] or "–"), e(str(r["erstellt"] or "")[:10]), hinweis))
    if not zeilen:
        zeilen = '<tr><td colspan="6" class="empty-state">Keine alten Kalender-Antworten vorhanden 🎉</td></tr>'
    knopf = ""
    if loeschbar:
        knopf = ('<button type="submit" class="btn btn-primary" onclick="return confirm(\'Die markierten Belege unwiderruflich löschen?\')">'
                 '🗑 Markierte löschen (%d)</button>' % len(loeschbar))
    return (
        '<div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:16px">'
        '<div><h1 class="page-title" style="margin:0">🧹 Alte Kalender-Antworten aufräumen</h1>'
        '<div style="color:var(--muted);font-size:13px">Antworten auf Outlook-Einladungen („Accepted: …“), die vor Version 3.52 als Beleg abgelegt wurden.</div></div>'
        '<a href="/einstellungen" class="btn btn-secondary">← Einstellungen</a></div>'
        + meldung +
        '<form method="post" action="/einstellungen/kalender-antworten">'
        '<div class="card" style="margin-bottom:16px"><div class="table-wrap"><table>'
        '<thead><tr><th></th><th>Beleg</th><th>Dateiname</th><th>Reise</th><th>Angelegt</th><th></th></tr></thead>'
        '<tbody>%s</tbody></table></div></div>%s</form>' % (zeilen, knopf))
