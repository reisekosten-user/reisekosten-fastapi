"""
Belegdeckung: Jede Buchung braucht echte Rechnungen oder Quittungen.

Beispiel: Buchungsbestätigung über 300,00 EUR, dazu zwei Rechnungen über 120,00 und 180,00 EUR.
- Die Belege werden zu einer Gruppe verknüpft (belege.beleg_gruppe_id).
- Die Buchung ist vollständig belegt (120 + 180 = 300) und zählt in der Kostensumme NICHT mit,
  nur die beiden Rechnungen.
- Fehlt eine Rechnung (z. B. nur 120 von 300), bleibt der offene Rest (180) als „vorläufig“ in der Summe.
- Gibt es gar keine Rechnung/Quittung, zählt die ganze Buchung vorläufig mit.

Die Rechenlogik (ohne Datenbank) steht oben und ist einzeln testbar; darunter folgen die
Datenbank-Helfer und die HTML-Bausteine für Beleg-Seite und Abschluss.
"""
import itertools
import re
from html import escape as _e

from mod_db import get_db, ph, is_postgres

# Von der App selbst erzeugte Belege (gleiche Liste wie EIGENBELEG_ARTEN in main.py).
# Sie decken keine Buchung ab: ein Bewirtungs-Eigenbeleg ist keine Rechnung zur Buchung.
EIGENBELEG_ARTEN = ("Bewirtungsbeleg", "Vorschussquittung", "Rueckgabequittung", "Bargeldbeschaffung")

TOLERANZ_GLEICHE_WAEHRUNG = 0.01       # Beträge in derselben Währung müssen auf den Cent stimmen
TOLERANZ_FREMDWAEHRUNG = 0.05          # Vergleich über Euro-Werte: 5 % (Kursabweichung), mindestens 1 EUR
MIN_CODE_LAENGE = 4                    # kürzere Buchungscodes sind für eine Automatik zu unsicher

_ART_NAMEN = {"Buchungsbestaetigung": "Buchungsbestätigung", "Flugaenderung": "Flugänderung"}


# ── Rechenlogik (ohne Datenbank) ─────────────────────────────────────────────

def _zahl(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def waehrung_von(b) -> str:
    return (b.get("waehrung") or "EUR").upper()


def ist_buchung(art) -> bool:
    """Buchungsbestätigung = Vorab-Beleg, der durch Rechnungen/Quittungen belegt werden muss."""
    return "buchungsbest" in (art or "").lower()


def ist_belegart_final(art) -> bool:
    """Bewusst eine BLACKLIST: Nur „Buchungsbestätigung“ ist vorläufig. Alle anderen Arten
    (Rechnung, Quittung, Tankbeleg, Taxi, Sonstiges …) sind endgültige Belege."""
    return not ist_buchung(art)


def ist_deckender_beleg(art) -> bool:
    """Zählt für die Abdeckung einer Buchung: endgültiger Beleg, aber kein Eigenbeleg der App."""
    return ist_belegart_final(art) and (art or "") not in EIGENBELEG_ARTEN


def hat_betrag(b) -> bool:
    z = _zahl(b.get("betrag_brutto"))
    return z is not None and z > 0


def eur_wert(b):
    """Euro-Betrag ohne Nebenkosten für den Vergleich. None = nicht bekannt (Fremdwährung ohne Kurs)."""
    brutto = _zahl(b.get("betrag_brutto"))
    if brutto is None:
        return None
    final = _zahl(b.get("betrag_eur_final"))
    if final is not None:
        return final
    if waehrung_von(b) == "EUR":
        return brutto
    return _zahl(b.get("betrag_eur"))


def _summe_eur(liste):
    werte = [eur_wert(b) for b in liste]
    return None if any(w is None for w in werte) else sum(werte)


def deckung_berechnen(belege) -> dict:
    """
    belege: Liste von dicts mit mindestens id, belegart, beleg_gruppe_id, betrag_brutto, waehrung,
    betrag_eur, betrag_eur_final. Ergebnis: {gruppen_id: info} für jede Gruppe.

    status:
      keine_buchung      Gruppe enthält keine Buchungsbestätigung mit Betrag (nichts zu prüfen)
      offen              Buchung, aber noch keine Rechnung/Quittung mit Betrag
      teilweise          Rechnungen decken die Buchung nur zum Teil (fehlt > 0)
      vollstaendig       Rechnungen decken die Buchung (ggf. mehr: Feld „mehr“)
      nicht_vergleichbar Fremdwährungen ohne Kurs, Beträge lassen sich nicht vergleichen
    """
    gruppen = {}
    for b in belege:
        gid = b.get("beleg_gruppe_id")
        if gid:
            gruppen.setdefault(gid, []).append(b)

    ergebnis = {}
    for gid, mitglieder in gruppen.items():
        buchungen = [b for b in mitglieder if ist_buchung(b.get("belegart"))]
        buchungen_mit = [b for b in buchungen if hat_betrag(b)]
        finale = [b for b in mitglieder if ist_belegart_final(b.get("belegart"))]
        deckende = [b for b in finale if ist_deckender_beleg(b.get("belegart"))]
        deckende_mit = [b for b in deckende if hat_betrag(b)]
        info = {
            "gruppe": gid,
            "buchung_ids": [b["id"] for b in buchungen],
            "buchung_mit_betrag_ids": [b["id"] for b in buchungen_mit],
            "finale_ids": [b["id"] for b in finale],
            "deckende_ids": [b["id"] for b in deckende],
            "deckende_mit_betrag_ids": [b["id"] for b in deckende_mit],
            # Änderungs-Dokumente (z. B. Flugänderung) haben absichtlich keinen Betrag: nicht anmahnen
            "ohne_betrag_ids": [b["id"] for b in deckende if not hat_betrag(b)
                                and "aenderung" not in (b.get("belegart") or "").lower()
                                and "änderung" not in (b.get("belegart") or "").lower()],
            "soll": 0.0, "ist": 0.0, "waehrung": "EUR",
            "fehlt": 0.0, "fehlt_eur": 0.0, "mehr": 0.0, "status": "keine_buchung",
        }
        ergebnis[gid] = info
        if not buchungen_mit:
            continue

        soll_eur = _summe_eur(buchungen_mit)
        waehrungen = {waehrung_von(b) for b in buchungen_mit + deckende_mit}

        if not deckende_mit:
            w_buchung = {waehrung_von(b) for b in buchungen_mit}
            if len(w_buchung) == 1:
                w = next(iter(w_buchung))
                soll = sum(float(b["betrag_brutto"]) for b in buchungen_mit)
            else:
                w, soll = "EUR", (soll_eur or 0.0)
            info.update(status="offen", soll=soll, ist=0.0, waehrung=w, fehlt=soll, fehlt_eur=soll_eur)
            continue

        if len(waehrungen) == 1:
            w = next(iter(waehrungen))
            soll = sum(float(b["betrag_brutto"]) for b in buchungen_mit)
            ist = sum(float(b["betrag_brutto"]) for b in deckende_mit)
            tol = TOLERANZ_GLEICHE_WAEHRUNG
            faktor = (soll_eur / soll) if (soll_eur is not None and soll > 0) else None
        else:
            ist_eur = _summe_eur(deckende_mit)
            if soll_eur is None or ist_eur is None:
                info.update(status="nicht_vergleichbar", waehrung="EUR")
                continue
            w, soll, ist = "EUR", soll_eur, ist_eur
            tol = max(1.0, TOLERANZ_FREMDWAEHRUNG * soll)
            faktor = 1.0

        diff = soll - ist
        info.update(soll=soll, ist=ist, waehrung=w)
        if diff > tol + 1e-9:
            info.update(status="teilweise", fehlt=diff,
                        fehlt_eur=(diff * faktor) if faktor is not None else None)
        else:
            info.update(status="vollstaendig", mehr=(-diff if -diff > tol + 1e-9 else 0.0))
    return ergebnis


def anteile_aus_buchung(belege, deckung=None):
    """
    Die Buchung (z. B. 19725 INR) trägt oft die echten Euro-Werte aus der Kreditkartenabrechnung
    (Euro-Betrag, Auslandsentgelt), die Teilrechnungen (9606 + 10119 INR) nicht. Damit beim Wechsel von
    der Buchung auf die Rechnungen nichts verloren geht, bekommt jede Rechnung ohne eigenen Euro-Betrag
    ihren Anteil: Rechnungsbetrag / Buchungsbetrag. Gilt nur für Gruppen mit Status vollständig/teilweise,
    einer einzigen Fremdwährung und einer Buchung mit bestätigtem Euro-Betrag.
    Ergebnis: (anteile, rest_gebuehr)
      anteile      {beleg_id: {"eur_final", "entgelt", "nebenkosten"}}; entgelt None = eigenen Wert behalten
      rest_gebuehr {gruppen_id: Euro}  Gebühren-Anteil des noch nicht belegten Rests (nur bei „teilweise“)
    """
    deckung = deckung if deckung is not None else deckung_berechnen(belege)
    by_id = {b["id"]: b for b in belege}
    anteile, rest_gebuehr = {}, {}
    for gid, info in deckung.items():
        if info["status"] not in ("vollstaendig", "teilweise"):
            continue
        buchungen = [by_id[i] for i in info["buchung_mit_betrag_ids"]]
        rechnungen = [by_id[i] for i in info["deckende_mit_betrag_ids"]]
        waehrungen = {waehrung_von(b) for b in buchungen + rechnungen}
        if len(waehrungen) != 1 or "EUR" in waehrungen:
            continue
        finale = [_zahl(b.get("betrag_eur_final")) for b in buchungen]
        soll = sum(float(b["betrag_brutto"]) for b in buchungen)
        if soll <= 0 or any(f is None for f in finale):
            continue
        faktor = sum(finale) / soll
        entgelte = [_zahl(b.get("kreditkarte_auslandsentgelt_eur")) for b in buchungen]
        entgelt_f = (sum(x or 0.0 for x in entgelte) / soll) if any(x is not None for x in entgelte) else None
        neben_f = sum(_zahl(b.get("nebenkosten_eur")) or 0.0 for b in buchungen) / soll
        for b in rechnungen:
            if _zahl(b.get("betrag_eur_final")) is not None:
                continue                      # hat schon einen eigenen, bestätigten Euro-Betrag
            brutto = float(b["betrag_brutto"])
            anteile[b["id"]] = {"eur_final": round(brutto * faktor, 2),
                                "entgelt": None if entgelt_f is None else round(brutto * entgelt_f, 2),
                                "nebenkosten": round(brutto * neben_f, 2)}
        if info["status"] == "teilweise":
            rest_gebuehr[gid] = round(info["fehlt"] * ((entgelt_f or 0.0) + neben_f), 2)
    return anteile, rest_gebuehr


def kategorisieren(belege) -> dict:
    """
    Teilt die Belege einer Reise für die Kostensumme auf:
      final        endgültige Belege (zählen immer)
      vorlaeufig   Buchung mit Betrag ohne (zählbare) Rechnung: zählt vorläufig voll
      ohne_betrag  Buchung ohne Betrag und ohne Rechnung in der Gruppe: fehlt in der Summe
      rest         je Gruppe mit nur teilweiser Deckung der offene Rest (zählt vorläufig mit)
    Buchungen, die vollständig durch Rechnungen belegt sind, tauchen NICHT auf (kein Doppelzählen).
    anteile      Euro-Anteile der Rechnungen an der Buchung (siehe anteile_aus_buchung)
    """
    deckung = deckung_berechnen(belege)
    anteile, rest_gebuehr = anteile_aus_buchung(belege, deckung)
    erg = {"deckung": deckung, "final": [], "vorlaeufig": [], "ohne_betrag": [], "rest": [],
           "anteile": anteile}
    rest_gesehen = set()
    for b in belege:
        bid = b["id"]
        if ist_belegart_final(b.get("belegart")):
            erg["final"].append(bid)
            continue
        gid = b.get("beleg_gruppe_id")
        d = deckung.get(gid) if gid else None
        if hat_betrag(b):
            if d is None or d["status"] == "offen":
                erg["vorlaeufig"].append(bid)
            elif d["status"] == "teilweise" and gid not in rest_gesehen:
                rest_gesehen.add(gid)
                erg["rest"].append({"gruppe": gid, "buchung_ids": list(d["buchung_mit_betrag_ids"]),
                                    "fehlt": d["fehlt"], "fehlt_eur": d["fehlt_eur"],
                                    "gebuehr_eur": rest_gebuehr.get(gid, 0.0),
                                    "waehrung": d["waehrung"]})
        else:
            if d is not None and d["finale_ids"]:
                continue
            erg["ohne_betrag"].append(bid)
    return erg


def luecken(belege, kat=None) -> list:
    """Alle Buchungen, denen Rechnungen/Quittungen fehlen (für Warnungen in Abschluss, PDF und Aufgaben)."""
    kat = kat or kategorisieren(belege)
    by_id = {b["id"]: b for b in belege}

    def ohne_betrag_ids(b):
        """Rechnungen/Quittungen derselben Gruppe, bei denen kein Betrag erkannt wurde."""
        d = kat["deckung"].get(b.get("beleg_gruppe_id")) if b.get("beleg_gruppe_id") else None
        return list(d["ohne_betrag_ids"]) if d else []

    out = []
    for bid in kat["vorlaeufig"]:
        b = by_id[bid]
        out.append({"art": "ohne_rechnung", "buchung_id": bid, "anbieter": b.get("anbieter") or "–",
                    "fehlt": float(b["betrag_brutto"]), "waehrung": waehrung_von(b),
                    "ohne_betrag_ids": ohne_betrag_ids(b)})
    for bid in kat["ohne_betrag"]:
        b = by_id[bid]
        out.append({"art": "ohne_betrag", "buchung_id": bid, "anbieter": b.get("anbieter") or "–",
                    "fehlt": None, "waehrung": waehrung_von(b), "ohne_betrag_ids": []})
    for r in kat["rest"]:
        b = by_id[r["buchung_ids"][0]]
        out.append({"art": "teilweise", "buchung_id": r["buchung_ids"][0], "anbieter": b.get("anbieter") or "–",
                    "fehlt": r["fehlt"], "waehrung": r["waehrung"], "ohne_betrag_ids": ohne_betrag_ids(b)})
    return out


def kombination_vorschlag(ziel, kandidaten, muss_id=None, max_anzahl=4):
    """
    Findet die kleinste Auswahl von Belegen, deren Summe genau dem Zielbetrag entspricht
    (z. B. 120 + 180 = 300). kandidaten: Liste (id, betrag), alle in derselben Währung.
    muss_id: Beleg, der in der Auswahl enthalten sein muss. Ergebnis: Liste der IDs oder [].
    """
    tol = TOLERANZ_GLEICHE_WAEHRUNG + 1e-9
    kand = [(i, float(b)) for i, b in kandidaten if b is not None and 0 < float(b) <= ziel + tol][:20]
    for n in range(1, max_anzahl + 1):
        for combo in itertools.combinations(kand, n):
            ids = [c[0] for c in combo]
            if muss_id is not None and muss_id not in ids:
                continue
            if abs(sum(c[1] for c in combo) - ziel) <= tol:
                return ids
    return []


def code_normalisieren(code) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(code or "").upper())


# ── Datenbank-Helfer ─────────────────────────────────────────────────────────

def _v(row, key, idx):
    return row[key] if hasattr(row, "keys") else row[idx]


_SPALTEN = ("id", "belegart", "beleg_gruppe_id", "betrag_brutto", "waehrung", "betrag_eur",
            "betrag_eur_final", "anbieter", "buchungscode", "transportart", "geprueft",
            "rechnungsnummer", "belegdatum", "nebenkosten_eur", "kreditkarte_auslandsentgelt_eur")


def belege_der_reise(rcode: str) -> list:
    """Alle Belege der Reise als dicts (für deckung_berechnen/kategorisieren)."""
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT {', '.join(_SPALTEN)} FROM belege WHERE reise_code={P} ORDER BY id", (rcode,))
        return [{k: _v(r, k, i) for i, k in enumerate(_SPALTEN)} for r in cur.fetchall()]
    finally:
        cur.close(); db.close()


def gruppe_zusammenfuehren(db, cur, a_id: int, b_id: int):
    """
    Fasst zwei Belege zu einer Gruppe zusammen (Commit macht der Aufrufer):
    - keiner hat eine Gruppe -> neue Gruppe für beide
    - einer hat eine Gruppe  -> der andere tritt bei
    - beide haben (andere) Gruppen -> Gruppen werden zusammengeführt
    """
    P = ph()
    cur.execute(f"SELECT beleg_gruppe_id FROM belege WHERE id={P}", (a_id,))
    r1 = cur.fetchone()
    cur.execute(f"SELECT beleg_gruppe_id FROM belege WHERE id={P}", (b_id,))
    r2 = cur.fetchone()
    g1 = _v(r1, "beleg_gruppe_id", 0) if r1 else None
    g2 = _v(r2, "beleg_gruppe_id", 0) if r2 else None
    if g1 and g2:
        if g1 != g2:
            cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE beleg_gruppe_id={P}", (g1, g2))
        return g1
    if g1:
        cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE id={P}", (g1, b_id))
        return g1
    if g2:
        cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE id={P}", (g2, a_id))
        return g2
    if is_postgres():
        cur.execute("INSERT INTO beleg_gruppen DEFAULT VALUES RETURNING id")
        gid = cur.fetchone()[0]
    else:
        cur.execute("INSERT INTO beleg_gruppen DEFAULT VALUES")
        gid = cur.lastrowid
    cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE id={P}", (gid, a_id))
    cur.execute(f"UPDATE belege SET beleg_gruppe_id={P} WHERE id={P}", (gid, b_id))
    return gid


def auto_verknuepfen(beleg_id: int) -> int:
    """
    Neuer Beleg: Bei EXAKT gleichem Buchungscode (Groß/Kleinschreibung, Leer- und Sonderzeichen egal,
    mind. 4 Zeichen) in DERSELBEN Reise wird eine Buchungsbestätigung mit der Rechnung/Quittung zur
    Gruppe verknüpft – egal in welcher Reihenfolge die Belege eingehen. Alles andere bleibt Handarbeit
    (die Beleg-Seite macht dort Vorschläge). Gibt die Zahl der verknüpften Partner zurück.
    """
    P = ph()
    db = get_db(); cur = db.cursor()
    try:
        cur.execute(f"SELECT reise_code, belegart, buchungscode FROM belege WHERE id={P}", (beleg_id,))
        r = cur.fetchone()
        if not r:
            return 0
        rcode, art, code = _v(r, "reise_code", 0), _v(r, "belegart", 1), _v(r, "buchungscode", 2)
        norm = code_normalisieren(code)
        if not rcode or len(norm) < MIN_CODE_LAENGE or (art or "") in EIGENBELEG_ARTEN:
            return 0
        cur.execute(f"""SELECT id, belegart, buchungscode FROM belege
                        WHERE reise_code={P} AND id!={P} AND buchungscode IS NOT NULL""", (rcode, beleg_id))
        partner = []
        for z in cur.fetchall():
            zart = _v(z, "belegart", 1) or ""
            if code_normalisieren(_v(z, "buchungscode", 2)) != norm:
                continue
            if ist_buchung(zart) == ist_buchung(art) or zart in EIGENBELEG_ARTEN:
                continue                       # gleiche Klasse oder Eigenbeleg: kein Partner
            partner.append(_v(z, "id", 0))
        for pid in partner:
            gruppe_zusammenfuehren(db, cur, beleg_id, pid)
        db.commit()
        return len(partner)
    except Exception:
        try:
            db.rollback()
        except Exception:
            pass
        return 0
    finally:
        cur.close(); db.close()


# ── HTML-/Text-Bausteine ─────────────────────────────────────────────────────

def _betrag(x, w="EUR") -> str:
    return f"{float(x):.2f} {w}"


def art_name(art) -> str:
    return _ART_NAMEN.get(art or "", art or "Beleg")


def ohne_betrag_text(ids) -> str:
    """„ – Beleg #232 hat keinen Betrag“ (leer, wenn alle Belege einen Betrag haben)."""
    ids = list(ids or [])
    if not ids:
        return ""
    nr = ", ".join(f"#{i}" for i in ids)
    return f" – Beleg {nr} hat keinen Betrag" if len(ids) == 1 else f" – Belege {nr} haben keinen Betrag"


def status_anzeige(info):
    """(Text, Textfarbe, Hintergrund) für den Deckungsstatus einer Gruppe; None, wenn nichts zu prüfen ist."""
    st, w = info["status"], info["waehrung"]
    ohne = ohne_betrag_text(info.get("ohne_betrag_ids"))
    if st == "vollstaendig":
        text = "✓ vollständig belegt"
        if info["mehr"]:
            text += f" (Rechnungen liegen {_betrag(info['mehr'], w)} über der Buchung)"
        return text, "#065f46", "#ecfdf5"
    if st == "teilweise":
        return (f"⚠ teilweise belegt – es fehlen noch {_betrag(info['fehlt'], w)}{ohne}", "#9a3412", "#fff7ed")
    if st == "offen":
        if info.get("ohne_betrag_ids"):
            return f"⚠ noch keine Rechnung/Quittung mit Betrag{ohne}", "#9a3412", "#fff7ed"
        return "⚠ noch keine Rechnung/Quittung zur Buchung", "#9a3412", "#fff7ed"
    if st == "nicht_vergleichbar":
        return "Beträge in verschiedenen Währungen ohne Kurs – bitte selbst prüfen", "#475569", "#f1f5f9"
    return None


def status_text(info) -> str:
    """Wie status_anzeige, aber als reiner Text (z. B. für das PDF)."""
    a = status_anzeige(info)
    return re.sub(r"^[✓⚠]\s*", "", a[0]) if a else ""


def zusammenfassung_html(info, by_id) -> str:
    """Kompakte Rechnung für die Beleg-Seite: Buchung x · Rechnungen x1 + x2 = x · Status."""
    a = status_anzeige(info)
    if not a:
        return ""
    text, farbe, bg = a
    w = info["waehrung"]
    zeilen = [f"<div>Buchung: <b>{_betrag(info['soll'], w)}</b></div>"]
    if info["deckende_mit_betrag_ids"]:
        teile = [f"{float(by_id[i]['betrag_brutto']):.2f}" for i in info["deckende_mit_betrag_ids"]]
        gleiche_w = all(waehrung_von(by_id[i]) == w for i in info["deckende_mit_betrag_ids"])
        if gleiche_w and len(teile) > 1:
            rechnung = " + ".join(teile) + f" = <b>{_betrag(info['ist'], w)}</b>"
        else:
            rechnung = f"<b>{_betrag(info['ist'], w)}</b>"
        zeilen.append(f"<div>Rechnungen/Quittungen: {rechnung}</div>")
    else:
        zeilen.append("<div>Rechnungen/Quittungen: –</div>")
    return (f'<div style="background:{bg};color:{farbe};border-radius:6px;padding:8px 10px;margin-bottom:10px;'
            f'font-size:12px">{"".join(zeilen)}<div style="font-weight:700;margin-top:2px">{_e(text)}</div></div>')


def luecken_html(liste, by_id) -> str:
    """Warnkasten für den Abschluss: Buchungen, denen Rechnungen/Quittungen fehlen."""
    if not liste:
        return ""
    items = ""
    for l in liste:
        link = f'<a href="/beleg/{l["buchung_id"]}" style="color:var(--blue)">Buchung #{l["buchung_id"]}</a>'
        b = by_id.get(l["buchung_id"], {})
        typ = _e(b.get("transportart") or "")
        kopf = f"{typ + ': ' if typ else ''}{_e(l['anbieter'])} – {link} "
        if l["art"] == "teilweise":
            items += (f'<li style="margin:4px 0">{kopf}<span style="color:#c2410c">'
                      f'⚠ Rechnungen decken nur einen Teil – es fehlen {_betrag(l["fehlt"], l["waehrung"])} '
                      f'(als vorläufig mitgezählt){_e(ohne_betrag_text(l.get("ohne_betrag_ids")))}</span></li>')
        elif l["art"] == "ohne_rechnung":
            items += (f'<li style="margin:4px 0">{kopf}<span style="color:#c2410c">'
                      f'⚠ noch keine Rechnung/Quittung – {_betrag(l["fehlt"], l["waehrung"])} '
                      f'als vorläufig mitgezählt{_e(ohne_betrag_text(l.get("ohne_betrag_ids")))}</span></li>')
        else:
            items += (f'<li style="margin:4px 0">{kopf}<span style="color:#ef4444">'
                      f'⚠ keine Rechnung/Quittung und kein Betrag erkannt</span></li>')
    return (f'<div class="alert alert-warn" style="margin-top:16px"><b>⚠ Jede Buchung braucht Rechnungen oder '
            f'Quittungen – hier fehlt etwas:</b><ul style="margin-top:8px;padding-left:16px">{items}</ul></div>')


def luecken_texte(liste) -> list:
    """Dieselben Hinweise als reine Textzeilen (PDF)."""
    out = []
    for l in liste:
        if l["art"] == "teilweise":
            out.append(f"Buchung #{l['buchung_id']} ({l['anbieter']}): Rechnungen decken nur einen Teil, "
                       f"es fehlen {_betrag(l['fehlt'], l['waehrung'])} (vorläufig mitgezählt){ohne_betrag_text(l.get('ohne_betrag_ids'))}")
        elif l["art"] == "ohne_rechnung":
            out.append(f"Buchung #{l['buchung_id']} ({l['anbieter']}): noch keine Rechnung/Quittung, "
                       f"{_betrag(l['fehlt'], l['waehrung'])} vorläufig mitgezählt{ohne_betrag_text(l.get('ohne_betrag_ids'))}")
        else:
            out.append(f"Buchung #{l['buchung_id']} ({l['anbieter']}): keine Rechnung/Quittung, kein Betrag erkannt")
    return out


def gruppen_uebersicht_html(deckung, by_id) -> str:
    """Karte „Buchungen und zugehörige Rechnungen“ für den Abschluss (nur Gruppen mit Buchung)."""
    bloecke = ""
    for gid, info in deckung.items():
        if not info["buchung_ids"] or not info["finale_ids"] and info["status"] != "offen":
            continue
        a = status_anzeige(info)
        w = info["waehrung"]
        zeilen = ""
        for bid in info["buchung_ids"] + [i for i in info["finale_ids"]]:
            b = by_id.get(bid)
            if not b:
                continue
            bet = _betrag(b["betrag_brutto"], waehrung_von(b)) if _zahl(b.get("betrag_brutto")) else "–"
            rolle = "Buchung" if ist_buchung(b.get("belegart")) else art_name(b.get("belegart"))
            zeilen += (f'<tr><td><a href="/beleg/{bid}" style="color:var(--blue);font-weight:600;'
                       f'text-decoration:none">#{bid}</a></td><td>{_e(rolle)}</td>'
                       f'<td>{_e(b.get("anbieter") or "–")}</td>'
                       f'<td style="text-align:right;font-family:monospace">{bet}</td></tr>')
        fuss = ""
        if a:
            fuss = (f'<div style="padding:8px 16px;background:{a[2]};color:{a[1]};font-size:12px;font-weight:600">'
                    f'Buchung {_betrag(info["soll"], w)} · Rechnungen/Quittungen {_betrag(info["ist"], w)} · {_e(a[0])}</div>')
        erste = by_id.get(info["buchung_ids"][0], {})
        bloecke += (f'<div style="padding:10px 16px 0;font-weight:600;font-size:13px;'
                    f'border-top:1px solid var(--border)">{_e(erste.get("anbieter") or "Buchung")}</div>'
                    f'<div class="table-wrap"><table><thead><tr><th>Beleg</th><th>Art</th><th>Anbieter</th>'
                    f'<th style="text-align:right">Betrag</th></tr></thead><tbody>{zeilen}</tbody></table></div>{fuss}')
    if not bloecke:
        return ""
    return ('<div class="card" style="margin-bottom:16px"><div class="card-header">'
            '<span class="card-title">🔗 Buchungen und zugehörige Rechnungen</span></div>'
            f'{bloecke}</div>')
