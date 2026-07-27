"""
mod_vma_tage.py – VMA-Tage Berechnung, Land-Ermittlung, Mahlzeiten-Abzug
"""
from __future__ import annotations
import json
from datetime import date, timedelta

from mod_db import get_db, ph, is_postgres, fmt_date
from mod_vma import VMA_SAETZE, IATA_TO_LAND

def vma_berechnen(voll: float, halb: float, ist_halb: bool,
                  frueh: bool, mittag: bool, abend: bool) -> tuple:
    """
    Berechnet VMA brutto und netto nach deutschem Steuerrecht.
    Abzüge: Frühstück 20%, Mittagessen 40%, Abendessen 40% vom vollen Satz.
    Basis: halber oder voller Tagessatz.
    Ergebnis nie negativ.
    """
    basis = float(halb) if ist_halb else float(voll)
    abzug = 0.0
    if frueh:   abzug += float(voll) * 0.20
    if mittag:  abzug += float(voll) * 0.40
    if abend:   abzug += float(voll) * 0.40
    brutto = basis
    netto = max(0.0, basis - abzug)
    return round(brutto, 2), round(netto, 2)

def land_fuer_tag(reise_code: str, datum: date, db) -> tuple:
    """
    Ermittelt das Land für einen Tag aus:
    1. Flug-Segmenten (Ankunftsland des letzten Segments des Tages)
    2. Hotel-Belegen (Land des Hotels an diesem Tag)
    3. Reise-Länder-Tabelle (manuell hinterlegt, inkl. Orts-/Städte-Sonderfälle)
    Gibt (land_code, land_name, quelle, override) zurück.
    override ist None oder {"voll": x, "halb": y} – wenn gesetzt, hat der beim
    manuellen Land-Eintrag hinterlegte Satz Vorrang vor dem Standard-Satz.
    """
    cur = db.cursor()
    P = ph()
    datum_s = datum.isoformat()

    # 1. Flug-Segmente: letztes Segment das an diesem Tag ankommt
    cur.execute(f"""SELECT ki_json FROM belege
        WHERE reise_code={P} AND transportart='Flug'
        AND (event_datum_von={P} OR event_datum_bis={P})
        ORDER BY erstellt DESC""", (reise_code, datum_s, datum_s))

    letztes_land = None
    letztes_iata = None
    for row in cur.fetchall():
        ki_str = row[0] if isinstance(row, tuple) else row["ki_json"]
        if not ki_str: continue
        try:
            ki = json.loads(ki_str)
            segs = ki.get("segmente") or []
            # Segmente die an diesem Tag ankommen
            for s in segs:
                an_dat = s.get("ankunft_datum","")
                if an_dat == datum_s or an_dat == datum.strftime("%d.%m.%Y"):
                    nach_iata = s.get("nach_iata","")
                    if nach_iata and nach_iata in IATA_TO_LAND:
                        letztes_iata = nach_iata
                        letztes_land = IATA_TO_LAND[nach_iata]
        except: pass

    if letztes_land:
        lname = VMA_SAETZE.get(letztes_land, {}).get("name", letztes_land)
        cur.close()
        return letztes_land, lname, "Flug-Segment", None

    # 2. Hotel-Beleg: Hotel das an diesem Tag aktiv ist
    cur.execute(f"""SELECT land_beleg, ki_json FROM belege
        WHERE reise_code={P} AND transportart='Hotel'
        AND hotel_checkin_datum<={P} AND hotel_checkout_datum>{P}""",
        (reise_code, datum_s, datum_s))
    row = cur.fetchone()
    if row:
        land = (row[0] if isinstance(row, tuple) else row["land_beleg"]) or ""
        if land and land in VMA_SAETZE:
            lname = VMA_SAETZE.get(land, {}).get("name", land)
            cur.close()
            return land, lname, "Hotel-Beleg", None

    # 3. Reise-Länder (manuell, inkl. Orts-Sonderfall wie z.B. Los Angeles)
    cur.execute(f"""SELECT land_code, land_name, vma_voll, vma_halb FROM reise_laender
        WHERE reise_code={P} AND datum_von<={P} AND datum_bis>={P}
        ORDER BY id LIMIT 1""", (reise_code, datum_s, datum_s))
    row = cur.fetchone()
    if row:
        lcode = row[0] if isinstance(row, tuple) else row["land_code"]
        lname = row[1] if isinstance(row, tuple) else row["land_name"]
        voll = row[2] if isinstance(row, tuple) else row["vma_voll"]
        halb = row[3] if isinstance(row, tuple) else row["vma_halb"]
        override = {"voll": float(voll), "halb": float(halb)} if voll is not None and halb is not None else None
        cur.close()
        return lcode, lname, "Manuell", override

    cur.close()
    return "DE", "Deutschland", "Standard", None

def fruehstueck_aus_beleg(reise_code: str, datum: date, db) -> bool:
    """
    Prüft ob ein Hotel-Beleg für diesen Tag Frühstück enthält.
    GPT erkennt 'inkl. Frühstück' → fruehstueck=True.
    """
    cur = db.cursor()
    P = ph()
    datum_s = datum.isoformat()
    cur.execute(f"""SELECT ki_json FROM belege
        WHERE reise_code={P} AND transportart='Hotel'
        AND hotel_checkin_datum<={P} AND hotel_checkout_datum>{P}""",
        (reise_code, datum_s, datum_s))
    row = cur.fetchone()
    cur.close()
    if not row: return False
    ki_str = row[0] if isinstance(row, tuple) else row["ki_json"]
    if not ki_str: return False
    try:
        ki = json.loads(ki_str)
        rohtext = ki.get("rohtext","") or ""
        notiz = ki.get("notiz","") or ""
        combined = (rohtext + notiz).lower()
        keywords = ["frühstück","fruehstueck","breakfast","petit-déjeuner",
                    "inkl. frühstück","with breakfast","bb ","b&b"]
        return any(k in combined for k in keywords)
    except: return False

def vma_tage_generieren(reise_code: str, db) -> int:
    """
    Generiert oder aktualisiert VMA-Tage für eine Reise.
    - Iteriert über alle Tage zwischen Abreise und Rückkehr
    - Ermittelt Land aus Belegen/Ländern
    - Erster + letzter Tag = halber Satz
    - Frühstück aus Hotel-Beleg automatisch
    - Überschreibt NICHT manuell geänderte Einträge (quelle='manuell')
    Gibt Anzahl erstellter/aktualisierter Tage zurück.
    """
    cur = db.cursor()
    P = ph()

    cur.execute(f"SELECT abreise, rueckkehr FROM reisen WHERE code={P}", (reise_code,))
    r = cur.fetchone()
    if not r:
        cur.close(); return 0

    ab = r[0] if isinstance(r, tuple) else r["abreise"]
    zu = r[1] if isinstance(r, tuple) else r["rueckkehr"]

    if isinstance(ab, str): ab = date.fromisoformat(ab[:10])
    if isinstance(zu, str): zu = date.fromisoformat(zu[:10])

    tage = (zu - ab).days + 1
    count = 0

    for i in range(tage):
        tag = ab + timedelta(days=i)
        ist_halb = (i == 0 or i == tage - 1)

        # Manuell geänderte Einträge nicht überschreiben
        cur.execute(f"SELECT id, quelle FROM vma_tage WHERE reise_code={P} AND datum={P}",
                    (reise_code, tag.isoformat()))
        existing = cur.fetchone()
        if existing:
            q = (existing[1] if isinstance(existing, tuple) else existing["quelle"]) or ""
            if q == "manuell":
                continue  # Manuell → nicht anfassen

        lcode, lname, quelle, override = land_fuer_tag(reise_code, tag, db)
        if override:
            voll = override["voll"]; halb = override["halb"]
        else:
            satz = VMA_SAETZE.get(lcode, VMA_SAETZE["DE"])
            voll = satz["voll"]; halb = satz["halb"]

        # Frühstück aus Beleg
        frueh = fruehstueck_aus_beleg(reise_code, tag, db)
        brutto, netto = vma_berechnen(voll, halb, ist_halb, frueh, False, False)

        if existing:
            cur.execute(f"""UPDATE vma_tage SET
                land_code={P}, land_name={P}, vma_satz_voll={P}, vma_satz_halb={P},
                ist_halber_satz={P}, fruehstueck={P}, vma_brutto={P}, vma_netto={P},
                quelle={P} WHERE reise_code={P} AND datum={P}""",
                (lcode, lname, voll, halb, ist_halb, frueh, brutto, netto,
                 quelle, reise_code, tag.isoformat()))
        else:
            cur.execute(f"""INSERT INTO vma_tage
                (reise_code, datum, land_code, land_name, vma_satz_voll, vma_satz_halb,
                 ist_halber_satz, fruehstueck, mittagessen, abendessen,
                 vma_brutto, vma_netto, quelle)
                VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
                (reise_code, tag.isoformat(), lcode, lname, voll, halb,
                 ist_halb, frueh, False, False, brutto, netto, quelle))
        count += 1

    db.commit()
    cur.close()
    return count


