"""
mod_vma_tage.py – VMA-Tage Berechnung, Land-Ermittlung, Mahlzeiten-Abzug
"""
from __future__ import annotations
import json, re
from datetime import date, timedelta

from mod_db import get_db, ph, is_postgres, fmt_date
from mod_vma import VMA_SAETZE, IATA_TO_LAND, STADT_ZU_LAND, vma_fuer_land_erweitert, staedte_fuer_land

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


def _staedte_override(db, land: str, ort: str | None) -> dict | None:
    """
    Prüft, ob für "ort" ein eigener importierter Städte-Satz existiert (z.B.
    Los Angeles, New York, Turin – teurer/günstiger als der Landesdurchschnitt).
    Zentral an EINER Stelle, damit alle Automatik-Pfade (Flug-Segment-Ankunft,
    Hotel-Beleg, Rückreisetag-Regel) konsequent denselben Satz für denselben
    Ort finden – vorher hat nur der Ankunftstag den Städte-Satz gefunden, der
    Abreisetag nicht, wodurch derselbe Ort (z.B. Turin) je nach Tag einen
    anderen VMA-Satz bekam.
    """
    if not land or not ort:
        return None
    try:
        cur = db.cursor()
        staedte = staedte_fuer_land(cur, land)
        cur.close()
    except Exception:
        return None
    ort_norm = ort.strip().lower()
    treffer = next((s for s in staedte if s.strip().lower() == ort_norm
                     or ort_norm in s.strip().lower()
                     or s.strip().lower() in ort_norm), None)
    if not treffer:
        return None
    try:
        cur = db.cursor()
        info = vma_fuer_land_erweitert(cur, land, ort=treffer)
        cur.close()
        return {"voll": info["voll"], "halb": info["halb"], "ort": treffer}
    except Exception:
        return None

def land_fuer_letzten_tag(reise_code: str, datum: date, db, eintaegig: bool, debug: bool = False):
    """
    Ermittelt das Land für den ABREISETAG nach der korrekten gesetzlichen Regel
    (§ 9 Abs. 4a EStG, BMF-Schreiben v. 5.12.2025): Hier zählt NICHT der
    Ankunftsort der Heimreise (der ist ja meist Deutschland), sondern:

    - Bei mehrtägigen Reisen: der letzte tatsächliche Tätigkeitsort VOR der
      Heimreise = Abflugort der ERSTEN Etappe des Tages. Zwischenlandungen/
      Umstiege auf dem Rückweg zählen nicht als Tätigkeitsort.
    - Bei eintägigen Reisen (Hin- und Rückreise am selben Tag): das zuletzt im
      Tagesverlauf besuchte AUSLÄNDISCHE Land (auch hier zählt ein reiner
      Umstieg nicht als Tätigkeitsort, wird hier vereinfachend über den
      letzten fremden Flughafen im chronologischen Verlauf angenähert).

    Gibt den Ländercode zurück, oder None wenn nicht ermittelbar (dann greift
    die normale Logik in land_fuer_tag als Rückfall). Mit debug=True wird
    stattdessen (land, diagnose_dict) zurückgegeben.
    """
    cur = db.cursor()
    P = ph()
    datum_s = datum.isoformat()
    datum_de = datum.strftime("%d.%m.%Y")
    diag = {"gesuchtes_datum": f"{datum_s} / {datum_de}", "belege_gefunden": 0,
            "segmente_gesamt": 0, "segmente_heute": [], "ergebnis": None, "hinweis": ""}

    cur.execute(f"""SELECT id, ki_json FROM belege
        WHERE reise_code={P} AND transportart='Flug'
        AND (event_datum_von={P} OR event_datum_bis={P})""",
        (reise_code, datum_s, datum_s))

    belege_rows = cur.fetchall()
    diag["belege_gefunden"] = len(belege_rows)
    segmente_heute = []
    for row in belege_rows:
        bid = row[0] if isinstance(row, tuple) else row["id"]
        ki_str = row[1] if isinstance(row, tuple) else row["ki_json"]
        if not ki_str: continue
        try:
            segs = json.loads(ki_str).get("segmente") or []
            diag["segmente_gesamt"] += len(segs)
            for s in segs:
                ab_dat = s.get("abreise_datum", "") or ""
                treffer = (ab_dat == datum_s or ab_dat == datum_de)
                if debug:
                    diag["segmente_heute"].append({
                        "beleg_id": bid, "abreise_datum_im_segment": ab_dat,
                        "von_iata": s.get("von_iata"), "von_ort": s.get("von_ort"),
                        "abreise_zeit": s.get("abreise_zeit"), "treffer": treffer,
                    })
                if treffer:
                    segmente_heute.append(s)
        except Exception as e:
            if debug: diag["hinweis"] += f" JSON-Fehler bei Beleg {bid}: {e}."
    cur.close()

    if not segmente_heute:
        diag["hinweis"] += " Keine Segmente mit passendem abreise_datum gefunden -> Rückfall auf normale Logik."
        return (None, None, diag) if debug else (None, None)

    segmente_heute.sort(key=lambda s: s.get("abreise_zeit") or "")

    if eintaegig:
        # Letztes im Tagesverlauf besuchtes ausländisches Land
        land = None
        ort_gefunden = None
        for s in segmente_heute:
            for ort_key, iata_key in (("von_ort","von_iata"), ("nach_ort","nach_iata")):
                iata = s.get(iata_key)
                if iata and iata in IATA_TO_LAND and IATA_TO_LAND[iata] != "DE":
                    land = IATA_TO_LAND[iata]
                    ort_gefunden = s.get(ort_key)
                elif not iata:
                    ort = (s.get(ort_key) or "").strip().lower()
                    if ort in STADT_ZU_LAND and STADT_ZU_LAND[ort] != "DE":
                        land = STADT_ZU_LAND[ort]
                        ort_gefunden = s.get(ort_key)
        diag["ergebnis"] = land
        override = _staedte_override(db, land, ort_gefunden) if land else None
        return (land, override, diag) if debug else (land, override)
    else:
        # Abflugort der ersten Etappe des Tages = letzter Tätigkeitsort
        erste_etappe = segmente_heute[0]
        iata = erste_etappe.get("von_iata")
        land = None
        von_ort_roh = erste_etappe.get("von_ort")
        if iata and iata in IATA_TO_LAND:
            land = IATA_TO_LAND[iata]
        else:
            von_ort = (von_ort_roh or "").strip().lower()
            if von_ort in STADT_ZU_LAND:
                land = STADT_ZU_LAND[von_ort]
            elif debug:
                diag["hinweis"] += f" von_iata='{iata}' nicht in IATA_TO_LAND UND von_ort='{von_ort}' nicht in STADT_ZU_LAND."
        diag["ergebnis"] = land
        # Städte-Sonderfall (z.B. Turin) auch hier prüfen – KONSISTENT mit dem
        # Flug-Segment-Ankunftsschritt oben, sonst bekommt derselbe Ort am
        # Ankunftstag einen anderen VMA-Satz als am Abreisetag.
        override = _staedte_override(db, land, von_ort_roh) if land else None
        return (land, override, diag) if debug else (land, override)


def land_fuer_tag(reise_code: str, datum: date, db,
                   ist_letzter_tag: bool = False, eintaegig: bool = False) -> tuple:
    """
    Ermittelt das Land für einen Tag. Reihenfolge:
    0. Sonderregel Abreisetag/eintägige Reise: letzter tatsächlicher
       Tätigkeitsort (siehe land_fuer_letzten_tag) – NICHT der Ankunftsort
       der Heimreise, der wäre bei einer Rückreise nach Deutschland meist
       falsch (würde fälschlich den Inlandssatz auslösen).
    1. Flug-Segmenten (Ankunftsland des letzten an diesem Tag ankommenden Segments)
    2. Hotel-Belegen (Land des Hotels an diesem Tag)
    3. Reise-Länder-Tabelle (manuell hinterlegt, inkl. Orts-/Städte-Sonderfälle)
    Gibt (land_code, land_name, quelle, override) zurück.
    override ist None oder {"voll": x, "halb": y} – wenn gesetzt, hat der beim
    manuellen Land-Eintrag hinterlegte Satz Vorrang vor dem Standard-Satz.
    """
    P = ph()
    datum_s = datum.isoformat()

    if ist_letzter_tag:
        sonderfall_land, sonderfall_override = land_fuer_letzten_tag(reise_code, datum, db, eintaegig)
        if sonderfall_land:
            # ALLGEMEINGÜLTIGE Ergänzung: Findet die Flugsegment-basierte Regel
            # zwar das richtige LAND, aber KEINEN Städte-Satz (z.B. weil der
            # Rückflug technisch woanders abgeht, keine Koordinaten/IATA hat,
            # oder die Stadt im Flugsegment nicht exakt zum importierten
            # Städtenamen passt), zusätzlich prüfen, ob ein HOTEL-Aufenthalt
            # (checkin bis checkout, inkl. Checkout-Tag selbst) diesen Tag
            # abdeckt – Hotels sind über die echte, geokodierte Adresse
            # zuverlässiger einer Stadt zuzuordnen als ein Flugsegment-Ortsname.
            # Das behebt diese Klasse von Inkonsistenz (unterschiedlicher Satz
            # für denselben Ort je nach Wochentag) grundsätzlich, nicht nur für
            # einen einzelnen zufällig aufgefallenen Fall.
            if not sonderfall_override:
                cur_h = db.cursor()
                cur_h.execute(f"""SELECT hotel_adresse FROM belege
                    WHERE reise_code={P} AND transportart='Hotel'
                    AND hotel_checkin_datum<={P} AND hotel_checkout_datum>={P}
                    LIMIT 1""", (reise_code, datum_s, datum_s))
                hrow = cur_h.fetchone()
                cur_h.close()
                if hrow:
                    hadresse = hrow[0] if isinstance(hrow, tuple) else hrow["hotel_adresse"]
                    if hadresse and "," in hadresse:
                        ort_teil = hadresse.rsplit(",", 1)[-1].strip()
                        ort_teil = re.sub(r'^\d+\s*', '', ort_teil).strip()
                        if ort_teil:
                            sonderfall_override = _staedte_override(db, sonderfall_land, ort_teil)

            if sonderfall_override:
                lname = f'{VMA_SAETZE.get(sonderfall_land, {}).get("name", sonderfall_land)} – {sonderfall_override["ort"]}'
            else:
                lname = VMA_SAETZE.get(sonderfall_land, {}).get("name", sonderfall_land)
            return sonderfall_land, lname, "Letzter Tätigkeitsort (Abreise)", sonderfall_override

    cur = db.cursor()

    # 1. Flug-Segmente: letztes Segment das an diesem Tag ankommt
    cur.execute(f"""SELECT ki_json FROM belege
        WHERE reise_code={P} AND transportart='Flug'
        AND (event_datum_von={P} OR event_datum_bis={P})
        ORDER BY erstellt DESC""", (reise_code, datum_s, datum_s))

    letztes_land = None
    letztes_iata = None
    letzter_ort = None
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
                        letzter_ort = s.get("nach_ort") or None
                    elif not letztes_land:
                        nach_ort = (s.get("nach_ort") or "").strip().lower()
                        if nach_ort in STADT_ZU_LAND:
                            letztes_land = STADT_ZU_LAND[nach_ort]
                            letzter_ort = s.get("nach_ort") or None
        except: pass

    if letztes_land:
        # Städte-Sonderfall automatisch erkennen (z.B. Los Angeles, New York –
        # diese Sätze sind über "VMA-Sätze importieren" bereits in der DB,
        # wurden bisher aber nur bei manueller Länder-Eingabe genutzt, nicht
        # bei der automatischen Erkennung aus Flugsegmenten).
        override = _staedte_override(db, letztes_land, letzter_ort)
        if override:
            lname = f'{VMA_SAETZE.get(letztes_land, {}).get("name", letztes_land)} – {override["ort"]}'
            cur.close()
            return letztes_land, lname, "Flug-Segment (Städte-Satz)", override
        lname = VMA_SAETZE.get(letztes_land, {}).get("name", letztes_land)
        cur.close()
        return letztes_land, lname, "Flug-Segment", None

    # 2. Hotel-Beleg: Hotel das an diesem Tag aktiv ist. WICHTIG: Checkout-Tag
    # bewusst MIT eingeschlossen (<=, nicht <) – der Reisende ist an diesem
    # Tag ja real noch am Ort, bevor er abreist. Vorher wurde der Checkout-Tag
    # ausgeschlossen, wodurch z.B. ein letzter Reisetag ohne eigenen Rückflug
    # (Checkout = letzter Tag) auf einen schlechteren Rückfall (Standard/alte
    # manuelle Reise-Land-Einträge) durchgefallen ist, obwohl das Hotel den
    # Tag eigentlich klar abdeckt.
    # Bei mehreren passenden Hotels (z.B. eine alte Dublette desselben
    # Buchungsvorgangs) wird bewusst eines mit einem KONKRETEN Land bevorzugt
    # – "DE" ist der allgemeine Rückfallwert, wenn nichts Genaueres erkannt
    # wurde, also die unsicherste Angabe, und soll bei einer Dublette nicht
    # "gewinnen".
    cur.execute(f"""SELECT land_beleg, hotel_adresse FROM belege
        WHERE reise_code={P} AND transportart='Hotel'
        AND hotel_checkin_datum<={P} AND hotel_checkout_datum>={P}
        ORDER BY (land_beleg = 'DE') ASC, (hotel_checkin_datum = {P}) DESC, id DESC""",
        (reise_code, datum_s, datum_s, datum_s))
    row = cur.fetchone()
    if row:
        land = (row[0] if isinstance(row, tuple) else row["land_beleg"]) or ""
        adresse = (row[1] if isinstance(row, tuple) else row["hotel_adresse"]) or ""
        if land and land in VMA_SAETZE:
            # Stadt aus der Adresse extrahieren (letzter Teil nach dem Komma,
            # PLZ-Ziffern entfernt) und gegen importierte Städte-Sätze prüfen
            ort_teil = None
            if adresse and "," in adresse:
                ort_teil = adresse.rsplit(",", 1)[-1].strip()
                ort_teil = re.sub(r'^\d+\s*', '', ort_teil).strip()
            override = _staedte_override(db, land, ort_teil) if ort_teil else None
            if override:
                lname = f'{VMA_SAETZE.get(land, {}).get("name", land)} – {override["ort"]}'
                cur.close()
                return land, lname, "Hotel-Beleg (Städte-Satz)", override
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
        AND hotel_checkin_datum<={P} AND hotel_checkout_datum>={P}""",
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
    eintaegig = (tage == 1)
    count = 0

    for i in range(tage):
        tag = ab + timedelta(days=i)
        ist_halb = (i == 0 or i == tage - 1)
        ist_erster_tag = (i == 0)
        ist_letzter_tag = (i == tage - 1)

        # Manuell geänderte Einträge nicht überschreiben
        cur.execute(f"""SELECT id, quelle, trennungspauschale_quelle, trennungspauschale,
                        tatsaechliche_uhrzeit
                        FROM vma_tage WHERE reise_code={P} AND datum={P}""",
                    (reise_code, tag.isoformat()))
        existing = cur.fetchone()
        trenn_quelle_alt = None
        trenn_alt = None
        tatsaechliche_zeit_alt = None
        if existing:
            q = (existing[1] if isinstance(existing, tuple) else existing["quelle"]) or ""
            trenn_quelle_alt = (existing[2] if isinstance(existing, tuple) else existing["trennungspauschale_quelle"]) or "auto"
            trenn_alt = existing[3] if isinstance(existing, tuple) else existing["trennungspauschale"]
            tatsaechliche_zeit_alt = existing[4] if isinstance(existing, tuple) else existing["tatsaechliche_uhrzeit"]
            if q == "manuell":
                continue  # Manuell → nicht anfassen

        lcode, lname, quelle, override = land_fuer_tag(reise_code, tag, db, ist_letzter_tag, eintaegig)
        if override:
            voll = override["voll"]; halb = override["halb"]
        else:
            # WICHTIG: Immer zuerst die importierte Liste (offizielle BMF-Quelle,
            # via "VMA-Sätze importieren") fragen – die statische VMA_SAETZE im
            # Code ist nur ein Rückfall für Länder, die noch nie importiert
            # wurden, und kann veraltet oder ungenau sein.
            info = vma_fuer_land_erweitert(cur, lcode, ort=None)
            voll = info["voll"]; halb = info["halb"]

        # Frühstück aus Beleg
        frueh = fruehstueck_aus_beleg(reise_code, tag, db)
        brutto, netto = vma_berechnen(voll, halb, ist_halb, frueh, False, False)

        # Trennungspauschale (Wochenend-Sonderregelung): manuell gesetzte Werte
        # bleiben unangetastet, sonst automatische Berechnung.
        if trenn_quelle_alt == "manuell":
            trennung = trenn_alt or 0
        else:
            trennung = trennungspauschale_berechnen(tag, ist_halb, ist_erster_tag, ist_letzter_tag,
                                                      tatsaechliche_zeit_alt)

        if existing:
            cur.execute(f"""UPDATE vma_tage SET
                land_code={P}, land_name={P}, vma_satz_voll={P}, vma_satz_halb={P},
                ist_halber_satz={P}, fruehstueck={P}, vma_brutto={P}, vma_netto={P},
                quelle={P}, trennungspauschale={P} WHERE reise_code={P} AND datum={P}""",
                (lcode, lname, voll, halb, ist_halb, frueh, brutto, netto,
                 quelle, trennung, reise_code, tag.isoformat()))
        else:
            cur.execute(f"""INSERT INTO vma_tage
                (reise_code, datum, land_code, land_name, vma_satz_voll, vma_satz_halb,
                 ist_halber_satz, fruehstueck, mittagessen, abendessen,
                 vma_brutto, vma_netto, quelle, trennungspauschale)
                VALUES ({P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P},{P})""",
                (reise_code, tag.isoformat(), lcode, lname, voll, halb,
                 ist_halb, frueh, False, False, brutto, netto, quelle, trennung))
        count += 1

    db.commit()
    cur.close()
    return count


def trennungspauschale_berechnen(tag: date, ist_halber_reisetag: bool,
                                  ist_erster_tag: bool = False, ist_letzter_tag: bool = False,
                                  tatsaechliche_zeit: str | None = None) -> float:
    """
    Trennungspauschale für Wochenend-Reisetage (betriebliche Sonderregelung):
    - Voller Reisetag an einem Samstag/Sonntag: 80 EUR
    - An- oder Abreisetag (halber VMA-Satz), der auf ein Wochenende fällt: 40 EUR,
      ABER nur wenn die 12-Uhr-Grenze tatsächlich erfüllt ist:
        · Antritt-Tag: 40 EUR nur, wenn VOR 12 Uhr losgefahren wurde
        · End-Tag: 40 EUR nur, wenn NACH 12 Uhr beendet wurde
      Ist die tatsächliche Uhrzeit noch nicht erfasst, gilt vorläufig die alte
      Pauschale (40 EUR) als Rückfall – sobald die echte Uhrzeit eingetragen
      wird, wird automatisch nachgerechnet (auch rückwirkend auf 0 EUR, falls
      die 12-Uhr-Grenze tatsächlich nicht erfüllt war).
    - An Werktagen: 0 EUR
    """
    ist_wochenende = tag.weekday() in (5, 6)  # 5=Samstag, 6=Sonntag
    if not ist_wochenende:
        return 0.0
    if not ist_halber_reisetag:
        return 80.0

    if tatsaechliche_zeit:
        try:
            stunde, minute = (int(x) for x in tatsaechliche_zeit.strip().split(":")[:2])
            vor_12 = (stunde, minute) < (12, 0)
            if ist_erster_tag:
                return 40.0 if vor_12 else 0.0
            if ist_letzter_tag:
                return 40.0 if not vor_12 else 0.0
        except Exception:
            pass  # unparsbare Uhrzeit -> Rückfall unten

    return 40.0  # tatsächliche Uhrzeit (noch) nicht bekannt -> alte Pauschale als Rückfall


