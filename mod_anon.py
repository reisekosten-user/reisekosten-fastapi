"""
mod_anon.py – Anonymisierung von Belegen
Stabil / abgeschlossen
"""
from __future__ import annotations
import re

def anonymisieren(text: str, ma_namen: list, ma_mails: list) -> str:
    """
    Suchen & Ersetzen – Reihenfolge wichtig:
    1. E-Mails zuerst (bevor Domain durch Herrhammer-Ersatz zerstört wird)
    2. Herrhammer
    3. Mitarbeiternamen
    4. Telefon
    """
    result = text

    # 1. E-Mail-Adressen ZUERST ersetzen (vor allen anderen Ersetzungen)
    result = re.sub(r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
                    'max.mustermann@beispiel.de', result)

    # 2. Herrhammer (alle Varianten)
    result = re.sub(r'HERRHAMMER\s+GMBH\s+\w*', 'Musterfirma GmbH', result, flags=re.IGNORECASE)
    result = re.sub(r'HERRHAMMER\s+GMBH', 'Musterfirma GmbH', result, flags=re.IGNORECASE)
    result = re.sub(r'HERRHAMMER', 'Musterfirma GmbH', result, flags=re.IGNORECASE)
    # Firmenadresse
    result = re.sub(r'Rudolf\s*-?\s*Diesel\s*-?\s*Str[a-z]*\.?\s*\d*',
                    'Musterstrasse 1', result, flags=re.IGNORECASE)
    result = re.sub(r'97199\s+Ochsenfurt', '00000 Musterstadt', result, flags=re.IGNORECASE)

    # 3. Mitarbeiternamen – jedes Wort, case-insensitive
    woerter = set()
    for name in ma_namen:
        if not name: continue
        woerter.add(name.strip())
        for teil in name.strip().split():
            if len(teil) > 1:
                woerter.add(teil)
        umlaut = [("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"),
                  ("Ä","Ae"),("Ö","Oe"),("Ü","Ue")]
        for wort in list(woerter):
            w2 = wort
            for von, nach in umlaut:
                w2 = w2.replace(von, nach)
            if w2 != wort:
                woerter.add(w2)

    for wort in sorted(woerter, key=len, reverse=True):
        if len(wort) < 2: continue
        result = re.sub(re.escape(wort), "Mustermann", result, flags=re.IGNORECASE)

    # 4. Telefon
    result = re.sub(r'\+49[\s\-./]?[\d\s\-./]{7,15}', '000/000000', result)
    result = re.sub(r'\b0\d{3,5}[\s\-./]?\d{4,8}\b', '000/000000', result)

    return result
    """
    Erstellt alle Schreibvarianten eines Namens für die Anonymisierung.
    Behandelt: Groß/Klein, Umlaute, Komma-Format, Initialen.
    """
    if not name or len(name) < 2:
        return []

    varianten = set()
    varianten.add(name)

    # Umlaut-Ersetzungen (beide Richtungen)
    umlaut_map = {"ä":"ae","ö":"oe","ü":"ue","ß":"ss",
                  "Ä":"Ae","Ö":"Oe","Ü":"Ue",
                  "ae":"ä","oe":"ö","ue":"ü"}
    name_ascii = name
    for k, v in umlaut_map.items():
        name_ascii = name_ascii.replace(k, v)
    varianten.add(name_ascii)

    # Teile (Vorname, Nachname einzeln)
    parts = name.split()
    for part in parts:
        if len(part) > 2:
            varianten.add(part)
            # Umlaut-Variante des Teils
            p_ascii = part
            for k, v in umlaut_map.items():
                p_ascii = p_ascii.replace(k, v)
            varianten.add(p_ascii)

    # Komma-Format: "NACHNAME,VORNAME" oder "Nachname, Vorname"
    if len(parts) >= 2:
        varianten.add(f"{parts[-1]},{parts[0]}")
        varianten.add(f"{parts[-1]}, {parts[0]}")
        varianten.add(f"{parts[-1].upper()},{parts[0].upper()}")
        varianten.add(f"{parts[-1].upper()}, {parts[0].upper()}")

    # Alles Großbuchstaben
    varianten.add(name.upper())
    varianten.add(name_ascii.upper())

    return [v for v in varianten if len(v) > 2]


