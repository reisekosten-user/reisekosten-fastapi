VERSION 6.5

Fokus: stabile Flug/Zug/Transport-Erkennung + robustere Mistral-Prompts

(gekürzt: nur relevante Änderungen gegenüber 6.4 – Rest identisch lassen in deiner Datei!)

🔥 WICHTIG: ERSETZE NUR DIESE FUNKTIONEN IN DEINER MAIN.PY

def mistral_classify(document_text: str, source_type: str): system_prompt = ( "Du klassifizierst Dokumente für ein Reisekosten-System. " "Arbeite vollständig allgemeingültig ohne feste Listen von Städten, Airlines oder Anbietern. "

"\n\nKLASSIFIKATION:\n"
    "1. Bestimme document_group:\n"
    "- transport (alle Arten von Fortbewegung)\n"
    "- accommodation (Hotel, Airbnb etc.)\n"
    "- restaurant (Verpflegung)\n"
    "- supporting_document (Kalender, Info)\n"

    "\n2. Bestimme document_type:\n"
    "- Flug, Zug, Transport, Hotel, Restaurant oder Unknown\n"

    "\n3. WICHTIG – TRANSPORTLOGIK:\n"
    "Ein Dokument ist TRANSPORT wenn Bewegung von A nach B stattfindet.\n"

    "UNTERSCHEIDUNG:\n"
    "- Flug: Luftverkehr, Flughäfen, Flugnummern, Airlines\n"
    "- Zug: Bahnverkehr (DB, Train, Rail etc.)\n"
    "- Transport: Taxi, Uber, Shuttle, Mietwagen, Transfer\n"

    "SONDERFALL:\n"
    "Wenn ein Flugticket SEGMENTE enthält, die durch Bahn ausgeführt werden (z.B. Lufthansa + Deutsche Bahn), dann:\n"
    "- Gesamt bleibt Transport\n"
    "- Details entscheiden später pro Segment\n"

    "\n4. Rolle bestimmen:\n"
    "- booking_confirmation\n"
    "- itinerary\n"
    "- invoice\n"
    "- receipt\n"

    "\n5. Confidence realistisch setzen"
)

user_prompt = f"Quelle: {source_type}\n\nText:\n{document_text[:20000]}"

return mistral_request(
    [{"role": "system", "content": system_prompt},
     {"role": "user", "content": user_prompt}],
    schema_classification(),
)

def mistral_extract_by_group(document_text: str, source_type: str, document_group: str):

if document_group == "transport":
    prompt = (
        "Extrahiere Transportdaten strukturiert.\n\n"

        "ENTSCHEIDUNGSLOGIK:\n"
        "Für jedes Segment MUSS transport_mode gesetzt werden:\n"

        "Flug wenn:\n"
        "- Flughäfen (FRA, ZRH etc.)\n"
        "- Flugnummern\n"
        "- Airline-Bezug\n\n"

        "Zug wenn:\n"
        "- Deutsche Bahn / DB / Train / Rail\n"
        "- oder 'operated by railway'\n\n"

        "Transport wenn:\n"
        "- Taxi / Uber / Shuttle / Transfer\n\n"

        "WICHTIG:\n"
        "- Ein Dokument kann mehrere Segmente enthalten\n"
        "- JEDES Segment separat klassifizieren\n"
        "- KEINE Vermischung\n"

        "- KEINE Währungsumrechnung"
    )
    schema = schema_transport()

elif document_group == "accommodation":
    prompt = "Extrahiere Unterkunftsdaten vollständig"
    schema = schema_accommodation()

elif document_group == "restaurant":
    prompt = "Extrahiere Restaurantdaten vollständig"
    schema = schema_restaurant()

else:
    prompt = "Extrahiere nur Basisinformationen"
    schema = schema_supporting()

return mistral_request(
    [{"role": "system", "content": prompt},
     {"role": "user", "content": f"Text:\n{document_text[:20000]}"}],
    schema,
)

🔥 NEU: bessere finale Entscheidung

def normalize_extraction(document_group: str, classification: dict, details: dict):

detected_type = classification.get("document_type", "Unbekannt")

if document_group == "transport":
    segments = details.get("segments", []) or []
    modes = [s.get("transport_mode") for s in segments if isinstance(s, dict)]

    if "Flug" in modes:
        detected_type = "Flug"
    elif "Zug" in modes:
        detected_type = "Zug"
    else:
        detected_type = "Transport"

return {
    "document_group": document_group,
    "detected_type": detected_type,
    "detected_role": classification.get("document_role", "unknown"),
    "booking_code": details.get("booking_code", ""),
    "person_name": details.get("person_name", ""),
    "detected_date": details.get("document_date", ""),
    "original_amount": details.get("total_amount", ""),
    "original_currency": details.get("currency", ""),
    "eur_amount_display": "",
    "eur_amount_final": "",
    "fx_status": "manuelle_korrektur_offen",
    "detected_vendor": "",
    "confidence": details.get("confidence", "mittel"),
    "review_flag": details.get("review_flag", "pruefen"),
}

🔥 DAS IST VERSION 6.5

Ziel: stabile KI-Logik statt harter Regeln