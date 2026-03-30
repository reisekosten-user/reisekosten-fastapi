@app.get("/trip-review", response_class=HTMLResponse)
def trip_review():
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        SELECT COALESCE(trip_code, '') AS trip_code
        FROM mail_attachments
        GROUP BY COALESCE(trip_code, '')
        ORDER BY COALESCE(trip_code, '')
    """)
    trip_codes = [r[0] for r in cur.fetchall()]

    rows_html = ""

    for trip_code in trip_codes:
        cur.execute("""
            SELECT detected_type, analysis_status, review_flag
            FROM mail_attachments
            WHERE COALESCE(trip_code, '') = %s
        """, (trip_code,))
        items = cur.fetchall()

        has_flight = any(x[0] == "Flug" for x in items)
        has_hotel = any(x[0] == "Hotel" for x in items)
        has_taxi = any(x[0] == "Taxi" for x in items)
        open_reviews = sum(1 for x in items if x[2] == "pruefen")

        warnings = []
        errors = []

        # ❌ Fehler: kein Code
        if trip_code == "":
            errors.append("Einträge ohne Reisecode")

        # ⚠️ Hotel fehlt
        if has_flight and not has_hotel:
            warnings.append("Hotel fehlt")

        # Status bestimmen
        if errors:
            status = "Fehler"
            badge = '<span class="badge-bad">Fehler</span>'
        elif open_reviews > 0 or warnings:
            status = "prüfen"
            badge = '<span class="badge-warn">prüfen</span>'
        else:
            status = "vollständig"
            badge = '<span class="badge-ok">vollständig</span>'

        rows_html += f"""
        <tr>
            <td class="code">{trip_code or '(ohne Code)'}</td>
            <td>{"ja" if has_flight else "nein"}</td>
            <td>{"ja" if has_hotel else "nein"}</td>
            <td>{"ja" if has_taxi else "nein"}</td>
            <td>{open_reviews}</td>
            <td>{", ".join(warnings) if warnings else ""}</td>
            <td>{", ".join(errors) if errors else ""}</td>
            <td>{badge}</td>
        </tr>
        """

    cur.close()
    conn.close()

    return page_shell("Reisebewertung", f"""
    <div class="card">
        <h2>Reisebewertung v2</h2>
        <table>
            <tr>
                <th>Code</th>
                <th>Flug</th>
                <th>Hotel</th>
                <th>Taxi</th>
                <th>Offene Prüfungen</th>
                <th>Warnungen</th>
                <th>Fehler</th>
                <th>Status</th>
            </tr>
            {rows_html}
        </table>
    </div>
    """)