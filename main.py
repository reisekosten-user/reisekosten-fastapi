<!DOCTYPE html>
<html lang="de">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Dashboard Mail Test 7.12b</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; background: #f5f7fb; color: #1f2937; }
    .card { background: white; border: 1px solid #dbe2f0; border-radius: 14px; padding: 18px; max-width: 1100px; }
    h2 { margin-top: 0; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: end; }
    .col { flex: 1; min-width: 220px; }
    label { display: block; font-size: 13px; font-weight: 700; margin-bottom: 6px; }
    input { width: 100%; padding: 10px 12px; border: 1px solid #cbd5e1; border-radius: 10px; }
    button { background: #1f4ed8; color: white; border: none; border-radius: 10px; padding: 11px 14px; font-weight: 700; cursor: pointer; }
    pre { background: #0f172a; color: #dbeafe; padding: 14px; border-radius: 12px; white-space: pre-wrap; word-break: break-word; overflow: auto; }
    .hint { color: #64748b; font-size: 13px; margin-top: 8px; }
  </style>
</head>
<body>
  <div class="card">
    <h2>Mail-Test / Mail-Analyse</h2>
    <div class="row">
      <div class="col">
        <label>Betreff enthält</label>
        <input id="subjectFilter" type="text" placeholder="z. B. 26-001" value="26-001" />
      </div>
      <div class="col" style="max-width:180px;">
        <label>Anzahl</label>
        <input id="mailLimit" type="number" min="1" max="10" value="3" />
      </div>
      <div class="col" style="max-width:240px;">
        <button onclick="testMails()">Mails testen</button>
      </div>
      <div class="col" style="max-width:260px;">
        <button onclick="analyzeMails()">Mails analysieren</button>
      </div>
    </div>
    <div class="hint">Verwendet: <code>/mail/test</code> und <code>/mail/analyze-latest</code> mit Betreff-Filter.</div>
    <pre id="resultBox">Noch kein Ergebnis.</pre>
  </div>

  <script>
    function api() {
      return window.location.origin;
    }

    async function parseJsonResponse(res) {
      const txt = await res.text();
      try {
        return JSON.parse(txt);
      } catch {
        throw new Error("Antwort war kein JSON: " + txt.substring(0, 500));
      }
    }

    function buildUrl(path) {
      const subject = document.getElementById('subjectFilter').value.trim();
      const limit = document.getElementById('mailLimit').value || '3';
      const qp = ["limit=" + encodeURIComponent(limit)];
      if (subject) qp.push("subject_contains=" + encodeURIComponent(subject));
      return api() + path + "?" + qp.join('&');
    }

    async function testMails() {
      const res = await fetch(buildUrl('/mail/test'));
      const data = await parseJsonResponse(res);
      document.getElementById('resultBox').textContent = JSON.stringify(data, null, 2);
    }

    async function analyzeMails() {
      const res = await fetch(buildUrl('/mail/analyze-latest'));
      const data = await parseJsonResponse(res);
      document.getElementById('resultBox').textContent = JSON.stringify(data, null, 2);
    }
  </script>
</body>
</html>
