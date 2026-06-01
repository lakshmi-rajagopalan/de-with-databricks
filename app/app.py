import functools
import os

from databricks.sdk import WorkspaceClient
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)


@functools.lru_cache(maxsize=1)
def _resolve_resources():
    """Build dashboard URLs from bundle-injected env vars; discover Genie space via SDK."""
    import sys
    host = os.getenv("DATABRICKS_HOST", "").rstrip("/")
    if host and not host.startswith("http"):
        host = f"https://{host}"
    client_dashboard_id = os.getenv("CLIENT_DASHBOARD_ID", "")
    insights_dashboard_id = os.getenv("INSIGHTS_DASHBOARD_ID", "")
    client_url = f"{host}/dashboardsv3/{client_dashboard_id}/published" if client_dashboard_id else ""
    insights_url = f"{host}/dashboardsv3/{insights_dashboard_id}/published" if insights_dashboard_id else ""
    genie_url = genie_id = ""
    if not host:
        return client_url, insights_url, genie_url, genie_id
    # Try bundle-injected space ID first; fall back to SDK discovery by name
    genie_id = os.getenv("GENIE_SPACE_ID", "")
    if genie_id:
        genie_url = f"{host}/genie/rooms/{genie_id}"
    else:
        try:
            w = WorkspaceClient()
            resp = w.genie.list_spaces()
            spaces = (
                getattr(resp, "genie_spaces", None)
                or getattr(resp, "spaces", None)
                or list(resp)
            )
            for space in (spaces or []):
                if getattr(space, "title", "") == os.getenv("GENIE_SPACE_NAME", "Clickstream Analytics"):
                    genie_id = space.space_id
                    genie_url = f"{host}/genie/rooms/{genie_id}"
                    break
        except Exception as exc:
            print(f"[genie] list_spaces failed: {exc}", file=sys.stderr)
    return client_url, insights_url, genie_url, genie_id

PAGE_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>{{ app_title }}</title>
    <style>
      :root {
        --bg: #f4efe5;
        --panel: #fffaf2;
        --ink: #1c1b18;
        --muted: #6a6459;
        --accent: #b45a2a;
        --accent-soft: #f3d8c7;
        --line: #ddd2c4;
      }
      * { box-sizing: border-box; }
      body {
        margin: 0;
        font-family: Georgia, "Iowan Old Style", "Times New Roman", serif;
        background: linear-gradient(180deg, #f7f1e7 0%, #f1eadf 100%);
        color: var(--ink);
        min-height: 100vh;
      }
      .page {
        max-width: 960px;
        margin: 0 auto;
        padding: 40px 24px 64px;
        display: grid;
        gap: 16px;
      }
      .hero {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 32px;
        box-shadow: 0 18px 40px rgba(71,50,28,0.08);
        display: flex;
        align-items: center;
        gap: 24px;
        flex-wrap: wrap;
      }
      .hero-text { flex: 1; min-width: 240px; }
      .eyebrow {
        font-size: 12px;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: var(--accent);
        margin: 0 0 8px;
      }
      h1 { font-size: 36px; line-height: 1; margin: 0; }
      .client-wrap { flex: 0 0 280px; }
      .client-label {
        display: block;
        font-size: 11px;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: var(--accent);
        margin-bottom: 6px;
      }
      .client-select {
        width: 100%;
        padding: 10px 14px;
        border-radius: 12px;
        border: 1px solid var(--line);
        background: #fff8ef;
        color: var(--ink);
        font: inherit;
        font-size: 15px;
        cursor: pointer;
      }
      .links-row {
        display: flex;
        gap: 12px;
        flex-wrap: wrap;
      }
      .link-btn {
        flex: 1;
        min-width: 160px;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 14px 20px;
        border-radius: 16px;
        border: 1px solid var(--accent);
        background: var(--accent);
        color: #fffaf2;
        text-decoration: none;
        font: inherit;
        font-size: 15px;
        font-weight: 700;
        text-align: center;
        transition: opacity 0.15s;
      }
      .link-btn:hover { opacity: 0.88; }
      .link-btn.secondary { background: transparent; color: var(--accent); }
      .link-btn.disabled {
        background: #efe5d9;
        border-color: #e0d2c1;
        color: #8a7d6c;
        pointer-events: none;
      }
      .section {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 28px;
      }
      .section-title { font-size: 26px; margin: 0 0 20px; }
      .scenario-row {
        display: flex;
        gap: 10px;
        flex-wrap: wrap;
        margin-bottom: 20px;
      }
      .scenario-btn {
        flex: 1;
        min-width: 140px;
        border: 1px solid var(--line);
        background: #fffdf8;
        color: var(--ink);
        border-radius: 14px;
        padding: 12px 16px;
        font: inherit;
        font-size: 14px;
        font-weight: 600;
        cursor: pointer;
        text-align: center;
      }
      .scenario-btn.active {
        border-color: var(--accent);
        background: #fff3ea;
        color: var(--accent);
      }
      .prompt-area {
        border: 1px solid var(--line);
        border-radius: 16px;
        background: #fffdf8;
        padding: 16px;
      }
      .prompt-label {
        display: block;
        font-size: 11px;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: var(--accent);
        margin-bottom: 8px;
      }
      .prompt-area textarea {
        width: 100%;
        min-height: 96px;
        resize: vertical;
        border: 0;
        outline: none;
        background: transparent;
        color: var(--ink);
        font: inherit;
        line-height: 1.55;
        font-size: 15px;
      }
      .prompt-footer {
        display: flex;
        justify-content: flex-end;
        gap: 10px;
        margin-top: 10px;
        flex-wrap: wrap;
      }
      .btn {
        display: inline-flex;
        align-items: center;
        gap: 6px;
        padding: 10px 18px;
        border-radius: 999px;
        border: 1px solid var(--accent);
        background: transparent;
        color: var(--accent);
        font: inherit;
        font-size: 14px;
        font-weight: 700;
        cursor: pointer;
      }
      .btn.primary { background: var(--accent); color: #fffaf2; }
      .btn:disabled { opacity: 0.5; cursor: not-allowed; }
      .response-box {
        margin-top: 16px;
        border: 1px solid var(--line);
        border-radius: 16px;
        background: #fffdf8;
        padding: 20px;
      }
      .response-box.hidden { display: none; }
      .response-label {
        display: block;
        font-size: 11px;
        letter-spacing: 0.14em;
        text-transform: uppercase;
        color: var(--accent);
        margin-bottom: 12px;
      }
      .response-content {
        font-size: 15px;
        line-height: 1.7;
        white-space: pre-wrap;
        color: var(--ink);
      }
      .spinner {
        display: inline-block;
        width: 16px; height: 16px;
        border: 2px solid var(--accent-soft);
        border-top-color: var(--accent);
        border-radius: 50%;
        animation: spin 0.7s linear infinite;
        vertical-align: middle;
        margin-right: 8px;
      }
      @keyframes spin { to { transform: rotate(360deg); } }
      @media (max-width: 700px) {
        h1 { font-size: 28px; }
        .client-wrap { flex: 0 0 100%; }
        .link-btn { min-width: 100%; }
      }
    </style>
  </head>
  <body>
    <main class="page">
      <section class="hero">
        <div class="hero-text">
          <p class="eyebrow">Sales Call Workspace</p>
          <h1>{{ app_title }}</h1>
        </div>
        <div class="client-wrap">
          <label class="client-label" for="client-select">Account</label>
          <select id="client-select" class="client-select">
            <option value="">Loading clients…</option>
          </select>
        </div>
      </section>

      <div class="links-row">
        {% if client_dashboard_url and client_dashboard_url != 'N/A' %}
        <a class="link-btn" href="{{ client_dashboard_url }}" target="_blank" rel="noreferrer">Client Demand ↗</a>
        {% else %}
        <span class="link-btn disabled">Client Demand</span>
        {% endif %}
        {% if insights_dashboard_url and insights_dashboard_url != 'N/A' %}
        <a class="link-btn" href="{{ insights_dashboard_url }}" target="_blank" rel="noreferrer">Marketplace Context ↗</a>
        {% else %}
        <span class="link-btn disabled">Marketplace Context</span>
        {% endif %}
        {% if genie_space_url and genie_space_url != 'N/A' %}
        <a class="link-btn secondary" href="{{ genie_space_url }}" target="_blank" rel="noreferrer">Genie Space ↗</a>
        {% else %}
        <span class="link-btn disabled secondary">Genie Space</span>
        {% endif %}
      </div>

      <section class="section">
        <h2 class="section-title">FlexHire Sales Assistant</h2>
        <div class="scenario-row">
          <button class="scenario-btn active" type="button" data-scenario="growth">Growth Pitch</button>
          <button class="scenario-btn" type="button" data-scenario="underperforming">Performance Recovery</button>
          <button class="scenario-btn" type="button" data-scenario="benchmark">Benchmark Review</button>
        </div>
        <div class="prompt-area">
          <label class="prompt-label" for="genie-prompt">Prompt</label>
          <textarea id="genie-prompt"></textarea>
          <div class="prompt-footer">
            <button class="btn" type="button" id="copy-prompt">Copy</button>
            <button class="btn primary" type="button" id="ask-genie">Ask Genie →</button>
          </div>
        </div>
        <div id="response-box" class="response-box hidden">
          <span class="response-label">Genie Response</span>
          <div id="response-content" class="response-content"></div>
        </div>
      </section>

    </main>
    <script>
      const scenarios = {
        growth: {
          prompt: "Which categories show the biggest mismatch between marketplace demand and {client}'s posting mix, and what opportunities should we discuss on the call?"
        },
        underperforming: {
          prompt: "Which jobs for {client} have the weakest CTR relative to category peers, and what actions should we recommend?"
        },
        benchmark: {
          prompt: "How does {client}'s posting performance compare with marketplace benchmarks by category, and which gaps should we focus on?"
        }
      };

      let currentClient = '';
      let currentScenarioName = 'growth';

      const clientSelect = document.getElementById('client-select');
      fetch('/api/clients')
        .then(r => r.json())
        .then(data => {
          if (data.error) {
            clientSelect.innerHTML = `<option value="">Error: ${data.error.slice(0, 80)}</option>`;
            return;
          }
          const clients = Array.isArray(data) ? data : [];
          clientSelect.innerHTML = '<option value="">Select a client\u2026</option>';
          clients.forEach(c => {
            const opt = document.createElement('option');
            opt.value = c.client_uid;
            opt.textContent = c.company_name;
            opt.dataset.name = c.company_name;
            clientSelect.appendChild(opt);
          });
        })
        .catch(() => {
          clientSelect.innerHTML = '<option value="">Could not load clients</option>';
        });

      clientSelect.addEventListener('change', () => {
        const opt = clientSelect.options[clientSelect.selectedIndex];
        currentClient = opt ? (opt.dataset.name || '') : '';
        refreshPrompt();
      });

      const scenarioBtns = Array.from(document.querySelectorAll('.scenario-btn'));
      scenarioBtns.forEach(btn => {
        btn.addEventListener('click', () => {
          currentScenarioName = btn.dataset.scenario;
          scenarioBtns.forEach(b => b.classList.toggle('active', b === btn));
          refreshPrompt();
        });
      });

      function refreshPrompt() {
        const s = scenarios[currentScenarioName];
        if (!s) return;
        document.getElementById('genie-prompt').value =
          s.prompt.replace('{client}', currentClient || 'this client');
      }

      document.getElementById('copy-prompt').addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(document.getElementById('genie-prompt').value);
        } catch (_) {}
      });

      const askBtn = document.getElementById('ask-genie');
      const responseBox = document.getElementById('response-box');
      const responseContent = document.getElementById('response-content');

      askBtn.addEventListener('click', async () => {
        const prompt = document.getElementById('genie-prompt').value.trim();
        if (!prompt) return;
        askBtn.disabled = true;
        responseBox.classList.remove('hidden');
        responseContent.innerHTML = '<span class="spinner"></span>Asking Genie\u2026';
        try {
          const res = await fetch('/api/ask-genie', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ prompt })
          });
          const data = await res.json();
          responseContent.textContent = data.error
            ? 'Error: ' + data.error
            : (data.response || '(no response)');
        } catch (err) {
          responseContent.textContent = 'Request failed: ' + err.message;
        } finally {
          askBtn.disabled = false;
        }
      });

      refreshPrompt();
    </script>
  </body>
</html>
"""


@app.route("/api/clients")
def api_clients():
    warehouse_id = os.getenv("WAREHOUSE_ID", "")
    if not warehouse_id:
        return jsonify({"error": "WAREHOUSE_ID not configured"})
    try:
        w = WorkspaceClient()
        resp = w.statement_execution.execute_statement(
            statement=(
                "SELECT client_uid, company_name "
                "FROM workspace.clickstream_workshop.silver_clients "
                "ORDER BY company_name"
            ),
            warehouse_id=warehouse_id,
            wait_timeout="50s",
        )
        if resp.status and resp.status.state and resp.status.state.value not in ("SUCCEEDED",):
            err = (resp.status.error.message if resp.status.error else None) or str(resp.status.state)
            return jsonify({"error": err}), 500
        rows = (resp.result.data_array or []) if resp.result else []
        return jsonify([{"client_uid": r[0], "company_name": r[1]} for r in rows])
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/ask-genie", methods=["POST"])
def api_ask_genie():
    data = request.get_json(silent=True) or {}
    prompt = (data.get("prompt") or "").strip()
    if not prompt:
        return jsonify({"error": "No prompt provided"}), 400
    _, _, _, genie_space_id = _resolve_resources()
    if not genie_space_id:
        return jsonify({"error": "Genie space not configured"}), 503
    try:
        w = WorkspaceClient()
        result = w.genie.start_conversation_and_wait(space_id=genie_space_id, content=prompt)
        parts = [
            att.text.content
            for att in (result.attachments or [])
            if att.text and att.text.content
        ]
        return jsonify({"response": "\n\n".join(parts) or "Genie did not return a text response."})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/")
def index() -> str:
    client_dashboard_url, insights_dashboard_url, genie_space_url, _ = _resolve_resources()
    return render_template_string(
        PAGE_TEMPLATE,
        app_title=os.getenv("APP_TITLE", "Client Demand Intelligence App"),
        app_subtitle=os.getenv(
            "APP_SUBTITLE",
            "A business-facing shell for sales calls that packages client performance, marketplace benchmarks, and guided follow-up questions into one experience.",
        ),
        client_dashboard_name=os.getenv("CLIENT_DASHBOARD_NAME", "Client Demand Intelligence"),
        insights_dashboard_name=os.getenv("INSIGHTS_DASHBOARD_NAME", "Clickstream Insights"),
        genie_space_name=os.getenv("GENIE_SPACE_NAME", "Clickstream Analytics"),
        workspace_host="",
        client_dashboard_url=client_dashboard_url,
        insights_dashboard_url=insights_dashboard_url,
        genie_space_url=genie_space_url,
        warehouse_label=os.getenv("WAREHOUSE_LABEL", "Attached SQL warehouse"),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
