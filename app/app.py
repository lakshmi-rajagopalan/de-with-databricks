import os

from flask import Flask, render_template_string

app = Flask(__name__)


def build_dashboard_url(host: str, dashboard_id: str) -> str:
  if not host or not dashboard_id:
    return ""
  return f"{host.rstrip('/')}/dashboardsv3/{dashboard_id}/published"

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
      }
      .page {
        max-width: 1100px;
        margin: 0 auto;
        padding: 40px 24px 64px;
      }
      .hero {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 32px;
        box-shadow: 0 18px 40px rgba(71, 50, 28, 0.08);
      }
      .eyebrow {
        font-size: 13px;
        letter-spacing: 0.18em;
        text-transform: uppercase;
        color: var(--accent);
        margin: 0 0 12px;
      }
      h1 {
        font-size: 46px;
        line-height: 1;
        margin: 0 0 16px;
      }
      .hero p {
        font-size: 18px;
        line-height: 1.6;
        color: var(--muted);
        max-width: 720px;
        margin: 0;
      }
      .grid {
        display: grid;
        grid-template-columns: repeat(12, 1fr);
        gap: 16px;
        margin-top: 24px;
      }
      .card {
        grid-column: span 4;
        background: rgba(255, 250, 242, 0.92);
        border: 1px solid var(--line);
        border-radius: 20px;
        padding: 24px;
      }
      .card h2 {
        margin: 0 0 10px;
        font-size: 24px;
      }
      .card p {
        margin: 0 0 16px;
        color: var(--muted);
        line-height: 1.55;
      }
      .actions {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-top: 18px;
      }
      .button {
        display: inline-flex;
        align-items: center;
        justify-content: center;
        padding: 10px 14px;
        border-radius: 999px;
        border: 1px solid var(--accent);
        background: var(--accent);
        color: #fffaf2;
        text-decoration: none;
        font-size: 14px;
        font-weight: 700;
      }
      .button.secondary {
        background: transparent;
        color: var(--accent);
      }
      .button.disabled {
        background: #efe5d9;
        border-color: #e0d2c1;
        color: #8a7d6c;
        cursor: not-allowed;
      }
      .pill {
        display: inline-block;
        padding: 8px 12px;
        border-radius: 999px;
        background: var(--accent-soft);
        color: var(--accent);
        font-size: 13px;
      }
      .section {
        margin-top: 24px;
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 24px;
        padding: 28px;
      }
      .section h3 {
        margin: 0 0 12px;
        font-size: 28px;
      }
      .section p {
        margin: 0 0 12px;
        color: var(--muted);
        line-height: 1.6;
      }
      .control-panel {
        display: grid;
        grid-template-columns: 1.05fr 1.35fr;
        gap: 18px;
        align-items: start;
      }
      .control-stack {
        display: grid;
        gap: 14px;
      }
      .scenario-list {
        display: grid;
        gap: 10px;
        margin-top: 12px;
      }
      .scenario-button {
        width: 100%;
        text-align: left;
        border: 1px solid var(--line);
        background: #fffdf8;
        color: var(--ink);
        border-radius: 16px;
        padding: 14px 16px;
        font: inherit;
        cursor: pointer;
      }
      .scenario-button.active {
        border-color: var(--accent);
        background: #fff3ea;
        box-shadow: 0 10px 24px rgba(180, 90, 42, 0.1);
      }
      .scenario-kicker {
        display: block;
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--accent);
        margin-bottom: 6px;
      }
      .briefing {
        border: 1px solid var(--line);
        border-radius: 20px;
        background: #fffdf8;
        padding: 20px;
        min-height: 100%;
      }
      .briefing h4 {
        margin: 0 0 10px;
        font-size: 24px;
      }
      .briefing p {
        margin: 0 0 14px;
      }
      .signal-list,
      .question-list {
        margin: 0;
        padding-left: 20px;
        display: grid;
        gap: 8px;
        color: var(--muted);
      }
      .question-chip-row {
        display: flex;
        flex-wrap: wrap;
        gap: 10px;
        margin-top: 16px;
      }
      .question-chip {
        border: 1px solid var(--line);
        background: #fffaf2;
        color: var(--ink);
        border-radius: 999px;
        padding: 10px 14px;
        font: inherit;
        cursor: pointer;
      }
      .prompt-box {
        margin-top: 16px;
        border-radius: 16px;
        border: 1px solid var(--line);
        background: #fffaf2;
        padding: 14px;
      }
      .prompt-box-label {
        display: block;
        font-size: 12px;
        letter-spacing: 0.12em;
        text-transform: uppercase;
        color: var(--accent);
        margin-bottom: 8px;
      }
      .prompt-box textarea {
        width: 100%;
        min-height: 110px;
        resize: vertical;
        border: 0;
        outline: none;
        background: transparent;
        color: var(--ink);
        font: inherit;
        line-height: 1.55;
      }
      .prompt-box-footer {
        display: flex;
        justify-content: space-between;
        align-items: center;
        gap: 12px;
        margin-top: 8px;
      }
      .micro-copy {
        font-size: 13px;
        color: var(--muted);
      }
      .resource-list {
        margin: 16px 0 0;
        padding: 0;
        list-style: none;
        display: grid;
        gap: 12px;
      }
      .resource-list li {
        padding: 14px 16px;
        border-radius: 16px;
        border: 1px solid var(--line);
        background: #fffdf8;
      }
      .resource-name {
        display: block;
        font-weight: 700;
        margin-bottom: 4px;
      }
      .footer-note {
        margin-top: 12px;
        font-size: 14px;
        color: var(--muted);
      }
      @media (max-width: 900px) {
        .card { grid-column: span 12; }
        h1 { font-size: 36px; }
        .control-panel { grid-template-columns: 1fr; }
      }
    </style>
  </head>
  <body>
    <main class="page">
      <section class="hero">
        <p class="eyebrow">Sales Call Workspace</p>
        <h1>{{ app_title }}</h1>
        <p>{{ app_subtitle }}</p>
      </section>

      <section class="grid">
        <article class="card">
          <span class="pill">Primary Story</span>
          <h2>Client Demand</h2>
          <p>Lead with marketplace visibility, engagement, and opportunity signals for each client account.</p>
          <div class="resource-name">{{ client_dashboard_name }}</div>
          <p>Use this dashboard during account reviews to discuss posting performance, benchmarks, and recommended actions.</p>
          <div class="actions">
            {% if client_dashboard_url %}
            <a class="button" href="{{ client_dashboard_url }}" target="_blank" rel="noreferrer">Open client dashboard</a>
            {% else %}
            <span class="button disabled">Client dashboard URL needed</span>
            {% endif %}
          </div>
        </article>
        <article class="card">
          <span class="pill">Supporting Analysis</span>
          <h2>Marketplace Context</h2>
          <div class="resource-name">{{ insights_dashboard_name }}</div>
          <p>Use the broader clickstream dashboard when clients ask how their results compare to platform-wide demand trends.</p>
          <div class="actions">
            {% if insights_dashboard_url %}
            <a class="button" href="{{ insights_dashboard_url }}" target="_blank" rel="noreferrer">Open insights dashboard</a>
            {% else %}
            <span class="button disabled">Insights dashboard URL needed</span>
            {% endif %}
          </div>
        </article>
        <article class="card">
          <span class="pill">Live Questions</span>
          <h2>Genie Follow-up</h2>
          <div class="resource-name">{{ genie_space_name }}</div>
          <p>Use Genie for ad hoc follow-up questions such as low-CTR jobs, category shifts, and pricing opportunities.</p>
          <div class="actions">
            {% if genie_space_url %}
            <a class="button" href="{{ genie_space_url }}" target="_blank" rel="noreferrer">Open Genie</a>
            {% else %}
            <span class="button disabled">Genie URL needed</span>
            {% endif %}
            {% if workspace_host %}
            <a class="button secondary" href="{{ workspace_host }}" target="_blank" rel="noreferrer">Open workspace</a>
            {% endif %}
          </div>
        </article>
      </section>

      <section class="section">
        <h3>Interactive Call Briefing</h3>
        <p>Pick a sales scenario to change the recommended storyline, talking points, and Genie-style follow-up prompt.</p>
        <div class="control-panel">
          <div class="control-stack">
            <div>
              <span class="pill">Scenario Selector</span>
              <div class="scenario-list">
                <button class="scenario-button active" type="button" data-scenario="growth">
                  <span class="scenario-kicker">Growth Pitch</span>
                  Client wants evidence of rising demand and whitespace opportunities.
                </button>
                <button class="scenario-button" type="button" data-scenario="underperforming">
                  <span class="scenario-kicker">Performance Recovery</span>
                  Client is worried about low CTR and weak posting performance.
                </button>
                <button class="scenario-button" type="button" data-scenario="benchmark">
                  <span class="scenario-kicker">Benchmark Review</span>
                  Client asks how their jobs compare with marketplace category peers.
                </button>
              </div>
            </div>
            <div class="prompt-box">
              <label class="prompt-box-label" for="genie-prompt">Suggested Genie Prompt</label>
              <textarea id="genie-prompt" readonly></textarea>
              <div class="prompt-box-footer">
                <span id="prompt-status" class="micro-copy">Prompt updates as you switch scenarios.</span>
                <button class="button secondary" type="button" id="copy-prompt">Copy prompt</button>
              </div>
            </div>
          </div>

          <div class="briefing">
            <span class="pill" id="briefing-kicker">Growth Pitch</span>
            <h4 id="briefing-title">Lead with demand growth and whitespace</h4>
            <p id="briefing-summary">Start with the client scorecard, then use marketplace context to show where demand is concentrated and which categories are growing faster than the client's current posting mix.</p>
            <div class="actions">
              {% if client_dashboard_url %}
              <a class="button" href="{{ client_dashboard_url }}" target="_blank" rel="noreferrer">Open primary dashboard</a>
              {% endif %}
              {% if insights_dashboard_url %}
              <a class="button secondary" href="{{ insights_dashboard_url }}" target="_blank" rel="noreferrer">Open support dashboard</a>
              {% endif %}
            </div>
            <h5>Signals to highlight</h5>
            <ul class="signal-list" id="signal-list"></ul>
            <h5>Suggested follow-up questions</h5>
            <ul class="question-list" id="question-list"></ul>
            <div class="question-chip-row" id="question-chip-row"></div>
          </div>
        </div>
      </section>

      <section class="section">
        <h3>Recommended Sales Flow</h3>
        <ul class="resource-list">
          <li>
            <span class="resource-name">1. Open Client Demand Intelligence</span>
            Start with the client scorecard, watchlist, category benchmark, and opportunity recommendations.
          </li>
          <li>
            <span class="resource-name">2. Add marketplace context</span>
            Use Clickstream Insights to show broader category demand, search behavior, and demand concentration.
          </li>
          <li>
            <span class="resource-name">3. Ask a live follow-up</span>
            Use Genie to answer client-specific questions without switching into SQL.
          </li>
        </ul>
      </section>

      <section class="section">
        <h3>Assets In This App Shell</h3>
        <p>This app shell is the presentation layer on top of the workshop analytics assets. It does not replace the dashboards; it packages them into a business-facing experience.</p>
        <ul class="resource-list">
          <li>
            <span class="resource-name">Client dashboard</span>
            {% if client_dashboard_url %}<a href="{{ client_dashboard_url }}" target="_blank" rel="noreferrer">{{ client_dashboard_name }}</a>{% else %}{{ client_dashboard_name }}{% endif %}
          </li>
          <li>
            <span class="resource-name">Insights dashboard</span>
            {% if insights_dashboard_url %}<a href="{{ insights_dashboard_url }}" target="_blank" rel="noreferrer">{{ insights_dashboard_name }}</a>{% else %}{{ insights_dashboard_name }}{% endif %}
          </li>
          <li>
            <span class="resource-name">Genie space</span>
            {% if genie_space_url %}<a href="{{ genie_space_url }}" target="_blank" rel="noreferrer">{{ genie_space_name }}</a>{% else %}{{ genie_space_name }}{% endif %}
          </li>
          <li>
            <span class="resource-name">SQL warehouse</span>
            {{ warehouse_label }}
          </li>
        </ul>
        <p class="footer-note">This shell now launches the deployed dashboards directly. Genie can also be linked once a stable workspace URL is configured for the space.</p>
      </section>
    </main>
    <script>
      const scenarios = {
        growth: {
          kicker: "Growth Pitch",
          title: "Lead with demand growth and whitespace",
          summary: "Start with the client scorecard, then use marketplace context to show where demand is concentrated and which categories are growing faster than the client's current posting mix.",
          prompt: "Which client categories show the biggest mismatch between marketplace demand and this client's current posting mix, and what opportunities should we discuss on the call?",
          signals: [
            "Category benchmark gaps between client mix and overall marketplace demand",
            "High-impression categories where the client has limited active job coverage",
            "Opportunity recommendations driven by CTR and demand concentration"
          ],
          questions: [
            "Where is demand growing faster than this client is currently hiring?",
            "Which categories have strong marketplace demand but weak client presence?",
            "What recommendation should the account team make next?"
          ]
        },
        underperforming: {
          kicker: "Performance Recovery",
          title: "Diagnose low-performing job postings",
          summary: "Use the client dashboard to identify weak CTR jobs, then bring in platform context to show whether the problem is posting quality, category competition, or marketplace positioning.",
          prompt: "Which jobs for this client have the weakest CTR relative to category peers, and what likely actions should we recommend based on category and position performance?",
          signals: [
            "Low CTR jobs with strong impressions but poor engagement",
            "Position bias effects that suggest visibility without compelling conversion",
            "Category-level benchmarks showing whether the weakness is client-specific or market-wide"
          ],
          questions: [
            "Which jobs are underperforming most versus benchmark?",
            "Is the issue visibility, competition, or listing quality?",
            "Which jobs should be rewritten, repriced, or reprioritized?"
          ]
        },
        benchmark: {
          kicker: "Benchmark Review",
          title: "Frame the client against marketplace peers",
          summary: "Anchor the conversation in comparative evidence so the client can see where they are ahead, where they lag, and which benchmark gaps matter most commercially.",
          prompt: "How does this client's posting performance compare with marketplace benchmarks by category, and which benchmark gaps should we focus on during the review?",
          signals: [
            "Category-level benchmark deltas for CTR, impressions, and opportunity volume",
            "Relative standing of the client's portfolio across active categories",
            "Client segments where benchmark gaps connect directly to commercial action"
          ],
          questions: [
            "Where is this client above benchmark?",
            "Which benchmark gaps are most material for the account conversation?",
            "What is the clearest story to tell in two minutes?"
          ]
        }
      };

      const scenarioButtons = Array.from(document.querySelectorAll('.scenario-button'));
      const briefingKicker = document.getElementById('briefing-kicker');
      const briefingTitle = document.getElementById('briefing-title');
      const briefingSummary = document.getElementById('briefing-summary');
      const signalList = document.getElementById('signal-list');
      const questionList = document.getElementById('question-list');
      const questionChipRow = document.getElementById('question-chip-row');
      const promptBox = document.getElementById('genie-prompt');
      const promptStatus = document.getElementById('prompt-status');
      const copyPromptButton = document.getElementById('copy-prompt');

      function fillList(element, values) {
        element.innerHTML = values.map((value) => `<li>${value}</li>`).join('');
      }

      function fillQuestionChips(values) {
        questionChipRow.innerHTML = values
          .map((value) => `<button class="question-chip" type="button" data-question="${value.replace(/"/g, '&quot;')}">${value}</button>`)
          .join('');
      }

      function setScenario(name) {
        const scenario = scenarios[name];
        if (!scenario) return;

        scenarioButtons.forEach((button) => {
          button.classList.toggle('active', button.dataset.scenario === name);
        });

        briefingKicker.textContent = scenario.kicker;
        briefingTitle.textContent = scenario.title;
        briefingSummary.textContent = scenario.summary;
        promptBox.value = scenario.prompt;
        fillList(signalList, scenario.signals);
        fillList(questionList, scenario.questions);
        fillQuestionChips(scenario.questions);
        promptStatus.textContent = 'Prompt updated for the selected scenario.';
      }

      scenarioButtons.forEach((button) => {
        button.addEventListener('click', () => setScenario(button.dataset.scenario));
      });

      questionChipRow.addEventListener('click', (event) => {
        const target = event.target;
        if (!(target instanceof HTMLElement) || !target.dataset.question) return;
        promptBox.value = target.dataset.question;
        promptStatus.textContent = 'Loaded a follow-up question into the prompt box.';
      });

      copyPromptButton.addEventListener('click', async () => {
        try {
          await navigator.clipboard.writeText(promptBox.value);
          promptStatus.textContent = 'Prompt copied to clipboard.';
        } catch (error) {
          promptStatus.textContent = 'Clipboard copy failed. Copy the prompt manually.';
        }
      });

      setScenario('growth');
    </script>
  </body>
</html>
"""


@app.route("/")
def index() -> str:
    workspace_host = os.getenv("WORKSPACE_HOST", "")
    client_dashboard_url = os.getenv("CLIENT_DASHBOARD_URL", "") or build_dashboard_url(
        workspace_host,
        os.getenv("CLIENT_DASHBOARD_ID", ""),
    )
    insights_dashboard_url = os.getenv("INSIGHTS_DASHBOARD_URL", "") or build_dashboard_url(
        workspace_host,
        os.getenv("INSIGHTS_DASHBOARD_ID", ""),
    )
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
        workspace_host=workspace_host,
        client_dashboard_url=client_dashboard_url,
        insights_dashboard_url=insights_dashboard_url,
        genie_space_url=os.getenv("GENIE_SPACE_URL", ""),
        warehouse_label=os.getenv("WAREHOUSE_LABEL", "Attached SQL warehouse"),
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000)
