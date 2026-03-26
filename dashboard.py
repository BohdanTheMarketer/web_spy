"""
Meta Ads Monitor - Dashboard Generator v4
Monthly CPM/CPC charts from real sources + Ad Library sample,
MoM dating, 30-day incident/outage bar charts, paginated news.
"""

import json
from datetime import datetime
from pathlib import Path

DATA_DIR = Path(__file__).parent / "data"
OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def generate_dashboard():
    report_path = DATA_DIR / "latest_report.json"
    if not report_path.exists():
        print("No report data found. Run fetcher.py first.")
        return

    with open(report_path) as f:
        data = json.load(f)

    benchmarks = data["benchmarks"]
    monthly = data["monthly_trends"]
    monthly_all = monthly["all"]
    monthly_dating = monthly["dating"]
    adlib_sample = monthly.get("adlib_sample")
    data_origin = monthly.get("data_origin", "fallback")

    dating_mom = data["dating_mom"]
    incidents = data["incidents"]
    incidents_30d = data["incidents_30d"]
    news = data["news"]
    outage_30d = data["outage_reports_30d"]
    releases = data["releases"]
    upcoming = data["upcoming_changes"]
    sources = data["data_sources"]

    all_ind = benchmarks.get("All Industries (Average)", {})
    dating_ind = benchmarks.get("Dating & Personals", {})

    incidents_major = len([i for i in incidents if i.get("severity") in ("major", "warning")])
    inc_color = "var(--red)" if incidents_major > 0 else "var(--green)"

    # --- Monthly chart data ---
    month_labels = json.dumps([p["label"] for p in monthly_all])
    cpm_all_values = json.dumps([p.get("cpm") for p in monthly_all])
    cpc_all_values = json.dumps([p.get("cpc") for p in monthly_all])
    cpm_dat_values = json.dumps([p.get("cpm") for p in monthly_dating])
    cpc_dat_values = json.dumps([p.get("cpc") for p in monthly_dating])

    adlib_cpm_js = json.dumps(adlib_sample["cpm"]) if adlib_sample else "null"
    adlib_label = f'"This Week (Ad Library, n={adlib_sample["sample_size"]})"' if adlib_sample else "null"

    latest_cpm = monthly_all[-1].get("cpm", "N/A") if monthly_all else "N/A"
    latest_cpc = monthly_all[-1].get("cpc", "N/A") if monthly_all else "N/A"
    latest_source = monthly_all[-1].get("source", "") if monthly_all else ""

    # --- 30-day bar chart data ---
    inc30_labels = json.dumps([d["label"] for d in incidents_30d])
    inc30_major = json.dumps([d["major"] for d in incidents_30d])
    inc30_minor = json.dumps([d["minor"] for d in incidents_30d])

    out30_labels = json.dumps([d["label"] for d in outage_30d])
    out30_critical = json.dumps([d["critical"] for d in outage_30d])
    out30_high = json.dumps([d["high"] for d in outage_30d])
    out30_medium = json.dumps([d["medium"] for d in outage_30d])
    out30_low = json.dumps([d.get("low", 0) for d in outage_30d])

    # --- Benchmark rows ---
    benchmark_rows = ""
    for industry, m in benchmarks.items():
        is_dating = "dating" in industry.lower()
        cls = ' class="highlight"' if is_dating else ""
        prefix = '<span style="color:var(--dating)">&#9829;</span> ' if is_dating else ""
        benchmark_rows += (
            f'<tr{cls}><td>{prefix}{industry}</td>'
            f'<td>${m["cpm"]}</td><td>${m["cpc"]}</td>'
            f'<td>{m["ctr"]}%</td><td>${m["cpa"]}</td></tr>\n'
        )

    # --- Dating MoM ---
    dm = dating_mom["dating"]
    da = dating_mom["all_industry"]
    months = dating_mom["months"]

    def _mom_badge(pct):
        if pct is None:
            return '<span class="badge info">N/A</span>'
        sign = "+" if pct > 0 else ""
        cls = "up" if pct > 0 else "down"
        return f'<span class="val {cls}">{sign}{pct}%</span>'

    dating_mom_html = f"""
    <div class="mom-grid">
        <div class="mom-card">
            <div class="mom-header">Dating CPM</div>
            <div class="mom-row">
                <div class="mom-col"><div class="mom-label">{months["current"]}</div><div class="mom-val">${dm["cpm_current"]}</div></div>
                <div class="mom-col"><div class="mom-label">{months["previous"]}</div><div class="mom-val dim">${dm["cpm_previous"]}</div></div>
                <div class="mom-col"><div class="mom-label">MoM</div><div class="mom-val">{_mom_badge(dm["cpm_mom_pct"])}</div></div>
            </div>
        </div>
        <div class="mom-card">
            <div class="mom-header">Dating CPC</div>
            <div class="mom-row">
                <div class="mom-col"><div class="mom-label">{months["current"]}</div><div class="mom-val">${dm["cpc_current"]}</div></div>
                <div class="mom-col"><div class="mom-label">{months["previous"]}</div><div class="mom-val dim">${dm["cpc_previous"]}</div></div>
                <div class="mom-col"><div class="mom-label">MoM</div><div class="mom-val">{_mom_badge(dm["cpc_mom_pct"])}</div></div>
            </div>
        </div>
        <div class="mom-card">
            <div class="mom-header">All Industries CPM</div>
            <div class="mom-row">
                <div class="mom-col"><div class="mom-label">{months["current"]}</div><div class="mom-val">${da["cpm_current"]}</div></div>
                <div class="mom-col"><div class="mom-label">{months["previous"]}</div><div class="mom-val dim">${da["cpm_previous"]}</div></div>
                <div class="mom-col"><div class="mom-label">MoM</div><div class="mom-val">{_mom_badge(da["cpm_mom_pct"])}</div></div>
            </div>
        </div>
        <div class="mom-card">
            <div class="mom-header">All Industries CPC</div>
            <div class="mom-row">
                <div class="mom-col"><div class="mom-label">{months["current"]}</div><div class="mom-val">${da["cpc_current"]}</div></div>
                <div class="mom-col"><div class="mom-label">{months["previous"]}</div><div class="mom-val dim">${da["cpc_previous"]}</div></div>
                <div class="mom-col"><div class="mom-label">MoM</div><div class="mom-val">{_mom_badge(da["cpc_mom_pct"])}</div></div>
            </div>
        </div>
    </div>
    <div style="font-size:11px;color:var(--text-dim);margin-top:8px;">
        Previous trend ({months["prev2"]} &rarr; {months["previous"]}):
        Dating CPM {_mom_badge(dm["cpm_trend"])},
        Dating CPC {_mom_badge(dm["cpc_trend"])}
    </div>
    """

    # --- Incidents table ---
    if incidents:
        inc_html = ('<table><thead><tr><th>Source</th><th>Component</th>'
                    '<th>Severity</th><th>Description</th><th>Date</th></tr></thead><tbody>')
        for i in incidents:
            sev = i.get("severity", "info")
            comp = i.get("component", "Platform")
            inc_html += (
                f'<tr><td><a href="{i["url"]}" target="_blank" '
                f'style="color:var(--accent)">{i["source"]}</a></td>'
                f'<td><span class="comp-tag">{comp}</span></td>'
                f'<td><span class="badge {sev}">{sev.upper()}</span></td>'
                f'<td>{i["title"][:200]}</td><td>{i["date"]}</td></tr>'
            )
        inc_html += '</tbody></table>'
    else:
        inc_html = '<div style="padding:20px;text-align:center;color:var(--green)"><span class="badge ok">ALL CLEAR</span></div>'

    # --- News (all items, pagination by JS) ---
    if news:
        news_html = ""
        for idx, a in enumerate(news):
            sev = a.get("severity", "medium")
            badge = f'<span class="badge {sev}">{sev.upper()}</span> '
            news_html += (
                f'<div class="news-item" data-news-idx="{idx}">'
                f'<div class="title">{badge}'
                f'<a href="{a["url"]}" target="_blank">{a["title"]}</a></div>'
                f'<div class="meta-info">{a["source"]} &middot; {a["date"]}</div>'
                f'<div class="summary">{a.get("summary", "")}</div></div>\n'
            )
        news_html += '<div id="newsPagination" class="pagination"></div>'
    else:
        news_html = '<div style="padding:20px;text-align:center;color:var(--green)">No outage reports found.</div>'

    # --- Releases ---
    releases_html = ""
    for r in releases:
        impact = r.get("impact", "medium")
        title_html = f'<a href="{r.get("url", "#")}" target="_blank" style="color:var(--accent)">{r["title"]}</a>' if r.get("url") else r["title"]
        releases_html += (
            f'<tr><td>{r["date"]}</td><td><span class="cat-tag">{r.get("category","")}</span></td>'
            f'<td><span class="badge {"high" if impact == "high" else "medium"}">{impact.upper()}</span></td>'
            f'<td><strong>{title_html}</strong><br><span style="color:var(--text-dim);font-size:12px">{r["description"]}</span></td></tr>\n'
        )
    upcoming_html = ""
    for u in upcoming:
        title_html = f'<a href="{u.get("url", "#")}" target="_blank" style="color:var(--accent)">{u["title"]}</a>' if u.get("url") else u["title"]
        upcoming_html += (
            f'<tr style="opacity:0.7"><td>{u["date"]}</td><td><span class="cat-tag">{u["category"]}</span></td>'
            f'<td><span class="badge warning">UPCOMING</span></td>'
            f'<td><strong>{title_html}</strong><br><span style="color:var(--text-dim);font-size:12px">{u["description"]}</span></td></tr>\n'
        )

    # --- Sources footnotes ---
    sources_footnotes = ""
    for s in sources:
        sources_footnotes += f'<div><strong>{s["id"]}</strong> {s["name"]} — <a href="{s["url"]}" target="_blank">{s["what"]}</a></div>\n'

    # --- Ad Library KPI sub ---
    adlib_kpi_sub = ""
    if adlib_sample:
        adlib_kpi_sub = f'<div class="sub">Ad Library est: ${adlib_sample["cpm"]}</div>'

    origin_badge = "Scraped" if data_origin == "scraped" else "Fallback"

    html = HTML_TEMPLATE.format(
        date=data["date"],
        generated_at=data["generated_at"],
        current_month=data["current_month"],
        avg_cpm=latest_cpm,
        avg_cpc=latest_cpc,
        latest_source=latest_source,
        adlib_kpi_sub=adlib_kpi_sub,
        dating_cpm=dating_ind.get("cpm", "N/A"),
        dating_cpc=dating_ind.get("cpc", "N/A"),
        incidents_count=incidents_major,
        inc_color=inc_color,
        news_count=len(news),
        releases_count=len(releases),
        month_labels=month_labels,
        cpm_all_values=cpm_all_values,
        cpc_all_values=cpc_all_values,
        cpm_dat_values=cpm_dat_values,
        cpc_dat_values=cpc_dat_values,
        adlib_cpm_js=adlib_cpm_js,
        adlib_label=adlib_label,
        origin_badge=origin_badge,
        inc30_labels=inc30_labels,
        inc30_major=inc30_major,
        inc30_minor=inc30_minor,
        out30_labels=out30_labels,
        out30_critical=out30_critical,
        out30_high=out30_high,
        out30_medium=out30_medium,
        out30_low=out30_low,
        news_total=len(news),
        benchmark_rows=benchmark_rows,
        dating_mom_html=dating_mom_html,
        inc_html=inc_html,
        news_html=news_html,
        releases_html=releases_html,
        upcoming_html=upcoming_html,
        sources_footnotes=sources_footnotes,
    )

    output_path = OUTPUT_DIR / "dashboard.html"
    with open(output_path, "w") as f:
        f.write(html)
    print(f"  ✓ Dashboard saved to {output_path}")
    return output_path


HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Meta Ads Monitor — {date}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7/dist/chart.umd.min.js"></script>
<style>
:root {{
    --bg:#0b0d14;--surface:#131620;--surface2:#1c2030;--border:#262b3e;
    --text:#e2e4ee;--text-dim:#7b7f96;--accent:#4f8cff;--accent2:#7c5cfc;
    --green:#34d399;--red:#f87171;--orange:#fbbf24;--dating:#f472b6;
    --dating-dim:rgba(244,114,182,.12);
}}
*{{margin:0;padding:0;box-sizing:border-box;}}
body{{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Inter,Roboto,sans-serif;background:var(--bg);color:var(--text);line-height:1.6;padding:20px;}}
.container{{max-width:1440px;margin:0 auto;}}
.header{{display:flex;justify-content:space-between;align-items:center;padding:20px 28px;background:linear-gradient(135deg,var(--surface),var(--surface2));border-radius:16px;border:1px solid var(--border);margin-bottom:18px;}}
.header h1{{font-size:22px;font-weight:800;background:linear-gradient(135deg,var(--accent),var(--accent2));-webkit-background-clip:text;-webkit-text-fill-color:transparent;}}
.header .sub{{color:var(--text-dim);font-size:12px;margin-top:2px;}}
.header .right{{text-align:right;color:var(--text-dim);font-size:12px;}}
.header .date{{font-size:15px;color:var(--text);font-weight:700;}}
.kpi-row{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;margin-bottom:18px;}}
.kpi{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:14px;text-align:center;}}
.kpi .label{{font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.6px;}}
.kpi .value{{font-size:24px;font-weight:800;margin:2px 0;}}
.kpi .sub{{font-size:10px;color:var(--text-dim);}}
.kpi.dating{{border-color:var(--dating);}}
.kpi.dating .value{{color:var(--dating);}}
.card{{background:var(--surface);border:1px solid var(--border);border-radius:14px;padding:20px;margin-bottom:16px;}}
.card h2{{font-size:14px;font-weight:700;margin-bottom:12px;display:flex;align-items:center;gap:8px;}}
.card h2 .icon{{font-size:16px;}}
.card .chart-source{{font-size:10px;color:var(--text-dim);margin-top:6px;font-style:italic;}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:16px;}}
@media(max-width:1000px){{.grid-2{{grid-template-columns:1fr;}}}}
.chart-wrap{{position:relative;height:260px;}}
table{{width:100%;border-collapse:collapse;font-size:12px;}}
th{{text-align:left;padding:7px 10px;background:var(--surface2);border-bottom:2px solid var(--border);font-weight:700;font-size:10px;text-transform:uppercase;letter-spacing:.5px;color:var(--text-dim);}}
td{{padding:7px 10px;border-bottom:1px solid var(--border);}}
tr:hover td{{background:rgba(79,140,255,.04);}}
tr.highlight{{background:var(--dating-dim);}}
tr.highlight td{{font-weight:600;}}
.badge{{display:inline-block;padding:2px 8px;border-radius:5px;font-size:10px;font-weight:700;white-space:nowrap;}}
.badge.major{{background:rgba(248,113,113,.15);color:var(--red);}}
.badge.warning{{background:rgba(251,191,36,.15);color:var(--orange);}}
.badge.minor{{background:rgba(79,140,255,.15);color:var(--accent);}}
.badge.info{{background:rgba(79,140,255,.1);color:var(--accent);}}
.badge.ok{{background:rgba(52,211,153,.15);color:var(--green);}}
.badge.critical{{background:rgba(248,113,113,.2);color:var(--red);}}
.badge.high{{background:rgba(251,191,36,.15);color:var(--orange);}}
.badge.medium{{background:rgba(79,140,255,.15);color:var(--accent);}}
.badge.low{{background:rgba(123,127,150,.15);color:var(--text-dim);}}
.comp-tag{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600;background:var(--surface2);color:var(--text-dim);border:1px solid var(--border);}}
.cat-tag{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600;background:rgba(124,92,252,.1);color:var(--accent2);border:1px solid rgba(124,92,252,.2);}}
.dating-card{{border-color:var(--dating);}}
.dating-card h2{{color:var(--dating);}}
.mom-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:10px;}}
.mom-card{{background:var(--surface2);border-radius:10px;padding:14px;}}
.mom-header{{font-size:12px;font-weight:700;color:var(--text);margin-bottom:8px;}}
.mom-row{{display:flex;gap:12px;align-items:center;}}
.mom-col{{flex:1;}}
.mom-label{{font-size:10px;color:var(--text-dim);text-transform:uppercase;letter-spacing:.3px;}}
.mom-val{{font-size:16px;font-weight:700;margin-top:2px;}}
.mom-val.dim{{color:var(--text-dim);}}
.val.up{{color:var(--red);}} .val.down{{color:var(--green);}}
.news-item{{padding:10px 0;border-bottom:1px solid var(--border);}}
.news-item:last-child{{border-bottom:none;}}
.news-item .title{{font-weight:600;font-size:13px;}}
.news-item .title a{{color:var(--accent);text-decoration:none;}}
.news-item .title a:hover{{text-decoration:underline;}}
.news-item .meta-info{{font-size:11px;color:var(--text-dim);margin-top:2px;}}
.news-item .summary{{font-size:12px;color:var(--text-dim);margin-top:3px;}}
.sources-box{{margin-top:20px;padding:14px 18px;background:var(--surface);border:1px solid var(--border);border-radius:10px;font-size:11px;color:var(--text-dim);line-height:1.8;}}
.sources-box a{{color:var(--accent);text-decoration:none;}}
.sources-box a:hover{{text-decoration:underline;}}
.pagination{{display:flex;gap:6px;justify-content:center;align-items:center;padding:14px 0;flex-wrap:wrap;}}
.pagination button{{background:var(--surface2);color:var(--text);border:1px solid var(--border);padding:5px 12px;border-radius:6px;font-size:12px;cursor:pointer;transition:background .15s;}}
.pagination button:hover{{background:var(--border);}}
.pagination button.active{{background:var(--accent);color:#fff;border-color:var(--accent);}}
.pagination button:disabled{{opacity:.4;cursor:default;}}
.pagination .page-info{{font-size:11px;color:var(--text-dim);}}
.refresh-btn{{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;border:none;padding:8px 20px;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer;transition:opacity .2s;margin-bottom:6px;}}
.refresh-btn:hover{{opacity:.85;}}
.refresh-btn:disabled{{opacity:.5;cursor:wait;}}
</style>
</head>
<body>
<div class="container">

<div class="header">
    <div>
        <h1>Meta Ads Monitor</h1>
        <div class="sub">Monthly CPM/CPC Benchmarks &middot; Platform Incidents &middot; Releases &middot; Outage News</div>
    </div>
    <div class="right">
        <button class="refresh-btn" onclick="doRefresh(this)">&#x21bb; Refresh Data</button>
        <div class="date">{date}</div>
        <div>Generated: {generated_at} &middot; Data: {origin_badge}</div>
    </div>
</div>

<div class="kpi-row">
    <div class="kpi"><div class="label">CPM (All)</div><div class="value" style="color:var(--accent)">${avg_cpm}</div><div class="sub">Latest month &middot; {latest_source}</div>{adlib_kpi_sub}</div>
    <div class="kpi"><div class="label">CPC (All)</div><div class="value" style="color:var(--accent)">${avg_cpc}</div><div class="sub">Latest month</div></div>
    <div class="kpi dating"><div class="label">Dating CPM</div><div class="value">${dating_cpm}</div></div>
    <div class="kpi dating"><div class="label">Dating CPC</div><div class="value">${dating_cpc}</div></div>
    <div class="kpi"><div class="label">Incidents (3d)</div><div class="value" style="color:{inc_color}">{incidents_count}</div></div>
    <div class="kpi"><div class="label">Outage News</div><div class="value" style="color:var(--red)">{news_count}</div></div>
    <div class="kpi"><div class="label">Releases</div><div class="value" style="color:var(--accent2)">{releases_count}</div><div class="sub">{current_month}</div></div>
</div>

<!-- MONTHLY CPM/CPC CHARTS -->
<div class="grid-2">
    <div class="card">
        <h2><span class="icon">&#128200;</span> Monthly CPM Trend (13 months)</h2>
        <div class="chart-wrap"><canvas id="cpmChart"></canvas></div>
        <div class="chart-source">[1] Sovran.ai / [3] SuperAds.ai &middot; [5] Adjust (Dating) &middot; [4] Meta Ad Library sample</div>
    </div>
    <div class="card">
        <h2><span class="icon">&#128201;</span> Monthly CPC Trend (13 months)</h2>
        <div class="chart-wrap"><canvas id="cpcChart"></canvas></div>
        <div class="chart-source">[3] SuperAds.ai &middot; [5] Adjust (Dating)</div>
    </div>
</div>

<!-- 30-DAY BAR CHARTS -->
<div class="grid-2">
    <div class="card">
        <h2><span class="icon">&#128308;</span> Platform Incidents — Last 30 Days</h2>
        <div class="chart-wrap"><canvas id="incidentsBar"></canvas></div>
        <div class="chart-source">[7] StatusGator &middot; [8] IsDown &middot; Downdetector user reports</div>
    </div>
    <div class="card">
        <h2><span class="icon">&#9888;&#65039;</span> Error/Outage Mentions — Last 30 Days</h2>
        <div class="chart-wrap"><canvas id="outageBar"></canvas></div>
        <div class="chart-source">Counted from actual news article publish dates &middot; Bing News RSS</div>
    </div>
</div>

<!-- BENCHMARKS + MOM -->
<div class="grid-2">
    <div class="card">
        <h2><span class="icon">&#128202;</span> CPM/CPC Benchmarks by Industry</h2>
        <div style="max-height:480px;overflow-y:auto;">
        <table><thead><tr><th>Industry</th><th>CPM</th><th>CPC</th><th>CTR</th><th>CPA</th></tr></thead>
        <tbody>{benchmark_rows}</tbody></table>
        </div>
        <div class="chart-source">[1] Sovran &middot; [3] SuperAds &middot; [6] Triple Whale</div>
    </div>
    <div class="card dating-card">
        <h2><span class="icon">&#128152;</span> Dating Niche — Month over Month</h2>
        {dating_mom_html}
        <div class="chart-source" style="margin-top:10px">[5] Adjust — State of Dating Apps 2026</div>
    </div>
</div>

<!-- INCIDENTS TABLE -->
<div class="card">
    <h2><span class="icon">&#128308;</span> Meta Platform Incidents (Last 3 Days)</h2>
    <div style="max-height:380px;overflow-y:auto;">{inc_html}</div>
</div>

<!-- OUTAGE NEWS -->
<div class="card">
    <h2><span class="icon">&#9888;&#65039;</span> Outage & Error Reports</h2>
    {news_html}
</div>

<!-- RELEASES -->
<div class="card">
    <h2><span class="icon">&#128640;</span> Meta Ads Releases & Rollouts — {current_month}</h2>
    <div style="max-height:480px;overflow-y:auto;">
    <table><thead><tr><th>Date</th><th>Category</th><th>Impact</th><th>Details</th></tr></thead>
    <tbody>{releases_html}
        <tr><td colspan="4" style="padding:10px;font-weight:700;color:var(--orange);font-size:11px;text-transform:uppercase;letter-spacing:1px">&#9203; Upcoming Changes</td></tr>
        {upcoming_html}
    </tbody></table>
    </div>
</div>

<!-- SOURCES -->
<div class="sources-box">
    <strong>Data Sources & Verification:</strong>
    {sources_footnotes}
    <div style="margin-top:6px"><em>Monthly values from benchmark reports. Ad Library sample via MetaAdsCollector. Refresh: <code>python run.py</code></em></div>
</div>

</div>

<script>
const lineOpts = {{
    responsive:true, maintainAspectRatio:false,
    interaction:{{intersect:false,mode:'index'}},
    plugins:{{
        legend:{{labels:{{color:'#7b7f96',font:{{size:10}}}}}},
        tooltip:{{backgroundColor:'#1c2030',titleColor:'#e2e4ee',bodyColor:'#e2e4ee',borderColor:'#262b3e',borderWidth:1}}
    }},
    scales:{{
        x:{{ticks:{{color:'#7b7f96',font:{{size:9}},maxRotation:45}},grid:{{color:'rgba(38,43,62,.4)'}}}},
        y:{{ticks:{{color:'#7b7f96',font:{{size:10}},callback:v=>'$'+parseFloat(v.toFixed(2))}},grid:{{color:'rgba(38,43,62,.4)'}}}}
    }}
}};

const barOpts = {{
    responsive:true, maintainAspectRatio:false,
    interaction:{{intersect:false,mode:'index'}},
    plugins:{{
        legend:{{labels:{{color:'#7b7f96',font:{{size:10}}}}}},
        tooltip:{{backgroundColor:'#1c2030',titleColor:'#e2e4ee',bodyColor:'#e2e4ee',borderColor:'#262b3e',borderWidth:1}}
    }},
    scales:{{
        x:{{stacked:true,ticks:{{color:'#7b7f96',font:{{size:9}},maxRotation:45}},grid:{{display:false}}}},
        y:{{stacked:true,ticks:{{color:'#7b7f96',font:{{size:10}},stepSize:1}},grid:{{color:'rgba(38,43,62,.4)'}}}}
    }}
}};

// --- Monthly CPM chart ---
(function() {{
    const labels = {month_labels};
    const cpmAll = {cpm_all_values};
    const cpmDat = {cpm_dat_values};
    const adlibCpm = {adlib_cpm_js};

    const datasets = [
        {{label:'All Industries CPM',data:cpmAll,borderColor:'#4f8cff',backgroundColor:'rgba(79,140,255,.12)',fill:true,tension:.3,pointRadius:4,pointHoverRadius:7,borderWidth:2}},
        {{label:'Dating CPM',data:cpmDat,borderColor:'#f472b6',backgroundColor:'rgba(244,114,182,.08)',fill:true,tension:.3,pointRadius:4,pointHoverRadius:7,borderWidth:2}}
    ];

    if (adlibCpm !== null) {{
        const adlibData = new Array(labels.length).fill(null);
        adlibData.push(adlibCpm);
        labels.push('This Wk');
        cpmAll.push(null);
        cpmDat.push(null);
        datasets.push({{
            label: {adlib_label},
            data: adlibData,
            borderColor: '#34d399',
            backgroundColor: 'rgba(52,211,153,.3)',
            pointRadius: 8,
            pointHoverRadius: 10,
            pointStyle: 'star',
            borderWidth: 2,
            showLine: false,
        }});
    }}

    new Chart(document.getElementById('cpmChart'), {{
        type: 'line',
        data: {{ labels: labels, datasets: datasets }},
        options: lineOpts,
    }});
}})();

// --- Monthly CPC chart ---
new Chart(document.getElementById('cpcChart'),{{
    type:'line',
    data:{{
        labels:{month_labels},
        datasets:[
            {{label:'All Industries CPC',data:{cpc_all_values},borderColor:'#4f8cff',backgroundColor:'rgba(79,140,255,.12)',fill:true,tension:.3,pointRadius:4,pointHoverRadius:7,borderWidth:2}},
            {{label:'Dating CPC',data:{cpc_dat_values},borderColor:'#f472b6',backgroundColor:'rgba(244,114,182,.08)',fill:true,tension:.3,pointRadius:4,pointHoverRadius:7,borderWidth:2}}
        ]
    }},
    options:lineOpts,
}});

// Incidents 30-day bar
new Chart(document.getElementById('incidentsBar'),{{
    type:'bar',
    data:{{
        labels:{inc30_labels},
        datasets:[
            {{label:'Major',data:{inc30_major},backgroundColor:'rgba(248,113,113,.7)',borderRadius:3}},
            {{label:'Minor',data:{inc30_minor},backgroundColor:'rgba(251,191,36,.5)',borderRadius:3}}
        ]
    }},
    options:barOpts,
}});

// Outage mentions 30-day bar
new Chart(document.getElementById('outageBar'),{{
    type:'bar',
    data:{{
        labels:{out30_labels},
        datasets:[
            {{label:'Critical',data:{out30_critical},backgroundColor:'rgba(248,113,113,.7)',borderRadius:3}},
            {{label:'High',data:{out30_high},backgroundColor:'rgba(251,191,36,.5)',borderRadius:3}},
            {{label:'Medium',data:{out30_medium},backgroundColor:'rgba(79,140,255,.4)',borderRadius:3}},
            {{label:'Low',data:{out30_low},backgroundColor:'rgba(123,127,150,.3)',borderRadius:3}}
        ]
    }},
    options:barOpts,
}});

function doRefresh(btn) {{
    btn.disabled = true;
    btn.textContent = 'Refreshing\u2026';
    fetch('/refresh', {{method: 'POST'}})
        .then(r => {{
            if (r.status === 409) {{ btn.textContent = 'Already running\u2026'; return; }}
            if (!r.ok) throw new Error('Server error');
            location.reload();
        }})
        .catch(e => {{
            btn.textContent = '\u26a0 Error';
            setTimeout(() => {{ btn.disabled = false; btn.innerHTML = '&#x21bb; Refresh Data'; }}, 3000);
        }});
}}

// --- Pagination for Outage & Error Reports ---
(function() {{
    const PER_PAGE = 10;
    const total = {news_total};
    if (total <= PER_PAGE) return;
    const items = document.querySelectorAll('.news-item[data-news-idx]');
    const pagDiv = document.getElementById('newsPagination');
    if (!pagDiv || items.length === 0) return;
    const pages = Math.ceil(total / PER_PAGE);
    let cur = 1;
    function render() {{
        const start = (cur - 1) * PER_PAGE;
        const end = start + PER_PAGE;
        items.forEach(el => {{
            const idx = parseInt(el.getAttribute('data-news-idx'));
            el.style.display = (idx >= start && idx < end) ? '' : 'none';
        }});
        let html = '<button class="pg-prev" ' + (cur <= 1 ? 'disabled' : '') + '>&laquo; Prev</button>';
        const maxBtns = 7;
        let startP = Math.max(1, cur - 3);
        let endP = Math.min(pages, startP + maxBtns - 1);
        if (endP - startP < maxBtns - 1) startP = Math.max(1, endP - maxBtns + 1);
        for (let p = startP; p <= endP; p++) {{
            html += '<button class="pg-num' + (p === cur ? ' active' : '') + '" data-p="' + p + '">' + p + '</button>';
        }}
        html += '<button class="pg-next" ' + (cur >= pages ? 'disabled' : '') + '>Next &raquo;</button>';
        html += '<span class="page-info">' + total + ' items &middot; page ' + cur + '/' + pages + '</span>';
        pagDiv.innerHTML = html;
        pagDiv.querySelector('.pg-prev').onclick = () => {{ if (cur > 1) {{ cur--; render(); }} }};
        pagDiv.querySelector('.pg-next').onclick = () => {{ if (cur < pages) {{ cur++; render(); }} }};
        pagDiv.querySelectorAll('.pg-num').forEach(b => {{
            b.onclick = () => {{ cur = parseInt(b.getAttribute('data-p')); render(); }};
        }});
    }}
    render();
}})()
</script>
</body>
</html>"""


if __name__ == "__main__":
    generate_dashboard()
