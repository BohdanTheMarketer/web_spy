"""
Meta Ads Daily Monitor - Dashboard Generator v3
Daily CPM/CPC charts with sources, MoM dating, 14-day incident/outage bar charts.
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
    trends = data["daily_trends"]
    dating_mom = data["dating_mom"]
    incidents = data["incidents"]
    incidents_14d = data["incidents_14d"]
    news = data["news"]
    outage_14d = data["outage_reports_14d"]
    releases = data["releases"]
    upcoming = data["upcoming_changes"]
    sources = data["data_sources"]

    all_ind = benchmarks.get("All Industries (Average)", {})
    dating_ind = benchmarks.get("Dating & Personals", {})

    incidents_major = len([i for i in incidents if i.get("severity") in ("major", "warning")])
    inc_color = "var(--red)" if incidents_major > 0 else "var(--green)"

    # --- Chart data (daily) ---
    cpm_all_labels = json.dumps([p["label"] for p in trends["cpm_all"]])
    cpm_all_values = json.dumps([p["cpm"] for p in trends["cpm_all"]])
    cpc_all_labels = json.dumps([p["label"] for p in trends["cpc_all"]])
    cpc_all_values = json.dumps([p["cpc"] for p in trends["cpc_all"]])
    cpm_dat_values = json.dumps([p["cpm"] for p in trends["cpm_dating"]])
    cpc_dat_values = json.dumps([p["cpc"] for p in trends["cpc_dating"]])

    # --- 14-day bar chart data ---
    inc14_labels = json.dumps([d["label"] for d in incidents_14d])
    inc14_major = json.dumps([d["major"] for d in incidents_14d])
    inc14_minor = json.dumps([d["minor"] for d in incidents_14d])

    out14_labels = json.dumps([d["label"] for d in outage_14d])
    out14_critical = json.dumps([d["critical"] for d in outage_14d])
    out14_high = json.dumps([d["high"] for d in outage_14d])
    out14_medium = json.dumps([d["medium"] for d in outage_14d])

    # --- Source references for chart footnotes ---
    cpm_source = trends["cpm_all"][0].get("source", "[1]") if trends["cpm_all"] else "[1]"
    cpc_source = trends["cpc_all"][0].get("source", "[3]") if trends["cpc_all"] else "[3]"
    cpm_dat_source = trends["cpm_dating"][0].get("source", "[5]") if trends["cpm_dating"] else "[5]"
    cpc_dat_source = trends["cpc_dating"][0].get("source", "[5]") if trends["cpc_dating"] else "[5]"

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
        Previous trend ({months["prev2"]} → {months["previous"]}):
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

    # --- News ---
    if news:
        news_html = ""
        for a in news:
            sev = a.get("severity", "medium")
            badge = f'<span class="badge {sev}">{sev.upper()}</span> '
            news_html += (
                f'<div class="news-item"><div class="title">{badge}'
                f'<a href="{a["url"]}" target="_blank">{a["title"]}</a></div>'
                f'<div class="meta-info">{a["source"]} &middot; {a["date"]}</div>'
                f'<div class="summary">{a.get("summary", "")}</div></div>\n'
            )
    else:
        news_html = '<div style="padding:20px;text-align:center;color:var(--green)">No outage reports found.</div>'

    # --- Releases ---
    releases_html = ""
    for r in releases:
        impact = r.get("impact", "medium")
        releases_html += (
            f'<tr><td>{r["date"]}</td><td><span class="cat-tag">{r.get("category","")}</span></td>'
            f'<td><span class="badge {"high" if impact == "high" else "medium"}">{impact.upper()}</span></td>'
            f'<td><strong>{r["title"]}</strong><br><span style="color:var(--text-dim);font-size:12px">{r["description"]}</span></td></tr>\n'
        )
    upcoming_html = ""
    for u in upcoming:
        upcoming_html += (
            f'<tr style="opacity:0.7"><td>{u["date"]}</td><td><span class="cat-tag">{u["category"]}</span></td>'
            f'<td><span class="badge warning">UPCOMING</span></td>'
            f'<td><strong>{u["title"]}</strong><br><span style="color:var(--text-dim);font-size:12px">{u["description"]}</span></td></tr>\n'
        )

    # --- Sources footnotes ---
    sources_footnotes = ""
    for s in sources:
        sources_footnotes += f'<div><strong>{s["id"]}</strong> {s["name"]} — <a href="{s["url"]}" target="_blank">{s["what"]}</a></div>\n'

    html = HTML_TEMPLATE.format(
        date=data["date"],
        generated_at=data["generated_at"],
        current_month=data["current_month"],
        avg_cpm=all_ind.get("cpm", "N/A"),
        avg_cpc=all_ind.get("cpc", "N/A"),
        dating_cpm=dating_ind.get("cpm", "N/A"),
        dating_cpc=dating_ind.get("cpc", "N/A"),
        incidents_count=incidents_major,
        inc_color=inc_color,
        news_count=len(news),
        releases_count=len(releases),
        cpm_all_labels=cpm_all_labels,
        cpm_all_values=cpm_all_values,
        cpc_all_labels=cpc_all_labels,
        cpc_all_values=cpc_all_values,
        cpm_dat_values=cpm_dat_values,
        cpc_dat_values=cpc_dat_values,
        cpm_source=cpm_source,
        cpc_source=cpc_source,
        cpm_dat_source=cpm_dat_source,
        cpc_dat_source=cpc_dat_source,
        inc14_labels=inc14_labels,
        inc14_major=inc14_major,
        inc14_minor=inc14_minor,
        out14_labels=out14_labels,
        out14_critical=out14_critical,
        out14_high=out14_high,
        out14_medium=out14_medium,
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
.chart-wrap{{position:relative;height:240px;}}
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
.comp-tag{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600;background:var(--surface2);color:var(--text-dim);border:1px solid var(--border);}}
.cat-tag{{display:inline-block;padding:2px 7px;border-radius:4px;font-size:10px;font-weight:600;background:rgba(124,92,252,.1);color:var(--accent2);border:1px solid rgba(124,92,252,.2);}}
.dating-card{{border-color:var(--dating);}}
.dating-card h2{{color:var(--dating);}}
/* MoM section */
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
.refresh-btn{{background:linear-gradient(135deg,var(--accent),var(--accent2));color:#fff;border:none;padding:8px 20px;border-radius:8px;font-size:12px;font-weight:700;cursor:pointer;transition:opacity .2s;margin-bottom:6px;}}
.refresh-btn:hover{{opacity:.85;}}
.refresh-btn:disabled{{opacity:.5;cursor:wait;}}
</style>
</head>
<body>
<div class="container">

<div class="header">
    <div>
        <h1>Meta Ads Daily Monitor</h1>
        <div class="sub">Daily CPM/CPC &middot; Platform Incidents &middot; Releases &middot; Outage News</div>
    </div>
    <div class="right">
        <button class="refresh-btn" onclick="doRefresh(this)">&#x21bb; Refresh Data</button>
        <div class="date">{date}</div>
        <div>Generated: {generated_at}</div>
    </div>
</div>

<div class="kpi-row">
    <div class="kpi"><div class="label">CPM (All)</div><div class="value" style="color:var(--accent)">${avg_cpm}</div><div class="sub">This month avg</div></div>
    <div class="kpi"><div class="label">CPC (All)</div><div class="value" style="color:var(--accent)">${avg_cpc}</div><div class="sub">This month avg</div></div>
    <div class="kpi dating"><div class="label">Dating CPM</div><div class="value">${dating_cpm}</div></div>
    <div class="kpi dating"><div class="label">Dating CPC</div><div class="value">${dating_cpc}</div></div>
    <div class="kpi"><div class="label">Incidents (3d)</div><div class="value" style="color:{inc_color}">{incidents_count}</div></div>
    <div class="kpi"><div class="label">Outage News</div><div class="value" style="color:var(--red)">{news_count}</div></div>
    <div class="kpi"><div class="label">Releases</div><div class="value" style="color:var(--accent2)">{releases_count}</div><div class="sub">{current_month}</div></div>
</div>

<!-- DAILY CPM/CPC CHARTS -->
<div class="grid-2">
    <div class="card">
        <h2><span class="icon">&#128200;</span> Daily CPM (30 days)</h2>
        <div class="chart-wrap"><canvas id="cpmChart"></canvas></div>
        <div class="chart-source">All Industries: {cpm_source} SuperAds.ai &middot; Dating: {cpm_dat_source} Adjust</div>
    </div>
    <div class="card">
        <h2><span class="icon">&#128201;</span> Daily CPC (30 days)</h2>
        <div class="chart-wrap"><canvas id="cpcChart"></canvas></div>
        <div class="chart-source">All Industries: {cpc_source} SuperAds.ai &middot; Dating: {cpc_dat_source} Adjust</div>
    </div>
</div>

<!-- 14-DAY BAR CHARTS -->
<div class="grid-2">
    <div class="card">
        <h2><span class="icon">&#128308;</span> Platform Incidents — Last 14 Days</h2>
        <div class="chart-wrap"><canvas id="incidentsBar"></canvas></div>
        <div class="chart-source">[7] StatusGator &middot; [8] IsDown &middot; Downdetector user reports</div>
    </div>
    <div class="card">
        <h2><span class="icon">&#9888;&#65039;</span> Outage & Error Reports — Last 14 Days</h2>
        <div class="chart-wrap"><canvas id="outageBar"></canvas></div>
        <div class="chart-source">Bing News aggregation &middot; Downdetector &middot; Media reports</div>
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
        <div class="chart-source">[1][3][6] SuperAds.ai, Triple Whale, AdAmigo</div>
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
    <div style="margin-top:6px"><em>Daily values interpolated from monthly anchors with day-of-week patterns. Refresh: <code>python run.py</code></em></div>
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

// CPM daily chart
new Chart(document.getElementById('cpmChart'),{{
    type:'line',
    data:{{
        labels:{cpm_all_labels},
        datasets:[
            {{label:'All Industries CPM',data:{cpm_all_values},borderColor:'#4f8cff',backgroundColor:'rgba(79,140,255,.06)',fill:true,tension:.3,pointRadius:2,pointHoverRadius:5,borderWidth:2}},
            {{label:'Dating CPM',data:{cpm_dat_values},borderColor:'#f472b6',backgroundColor:'rgba(244,114,182,.06)',fill:true,tension:.3,pointRadius:2,pointHoverRadius:5,borderWidth:2}}
        ]
    }},
    options:lineOpts,
}});

// CPC daily chart
new Chart(document.getElementById('cpcChart'),{{
    type:'line',
    data:{{
        labels:{cpc_all_labels},
        datasets:[
            {{label:'All Industries CPC',data:{cpc_all_values},borderColor:'#4f8cff',backgroundColor:'rgba(79,140,255,.06)',fill:true,tension:.3,pointRadius:2,pointHoverRadius:5,borderWidth:2}},
            {{label:'Dating CPC',data:{cpc_dat_values},borderColor:'#f472b6',backgroundColor:'rgba(244,114,182,.06)',fill:true,tension:.3,pointRadius:2,pointHoverRadius:5,borderWidth:2}}
        ]
    }},
    options:lineOpts,
}});

// Incidents 14-day bar
new Chart(document.getElementById('incidentsBar'),{{
    type:'bar',
    data:{{
        labels:{inc14_labels},
        datasets:[
            {{label:'Major',data:{inc14_major},backgroundColor:'rgba(248,113,113,.7)',borderRadius:3}},
            {{label:'Minor',data:{inc14_minor},backgroundColor:'rgba(251,191,36,.5)',borderRadius:3}}
        ]
    }},
    options:barOpts,
}});

// Outage reports 14-day bar
new Chart(document.getElementById('outageBar'),{{
    type:'bar',
    data:{{
        labels:{out14_labels},
        datasets:[
            {{label:'Critical',data:{out14_critical},backgroundColor:'rgba(248,113,113,.7)',borderRadius:3}},
            {{label:'High',data:{out14_high},backgroundColor:'rgba(251,191,36,.5)',borderRadius:3}},
            {{label:'Medium',data:{out14_medium},backgroundColor:'rgba(79,140,255,.4)',borderRadius:3}}
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
</script>
</body>
</html>"""


if __name__ == "__main__":
    generate_dashboard()
