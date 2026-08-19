"""Render the unified daily-run dashboard as a single self-contained HTML file.

Constraints honored here:

- single static file: embedded CSS/JS/data, no CDN, no web fonts, no network
- every dynamic string is HTML-escaped; embedded JSON escapes ``</``
- works with JavaScript disabled (filters/details degrade gracefully)
- WCAG-minded: semantic landmarks, visible focus, AA contrast, print CSS,
  ``prefers-reduced-motion``, no horizontal overflow at 390px
"""

from __future__ import annotations

import html
import json
from pathlib import Path

from domain.daily_run import (
    ArtifactKind,
    AttentionSeverity,
    ChinaAShareOverview,
    DailyRunSnapshot,
    FreshnessState,
    IndexHealth,
    MacroOverview,
    ModuleResult,
    ModuleStatus,
    ThemeOverview,
)
from quant_agent.daily_run.serialization import dumps_safe, jsonable


def esc(value: object) -> str:
    return html.escape("" if value is None else str(value))


def _pct(value: float | None) -> str:
    return "N/A" if value is None else f"{value * 100:.1f}%"


def _score(value: float | None) -> str:
    return "N/A" if value is None else f"{value:.1f}"


def _signed(value: float | None) -> str:
    return "N/A" if value is None else f"{value:+.2f}"


def _int(value: int | None) -> str:
    return "N/A" if value is None else f"{value:,}"


_STATUS_BADGE = {
    ModuleStatus.SUCCESS: ("badge-ok", "正常 SUCCESS"),
    ModuleStatus.WARNING: ("badge-warn", "警告 WARNING"),
    ModuleStatus.FAILED: ("badge-bad", "失败 FAILED"),
    ModuleStatus.SKIPPED: ("badge-muted", "跳过 SKIPPED"),
}

_FRESHNESS_LABEL = {
    FreshnessState.FRESH: "新鲜 FRESH",
    FreshnessState.STALE: "陈旧 STALE",
    FreshnessState.UNKNOWN: "N/A",
}


def _badge(status: ModuleStatus) -> str:
    css, label = _STATUS_BADGE[status]
    return f'<span class="badge {css}">{label}</span>'


# --------------------------------------------------------------------------- #
# CSS / JS (plain strings: no f-string brace escaping needed)
# --------------------------------------------------------------------------- #

CSS = """
:root{
  --navy:#10263b;--navy-2:#16334e;--ink:#1d2733;--bg:#f4f2ec;--panel:#ffffff;
  --line:#d9d5ca;--muted:#5b6470;
  --ok:#1a7f37;--ok-bg:#e4f3e9;--warn:#8a5a00;--warn-bg:#fdf3d7;
  --bad:#b3261e;--bad-bg:#fbe9e7;--info:#175cd3;--info-bg:#e8f0fe;
}
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
@media (prefers-reduced-motion:no-preference){html{scroll-behavior:smooth}}
body{margin:0;background:var(--bg);color:var(--ink);
  font:14px/1.55 system-ui,-apple-system,"Segoe UI","PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;
  font-variant-numeric:tabular-nums}
a{color:var(--info);text-decoration:none}
a:hover{text-decoration:underline}
:focus-visible{outline:2px solid var(--info);outline-offset:2px;border-radius:2px}
.skip-link{position:absolute;left:-9999px;top:0;background:var(--navy);color:#fff;padding:8px 12px;z-index:20}
.skip-link:focus{left:8px;top:8px}
img,svg{max-width:100%}
code,.mono{font-family:ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;overflow-wrap:anywhere}

/* run bar */
.runbar{background:var(--navy);color:#eef2f6;padding:18px 0 14px}
.wrap{max-width:1280px;margin:0 auto;padding:0 20px}
.runbar h1{font-size:20px;font-weight:650;margin:0 0 10px;letter-spacing:.02em}
.runbar h1 .zh{font-weight:400;font-size:14px;color:#b9c6d4;margin-left:10px}
.runbar-meta{display:flex;flex-wrap:wrap;gap:8px 22px;font-size:13px;color:#c8d3de;align-items:center}
.runbar-meta b{color:#fff;font-weight:600}
.runbar-actions{display:flex;gap:8px;margin-top:12px;flex-wrap:wrap}
.btn{font:inherit;font-size:13px;padding:6px 14px;border:1px solid #4a6c8a;background:var(--navy-2);
  color:#eef2f6;border-radius:4px;cursor:pointer}
.btn:hover{background:#1d4062}

/* synthetic banner */
.synthetic{background:var(--warn-bg);color:var(--warn);border-bottom:1px solid #e5cf8f;
  padding:8px 0;font-weight:600;font-size:13px}

/* sticky nav */
#section-nav{position:sticky;top:0;z-index:10;background:var(--panel);border-bottom:1px solid var(--line)}
#section-nav .wrap{display:flex;flex-wrap:wrap;gap:2px;padding-top:6px;padding-bottom:6px}
#section-nav a{color:var(--ink);font-size:13px;padding:6px 10px;border-radius:4px;white-space:nowrap}
#section-nav a:hover{background:var(--bg);text-decoration:none}

main .wrap{padding-top:18px;padding-bottom:8px}
section{margin:0 0 26px}
h2{font-size:16px;font-weight:650;margin:0 0 10px;padding-bottom:6px;border-bottom:2px solid var(--navy);letter-spacing:.02em}
h2 .en{font-size:12px;color:var(--muted);font-weight:400;margin-left:8px}
h3{font-size:14px;font-weight:600;margin:14px 0 8px}
.panel{background:var(--panel);border:1px solid var(--line);border-radius:4px;padding:14px}

/* badges */
.badge{display:inline-block;font-size:12px;font-weight:600;padding:2px 9px;border-radius:3px;
  border:1px solid transparent;white-space:nowrap}
.badge-ok{color:var(--ok);background:var(--ok-bg);border-color:#b7dfc4}
.badge-warn{color:var(--warn);background:var(--warn-bg);border-color:#ecd391}
.badge-bad{color:var(--bad);background:var(--bad-bg);border-color:#efc2bd}
.badge-info{color:var(--info);background:var(--info-bg);border-color:#c2d6f5}
.badge-muted{color:var(--muted);background:#eceae3;border-color:var(--line)}

/* KPI grid */
.kpi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px}
.kpi{background:var(--panel);border:1px solid var(--line);border-radius:4px;padding:10px 12px}
.kpi .k-label{font-size:12px;color:var(--muted);margin-bottom:4px}
.kpi .k-value{font-size:18px;font-weight:650;font-variant-numeric:tabular-nums}
.kpi .k-sub{font-size:12px;color:var(--muted);margin-top:2px}

/* attention */
.attn{border:1px solid var(--line);border-left-width:4px;border-radius:4px;background:var(--panel);
  padding:10px 14px;margin:0 0 8px}
.attn.sev-CRITICAL{border-left-color:var(--bad)}
.attn.sev-WARNING{border-left-color:var(--warn)}
.attn.sev-INFO{border-left-color:var(--info)}
.attn .a-head{display:flex;gap:10px;align-items:baseline;flex-wrap:wrap}
.attn .a-title{font-weight:650}
.attn .a-module{font-size:12px;color:var(--muted)}
.attn p{margin:4px 0 0;font-size:13px}
.attn .a-action{color:var(--muted)}

/* macro */
.split{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.regime-line{display:flex;flex-wrap:wrap;gap:10px 26px;margin-bottom:10px}
.regime-line .item .lab{font-size:12px;color:var(--muted)}
.regime-line .item .val{font-size:15px;font-weight:650}
.flow-row{display:grid;grid-template-columns:minmax(120px,200px) 1fr minmax(150px,auto);gap:10px;
  align-items:center;padding:5px 0;border-bottom:1px dashed var(--line);font-size:13px}
.flow-row:last-child{border-bottom:none}
.flow-track{position:relative;height:10px;background:#eceae3;border-radius:2px}
.flow-track::before{content:"";position:absolute;left:50%;top:-2px;bottom:-2px;width:1px;background:var(--muted)}
.flow-bar{position:absolute;top:0;bottom:0;border-radius:2px}
.flow-bar.pos{left:50%;background:var(--ok)}
.flow-bar.neg{right:50%;background:var(--bad)}
.chips{display:flex;flex-wrap:wrap;gap:6px;margin:6px 0 0}
.chip{font-size:12px;padding:2px 8px;border:1px solid var(--line);border-radius:3px;background:#faf9f5}

/* tables */
.table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border:1px solid var(--line);border-radius:4px;background:var(--panel)}
table{width:100%;border-collapse:collapse;min-width:640px}
th,td{padding:7px 10px;text-align:left;border-bottom:1px solid var(--line);font-size:13px;white-space:nowrap}
th{background:#efede6;font-weight:650;font-size:12px;color:#3a4450}
tbody tr:last-child td{border-bottom:none}
td.num,th.num{text-align:right}
tr.theme-row{cursor:pointer}
tr.theme-row:hover td,tr.theme-row:focus-visible td{background:#f2f6fb}
tr.theme-row.selected td{background:var(--info-bg)}
.rankbar{display:inline-block;vertical-align:middle;width:90px;height:8px;background:#eceae3;border-radius:2px;margin-right:8px;overflow:hidden}
.rankbar span{display:block;height:100%;background:var(--navy-2)}
.pos-text{color:var(--ok);font-weight:600}
.neg-text{color:var(--bad);font-weight:600}

/* filter bars */
.filter-bar{display:flex;flex-wrap:wrap;gap:8px;align-items:center;margin:0 0 10px}
.filter-bar button{font:inherit;font-size:13px;padding:5px 12px;border:1px solid var(--line);
  background:var(--panel);border-radius:4px;cursor:pointer;color:var(--ink)}
.filter-bar button[aria-pressed="true"]{background:var(--navy);color:#fff;border-color:var(--navy)}
.filter-bar input,.filter-bar select{font:inherit;font-size:13px;padding:5px 9px;border:1px solid var(--line);
  border-radius:4px;background:var(--panel);color:var(--ink);max-width:100%}
.filter-bar label{font-size:13px;color:var(--muted)}

/* theme detail */
#theme-detail{margin:12px 0}
#theme-detail .d-grid{display:flex;flex-wrap:wrap;gap:6px 24px;font-size:13px}
#theme-detail .d-grid b{font-weight:650}

/* grid children must be allowed to shrink below their content's min-content
   size, otherwise nowrap tables blow out the 390px viewport */
.split>*,.kpi-grid>*,#module-list>*{min-width:0}
.flow-row{min-width:0}

/* module cards */
#module-list{display:grid;grid-template-columns:1fr 1fr;gap:12px}
.module-card{background:var(--panel);border:1px solid var(--line);border-radius:4px;padding:12px 14px}
.module-card .m-head{display:flex;justify-content:space-between;align-items:baseline;gap:10px;flex-wrap:wrap}
.module-card .m-title{font-weight:650;font-size:14px}
.module-card .m-id{font-size:12px;color:var(--muted)}
.module-card .m-summary{font-size:13px;color:#3a4450;margin:6px 0 8px}
.m-meta{display:grid;grid-template-columns:repeat(auto-fit,minmax(120px,1fr));gap:4px 14px;font-size:12px;margin:0 0 8px}
.m-meta .lab{color:var(--muted)}
.m-msg{font-size:13px;border-radius:3px;padding:6px 10px;margin:6px 0}
.m-msg.warn{background:var(--warn-bg);color:var(--warn);border:1px solid #ecd391}
.m-msg.err{background:var(--bad-bg);color:var(--bad);border:1px solid #efc2bd}
.m-artifacts{font-size:13px;margin:6px 0}
.m-artifacts li{margin:2px 0}
details{margin-top:8px;font-size:13px}
summary{cursor:pointer;color:var(--info);font-size:13px}
details table{min-width:0}
details th,details td{white-space:normal}

/* empty state & notes */
.empty{padding:14px;color:var(--muted);font-size:13px;background:var(--panel);
  border:1px dashed var(--line);border-radius:4px}
.note{font-size:13px;color:var(--info);background:var(--info-bg);border:1px solid #c2d6f5;
  border-radius:4px;padding:8px 12px;margin:0 0 12px}

/* index health */
.ih-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-bottom:12px}

/* footer */
footer{border-top:1px solid var(--line);margin-top:10px;padding:16px 0 30px;color:var(--muted);font-size:12px}
footer p{margin:4px 0}

@media (max-width:900px){
  .split{grid-template-columns:1fr}
  #module-list{grid-template-columns:1fr}
}
@media (max-width:480px){
  .wrap{padding:0 10px}
  .runbar h1{font-size:17px}
  .kpi-grid{grid-template-columns:1fr 1fr}
  .flow-row{grid-template-columns:1fr;gap:4px;padding:8px 0}
  .flow-track{height:12px}
  .m-meta{grid-template-columns:1fr 1fr}
}
@media (prefers-reduced-motion:reduce){
  *{animation:none!important;transition:none!important;scroll-behavior:auto!important}
}
@media print{
  body{background:#fff;font-size:12px}
  #section-nav,.runbar-actions,.filter-bar,.skip-link{display:none!important}
  .runbar{background:#fff;color:#000;border-bottom:2px solid #000;padding:8px 0}
  .runbar-meta,.runbar-meta b{color:#000}
  .panel,.kpi,.module-card,.attn,.table-wrap{border-color:#999;break-inside:avoid}
  section{break-inside:avoid-page}
  details>*{display:block!important}
  a{color:#000;text-decoration:underline}
}
"""

JS = """
(function(){
"use strict";
function $(id){return document.getElementById(id);}
function esc(s){return String(s).replace(/[&<>"']/g,function(c){
  return {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c];});}
function num(v,d){return (typeof v==="number"&&isFinite(v))?v.toFixed(d):"N/A";}
function pct(v){return (typeof v==="number"&&isFinite(v))?(v*100).toFixed(1)+"%":"N/A";}
function sgn(v){return (typeof v==="number"&&isFinite(v))?(v>=0?"+":"")+v.toFixed(2):"N/A";}

var DATA=null;
var dataEl=$("daily-run-data");
if(dataEl){try{DATA=JSON.parse(dataEl.textContent);}catch(e){DATA=null;}}

var copyBtn=$("copy-run-id");
if(copyBtn){
  copyBtn.addEventListener("click",function(){
    var id=copyBtn.getAttribute("data-run-id")||"";
    function done(){copyBtn.textContent="已复制";setTimeout(function(){copyBtn.textContent="复制 run_id";},1500);}
    function fallback(){
      var ta=document.createElement("textarea");ta.value=id;ta.style.position="fixed";ta.style.opacity="0";
      document.body.appendChild(ta);ta.select();
      try{document.execCommand("copy");}catch(e){}
      document.body.removeChild(ta);done();
    }
    if(navigator.clipboard&&navigator.clipboard.writeText){
      navigator.clipboard.writeText(id).then(done,fallback);
    }else{fallback();}
  });
}
var printBtn=$("print-button");
if(printBtn){printBtn.addEventListener("click",function(){window.print();});}

var mf=$("module-filter");
if(mf){
  mf.addEventListener("click",function(ev){
    var btn=ev.target&&ev.target.closest?ev.target.closest("button[data-filter]"):null;
    if(!btn){return;}
    mf.querySelectorAll("button[data-filter]").forEach(function(b){
      b.setAttribute("aria-pressed",b===btn?"true":"false");});
    var f=btn.getAttribute("data-filter");
    document.querySelectorAll("#module-list .module-card").forEach(function(card){
      var st=card.getAttribute("data-status");
      var show=(f==="all")||(f==="issues"&&(st==="FAILED"||st==="WARNING"))||
        (f==="success"&&st==="SUCCESS")||(f==="skipped"&&st==="SKIPPED");
      card.hidden=!show;
    });
  });
}

var ti=$("theme-filter-input"),ts=$("theme-state-filter"),tb=$("theme-table-body");
function applyThemeFilter(){
  if(!tb){return;}
  var q=(ti&&ti.value?ti.value:"").trim().toLowerCase();
  var st=ts?ts.value:"all";
  tb.querySelectorAll("tr.theme-row").forEach(function(row){
    var hay=row.getAttribute("data-search")||"";
    var rst=row.getAttribute("data-state")||"";
    row.hidden=!((!q||hay.indexOf(q)!==-1)&&(st==="all"||rst===st));
  });
}
if(ti){ti.addEventListener("input",applyThemeFilter);}
if(ts){ts.addEventListener("change",applyThemeFilter);}

var td=$("theme-detail");
function themeLabel(id){
  if(!DATA||!DATA.themes||!DATA.themes.themes){return null;}
  var found=null;
  DATA.themes.themes.forEach(function(t){if(t.theme_id===id){found=t;}});
  return found;
}
function showTheme(id,row){
  if(!td){return;}
  var t=themeLabel(id);
  if(!t){return;}
  if(tb){tb.querySelectorAll("tr.theme-row").forEach(function(r){r.classList.remove("selected");});}
  if(row){row.classList.add("selected");}
  td.innerHTML='<h3>重点主题 · '+esc(t.label)+' <span class="m-id">'+esc(t.theme_id)+'</span></h3>'+
    '<div class="d-grid">'+
    '<span>代理 <b>'+esc(t.proxy_symbol)+'</b></span>'+
    '<span>Score <b>'+num(t.score,1)+'</b></span>'+
    '<span>状态 <b>'+esc(t.state||"N/A")+'</b></span>'+
    '<span>趋势 <b>'+esc(t.trend||"N/A")+'</b></span>'+
    '<span>覆盖 <b>'+pct(t.coverage)+'</b></span>'+
    '<span>5D相对 <b>'+sgn(t.relative_5d)+'%</b></span>'+
    '<span>20D相对 <b>'+sgn(t.relative_20d)+'%</b></span>'+
    '</div>';
}
if(tb){
  tb.addEventListener("click",function(ev){
    var row=ev.target&&ev.target.closest?ev.target.closest("tr.theme-row"):null;
    if(row){showTheme(row.getAttribute("data-theme"),row);}
  });
  tb.addEventListener("keydown",function(ev){
    if(ev.key!=="Enter"&&ev.key!==" "){return;}
    var row=ev.target&&ev.target.closest?ev.target.closest("tr.theme-row"):null;
    if(row){ev.preventDefault();showTheme(row.getAttribute("data-theme"),row);}
  });
}
})();
"""


# --------------------------------------------------------------------------- #
# section renderers
# --------------------------------------------------------------------------- #

def _render_runbar(snapshot: DailyRunSnapshot) -> str:
    return f"""<a class="skip-link" href="#executive-summary">跳到主要内容</a>
{f'<div class="synthetic" role="note"><div class="wrap">SYNTHETIC FIXTURE DATA · 本页为合成演示数据，不反映真实市场状态</div></div>' if snapshot.synthetic else ''}
<header class="runbar">
  <div class="wrap">
    <h1>Daily Research Command Center<span class="zh">每日研究指挥中心</span></h1>
    <div class="runbar-meta">
      <span>总状态 {_badge(snapshot.overall_status)}</span>
      <span>as_of <b>{esc(snapshot.as_of.isoformat())}</b></span>
      <span>mode <b>{esc(snapshot.mode.value)}</b></span>
      <span>run_id <code class="mono">{esc(snapshot.run_id)}</code></span>
      <span>生成时间 <b>{esc(snapshot.generated_at.isoformat())}</b></span>
    </div>
    <div class="runbar-actions">
      <button type="button" class="btn" id="copy-run-id" data-run-id="{esc(snapshot.run_id)}">复制 run_id</button>
      <button type="button" class="btn" id="print-button">打印 / 导出 PDF</button>
    </div>
  </div>
</header>"""


def _render_nav(snapshot: DailyRunSnapshot) -> str:
    links = [
        ("executive-summary", "摘要"),
        *( [("attention", "需注意")] if snapshot.attention else [] ),
        ("macro", "宏观与跨资产"),
        ("themes", "主题轮动"),
        ("china-a-share", "A股研究"),
        ("index-health", "索引健康"),
        ("modules", "模块详情"),
    ]
    items = "".join(f'<a href="#{target}">{label}</a>' for target, label in links)
    return f'<nav id="section-nav" aria-label="页面分区导航"><div class="wrap">{items}</div></nav>'


def _render_summary(snapshot: DailyRunSnapshot) -> str:
    modules = snapshot.modules
    counts = {status: 0 for status in ModuleStatus}
    for module in modules:
        counts[module.status] += 1
    stale = [m for m in modules if m.freshness.stale_days is not None and m.freshness.stale_days > 0]
    most_stale = max(stale, key=lambda m: m.freshness.stale_days or 0, default=None)
    stale_value = (
        f"{esc(most_stale.label)} · {most_stale.freshness.stale_days}d" if most_stale else "全部新鲜"
    )
    coverages = [m.coverage for m in modules if m.coverage is not None]
    avg_coverage = f"{sum(coverages) / len(coverages) * 100:.1f}%" if coverages else "N/A"
    theme_alerts = len(snapshot.themes.alerts) if snapshot.themes else 0
    if snapshot.index_health is None or snapshot.index_health.indexes_in_parity is None:
        parity = '<span class="badge badge-muted">N/A</span>'
    elif snapshot.index_health.indexes_in_parity:
        parity = '<span class="badge badge-ok">一致 IN PARITY</span>'
    else:
        parity = '<span class="badge badge-bad">不一致 MISMATCH</span>'
    return f"""<section id="executive-summary" aria-labelledby="executive-summary-h">
  <h2 id="executive-summary-h">Executive Summary<span class="en">运行结论</span></h2>
  <div class="kpi-grid">
    <div class="kpi"><div class="k-label">总状态</div><div class="k-value">{_badge(snapshot.overall_status)}</div>
      <div class="k-sub">{esc(snapshot.mode.value)} 模式</div></div>
    <div class="kpi"><div class="k-label">模块 成功/警告/失败/跳过</div>
      <div class="k-value">{counts[ModuleStatus.SUCCESS]}/{counts[ModuleStatus.WARNING]}/{counts[ModuleStatus.FAILED]}/{counts[ModuleStatus.SKIPPED]}</div>
      <div class="k-sub">共 {len(modules)} 个模块</div></div>
    <div class="kpi"><div class="k-label">最陈旧数据</div><div class="k-value">{stale_value}</div>
      <div class="k-sub">滞后阈值 2 天</div></div>
    <div class="kpi"><div class="k-label">平均覆盖率</div><div class="k-value">{avg_coverage}</div>
      <div class="k-sub">按报告模块计</div></div>
    <div class="kpi"><div class="k-label">待处理事项</div><div class="k-value">{len(snapshot.attention)}</div>
      <div class="k-sub">按严重程度排序见下</div></div>
    <div class="kpi"><div class="k-label">主题告警</div><div class="k-value">{theme_alerts}</div>
      <div class="k-sub">阈值穿越（双向）</div></div>
    <div class="kpi"><div class="k-label">索引一致性</div><div class="k-value">{parity}</div>
      <div class="k-sub">canonical / lexical / vector</div></div>
  </div>
</section>"""


def _render_attention(snapshot: DailyRunSnapshot) -> str:
    if not snapshot.attention:
        return ""
    _sev_badge = {
        AttentionSeverity.CRITICAL: '<span class="badge badge-bad">严重 CRITICAL</span>',
        AttentionSeverity.WARNING: '<span class="badge badge-warn">警告 WARNING</span>',
        AttentionSeverity.INFO: '<span class="badge badge-info">提示 INFO</span>',
    }
    items = []
    for item in snapshot.attention:
        items.append(
            f'<div class="attn sev-{item.severity.value}">'
            f'<div class="a-head">{_sev_badge[item.severity]}'
            f'<span class="a-title">{esc(item.title)}</span>'
            f'<span class="a-module">模块 {esc(item.module_id)}</span></div>'
            f'<p><b>影响：</b>{esc(item.impact)}</p>'
            f'<p class="a-action"><b>建议：</b>{esc(item.recommended_action)}</p></div>'
        )
    return f"""<section id="attention" aria-labelledby="attention-h">
  <h2 id="attention-h">Attention Required<span class="en">需要处理（{len(snapshot.attention)}）</span></h2>
  {''.join(items)}
</section>"""


def _render_macro(snapshot: DailyRunSnapshot) -> str:
    macro = snapshot.macro
    if macro is None:
        return """<section id="macro" aria-labelledby="macro-h">
  <h2 id="macro-h">Macro &amp; Cross-Asset<span class="en">宏观与跨资产</span></h2>
  <div class="empty">本次运行没有宏观数据（模块被跳过或无已发布快照）。</div>
</section>"""
    if macro.stale_series:
        chips = "".join('<span class="chip">' + esc(series) + "</span>" for series in macro.stale_series)
        stale = f'<div class="chips">{chips}</div>'
    else:
        stale = '<p style="margin:4px 0">无陈旧序列。</p>'
    flows = []
    for flow in macro.target_flows:
        score = flow.absorption_score
        if score is None:
            bar = '<span class="flow-bar" style="width:0"></span>'
            score_text = "N/A"
        else:
            width = min(abs(score), 1.0) * 50
            direction = "pos" if score >= 0 else "neg"
            bar = f'<span class="flow-bar {direction}" style="width:{width:.1f}%"></span>'
            score_text = f"{score:+.2f}"
        flows.append(
            f'<div class="flow-row"><span>{esc(flow.proxy_symbol)}'
            f' <span class="m-id">{esc(flow.target_id)}</span></span>'
            f'<span class="flow-track">{bar}</span>'
            f'<span>{esc(flow.state)} · <b>{score_text}</b></span></div>'
        )
    return f"""<section id="macro" aria-labelledby="macro-h">
  <h2 id="macro-h">Macro &amp; Cross-Asset<span class="en">宏观与跨资产</span></h2>
  <div class="split">
    <div class="panel">
      <h3>宏观状态</h3>
      <div class="regime-line">
        <div class="item"><div class="lab">Regime 体制</div><div class="val">{esc(macro.primary_regime or 'N/A')}</div></div>
        <div class="item"><div class="lab">Risk 风险</div><div class="val">{esc(macro.risk_state or 'N/A')} <span class="m-id">{_signed(macro.risk_score)}</span></div></div>
        <div class="item"><div class="lab">Liquidity 流动性</div><div class="val">{esc(macro.liquidity_state or 'N/A')} <span class="m-id">{_signed(macro.liquidity_score)}</span></div></div>
        <div class="item"><div class="lab">Rate 利率约束</div><div class="val">{esc(macro.rate_pressure_state or 'N/A')} <span class="m-id">{_signed(macro.rate_pressure_score)}</span></div></div>
        <div class="item"><div class="lab">Inflation 通胀象限</div><div class="val">{esc(macro.inflation_quadrant or 'N/A')}</div></div>
      </div>
      <div class="m-meta">
        <span><span class="lab">数据覆盖 </span>{_pct(macro.data_coverage)}</span>
        <span><span class="lab">置信度 </span>{_pct(macro.confidence)}</span>
        <span><span class="lab">模型版本 </span><code class="mono">{esc(macro.model_version or 'N/A')}</code></span>
      </div>
      <h3>陈旧序列（{len(macro.stale_series)}）</h3>
      {stale}
    </div>
    <div class="panel">
      <h3>跨资产流动性吸收</h3>
      {''.join(flows) if flows else '<div class="empty">无跨资产吸收数据。</div>'}
    </div>
  </div>
</section>"""


def _theme_row_html(row_index: int, theme) -> str:
    score = theme.score
    if score is None:
        bar = '<span class="rankbar" aria-hidden="true"><span style="width:0"></span></span>'
        score_text = "N/A"
    else:
        width = min(max(score, 0.0), 100.0)
        bar = f'<span class="rankbar" aria-hidden="true"><span style="width:{width:.0f}%"></span></span>'
        score_text = f"{score:.1f}"
    rel5 = _signed(theme.relative_5d)
    rel20 = _signed(theme.relative_20d)
    rel5_cls = "pos-text" if (theme.relative_5d or 0) >= 0 else "neg-text"
    rel20_cls = "pos-text" if (theme.relative_20d or 0) >= 0 else "neg-text"
    search = f"{theme.label} {theme.theme_id} {theme.proxy_symbol}".lower()
    selected = " selected" if theme.selected else ""
    selected_badge = ' <span class="badge badge-info">选中</span>' if theme.selected else ""
    return (
        f'<tr class="theme-row{selected}" tabindex="0" data-theme="{esc(theme.theme_id)}"'
        f' data-state="{esc(theme.state)}" data-search="{esc(search)}">'
        f"<td class=\"num\">{row_index}</td>"
        f"<td>{esc(theme.label)}{selected_badge}</td>"
        f'<td><code class="mono">{esc(theme.proxy_symbol)}</code></td>'
        f'<td class="num">{bar}{score_text}</td>'
        f"<td>{esc(theme.state)}</td>"
        f"<td>{esc(theme.trend or 'N/A')}</td>"
        f'<td class="num">{_pct(theme.coverage)}</td>'
        f'<td class="num {rel5_cls}">{rel5}</td>'
        f'<td class="num {rel20_cls}">{rel20}</td></tr>'
    )


def _render_themes(snapshot: DailyRunSnapshot) -> str:
    themes = snapshot.themes
    if themes is None:
        return """<section id="themes" aria-labelledby="themes-h">
  <h2 id="themes-h">Technology Theme Rotation<span class="en">科技主题轮动</span></h2>
  <div class="empty">本次运行没有主题轮动数据（模块被跳过或无已发布快照）。</div>
</section>"""
    ordered = sorted(themes.themes, key=lambda t: (t.score is not None, t.score or 0.0), reverse=True)
    rows = "".join(_theme_row_html(i + 1, theme) for i, theme in enumerate(ordered))
    states = sorted({theme.state for theme in themes.themes})
    options = '<option value="all">全部状态</option>' + "".join(
        f'<option value="{esc(state)}">{esc(state)}</option>' for state in states
    )
    selected = next((t for t in themes.themes if t.selected), ordered[0] if ordered else None)
    if selected is not None:
        detail = (
            f'<h3>重点主题 · {esc(selected.label)} <span class="m-id">{esc(selected.theme_id)}</span></h3>'
            '<div class="d-grid">'
            f'<span>代理 <b>{esc(selected.proxy_symbol)}</b></span>'
            f'<span>Score <b>{_score(selected.score)}</b></span>'
            f'<span>状态 <b>{esc(selected.state)}</b></span>'
            f'<span>趋势 <b>{esc(selected.trend or "N/A")}</b></span>'
            f'<span>覆盖 <b>{_pct(selected.coverage)}</b></span>'
            f'<span>5D相对 <b>{_signed(selected.relative_5d)}%</b></span>'
            f'<span>20D相对 <b>{_signed(selected.relative_20d)}%</b></span>'
            "</div>"
        )
    else:
        detail = '<div class="empty">无主题数据。</div>'
    alerts = []
    for alert in themes.alerts:
        if alert.direction == "CROSS_ABOVE":
            badge = '<span class="badge badge-ok">▲ CROSS_ABOVE</span>'
        else:
            badge = '<span class="badge badge-bad">▼ CROSS_BELOW</span>'
        alerts.append(
            f"<li>{badge} {esc(alert.label)}：{alert.previous_value:.1f} → {alert.current_value:.1f}"
            f"（阈值 {alert.threshold:.0f}）</li>"
        )
    alerts_html = f"<ul>{''.join(alerts)}</ul>" if alerts else '<div class="empty">本次没有新的阈值穿越。</div>'
    attr_rows = "".join(
        f"<tr><td>{esc(a.label)}</td><td class=\"num\">{a.score:.1f}</td>"
        f"<td>{esc(a.core_signal)}</td><td>{esc(a.invalidation)}</td></tr>"
        for a in themes.attributions
    )
    attr_html = (
        '<div class="table-wrap"><table aria-label="主题归因">'
        "<thead><tr><th>归因</th><th class=\"num\">分数</th><th>核心信号</th><th>失效条件</th></tr></thead>"
        f"<tbody>{attr_rows}</tbody></table></div>"
        if attr_rows
        else '<div class="empty">无归因数据。</div>'
    )
    return f"""<section id="themes" aria-labelledby="themes-h">
  <h2 id="themes-h">Technology Theme Rotation<span class="en">科技主题轮动</span></h2>
  <div class="filter-bar" role="group" aria-label="主题筛选">
    <label for="theme-filter-input">关键词</label>
    <input id="theme-filter-input" type="search" placeholder="主题名 / ID / 代理" autocomplete="off">
    <label for="theme-state-filter">状态</label>
    <select id="theme-state-filter">{options}</select>
  </div>
  <div class="table-wrap">
    <table aria-label="科技主题轮动排名">
      <thead><tr><th class="num">#</th><th>主题</th><th>代理</th><th class="num">Score</th><th>状态</th>
      <th>趋势</th><th class="num">覆盖</th><th class="num">5D相对%</th><th class="num">20D相对%</th></tr></thead>
      <tbody id="theme-table-body">{rows}</tbody>
    </table>
  </div>
  <div id="theme-detail" class="panel" aria-live="polite">{detail}</div>
  <div class="split">
    <div><h3>阈值穿越告警</h3>{alerts_html}</div>
    <div><h3>轮动归因</h3>{attr_html}</div>
  </div>
</section>"""


def _focus_badge(candidate) -> str:
    return ' <span class="badge badge-info">FOCUS</span>' if candidate.focus_selected else ""


def _render_china(snapshot: DailyRunSnapshot) -> str:
    china = snapshot.china_a_share
    if china is None:
        return """<section id="china-a-share" aria-labelledby="china-h">
  <h2 id="china-h">China A-Share Research<span class="en">A股研究</span></h2>
  <div class="empty">本次运行没有 A 股研究数据（模块被跳过）。</div>
</section>"""
    if china.wave_candidates:
        wave_rows = "".join(
            f'<tr><td><code class="mono">{esc(c.ticker)}</code></td><td>{esc(c.name)}</td>'
            f'<td class="num">{c.leader_score if c.leader_score is not None else "N/A"}</td>'
            f"<td>{esc(c.stage_label or 'N/A')}</td>"
            f"<td>{esc('；'.join(c.top_reasons) or '—')}</td>"
            f"<td>{esc('；'.join(c.risk_flags) or '—')}</td>"
            f'<td class="num">{_pct(c.coverage)}</td></tr>'
            for c in china.wave_candidates
        )
        wave_html = (
            '<div class="table-wrap"><table aria-label="WaveScore 候选">'
            '<thead><tr><th>代码</th><th>名称</th><th class="num">LeaderScore</th><th>阶段</th>'
            '<th>入选原因</th><th>风险</th><th class="num">覆盖</th></tr></thead>'
            f"<tbody>{wave_rows}</tbody></table></div>"
        )
    else:
        wave_html = '<div class="empty">当前无符合条件候选。</div>'
    if china.repair_candidates:
        repair_rows = "".join(
            f'<tr><td><code class="mono">{esc(c.ticker)}</code></td><td>{esc(c.name)}</td>'
            f'<td class="num">{_score(c.reversal_score)}</td>'
            f"<td>{esc(c.stage or 'N/A')}{_focus_badge(c)}</td>"
            f"<td>{esc('；'.join(c.top_reasons) or '—')}</td>"
            f"<td>{esc('；'.join(c.risk_flags) or '—')}</td></tr>"
            for c in china.repair_candidates
        )
        repair_html = (
            '<div class="table-wrap"><table aria-label="Selloff repair 候选">'
            '<thead><tr><th>代码</th><th>名称</th><th class="num">ReversalScore</th><th>阶段</th>'
            '<th>入选原因</th><th>风险</th></tr></thead>'
            f"<tbody>{repair_rows}</tbody></table></div>"
        )
    else:
        repair_html = '<div class="empty">当前无符合条件候选。</div>'
    versions = (
        f'<div class="m-meta">'
        f'<span><span class="lab">WaveScore 版本 </span><code class="mono">{esc(china.wave_model_version or "N/A")}</code></span>'
        f'<span><span class="lab">Repair 版本 </span><code class="mono">{esc(china.repair_model_version or "N/A")}</code></span>'
        f"</div>"
    )
    return f"""<section id="china-a-share" aria-labelledby="china-h">
  <h2 id="china-h">China A-Share Research<span class="en">A股研究</span></h2>
  <div class="note" role="note">{esc(china.universe_note)}</div>
  {versions}
  <div class="split">
    <div><h3>WaveScore 候选（{len(china.wave_candidates)}）</h3>{wave_html}</div>
    <div><h3>Selloff Repair 候选（{len(china.repair_candidates)}）</h3>{repair_html}</div>
  </div>
</section>"""


def _render_index_health(snapshot: DailyRunSnapshot) -> str:
    health = snapshot.index_health
    if health is None:
        return """<section id="index-health" aria-labelledby="index-h">
  <h2 id="index-h">RAG / Index Health<span class="en">索引健康</span></h2>
  <div class="empty">本次运行没有索引健康数据。</div>
</section>"""

    def _parity(flag: bool | None) -> str:
        if flag is None:
            return '<span class="badge badge-muted">N/A</span>'
        return (
            '<span class="badge badge-ok">✓ 一致</span>' if flag else '<span class="badge badge-bad">✗ 不一致</span>'
        )

    diff_note = ""
    if (
        health.indexes_in_parity is False
        and health.canonical_chunks is not None
        and health.vector_chunks is not None
    ):
        gap = health.canonical_chunks - health.vector_chunks
        diff_note = (
            f'<p class="m-msg err">vector 索引与 canonical 相差 {gap:+,} 个 chunk'
            f"（lexical 一致：{'是' if health.lexical_in_parity else '否'}）。</p>"
        )
    elif health.indexes_in_parity is True:
        diff_note = '<p class="m-msg" style="background:var(--ok-bg);color:var(--ok);border:1px solid #b7dfc4">三类索引 manifest 完全一致。</p>'
    return f"""<section id="index-health" aria-labelledby="index-h">
  <h2 id="index-h">RAG / Index Health<span class="en">索引健康</span></h2>
  <div class="ih-grid">
    <div class="kpi"><div class="k-label">canonical chunks</div><div class="k-value">{_int(health.canonical_chunks)}</div></div>
    <div class="kpi"><div class="k-label">lexical chunks</div><div class="k-value">{_int(health.lexical_chunks)}</div>
      <div class="k-sub">parity {_parity(health.lexical_in_parity)}</div></div>
    <div class="kpi"><div class="k-label">vector chunks</div><div class="k-value">{_int(health.vector_chunks)}</div>
      <div class="k-sub">parity {_parity(health.vector_in_parity)}</div></div>
    <div class="kpi"><div class="k-label">总体一致性</div><div class="k-value">{_parity(health.indexes_in_parity)}</div></div>
    <div class="kpi"><div class="k-label">outbox pending / running</div>
      <div class="k-value">{_int(health.outbox_pending)} / {_int(health.outbox_running)}</div></div>
    <div class="kpi"><div class="k-label">outbox completed / failed</div>
      <div class="k-value">{_int(health.outbox_completed)} / {_int(health.outbox_failed)}</div></div>
  </div>
  {diff_note}
  <div class="m-meta">
    <span><span class="lab">lexical 索引版本 </span><code class="mono">{esc(health.lexical_index_version or 'N/A')}</code></span>
    <span><span class="lab">vector 索引版本 </span><code class="mono">{esc(health.vector_index_version or 'N/A')}</code></span>
  </div>
</section>"""


def _render_module_card(module: ModuleResult) -> str:
    freshness = module.freshness
    fresh_label = _FRESHNESS_LABEL[freshness.state]
    fresh_detail = ""
    if freshness.data_as_of:
        fresh_detail = f"（数据 {esc(freshness.data_as_of)}"
        if freshness.stale_days is not None:
            fresh_detail += f"，滞后 {freshness.stale_days}d"
        fresh_detail += "）"
    cache = "N/A" if module.cache_used is None else ("命中 HIT" if module.cache_used else "未命中 MISS")
    duration = "N/A" if module.duration_ms is None else f"{module.duration_ms / 1000:.2f}s"
    messages = "".join(f'<p class="m-msg err">✗ {esc(error)}</p>' for error in module.errors)
    messages += "".join(f'<p class="m-msg warn">⚠ {esc(warning)}</p>' for warning in module.warnings)
    artifacts = ""
    if module.artifacts:
        items = "".join(
            f'<li><a href="{esc(a.path)}">{esc(a.label)}</a> <span class="m-id">{esc(a.kind.value)}</span></li>'
            for a in module.artifacts
        )
        artifacts = f'<ul class="m-artifacts">{items}</ul>'
    metadata = ""
    if module.metadata:
        rows = "".join(
            f"<tr><th>{esc(key)}</th><td>{esc(value)}</td></tr>" for key, value in module.metadata
        )
        metadata = f"<details><summary>次要元数据（{len(module.metadata)}）</summary><table>{rows}</table></details>"
    return f"""<article class="module-card" data-status="{module.status.value}">
  <div class="m-head"><span class="m-title">{esc(module.label)} <span class="m-id">{esc(module.module_id)}</span></span>{_badge(module.status)}</div>
  <p class="m-summary">{esc(module.summary)}</p>
  <div class="m-meta">
    <span><span class="lab">新鲜度 </span>{fresh_label}{fresh_detail}</span>
    <span><span class="lab">覆盖率 </span>{_pct(module.coverage)}</span>
    <span><span class="lab">缓存 </span>{cache}</span>
    <span><span class="lab">耗时 </span>{duration}</span>
    <span><span class="lab">模型版本 </span><code class="mono">{esc(module.model_version or 'N/A')}</code></span>
    <span><span class="lab">规则版本 </span><code class="mono">{esc(module.rule_version or 'N/A')}</code></span>
  </div>
  {messages}
  {artifacts}
  {metadata}
</article>"""


def _render_modules(snapshot: DailyRunSnapshot) -> str:
    cards = "".join(_render_module_card(module) for module in snapshot.modules)
    return f"""<section id="modules" aria-labelledby="modules-h">
  <h2 id="modules-h">Module Details<span class="en">模块详情</span></h2>
  <div class="filter-bar" id="module-filter" role="group" aria-label="模块状态筛选">
    <button type="button" data-filter="all" aria-pressed="true">All 全部</button>
    <button type="button" data-filter="issues" aria-pressed="false">Issues 异常</button>
    <button type="button" data-filter="success" aria-pressed="false">Success 成功</button>
    <button type="button" data-filter="skipped" aria-pressed="false">Skipped 跳过</button>
  </div>
  <div id="module-list">{cards}</div>
</section>"""


def _render_footer(snapshot: DailyRunSnapshot) -> str:
    artifact_items = "".join(
        f'<li><a href="{esc(a.path)}">{esc(a.label)}</a> <span class="m-id">{esc(a.kind.value)}</span></li>'
        for a in snapshot.artifacts
    )
    notes = "".join(f"<p>{esc(note)}</p>" for note in snapshot.notes)
    synthetic = (
        "<p><b>SYNTHETIC：</b>本页数据为 fixture 合成数据，仅用于演示与评审，"
        "不得与真实市场状态混淆，不得用于任何投资决策。</p>"
        if snapshot.synthetic
        else ""
    )
    return f"""<footer aria-label="免责声明">
  <div class="wrap">
    {synthetic}
    {notes}
    <p>数据与模型限制：宏观、主题与选股结果均由确定性规则或历史数据统计产生，存在数据缺口、
    滞后与模型简化；覆盖率与新鲜度指标反映数据质量而非结论正确性。</p>
    <p>本页面仅供信息参考与研究流程审计，不构成投资建议。</p>
    <p>run_id <code class="mono">{esc(snapshot.run_id)}</code> · as_of {esc(snapshot.as_of.isoformat())} ·
    生成于 {esc(snapshot.generated_at.isoformat())}</p>
    <ul>{artifact_items}</ul>
  </div>
</footer>"""


def render_daily_run_dashboard(snapshot: DailyRunSnapshot) -> str:
    data_block = dumps_safe(snapshot)
    body = "\n".join(
        [
            _render_runbar(snapshot),
            _render_nav(snapshot),
            '<main><div class="wrap">',
            _render_summary(snapshot),
            _render_attention(snapshot),
            _render_macro(snapshot),
            _render_themes(snapshot),
            _render_china(snapshot),
            _render_index_health(snapshot),
            _render_modules(snapshot),
            "</div></main>",
            _render_footer(snapshot),
        ]
    )
    title = f"Daily Research Command Center {snapshot.as_of.date().isoformat()}"
    return f"""<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{esc(title)}</title>
<style>{CSS}</style>
</head>
<body>
{body}
<script type="application/json" id="daily-run-data">{data_block}</script>
<script>{JS}</script>
</body>
</html>
"""


def render_daily_run_markdown(snapshot: DailyRunSnapshot) -> str:
    lines = [
        f"# Daily Run — {snapshot.as_of.date().isoformat()}",
        "",
        f"- run_id: `{snapshot.run_id}`",
        f"- mode: `{snapshot.mode.value}` · overall: **{snapshot.overall_status.value}**"
        + (" · **SYNTHETIC FIXTURE**" if snapshot.synthetic else ""),
        f"- generated_at: {snapshot.generated_at.isoformat()}",
        "",
        "## Modules",
        "",
        "| 模块 | 状态 | 覆盖 | 新鲜度 | 摘要 |",
        "|---|---|---:|---|---|",
    ]
    for module in snapshot.modules:
        lines.append(
            f"| {module.label} (`{module.module_id}`) | {module.status.value} | {_pct(module.coverage)} | "
            f"{_FRESHNESS_LABEL[module.freshness.state]} | {module.summary} |"
        )
    if snapshot.attention:
        lines.extend(["", "## Attention", ""])
        lines.extend(
            f"- [{item.severity.value}] {item.title} — {item.impact}" for item in snapshot.attention
        )
    lines.extend(["", "> 本摘要仅供研究流程审计，不构成投资建议。", ""])
    return "\n".join(lines)


def publish_daily_run_outputs(output_dir: Path | str, snapshot: DailyRunSnapshot) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    suffix = snapshot.as_of.date().isoformat()
    json_path = output_dir / f"daily_run_snapshot_{suffix}.json"
    markdown_path = output_dir / f"daily_run_report_{suffix}.md"
    html_path = output_dir / f"daily_run_dashboard_{suffix}.html"
    json_path.write_text(
        json.dumps(jsonable(snapshot), ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8"
    )
    markdown_path.write_text(render_daily_run_markdown(snapshot), encoding="utf-8")
    html_path.write_text(render_daily_run_dashboard(snapshot), encoding="utf-8")
    return {"json": json_path, "markdown": markdown_path, "html": html_path}
