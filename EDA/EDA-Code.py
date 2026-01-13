!pip install sweetviz wordcloud matplotlib seaborn scikit-learn pillow

import pandas as pd
import numpy as np

# Monkey-patch for Sweetviz compatibility
if not hasattr(np, "VisibleDeprecationWarning"):
    np.VisibleDeprecationWarning = DeprecationWarning

import os
import re
import base64
import html as html_lib
import numpy as np
import pandas as pd
import json

import os
import json
import re
import pandas as pd
import sweetviz as sv
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

import sweetviz as sv

import plotly.express as px
import plotly.graph_objects as go

from sklearn.feature_extraction.text import CountVectorizer

from wordcloud import WordCloud, STOPWORDS
from PIL import Image
import matplotlib.pyplot as plt

# =========================================================
# INTERACTIVE PLOTLY DASHBOARD (XLSX) + SWEETVIZ
# Cascading filters (Region->Country->Sector->Company_Name)
# + NEW: Start Month / End Month filters (interactive + affect ALL plots)
# Keep everything else the same (logic + layout), only adding month filters.
# =========================================================

import os
import re
import json
import math
import pandas as pd

import sweetviz as sv

# -----------------------------
# PATHS
# -----------------------------
file_path = r"C:\Users\Dell\OneDrive\Documents\IIMV -MBA\IIMV_Capstone_Project\StockNews_GDELT_SCRAPED_20200101_20251201_V8.xlsx"
out_base_dir = r"C:\Users\Dell\OneDrive\Documents\IIMV -MBA\IIMV_Capstone_Project"
os.makedirs(out_base_dir, exist_ok=True)

sweetviz_html = os.path.join(out_base_dir, "EDA_Sweetviz_IT_Global_News.html")
plotly_html   = os.path.join(out_base_dir, "EDA_Plotly_IT_Global_News.html")

# -----------------------------
# SETTINGS
# -----------------------------
TOP_TFIDF = 20
TOP_BAR = 20
MAX_TOKENS_PER_ROW = 120
MAX_ROWS_EXPORT = None  # set to e.g. 250000 if file is huge

# -----------------------------
# STOPWORDS
# -----------------------------
EXTRA_STOPWORDS = {
    "inc","ltd","limited","plc","corp","corporation","company",
    "said","says","say","will","new","today","week","weeks","year","years",
    "stock","stocks","share","shares","market","markets",
    "earnings","quarter","q1","q2","q3","q4",
    "percent","billion","million","trillion",
    "reuters","yahoo","finance","update","breaking","report","reports",
    "according","also","among","may","might","could","would","still",
    "one","two","three","four","five","first","second",
    "mr","ms","dr","ceo","cfo","coo",
    "us","u","s","uk","eu","india"
}
BASIC_STOPWORDS = {
 "a","about","above","after","again","against","all","am","an","and","any","are","as","at",
 "be","because","been","before","being","below","between","both","but","by",
 "can","did","do","does","doing","down","during",
 "each","few","for","from","further",
 "had","has","have","having","he","her","here","hers","herself","him","himself","his","how",
 "i","if","in","into","is","it","its","itself",
 "just",
 "me","more","most","my","myself",
 "no","nor","not","now",
 "of","off","on","once","only","or","other","our","ours","ourselves","out","over","own",
 "same","she","should","so","some","such",
 "than","that","the","their","theirs","them","themselves","then","there","these","they","this","those","through","to","too",
 "under","until","up",
 "very",
 "was","we","were","what","when","where","which","while","who","whom","why","with","you","your","yours","yourself","yourselves"
}
STOPWORDS = BASIC_STOPWORDS.union(EXTRA_STOPWORDS)

# -----------------------------
# TOKENIZER
# -----------------------------
TOKEN_RE = re.compile(r"[a-zA-Z][a-zA-Z0-9\-\_]{2,}")  # len>=3

def clean_tokens(text: str):
    if text is None:
        return []
    t = str(text).lower()
    t = re.sub(r"http\S+|www\.\S+", " ", t)
    t = re.sub(r"[^a-z0-9\-\_\s]", " ", t)
    toks = TOKEN_RE.findall(t)
    out = []
    for w in toks:
        if w in STOPWORDS:
            continue
        if w.isdigit():
            continue
        if len(w) < 3:
            continue
        out.append(w)
    return out

# -----------------------------
# LOAD DATA (XLSX)
# -----------------------------
df = pd.read_excel(file_path, engine="openpyxl")
if MAX_ROWS_EXPORT is not None and len(df) > MAX_ROWS_EXPORT:
    df = df.iloc[:MAX_ROWS_EXPORT].copy()

for c in ["Region","Country","Sector","Company_Name","headline","snippet","date"]:
    if c not in df.columns:
        df[c] = ""

df["Region"] = df["Region"].fillna("Unknown").astype(str)
df["Country"] = df["Country"].fillna("Unknown").astype(str)
df["Sector"] = df["Sector"].fillna("Unknown").astype(str)
df["Company_Name"] = df["Company_Name"].fillna("Unknown").astype(str)

df["headline"] = df["headline"].fillna("").astype(str)
df["snippet"]  = df["snippet"].fillna("").astype(str)

df["date"] = pd.to_datetime(df["date"], errors="coerce")
df = df.dropna(subset=["date"]).copy()

df["month"] = df["date"].dt.to_period("M").astype(str)

df["headline_word_len"] = df["headline"].str.split().str.len()
df["snippet_word_len"]  = df["snippet"].str.split().str.len()

df["tokens_headline"] = df["headline"].apply(clean_tokens).apply(lambda x: x[:MAX_TOKENS_PER_ROW])
df["tokens_snippet"]  = df["snippet"].apply(clean_tokens).apply(lambda x: x[:MAX_TOKENS_PER_ROW])
df["tokens_all"]      = (df["headline"] + " " + df["snippet"]).apply(clean_tokens).apply(lambda x: x[:MAX_TOKENS_PER_ROW])

print("Dataset rows after date filter:", len(df))

# -----------------------------
# SWEETVIZ (FIXED)
# -----------------------------
df_sweetviz = df.drop(columns=["tokens_headline","tokens_snippet","tokens_all"], errors="ignore").copy()

skip_candidates = ["url", "text"]
skip_cols = [c for c in skip_candidates if c in df_sweetviz.columns]
feat_cfg = sv.FeatureConfig(skip=skip_cols) if skip_cols else None

report = sv.analyze(
    df_sweetviz,
    target_feat=None,
    pairwise_analysis="on",
    feat_cfg=feat_cfg
)
report.show_html(filepath=sweetviz_html, open_browser=False)

sweetviz_color_css = """
<style>
body { background: linear-gradient(180deg, #f7f9fc, #ffffff) !important; }
h1, h2, h3 { color: #1f4fd8 !important; font-family: "Segoe UI", Arial, sans-serif !important; }
a { color: #00b4d8 !important; font-weight: 700 !important; }
th { background: linear-gradient(90deg, #1f4fd8, #00b4d8) !important; color: white !important; }
td { background: #f8fbff !important; }
</style>
"""
with open(sweetviz_html, "r", encoding="utf-8") as f:
    sv_doc = f.read()
if "linear-gradient(90deg, #1f4fd8" not in sv_doc:
    if "<head>" in sv_doc:
        sv_doc = sv_doc.replace("<head>", "<head>" + sweetviz_color_css)
    else:
        sv_doc = sweetviz_color_css + sv_doc
with open(sweetviz_html, "w", encoding="utf-8") as f:
    f.write(sv_doc)

print("✅ Sweetviz generated:", sweetviz_html)

# -----------------------------
# IDF (TF-IDF)
# -----------------------------
def compute_idf(token_lists):
    N = len(token_lists)
    df_counts = {}
    for toks in token_lists:
        seen = set(toks)
        for t in seen:
            df_counts[t] = df_counts.get(t, 0) + 1
    idf = {}
    for t, d in df_counts.items():
        idf[t] = math.log((1 + N) / (1 + d)) + 1.0
    return idf

idf_head = compute_idf(df["tokens_headline"].tolist())
idf_snip = compute_idf(df["tokens_snippet"].tolist())

# -----------------------------
# DATA FOR JS
# -----------------------------
months_sorted = sorted(df["month"].unique().tolist())

DATA = {
    "month": df["month"].tolist(),
    "Region": df["Region"].tolist(),
    "Country": df["Country"].tolist(),
    "Sector": df["Sector"].tolist(),
    "Company_Name": df["Company_Name"].tolist(),
    "headline_word_len": df["headline_word_len"].astype(int).tolist(),
    "snippet_word_len": df["snippet_word_len"].astype(int).tolist(),
    "tokens_headline": df["tokens_headline"].tolist(),
    "tokens_snippet": df["tokens_snippet"].tolist(),
    "idf_headline": idf_head,
    "idf_snippet": idf_snip,
    "months_sorted": months_sorted
}
DATA_JSON = json.dumps(DATA)

# -----------------------------
# HTML TEMPLATE
# -----------------------------
html_template = r"""
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1"/>
<title>Interactive Plotly EDA – IT Global News</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>

<style>
body { margin:0; font-family:Segoe UI, Arial; background:linear-gradient(180deg,#f5f7fb,#ffffff); color:#111827; }
header { padding:16px 22px; background:linear-gradient(90deg,#1f4fd8,#00b4d8); color:white; position:sticky; top:0; z-index:10; box-shadow:0 6px 18px rgba(0,0,0,0.12); }
header h2 { margin:0 0 6px 0; font-size:20px; font-weight:800; }
header .sub { font-size:13px; opacity:0.95; display:flex; gap:10px; align-items:center; flex-wrap:wrap; }
header a.btn { background:rgba(255,255,255,0.18); border:1px solid rgba(255,255,255,0.35); color:white; padding:8px 10px; border-radius:10px; text-decoration:none; font-size:13px; font-weight:700; }
.wrap { padding:18px 22px 44px 22px; max-width:1400px; margin:0 auto; }
.controls { display:flex; gap:12px; flex-wrap:wrap; padding:12px; background:white; border-radius:14px; border:1px solid #e5eaf3; box-shadow:0 10px 26px rgba(0,0,0,0.05); }
.ctrl { display:flex; flex-direction:column; gap:6px; min-width:220px; }
label { font-weight:800; color:#1f4fd8; font-size:13px; }
select { padding:8px 10px; border-radius:10px; border:1px solid #d6e0f0; background:white; font-weight:600; outline:none; }
.grid { margin-top:14px; display:grid; grid-template-columns:repeat(auto-fit,minmax(460px,1fr)); gap:14px; }
.card { background:white; border-radius:16px; border:1px solid #e5eaf3; box-shadow:0 10px 26px rgba(0,0,0,0.05); padding:10px; }
.wide { grid-column:1/-1; }
.two { display:grid; grid-template-columns:repeat(auto-fit,minmax(420px,1fr)); gap:14px; }
.note { font-size:12px; color:#4b5563; margin-top:10px; }
</style>
</head>

<body>

<header>
  <h2>Interactive EDA Dashboard — Global Stock News</h2>
  <div class="sub">
    Filters cascade and all charts update together.
    <a class="btn" href="__SWEETVIZ_BASENAME__" target="_blank">Open Sweetviz EDA</a>
  </div>
</header>

<div class="wrap">
  <div class="controls">
    <div class="ctrl">
      <label for="Region">Region</label>
      <select id="Region"></select>
    </div>
    <div class="ctrl">
      <label for="Country">Country</label>
      <select id="Country"></select>
    </div>
    <div class="ctrl">
      <label for="Sector">Sector</label>
      <select id="Sector"></select>
    </div>
    <div class="ctrl">
      <label for="Company_Name">Company_Name</label>
      <select id="Company_Name"></select>
    </div>

    <!-- NEW: Month filters -->
    <div class="ctrl">
      <label for="Start_Month">Start Month</label>
      <select id="Start_Month"></select>
    </div>
    <div class="ctrl">
      <label for="End_Month">End Month</label>
      <select id="End_Month"></select>
    </div>
  </div>

  <div class="note">
    Start from <b>All</b>, then filter by Region/Country/Sector/Company and Month Range. Dropdowns update based on selections.
  </div>

  <div class="grid">

    <div class="card wide"><div id="ts_month"></div></div>

    <div class="card wide">
      <div class="two">
        <div><div id="tfidf_headlines"></div></div>
        <div><div id="tfidf_snippets"></div></div>
      </div>
    </div>

    <div class="card"><div id="top_countries"></div></div>
    <div class="card"><div id="region_pie"></div></div>

    <div class="card wide"><div id="top_sectors"></div></div>

    <div class="card"><div id="hlen_hist"></div></div>
    <div class="card"><div id="slen_hist"></div></div>

    <div class="card wide"><div id="sector_region_heatmap"></div></div>

    <div class="card wide"><div id="wordcloud_plot"></div></div>
  </div>
</div>

<script>
const DATA = __DATA_JSON__;
const ALL = "All";

/* Color palettes */
const BAR_SCALE = "Turbo";
const HEAT_SCALE = "Viridis";
const LINE_COLOR = "#1f4fd8";
const MARKER_COLOR = "#00b4d8";

function uniq(arr){
  const set = new Set(arr);
  return [ALL, ...Array.from(set).sort()];
}

function setOptions(selectEl, options, preferred){
  const cur = (preferred !== undefined) ? preferred : selectEl.value;
  selectEl.innerHTML = "";
  for(const v of options){
    const o = document.createElement("option");
    o.value = v;
    o.textContent = v;
    selectEl.appendChild(o);
  }
  if(options.includes(cur)) selectEl.value = cur;
  else selectEl.value = options.includes(ALL) ? ALL : (options[0] || "");
}

function monthListWithAll(){
  const ms = DATA.months_sorted || [];
  return [ALL, ...ms];
}

function currentFilters(){
  return {
    Region: document.getElementById("Region").value,
    Country: document.getElementById("Country").value,
    Sector: document.getElementById("Sector").value,
    Company_Name: document.getElementById("Company_Name").value,
    Start_Month: document.getElementById("Start_Month").value,
    End_Month: document.getElementById("End_Month").value
  };
}

function inMonthRange(m, startM, endM){
  if(startM===ALL && endM===ALL) return true;
  if(startM!==ALL && endM===ALL) return (m >= startM);
  if(startM===ALL && endM!==ALL) return (m <= endM);
  // both set
  if(startM <= endM) return (m >= startM && m <= endM);
  // if user accidentally sets start > end, treat as swapped range
  return (m >= endM && m <= startM);
}

function passes(i, f){
  return (f.Region===ALL || DATA.Region[i]===f.Region) &&
         (f.Country===ALL || DATA.Country[i]===f.Country) &&
         (f.Sector===ALL || DATA.Sector[i]===f.Sector) &&
         (f.Company_Name===ALL || DATA.Company_Name[i]===f.Company_Name) &&
         inMonthRange(DATA.month[i], f.Start_Month, f.End_Month);
}

function filteredIndices(f){
  const out = [];
  for(let i=0;i<DATA.month.length;i++){
    if(passes(i,f)) out.push(i);
  }
  return out;
}

function recomputeDropdowns(changedId){
  const f = currentFilters();

  function okExcept(field, i){
    const okRegion  = (field==="Region") ? true : (f.Region===ALL || DATA.Region[i]===f.Region);
    const okCountry = (field==="Country") ? true : (f.Country===ALL || DATA.Country[i]===f.Country);
    const okSector  = (field==="Sector") ? true : (f.Sector===ALL || DATA.Sector[i]===f.Sector);
    const okComp    = (field==="Company_Name") ? true : (f.Company_Name===ALL || DATA.Company_Name[i]===f.Company_Name);
    const okMonth   = (field==="Start_Month" || field==="End_Month") ? true : inMonthRange(DATA.month[i], f.Start_Month, f.End_Month);
    return okRegion && okCountry && okSector && okComp && okMonth;
  }

  function optionsFor(field){
    if(field==="Start_Month" || field==="End_Month"){
      // month dropdowns should still reflect other selections (Region/Country/Sector/Company)
      const vals = [];
      for(let i=0;i<DATA.month.length;i++){
        const ok =
          (f.Region===ALL || DATA.Region[i]===f.Region) &&
          (f.Country===ALL || DATA.Country[i]===f.Country) &&
          (f.Sector===ALL || DATA.Sector[i]===f.Sector) &&
          (f.Company_Name===ALL || DATA.Company_Name[i]===f.Company_Name);
        if(ok) vals.push(DATA.month[i]);
      }
      const set = new Set(vals);
      return [ALL, ...Array.from(set).sort()];
    }

    const vals = [];
    for(let i=0;i<DATA.month.length;i++){
      if(okExcept(field, i)) vals.push(DATA[field][i]);
    }
    return uniq(vals);
  }

  const fields = ["Region","Country","Sector","Company_Name","Start_Month","End_Month"];
  for(const field of fields){
    const sel = document.getElementById(field);
    const opts = optionsFor(field);
    const pref = (field===changedId) ? f[field] : sel.value;
    setOptions(sel, opts, pref);
  }

  // Additional safety: if Start/End months are not in options anymore, set to ALL
  const sm = document.getElementById("Start_Month");
  const em = document.getElementById("End_Month");
  if(!Array.from(sm.options).map(o=>o.value).includes(sm.value)) sm.value = ALL;
  if(!Array.from(em.options).map(o=>o.value).includes(em.value)) em.value = ALL;
}

function tfidfTopTerms(indices, tokenField, idfField, topN){
  const score = {};
  const idf = DATA[idfField] || {};
  for(const i of indices){
    const toks = DATA[tokenField][i] || [];
    const tf = {};
    for(const t of toks) tf[t] = (tf[t]||0) + 1;
    for(const t in tf){
      const w = tf[t] * (idf[t] || 1.0);
      score[t] = (score[t] || 0) + w;
    }
  }
  return Object.entries(score).sort((a,b)=>b[1]-a[1]).slice(0, topN);
}

function wordColors(weights){
  if(weights.length === 0) return [];
  const maxW = Math.max(...weights);
  const minW = Math.min(...weights);
  const colors = [];
  for(const w of weights){
    const t = (w - minW) / ((maxW - minW) || 1);
    let c;
    if(t < 0.5){
      const u = t/0.5;
      const r = Math.round(0x1f + u*(0x00-0x1f));
      const g = Math.round(0x4f + u*(0xb4-0x4f));
      const b = Math.round(0xd8 + u*(0xd8-0xd8));
      c = `rgb(${r},${g},${b})`;
    } else {
      const u = (t-0.5)/0.5;
      const r = Math.round(0x00 + u*(0x7c-0x00));
      const g = Math.round(0xb4 + u*(0x3a-0xb4));
      const b = Math.round(0xd8 + u*(0xed-0xd8));
      c = `rgb(${r},${g},${b})`;
    }
    colors.push(c);
  }
  return colors;
}

function wordcloudPoints(items){
  const n = items.length;
  const x = [], y = [], size = [], text = [], hover = [], weight = [];
  const maxW = n>0 ? items[0][1] : 1;

  for(let k=0;k<n;k++){
    const w = items[k][0];
    const wt = items[k][1];
    weight.push(wt);

    const angle = k * 0.75;
    const radius = 0.035 * Math.sqrt(k);
    x.push(0.5 + radius * Math.cos(angle));
    y.push(0.5 + radius * Math.sin(angle));
    text.push(w);

    const s = 14 + 46 * (wt / (maxW || 1));
    size.push(Math.max(12, Math.min(70, s)));
    hover.push(w + "<br>weight: " + wt.toFixed(2));
  }

  const colors = wordColors(weight);
  return {x,y,size,text,hover,colors,weight};
}

function updateAll(changedId){
  if(changedId){
    recomputeDropdowns(changedId);
  }

  const f = currentFilters();
  const I = filteredIndices(f);

  const m = {};
  for(const i of I){
    const mo = DATA.month[i];
    m[mo] = (m[mo]||0) + 1;
  }
  const months = Object.keys(m).sort();
  const counts = months.map(k=>m[k]);

  Plotly.react("ts_month", [{
    x: months, y: counts, mode: "lines+markers",
    line: {color: LINE_COLOR, width: 3},
    marker: {color: MARKER_COLOR, size: 7},
    hovertemplate: "Month=%{x}<br>Articles=%{y}<extra></extra>"
  }], {
    title: "News Volume Over Time (Monthly) — Filtered",
    height: 520,
    margin: {l:50,r:20,t:60,b:40}
  });

  const head = tfidfTopTerms(I, "tokens_headline", "idf_headline", __TOP_TFIDF__);
  const snip = tfidfTopTerms(I, "tokens_snippet",  "idf_snippet",  __TOP_TFIDF__);

  Plotly.react("tfidf_headlines", [{
    x: head.map(d=>d[1]),
    y: head.map(d=>d[0]),
    type: "bar", orientation: "h",
    marker: {color: head.map(d=>d[1]), colorscale: BAR_SCALE},
    hovertemplate: "%{y}<br>TF-IDF=%{x:.2f}<extra></extra>"
  }], {
    title: "Top Terms (TF-IDF) — Headlines (Filtered)",
    height: 520,
    margin: {l:140,r:15,t:60,b:40}
  });

  Plotly.react("tfidf_snippets", [{
    x: snip.map(d=>d[1]),
    y: snip.map(d=>d[0]),
    type: "bar", orientation: "h",
    marker: {color: snip.map(d=>d[1]), colorscale: BAR_SCALE},
    hovertemplate: "%{y}<br>TF-IDF=%{x:.2f}<extra></extra>"
  }], {
    title: "Top Terms (TF-IDF) — Snippets (Filtered)",
    height: 520,
    margin: {l:140,r:15,t:60,b:40}
  });

  const cc = {};
  for(const i of I){
    const c = DATA.Country[i];
    cc[c] = (cc[c]||0)+1;
  }
  const topCountries = Object.entries(cc).sort((a,b)=>b[1]-a[1]).slice(0, __TOP_BAR__);
  const tcVals = topCountries.map(d=>d[1]);

  Plotly.react("top_countries", [{
    x: topCountries.map(d=>d[0]),
    y: tcVals,
    type: "bar",
    marker: {color: tcVals, colorscale: BAR_SCALE},
    hovertemplate: "%{x}<br>Articles=%{y}<extra></extra>"
  }], {
    title: "Top Countries by Article Count — Filtered",
    height: 420,
    margin: {l:50,r:15,t:60,b:90},
    xaxis: {tickangle: -35}
  });

  const sc = {};
  for(const i of I){
    const s = DATA.Sector[i];
    sc[s] = (sc[s]||0)+1;
  }
  const topSectors = Object.entries(sc).sort((a,b)=>b[1]-a[1]).slice(0, __TOP_BAR__);
  const tsVals = topSectors.map(d=>d[1]);

  Plotly.react("top_sectors", [{
    x: topSectors.map(d=>d[0]),
    y: tsVals,
    type: "bar",
    marker: {color: tsVals, colorscale: BAR_SCALE},
    hovertemplate: "%{x}<br>Articles=%{y}<extra></extra>"
  }], {
    title: "Top Sectors by Article Count — Filtered",
    height: 450,
    margin: {l:50,r:15,t:60,b:120},
    xaxis: {tickangle: -30}
  });

  const rc = {};
  for(const i of I){
    const r = DATA.Region[i];
    rc[r] = (rc[r]||0)+1;
  }
  const reg = Object.entries(rc).sort((a,b)=>b[1]-a[1]);

  Plotly.react("region_pie", [{
    labels: reg.map(d=>d[0]),
    values: reg.map(d=>d[1]),
    type: "pie",
    textinfo: "label+percent",
    marker: {colors: reg.map((_,k)=>`hsl(${(k*45)%360},70%,55%)`)},
    hovertemplate: "%{label}<br>Articles=%{value}<extra></extra>"
  }], {
    title: "Region Share of Articles — Filtered",
    height: 420,
    margin: {l:20,r:20,t:60,b:20}
  });

  Plotly.react("hlen_hist", [{
    x: I.map(i=>DATA.headline_word_len[i]),
    type: "histogram",
    nbinsx: 60,
    marker: {color: MARKER_COLOR, opacity: 0.85},
    hovertemplate: "Count=%{y}<extra></extra>"
  }], {
    title: "Headline Word Length Distribution — Filtered",
    height: 420,
    margin: {l:60,r:15,t:60,b:60}
  });

  Plotly.react("slen_hist", [{
    x: I.map(i=>DATA.snippet_word_len[i]),
    type: "histogram",
    nbinsx: 60,
    marker: {color: LINE_COLOR, opacity: 0.85},
    hovertemplate: "Count=%{y}<extra></extra>"
  }], {
    title: "Snippet Word Length Distribution — Filtered",
    height: 420,
    margin: {l:60,r:15,t:60,b:60}
  });

  const regions = Array.from(new Set(I.map(i=>DATA.Region[i]))).sort();
  const sectors = Array.from(new Set(I.map(i=>DATA.Sector[i]))).sort();

  const z = sectors.map(()=>regions.map(()=>0));
  const rIndex = {}; regions.forEach((v,idx)=>rIndex[v]=idx);
  const sIndex = {}; sectors.forEach((v,idx)=>sIndex[v]=idx);

  for(const i of I){
    const rr = DATA.Region[i];
    const ss = DATA.Sector[i];
    z[sIndex[ss]][rIndex[rr]] += 1;
  }

  Plotly.react("sector_region_heatmap", [{
    x: regions,
    y: sectors,
    z: z,
    type: "heatmap",
    colorscale: HEAT_SCALE,
    hovertemplate: "Sector=%{y}<br>Region=%{x}<br>Articles=%{z}<extra></extra>"
  }], {
    title: "Heatmap: Sector vs Region (Article Count) — Filtered",
    height: 650,
    margin: {l:220,r:20,t:60,b:60}
  });

  const merged = {};
  for(const d of head){ merged[d[0]] = (merged[d[0]]||0) + d[1]; }
  for(const d of snip){ merged[d[0]] = (merged[d[0]]||0) + d[1]; }

  const wcItems = Object.entries(merged).sort((a,b)=>b[1]-a[1]).slice(0, 120);
  const pts = wordcloudPoints(wcItems);

  Plotly.react("wordcloud_plot", [{
    x: pts.x,
    y: pts.y,
    text: pts.text,
    mode: "text",
    textfont: { size: pts.size, color: pts.colors },
    hovertext: pts.hover,
    hoverinfo: "text"
  }], {
    title: "WordCloud (TF-IDF blended) — Filtered",
    height: 650,
    margin: {l:20,r:20,t:60,b:20},
    xaxis: {visible:false, range:[0,1]},
    yaxis: {visible:false, range:[0,1]}
  });
}

/* Init dropdowns */
setOptions(document.getElementById("Region"), uniq(DATA.Region), ALL);
setOptions(document.getElementById("Country"), uniq(DATA.Country), ALL);
setOptions(document.getElementById("Sector"), uniq(DATA.Sector), ALL);
setOptions(document.getElementById("Company_Name"), uniq(DATA.Company_Name), ALL);

setOptions(document.getElementById("Start_Month"), monthListWithAll(), ALL);
setOptions(document.getElementById("End_Month"), monthListWithAll(), ALL);

/* Events */
document.getElementById("Region").addEventListener("change", function(){ updateAll("Region"); });
document.getElementById("Country").addEventListener("change", function(){ updateAll("Country"); });
document.getElementById("Sector").addEventListener("change", function(){ updateAll("Sector"); });
document.getElementById("Company_Name").addEventListener("change", function(){ updateAll("Company_Name"); });

document.getElementById("Start_Month").addEventListener("change", function(){ updateAll("Start_Month"); });
document.getElementById("End_Month").addEventListener("change", function(){ updateAll("End_Month"); });

updateAll(null);
</script>
</body>
</html>
"""

html = html_template.replace("__DATA_JSON__", DATA_JSON)
html = html.replace("__SWEETVIZ_BASENAME__", os.path.basename(sweetviz_html))
html = html.replace("__TOP_TFIDF__", str(TOP_TFIDF))
html = html.replace("__TOP_BAR__", str(TOP_BAR))

with open(plotly_html, "w", encoding="utf-8") as f:
    f.write(html)

print("✅ Plotly dashboard generated:", plotly_html)
print("✅ Sweetviz generated:", sweetviz_html)
print("DONE.")
