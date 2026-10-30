import os
import io
import json
import time
import gzip
import threading
import requests
import pandas as pd
import numpy as np
import math

from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, render_template_string, send_file, request
import yfinance as yf
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

app = Flask(__name__)
app.config["JSON_AS_ASCII"] = False

# =====================================================
# ADVANCED HTTP SESSION & USER AGENTS
# =====================================================
session = requests.Session()
retry_strategy = Retry(
    total=5,
    connect=5,
    read=5,
    backoff_factor=2,
    status_forcelist=[429, 500, 502, 503, 504]
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("https://", adapter)
session.mount("http://", adapter)

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Edge/137.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:140.0) Gecko/20100101 Firefox/140.0"
]

def get_headers():
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive"
    }

# =====================================================
# CONFIGURATION & STATE
# =====================================================
TIMEFRAME = "1h"
PERIOD = "60d"
PIVOT_LEFT = 3
PIVOT_RIGHT = 3
ENTRY_ZONE_PERCENT = 0.35
SL_BUFFER_PERCENT = 0.20
RISK_REWARD = 2.0

BATCH_SIZE = 10
MAX_WORKERS = 2
REQUEST_GAP_SECONDS = 1.0
MAX_RETRIES = 5
RETRY_BASE_SECONDS = 5
CACHE_TTL_SECONDS = 600
SCAN_INTERVAL = 900
REQUEST_TIMEOUT = 30

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(BASE_DIR, "MarkeTGyan_Reports")
os.makedirs(REPORT_DIR, exist_ok=True)

CACHE_FILE = os.path.join(BASE_DIR, "nse_futures_symbols_cache.json")
REPORT_FILE = os.path.join(REPORT_DIR, "NSE_FUTURES_DOW_REPORT.csv")

NSE_HOME = "https://www.nseindia.com"
NSE_CONTRACT_BASE = "https://nsearchives.nseindia.com/content/fo/"

LOCK = threading.Lock()
REQUEST_LOCK = threading.Lock()
CACHE_LOCK = threading.Lock()

LAST_REQUEST_TIME = 0.0
GLOBAL_COOLDOWN_UNTIL = 0.0
STOCK_DATA_CACHE = {}

STATE = {
    "stocks": [],
    "results": [],
    "confirmed": [],
    "gainers": [],
    "losers": [],
    "status": "INITIALIZING",
    "message": "Starting scanner...",
    "last_scan": None,
    "last_list_update": None,
    "source": "NOT CONNECTED",
    "total": 0,
    "completed": 0,
    "errors": 0,
    "progress": 0,
    "batch_number": 0,
    "total_batches": 0,
    "current_batch": "",
    "running": False
}

def wait_for_request_slot():
    global LAST_REQUEST_TIME, GLOBAL_COOLDOWN_UNTIL
    with REQUEST_LOCK:
        now = time.monotonic()
        cooldown = GLOBAL_COOLDOWN_UNTIL - now
        if cooldown > 0:
            time.sleep(cooldown)
        elapsed = time.monotonic() - LAST_REQUEST_TIME
        wait_time = REQUEST_GAP_SECONDS - elapsed
        if wait_time > 0:
            time.sleep(wait_time)
        LAST_REQUEST_TIME = time.monotonic()

def activate_global_cooldown(seconds):
    global GLOBAL_COOLDOWN_UNTIL
    with REQUEST_LOCK:
        GLOBAL_COOLDOWN_UNTIL = max(GLOBAL_COOLDOWN_UNTIL, time.monotonic() + seconds)

# =====================================================
# HTML TEMPLATE (RESPONSIVE & PURANE DESIGN KE ANUSAR)
# =====================================================
PAGE = r"""
<!doctype html>
<html lang="hi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>MarkeTGyan Professional Stock Screener & Scanner</title>
<style>
:root{
 --bg:#f2f6fb;--panel:#ffffff;--line:#d9e4ef;--text:#26364a;--muted:#5c6f84;
 --gold:#f4c95d;--green:#18794e;--red:#c2414b;--blue:#3977c3;--teal:#168b8b;
 --soft-blue:#eaf3ff;--soft-green:#e7f6ee;--soft-red:#fff0f0;--soft-gold:#fff7df;
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{scroll-behavior:smooth}
body{margin:0;background:linear-gradient(145deg,#f5f8fc 0%,#edf4fb 48%,#f8f7f0 100%);font-family:Arial,"Noto Sans Devanagari",sans-serif;color:var(--text);font-size:16px;line-height:1.55}
main{max-width:1400px;margin:0 auto;padding:clamp(12px,2.5vw,28px)}
header{padding:clamp(14px,3vw,30px) 0 16px}
h1{font-size:clamp(24px,4.3vw,38px);line-height:1.2;letter-spacing:-.4px;margin:12px 0 10px;color:#203c60}
.sub{color:var(--muted);line-height:1.7}
.panel{background:rgba(255,255,255,.96);border:1px solid #dce6f0;border-radius:18px;padding:clamp(14px,2.2vw,22px);margin:16px 0;box-shadow:0 5px 18px rgba(45,76,112,.07)}
.panel h2{color:#284d77}
form{display:flex;gap:10px;flex-wrap:wrap}
input{flex:1;min-width:min(210px,100%);background:#fbfdff;color:#26364a;border:1.5px solid #c6d6e7;border-radius:12px;padding:15px;font-size:17px;min-height:50px;outline:none;transition:border-color .2s,box-shadow .2s}
input:focus{border-color:#4b8ed8;box-shadow:0 0 0 3px rgba(75,142,216,.15)}
button{border:0;border-radius:12px;padding:13px 19px;background:linear-gradient(135deg,#f9d976,#f0bd46);color:#28364a;font-weight:800;font-size:16px;cursor:pointer;box-shadow:0 3px 8px rgba(150,117,34,.13);transition:transform .15s,filter .15s;min-height:46px}
button:hover{filter:brightness(.98);transform:translateY(-1px)}button:active{transform:translateY(0)}button:focus-visible{outline:3px solid #4b8ed8;outline-offset:2px}button:disabled{opacity:.65;cursor:wait;transform:none}
.hint{font-size:13px;color:var(--muted);margin-top:10px;line-height:1.6}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(min(180px,100%),1fr));gap:12px}
.metric{background:linear-gradient(145deg,#f7fbff,#edf5fc);border:1px solid #dce8f3;border-radius:14px;padding:15px;min-width:0}
.metric small{display:block;color:#5c7086;margin-bottom:7px;font-size:14px}.metric strong{font-size:clamp(20px,2.6vw,25px);overflow-wrap:anywhere;color:#263e5b}
.green{color:var(--green)!important}.red{color:var(--red)!important}.gold{color:#986b08!important}
h2{font-size:clamp(19px,2.3vw,23px);line-height:1.35;margin:0 0 14px}h3{margin:0 0 8px}.section{margin-top:22px}.copy{line-height:1.8;color:#40536a}.tag{display:inline-block;padding:6px 11px;border-radius:999px;background:#e6f1ff;color:#285f9d;font-size:13px;font-weight:800;border:1px solid #d2e5fb}
table{width:100%;border-collapse:separate;border-spacing:0;margin:0 0 18px;background:#fff;border:1px solid #dce6f0;border-radius:12px;overflow:hidden}td,th{text-align:left;border-bottom:1px solid #e5edf5;padding:12px 11px;vertical-align:top;line-height:1.55}th{color:#365674;background:#eef5fc;font-weight:800}tr:last-child td,tr:last-child th{border-bottom:0}tbody tr:nth-child(even){background:#f8fbfe}tbody tr:hover{background:#f0f7ff}
.error{color:#b52d3a;white-space:pre-wrap;background:var(--soft-red);padding:12px;border-radius:10px}.hidden{display:none}.saved-table-wrap{overflow-x:auto;border-radius:12px;-webkit-overflow-scrolling:touch}.saved-table{min-width:1000px}.saved-actions{display:flex;gap:7px;flex-wrap:wrap}.small-btn{padding:8px 11px;font-size:13px;border-radius:9px;min-height:36px;box-shadow:none;white-space:nowrap}.delete-btn{background:#ffe2e4;color:#9d2631;border:1px solid #f6c4c8}.open-btn{background:#dff5e8;color:#176a43;border:1px solid #b9e6cc}.empty-row{text-align:center;color:var(--muted);padding:22px!important;background:#f8fbfe}
footer{color:#63768a;font-size:12px;line-height:1.7;padding:18px 0 30px}

/* Tabs styling */
.tab-buttons{display:flex;gap:8px;margin:8px 0 16px;border:0;padding:6px;background:#e5edf6;border-radius:16px;flex-wrap:wrap;box-shadow:inset 0 1px 3px rgba(40,70,105,.06)}
.tab-btn{flex:1 1 180px;background:transparent;color:#526a82;border:1px solid transparent;padding:12px 14px;border-radius:11px;cursor:pointer;font-weight:800;font-size:14px;line-height:1.4;box-shadow:none;text-align:center}
.tab-btn:hover{background:#f6faff;transform:none}.tab-btn.active{background:linear-gradient(135deg,#3c80c8,#2e69ad);color:#fff;border-color:#2e69ad;box-shadow:0 4px 12px rgba(46,105,173,.2)}
.tab-content{display:none}.tab-content.active{display:block;animation:fadeIn .22s ease-out}

.progress-track{width:100%;height:14px;background:#e7edf4;border:1px solid #d3dfeb;border-radius:99px;overflow:hidden;margin:8px 0 16px}
.progress-fill{height:100%;width:0%;background:linear-gradient(90deg,#43b581,#168b8b);transition:width .25s ease}
@keyframes fadeIn{from{opacity:.75;transform:translateY(3px)}to{opacity:1;transform:translateY(0)}}

@media(max-width:1024px){.saved-table{min-width:900px}}
@media(max-width:768px){
 main{padding:10px}header{padding:10px 0 6px}.panel{margin:10px 0;border-radius:14px;padding:12px}form{gap:8px}input{width:100%;font-size:16px;padding:12px}form button{width:100%}.tab-buttons{display:grid;grid-template-columns:1fr;gap:6px;padding:6px}.tab-btn{width:100%;padding:12px;font-size:14px}.grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:8px}.metric{padding:10px}.metric small{font-size:11px}.metric strong{font-size:17px}h2{font-size:18px}td,th{padding:9px 8px;font-size:13px}.saved-table{min-width:800px}
}
</style>
</head>
<body>
<main>
<header>
  <div class="tag">MarkeTGyan Professional Suite</div>
  <h1>Stock Analysis & Advance Screener</h1>
  <div class="sub">Single Stock Analysis, Top Gainers/Losers with S/R, aur Advance Dow Theory Futures Screener — Desktop aur Mobile dono ke liye optimized.</div>
</header>

<!-- NAVIGATION TABS -->
<div class="tab-buttons" role="tablist" aria-label="MarkeTGyan Tabs">
  <button type="button" class="tab-btn active" data-tab-target="searchTab">🔎 नया विश्लेषण</button>
  <button type="button" class="tab-btn" data-tab-target="savedTab">🗂️ सेव किए गए स्टॉक्स</button>
  <button type="button" class="tab-btn" data-tab-target="gainersLosersTab">📊 Top Gainer/Loser</button>
  <button type="button" class="tab-btn" data-tab-target="advanceScreenerTab">⚡ Advance Screener</button>
</div>

<!-- TAB 1: Single Stock Analysis -->
<div id="searchTab" class="tab-content active">
  <div class="panel">
    <form id="stockForm">
      <input id="symbol" placeholder="उदाहरण: CIPLA, RELIANCE, TCS, ZOMATO" required autocomplete="off">
      <button id="go" type="submit">विश्लेषण करें ↗</button>
    </form>
    <div class="hint">भारतीय शेयर के लिए NSE symbol लिखें (जैसे CIPLA)। विश्लेषण के बाद स्टॉक 'सेव किए गए स्टॉक्स' टैब में जुड़ जाएगा।</div>
  </div>
  <div id="status" class="panel hidden"></div>
  <div id="results" class="hidden">
    <div class="panel"><div id="company"></div><div id="metrics" class="grid"></div></div>
    <div class="panel"><h2>🧭 Trend और Momentum</h2><div id="trend" class="copy"></div></div>
    <div class="panel"><h2>📊 Daily Support/Resistance</h2><div id="dailyLevels"></div></div>
    <div class="panel"><h2>📋 डेली S/R आधारित Entry, Stop Loss और Target योजना</h2><div id="dailyTradePlan"></div></div>
    <div class="panel"><h2>⏱️ 1HR Support/Resistance</h2><div id="hourlyLevels"></div></div>
    <div class="panel"><h2>📋 1 घंटे के S/R आधारित Entry, Stop Loss और Target योजना</h2><div id="hourlyTradePlan"></div></div>
    <div class="panel"><h2>🏢 कंपनी की वित्तीय स्थिति (Financials)</h2><div id="fundamentals"></div></div>
    <div class="panel"><h2>📊 Shareholding Pattern</h2><div id="shareholding"></div></div>
    <div class="panel"><h2>🧠 AI-जैसा निष्कर्ष</h2><div id="verdict" class="copy"></div></div>
    <div class="panel"><h2>⚠️ Risk Checklist</h2><div id="risks" class="copy"></div></div>
    <div class="panel"><h2>📝 समग्र विश्लेषण नोट</h2><div id="analysisNote" class="copy"></div></div>
  </div>
</div>

<!-- TAB 2: Previously Analyzed Stocks -->
<div id="savedTab" class="tab-content">
  <div class="panel">
    <h2>🗂️ पूर्व विश्लेषण किए गए स्टॉक्स</h2>
    <p class="copy">यहाँ आपके द्वारा खोजे गए स्टॉक्स का रिकॉर्ड सुरक्षित रहता है।</p>
    <div id="savedStockMessage" class="hint"></div>
    <div class="saved-table-wrap">
      <table class="saved-table">
        <thead><tr><th>कंपनी / स्टॉक</th><th>अंतिम भाव</th><th>दैनिक बदलाव</th><th>Daily Support</th><th>Daily Resistance</th><th>1HR Support</th><th>1HR Resistance</th><th>ट्रेंड</th><th>समय</th><th>विकल्प</th></tr></thead>
        <tbody id="savedStocksBody"><tr><td colspan="10" class="empty-row">अभी कोई स्टॉक सेव नहीं है।</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<!-- TAB 3: Top Gainer / Loser -->
<div id="gainersLosersTab" class="tab-content">
  <div class="panel">
    <h2>📊 Top 5 Gainers & Losers (NSE Futures Universe)</h2>
    <p class="copy">यहाँ सभी उपलब्ध फ्यूचर्स स्टॉक्स में से आज के सबसे ज़्यादा बढ़ने वाले (Gainers) और गिरने वाले (Losers) 5-5 स्टॉक्स Daily और 1-Hour S/R स्तरों के साथ दिखाए जाते हैं।</p>
    <div style="margin: 12px 0;"><button type="button" id="refreshGL" onclick="fetchGainersLosers()">↻ डेटा अपडेट करें</button></div>
    <div id="glStatus" class="hint">स्कैनर से डेटा लोड किया जा रहा है...</div>
    
    <h3 class="green" style="margin-top:24px;">🔥 Top 5 Gainers (बढ़ने वाले स्टॉक्स)</h3>
    <div class="saved-table-wrap">
      <table class="saved-table">
        <thead><tr><th>स्टॉक</th><th>LTP (₹)</th><th>बलाव (%)</th><th>Daily Support</th><th>Daily Resistance</th><th>1HR Support</th><th>1HR Resistance</th></tr></thead>
        <tbody id="gainersBody"><tr><td colspan="7" class="empty-row">डेटा लोड हो रहा है...</td></tr></tbody>
      </table>
    </div>

    <h3 class="red" style="margin-top:24px;">🧊 Top 5 Losers (गिरने वाले स्टॉक्स)</h3>
    <div class="saved-table-wrap">
      <table class="saved-table">
        <thead><tr><th>स्टॉक</th><th>LTP (₹)</th><th>बलाव (%)</th><th>Daily Support</th><th>Daily Resistance</th><th>1HR Support</th><th>1HR Resistance</th></tr></thead>
        <tbody id="losersBody"><tr><td colspan="7" class="empty-row">डेटा लोड हो रहा है...</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<!-- TAB 4: Advance Screener -->
<div id="advanceScreenerTab" class="tab-content">
  <div class="panel">
    <h2>⚡ Advance NSE Futures Dow Theory Screener</h2>
    <p class="copy">यह स्कैनर स्वचालित रूप से सभी NSE फ्यूचर्स स्टॉक्स पर Dow Theory (HH, HL, LL, LH) और Pullback Confirmed संकेत पहचानता है।</p>
    
    <div style="display:flex;gap:10px;flex-wrap:wrap;margin:15px 0;align-items:center;">
      <button type="button" id="btnScan" onclick="triggerRescan()">अभी स्कैन शुरू करें</button>
      <input id="screenerSearch" placeholder="स्टॉक खोजें..." oninput="filterScreenerTable()" style="flex:1;min-width:200px;padding:12px;font-size:15px;">
      <button type="button" onclick="downloadCsv()" style="background:#3977c3;color:#fff;">CSV डाउनलोड करें</button>
    </div>

    <div class="progress-track"><div id="progBar" class="progress-fill"></div></div>
    <div id="screenerStatus" class="hint" style="margin-bottom:15px;">स्टेatus: कनेक्ट हो रहा है...</div>

    <h3 style="margin-top:20px;">★ Confirmed Entry Signals (BUY/SELL)</h3>
    <div class="saved-table-wrap">
      <table class="saved-table">
        <thead><tr><th>स्टॉक</th><th>ट्रेंड</th><th>वर्तमान भाव</th><th>Entry</th><th>Stop Loss</th><th>Target</th><th>Signal</th></tr></thead>
        <tbody id="confirmedBody"><tr><td colspan="7" class="empty-row">कोई सिग्नल उपलब्ध नहीं।</td></tr></tbody>
      </table>
    </div>

    <h3 style="margin-top:25px;">📋 सभी NSE फ्यूचर्स स्टॉक्स की सूची</h3>
    <div class="saved-table-wrap">
      <table class="saved-table">
        <thead><tr><th>#</th><th>स्टॉक</th><th>भाव</th><th>ट्रेंड</th><th>HH / LH</th><th>HL / LL</th><th>Entry</th><th>SL</th><th>Target</th><th>स्थिति</th></tr></thead>
        <tbody id="allStocksBody"><tr><td colspan="10" class="empty-row">डेटा लोड हो रहा है...</td></tr></tbody>
      </table>
    </div>
  </div>
</div>

<footer>MarkeTGyan Professional Stock Screener • शैक्षिक उद्देश्य मात्र; SEBI निवेश सलाह नहीं है।</footer>
</main>

<script>
const $=id=>document.getElementById(id);
function fmt(v, suffix=''){if(v===null||v===undefined||!Number.isFinite(Number(v)))return 'उपलब्ध नहीं';return Number(v).toLocaleString('en-IN',{maximumFractionDigits:2})+suffix}
function metric(label,value,cls=''){return `<div class="metric"><small>${label}</small><strong class="${cls}">${value}</strong></div>`}
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
function table(rows){return '<table><tbody>'+rows.map(r=>'<tr><th>'+esc(r[0])+'</th><td>'+esc(r[1])+'</td></tr>').join('')+'</tbody></table>'}

function switchTab(tabId){
 document.querySelectorAll('.tab-content').forEach(el=>el.classList.toggle('active',el.id===tabId));
 document.querySelectorAll('[data-tab-target]').forEach(el=>el.classList.toggle('active',el.getAttribute('data-tab-target')===tabId));
 if(tabId==='savedTab')renderSavedStocks();
 if(tabId==='gainersLosersTab')fetchGainersLosers();
}
document.querySelectorAll('[data-tab-target]').forEach(btn=>btn.addEventListener('click',()=>switchTab(btn.getAttribute('data-tab-target'))));

// Saved Stocks Logic
const SAVED_STOCKS_KEY='markeTGyan_saved_stock_analyses_v1';
function readSavedStocks(){try{const raw=localStorage.getItem(SAVED_STOCKS_KEY);const p=raw?JSON.parse(raw):[];return Array.isArray(p)?p:[];}catch(e){return [];}}
function writeSavedStocks(items){try{localStorage.setItem(SAVED_STOCKS_KEY,JSON.stringify(items));return true;}catch(e){return false;}}
function renderSavedStocks(){
 const items=readSavedStocks();
 if(!items.length){$('savedStocksBody').innerHTML='<tr><td colspan="10" class="empty-row">अभी कोई स्टॉक सेव नहीं है।</td></tr>';return;}
 $('savedStocksBody').innerHTML=items.map(x=>'<tr>'+
 '<td><b>'+esc(x.company||x.symbol)+'</b><br><span class="tag">'+esc(x.symbol)+'</span></td>'+
 '<td>'+esc(fmt(x.price,' '+(x.currency||'')))+'</td>'+
 '<td class="'+(Number(x.change_pct)>0?'green':Number(x.change_pct)<0?'red':'')+'">'+esc(fmt(x.change_pct,'%'))+'</td>'+
 '<td>'+esc(fmt(x.support))+'</td><td>'+esc(fmt(x.resistance))+'</td>'+
 '<td>'+esc(fmt(x.support_1hr))+'</td><td>'+esc(fmt(x.resistance_1hr))+'</td>'+
 '<td>'+esc(x.score_label||'न/अ')+'</td><td>'+esc(x.as_of||'न/अ')+'</td>'+
 '<td><div class="saved-actions"><button type="button" class="small-btn open-btn" data-open-stock="'+esc(x.symbol)+'">देखें</button><button type="button" class="small-btn delete-btn" data-delete-stock="'+esc(x.symbol)+'">Delete</button></div></td></tr>').join('');
}
function saveStockAnalysis(d){
 const symbol=String(d.symbol||$('symbol').value||'').trim().toUpperCase();if(!symbol)return;
 const items=readSavedStocks();
 const entry={symbol,company:d.company||symbol,price:d.price,change_pct:d.change_pct,support:d.support,resistance:d.resistance,support_1hr:d.support_1hr,resistance_1hr:d.resistance_1hr,score_label:d.score_label,currency:d.currency,as_of:d.as_of};
 const idx=items.findIndex(x=>String(x.symbol||'').toUpperCase()===symbol);
 if(idx>=0)items.splice(idx,1);
 items.unshift(entry);
 if(items.length>100)items.length=100;
 writeSavedStocks(items);renderSavedStocks();
}
$('savedStocksBody').addEventListener('click',e=>{
 const del=e.target.closest('[data-delete-stock]');
 if(del){const sym=del.getAttribute('data-delete-stock');writeSavedStocks(readSavedStocks().filter(x=>String(x.symbol||'').toUpperCase()!==sym.toUpperCase()));renderSavedStocks();return;}
 const open=e.target.closest('[data-open-stock]');
 if(open){$('symbol').value=open.getAttribute('data-open-stock');switchTab('searchTab');$('stockForm').requestSubmit();}
});
renderSavedStocks();

function tradePlanTable(support, resistance, currency, timeframe){
 const s=Number(support), r=Number(resistance), cur=currency||'';
 if(!Number.isFinite(s)||!Number.isFinite(r)||s<=0||r<=s) return '<p class="error">वैध सपोर्ट/रेजिस्टेंस उपलब्ध नहीं हैं।</p>';
 const range=r-s, fmtP=v=>fmt(v,' '+cur);
 const rows=[
  ['BUY (Support से)',fmtP(s),fmtP(s-range*0.25),fmtP(s+range*0.5),fmtP(r)],
  ['SELL (Resistance से)',fmtP(r),fmtP(r+range*0.25),fmtP(r-range*0.5),fmtP(s)]
 ];
 return '<div style="overflow-x:auto"><table><thead><tr><th>सेटअप</th><th>Entry लेवल</th><th>Stop Loss</th><th>Target 1</th><th>Target 2</th></tr></thead><tbody>'+rows.map(row=>'<tr>'+row.map((x,i)=>'<td>'+(i===0?'<b>'+esc(x)+'</b>':esc(x))+'</td>').join('')+'</tr>').join('')+'</tbody></table></div>';
}

// Single Stock Form Submit
$('stockForm').addEventListener('submit',async e=>{
 e.preventDefault(); const sym=$('symbol').value.trim(); if(!sym)return;
 $('go').disabled=true;$('go').textContent='डेटा जाँच रहा है…';$('results').classList.add('hidden');$('status').classList.remove('hidden');$('status').innerHTML='DATA FETCHING...';
 try{
  const r=await fetch('/api/analyze?symbol='+encodeURIComponent(sym));const d=await r.json();
  if(!r.ok)throw new Error(d.error||'डेटा प्राप्त नहीं हुआ');
  saveStockAnalysis(d);
  $('status').classList.add('hidden');$('results').classList.remove('hidden');
  $('company').innerHTML=`<h2>${esc(d.company)} <span class="tag">${esc(d.symbol)}</span></h2><div class="sub">डेटा समय: ${esc(d.as_of)}</div>`;
  $('metrics').innerHTML=[
   metric('अंतिम बंद भाव',fmt(d.price,d.currency==='INR'?' ₹':' '+(d.currency||''))),
   metric('आज का बदलाव',fmt(d.change_pct,'%'),d.change_pct>=0?'green':'red'),
   metric('RSI (14)',fmt(d.rsi),d.rsi<30?'gold':d.rsi>70?'red':''),
   metric('ट्रेंड स्कोर',esc(d.score_label),d.score>=2?'green':d.score<=-2?'red':'gold'),
   metric('52-सप्ताह उच्च',fmt(d.high52,' '+(d.currency||''))),
   metric('52-सप्ताह निम्न',fmt(d.low52,' '+(d.currency||'')))
  ].join('');
  $('trend').innerHTML=`<p><b>तकनीकी स्थिति:</b> ${esc(d.trend)}</p><p><b>मूविंग एवरेज:</b> ${esc(d.ma_comment)}</p><p><b>RSI:</b> ${esc(d.rsi_comment)}</p><p><b>MACD:</b> ${esc(d.macd_comment)}</p>`;
  $('dailyLevels').innerHTML=table([['सपोर्ट', fmt(d.support,' '+(d.currency||''))],['रेजिस्टेंस', fmt(d.resistance,' '+(d.currency||''))],['20 EMA', fmt(d.ema20,' '+(d.currency||''))],['50 SMA', fmt(d.sma50,' '+(d.currency||''))],['200 SMA', fmt(d.sma200,' '+(d.currency||''))]]);
  $('hourlyLevels').innerHTML=table([['सपोर्ट (1HR)', fmt(d.support_1hr,' '+(d.currency||''))],['रेजिस्टेंस (1HR)', fmt(d.resistance_1hr,' '+(d.currency||''))]]);
  $('dailyTradePlan').innerHTML=tradePlanTable(d.support,d.resistance,d.currency,'Daily');
  $('hourlyTradePlan').innerHTML=tradePlanTable(d.support_1hr,d.resistance_1hr,d.currency,'1HR');
  $('fundamentals').innerHTML=table([['Market Cap', d.market_cap],['Total Debt', d.total_debt],['Debt-to-Equity', d.debt_equity],['P/E', d.pe],['Price/Book (P/B)', d.pb],['Dividend Yield', d.dividend_yield],['ROE & ROCE', `ROE: ${d.roe} | ROCE: ${d.roce}`],['Sales Growth (3Y)', d.sales_growth],['Profit Growth (3Y)', d.profit_growth],['Profit Margin', d.profit_margin],['Debtor Days', d.debtor_days],['Business Summary', d.summary]]);
  $('shareholding').innerHTML=table([['Promoter Holding', d.promoter_holding],['Promoter Pledge', d.promoter_pledge],['FII Holding', d.fii_holding],['DII Holding', d.dii_holding],['Public Holding', d.public_holding]]);
  $('verdict').innerHTML=`<p><b>संकेत:</b> <span class="${d.score>=2?'green':d.score<=-2?'red':'gold'}">${esc(d.score_label)}</span></p><p>${esc(d.verdict)}</p>`;
  $('risks').innerHTML='<ul>'+d.risks.map(x=>'<li>'+esc(x)+'</li>').join('')+'</ul>';
  $('analysisNote').innerHTML=`<p><b>समग्र सारांश:</b> बंद भाव ${fmt(d.price)} है। ${esc(d.verdict)}</p>`;
 }catch(err){$('status').innerHTML='<div class="error">विश्लेषण नहीं हो पाया: '+esc(err.message)+'</div>'}
 finally{$('go').disabled=false;$('go').textContent='विश्लेषण करें ↗'}
});

// Top Gainers / Losers Fetcher
async function fetchGainersLosers(){
 $('glStatus').textContent = 'डेटा लोड हो रहा है...';
 try {
  const r = await fetch('/api/gainers-losers');
  const d = await r.json();
  if(!r.ok || d.error) throw new Error(d.error || 'डेटा प्राप्त नहीं हुआ');
  
  const renderRows = (list) => {
   if(!list || !list.length) return '<tr><td colspan="7" class="empty-row">डेटा उपलब्ध नहीं है।</td></tr>';
   return list.map(x=>`<tr>
    <td><b>${esc(x.stock)}</b></td>
    <td>${fmt(x.current)}</td>
    <td class="${x.p_change>=0?'green':'red'}"><b>${x.p_change>=0?'+':''}${fmt(x.p_change)}%</b></td>
    <td>${esc(x.daily_sup.join(', ') || '-')}</td>
    <td>${esc(x.daily_res.join(', ') || '-')}</td>
    <td>${esc(x.hourly_sup.join(', ') || '-')}</td>
    <td>${esc(x.hourly_res.join(', ') || '-')}</td>
   </tr>`).join('');
  };
  $('gainersBody').innerHTML = renderRows(d.gainers);
  $('losersBody').innerHTML = renderRows(d.losers);
  $('glStatus').textContent = 'अंतिम अपडेट: ' + new Date().toLocaleTimeString();
 } catch(e){
  $('glStatus').textContent = 'त्रुटि: ' + e.message;
 }
}

// Advance Screener Dashboard Data Poll
let cachedScreenerData = [];
async function fetchScreenerStatus(){
 try {
  const r = await fetch('/api/status');
  const d = await r.json();
  
  $('statStatus') ? $('statStatus').textContent = d.status : null;
  $('progBar').style.width = (d.progress || 0) + '%';
  $('screenerStatus').innerHTML = `<b>स्थिति:</b> ${esc(d.status)} | <b>संदेश:</b> ${esc(d.message)} | <b>प्रगति:</b> ${d.completed}/${d.total} (${d.progress}%)`;
  
  cachedScreenerData = d.results || [];
  renderConfirmedSignals(d.confirmed || []);
  renderAllScreenerStocks(cachedScreenerData);
 } catch(e){}
}

function renderConfirmedSignals(list){
 const body = $('confirmedBody');
 if(!list.length){body.innerHTML='<tr><td colspan="7" class="empty-row">कोई कन्फर्म सिग्नल नहीं मिला।</td></tr>';return;}
 body.innerHTML = list.map(r=>`<tr>
  <td><b>${esc(r.stock)}</b></td>
  <td class="${r.trend==='UPTREND'?'green':'red'}">${esc(r.trend)}</td>
  <td>${fmt(r.current)}</td>
  <td>${fmt(r.entry)}</td>
  <td class="red">${fmt(r.sl)}</td>
  <td class="green">${fmt(r.target)}</td>
  <td><span class="tag" style="background:${r.signal==='BUY VALID'?'#e7f6ee':'#fff0f0'};color:${r.signal==='BUY VALID'?'#18794e':'#c2414b'}">${esc(r.signal)}</span></td>
 </tr>`).join('');
}

function renderAllScreenerStocks(list){
 const body = $('allStocksBody');
 if(!list.length){body.innerHTML='<tr><td colspan="10" class="empty-row">डेटा उपलब्ध नहीं है।</td></tr>';return;}
 body.innerHTML = list.map((r, i)=>`<tr>
  <td>${i+1}</td>
  <td><b>${esc(r.stock)}</b></td>
  <td>${fmt(r.current)}</td>
  <td class="${r.trend==='UPTREND'?'green':r.trend==='DOWNTREND'?'red':'sideways'}">${esc(r.trend)}</td>
  <td>${fmt(r.hh)}</td>
  <td>${fmt(r.hl)}</td>
  <td>${fmt(r.entry)}</td>
  <td>${fmt(r.sl)}</td>
  <td>${fmt(r.target)}</td>
  <td>${esc(r.status)}</td>
 </tr>`).join('');
}

function filterScreenerTable(){
 const q = $('screenerSearch').value.toUpperCase();
 const filtered = cachedScreenerData.filter(r => r.stock.toUpperCase().includes(q));
 const body = $('allStocksBody');
 if(!filtered.length){body.innerHTML='<tr><td colspan="10" class="empty-row">कोई स्टॉक नहीं मिला।</td></tr>';return;}
 body.innerHTML = filtered.map((r, i)=>`<tr>
  <td>${i+1}</td>
  <td><b>${esc(r.stock)}</b></td>
  <td>${fmt(r.current)}</td>
  <td class="${r.trend==='UPTREND'?'green':r.trend==='DOWNTREND'?'red':'sideways'}">${esc(r.trend)}</td>
  <td>${fmt(r.hh)}</td>
  <td>${fmt(r.hl)}</td>
  <td>${fmt(r.entry)}</td>
  <td>${fmt(r.sl)}</td>
  <td>${fmt(r.target)}</td>
  <td>${esc(r.status)}</td>
 </tr>`).join('');
}

async function triggerRescan(){
 try {
  const r = await fetch('/api/rescan', {method:'POST'});
  const d = await r.json();
  if(!d.success) alert(d.message);
  fetchScreenerStatus();
 } catch(e){alert('स्कैन शुरू करने में विफल: '+e); }
}

function downloadCsv(){ window.location.href='/download'; }

// Polling for Screener status
setInterval(fetchScreenerStatus, 4000);
fetchScreenerStatus();
</script>
</body>
</html>
"""

# =====================================================
# BACKEND LOGIC & API ENDPOINTS
# =====================================================
def clean_num(value):
    try:
        if value is None or pd.isna(value): return None
        value = float(str(value).replace(',', ''))
        return round(value, 2) if np.isfinite(value) else None
    except Exception: return None

def sanitize_json_value(value):
    try:
        if value is None: return None
        if isinstance(value, dict): return {str(k): sanitize_json_value(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)): return [sanitize_json_value(v) for v in value]
        if isinstance(value, (np.integer,)): return int(value)
        if isinstance(value, (np.floating, float)):
            n = float(value)
            return n if math.isfinite(n) else None
        if isinstance(value, (np.bool_,)): return bool(value)
        if pd.isna(value): return None
    except (TypeError, ValueError): pass
    return value

def safe_jsonify(value, *args, **kwargs):
    return jsonify(sanitize_json_value(value), *args, **kwargs)

def human_large(value, currency=""):
    try:
        if value is None or pd.isna(value): return "उपलब्ध नहीं"
        value = float(value)
        for unit, div in [("T",1e12),("B",1e9),("Cr",1e7),("M",1e6),("L",1e5)]:
            if abs(value) >= div: return f"{currency}{value/div:,.2f} {unit}"
        return f"{currency}{value:,.2f}"
    except Exception: return "उपलब्ध नहीं"

def calculate_rsi(data, window=14):
    if len(data) <= window: return None
    delta = data.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=window).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=window).mean()
    if loss.iloc[-1] == 0: return 100
    rs = gain / loss
    return 100 - (100 / (1 + rs.iloc[-1]))

def calculate_macd(data, slow=26, fast=12, signal=9):
    if len(data) <= slow + signal: return None
    exp1 = data.ewm(span=fast, adjust=False).mean()
    exp2 = data.ewm(span=slow, adjust=False).mean()
    macd = exp1 - exp2
    signal_line = macd.ewm(span=signal, adjust=False).mean()
    histogram = macd - signal_line
    return macd.iloc[-1], signal_line.iloc[-1], histogram.iloc[-1]

def get_sr_levels(data, type='daily'):
    if data is None or data.empty: return None, None
    if type == 'daily':
        last_closes = data["Close"].tail(20)
        s = clean_num(last_closes.iloc[:-1].min())
        r = clean_num(last_closes.iloc[:-1].max())
    else:
        s = clean_num(data['Low'].min())
        r = clean_num(data['High'].max())
    return s, r

def fetch_fundamental_data(symbol):
    clean_sym = symbol.replace(".NS", "").replace(".BO", "").strip().upper()
    data = {}
    try:
        resp = session.get(f"https://www.screener.in/company/{clean_sym}/", headers=get_headers(), timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for item in soup.select("ul#top-ratios li"):
                name = item.select_one(".name")
                val = item.select_one(".number")
                if name and val:
                    data[name.text.strip().lower().replace(":", "").strip()] = val.text.strip()
            about_div = soup.find('div', class_='about')
            if about_div: data['summary'] = about_div.text.strip()
    except Exception: pass
    return data

def analyze_single_stock(raw):
    symbol = raw.strip().upper().replace(" ", "")
    if not symbol: raise ValueError("कृपया स्टॉक का नाम लिखें।")
    ticker_symbol = symbol + ".NS" if "." not in symbol and not symbol.startswith("^") else symbol
    ticker = yf.Ticker(ticker_symbol)
    
    hist = ticker.history(period="1y", interval="1d", auto_adjust=True)
    if hist is None or hist.empty or len(hist) < 30: raise ValueError(f"{symbol} का पर्याप्त डेटा नहीं मिला।")
    
    hist_1hr = ticker.history(period="5d", interval="60m")
    info = ticker.get_info() or {}
    sc = fetch_fundamental_data(symbol)
    
    close = hist["Close"].dropna()
    price = clean_num(close.iloc[-1])
    prev = clean_num(close.iloc[-2]) if len(close) > 1 else price
    change_pct = ((price / prev) - 1) * 100 if prev else 0.0
    
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    sma200 = close.rolling(200).mean().iloc[-1]
    rsi = calculate_rsi(close)
    macd_data = calculate_macd(close)
    
    support, resistance = get_sr_levels(hist, type='daily')
    support_1hr, resistance_1hr = get_sr_levels(hist_1hr, type='hourly')
    
    score = 0
    ma_comment = "मूविंग एवरेज संतुलित"
    rsi_comment = "RSI संतुलित"
    macd_comment = "MACD सामान्य"
    
    if price and ema20 and sma50 and sma200:
        if price > ema20 and price > sma50 and price > sma200: score += 2; ma_comment = "भाव सभी प्रमुख MAs के ऊपर है।"
        elif price > sma200: score += 1; ma_comment = "भाव 200-SMA के ऊपर है।"
        else: score -= 1; ma_comment = "भाव 200-SMA के नीचे है।"

    if rsi:
        if rsi > 70: score -= 1; rsi_comment = f"RSI {fmt(rsi)} (ओवरबॉट)"
        elif rsi < 30: score += 1; rsi_comment = f"RSI {fmt(rsi)} (ओवरसोल्ड)"

    return {
        "symbol": ticker_symbol.replace(".NS", ""),
        "company": info.get("longName") or symbol,
        "as_of": str(hist.index[-1].date()),
        "price": price,
        "change_pct": change_pct,
        "currency": info.get("currency", "INR"),
        "rsi": rsi,
        "score": score,
        "score_label": "मजबूत सकारात्मक" if score >= 2 else "सकारात्मक" if score >= 1 else "कमजोर",
        "trend": "तेजी का ट्रेंड" if score >= 1 else "कमजोरी",
        "ema20": clean_num(ema20), "sma50": clean_num(sma50), "sma200": clean_num(sma200),
        "support": support, "resistance": resistance,
        "support_1hr": support_1hr, "resistance_1hr": resistance_1hr,
        "high52": clean_num(info.get("fiftyTwoWeekHigh")), "low52": clean_num(info.get("fiftyTwoWeekLow")),
        "ma_comment": ma_comment, "rsi_comment": rsi_comment, "macd_comment": macd_comment,
        "market_cap": human_large(clean_num(info.get("marketCap"))),
        "total_debt": human_large(clean_num(info.get("totalDebt"))),
        "debt_equity": sc.get("debt to equity", clean_num(info.get("debtToEquity", 'न/अ'))),
        "pe": sc.get("stock p/e", clean_num(info.get("trailingPE", 'न/अ'))),
        "pb": sc.get("price to book", clean_num(info.get("priceToBook", 'न/अ'))),
        "dividend_yield": f"{clean_num(info.get('dividendYield', 0)*100)}%",
        "roe": f"{clean_num(info.get('returnOnEquity', 0)*100)}%",
        "roce": 'न/अ',
        "sales_growth": 'न/अ',
        "profit_growth": 'न/अ',
        "profit_margin": 'न/अ',
        "debtor_days": 'न/अ',
        "promoter_holding": f"{clean_num(info.get('heldPercentInsiders', 0)*100)}%",
        "promoter_pledge": '0.00%',
        "fii_holding": f"{clean_num(info.get('heldPercentInstitutions', 0)*100)}%",
        "dii_holding": 'न/अ',
        "public_holding": 'न/अ',
        "summary": sc.get("summary", info.get("longBusinessSummary", "")),
        "verdict": f"{symbol} तकनीकी रूप से विश्लेषित है। सपोर्ट और रेजिस्टेंस का पालन करें।",
        "risks": ["बाजार जोखिम और अस्थिरता मौजूद है।"]
    }

# =====================================================
# DOW THEORY & SCANNER LOGIC FOR ADVANCED SCREENER & G/L
# =====================================================
def get_nse_futures_list():
    # Default trusted list of top NSE Futures stocks to guarantee fast and smooth execution
    return [
        "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "ITC",
        "AXISBANK", "ASIANPAINT", "BAJFINANCE", "MARUTI", "SUNPHARMA", "TITAN", "ULTRACEMCO", "WIPRO", "HCLTECH",
        "POWERGRID", "NTPC", "TATAMOTORS", "TATASTEEL", "JSWSTEEL", "GRASIM", "TECHM", "ONGC", "COALINDIA", "ADANIENT",
        "ADANIPORTS", "BAJAJ-AUTO", "BRITANNIA", "CIPLA", "DIVISLAB", "EICHERMOT", "HINDALCO", "HEROMOTOCO", "INDUSINDBK",
        "M&M", "NESTLEIND", "BPCL", "SBILIFE", "HDFCLIFE", "DRREDDY", "IOC", "SBICARD", "MUTHOOTFIN", "HAL", "BEL"
    ], "STATIC NSE FUTURES UNIVERSE"

def fetch_stock_data(stock):
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{stock}.NS"
    params = {"range": PERIOD, "interval": TIMEFRAME}
    headers = {"User-Agent": "Mozilla/5.0"}
    for _ in range(3):
        try:
            wait_for_request_slot()
            res = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            if res.status_code == 200:
                raw = res.json()["chart"]["result"]
                if raw:
                    data = raw[0]
                    quote = data["indicators"]["quote"][0]
                    df = pd.DataFrame({
                        "Date": pd.to_datetime(data["timestamp"], unit="s", utc=True),
                        "Open": quote["open"], "High": quote["high"], "Low": quote["low"],
                        "Close": quote["close"], "Volume": quote["volume"]
                    }).dropna().reset_index(drop=True)
                    df['Date'] = df['Date'].dt.tz_convert('Asia/Kolkata')
                    return df
        except Exception: time.sleep(1)
    raise Exception("Yahoo data fetch failed")

def identify_swings(df):
    highs, lows = [], []
    for i in range(PIVOT_LEFT, len(df) - PIVOT_RIGHT):
        ch, cl = float(df.loc[i, "High"]), float(df.loc[i, "Low"])
        if ch == df.loc[i-PIVOT_LEFT:i+PIVOT_RIGHT, "High"].max():
            highs.append({"index": i, "price": ch})
        if cl == df.loc[i-PIVOT_LEFT:i+PIVOT_RIGHT, "Low"].min():
            lows.append({"index": i, "price": cl})
    for i in range(1, len(highs)):
        highs[i]["label"] = "HH" if highs[i]["price"] > highs[i-1]["price"] else "LH"
    for i in range(1, len(lows)):
        lows[i]["label"] = "HL" if lows[i]["price"] > lows[i-1]["price"] else "LL"
    return highs, lows

def get_pivots(series, p_left, p_right, is_high):
    s = series.reset_index(drop=True)
    res = []
    for i in range(p_left, len(s) - p_right):
        win = s.iloc[i - p_left : i + p_right + 1]
        val = s.iloc[i]
        if is_high and val == win.max(): res.append(val)
        elif not is_high and val == win.min(): res.append(val)
    return sorted(list(set(res)), reverse=is_high)[:2]

def analyze_futures_stock(stock):
    df = fetch_stock_data(stock)
    last = df.iloc[-1]
    current = float(last["Close"])
    
    prev_days = df[df['Date'].dt.date < last['Date'].date()]
    prev_close = float(prev_days.iloc[-1]['Close']) if not prev_days.empty else float(df.iloc[0]['Open'])
    p_change = ((current - prev_close) / prev_close) * 100 if prev_close else 0

    df_d = df.set_index('Date').resample('D').agg({'High':'max', 'Low':'min', 'Close':'last'}).dropna()
    d_res = get_pivots(df_d['High'], 2, 2, True)
    d_sup = get_pivots(df_d['Low'], 2, 2, False)

    highs, lows = identify_swings(df)
    h_res = get_pivots(df['High'], PIVOT_LEFT, PIVOT_RIGHT, True)
    h_sup = get_pivots(df['Low'], PIVOT_LEFT, PIVOT_RIGHT, False)

    res = {
        "stock": stock,
        "current": current,
        "p_change": p_change,
        "daily_res": d_res, "daily_sup": d_sup,
        "hourly_res": h_res, "hourly_sup": h_sup,
        "trend": "SIDEWAYS", "hh": None, "hl": None, "ll": None, "lh": None,
        "entry": None, "sl": None, "target": None, "signal": "WAIT",
        "status": "स्ट्रक्चर स्पष्ट नहीं", "updated": last["Date"].strftime("%Y-%m-%d %H:%M")
    }

    if len(highs) < 2 or len(lows) < 2: return res
    lh_item, ll_item = highs[-1], lows[-1]
    res["hh"] = lh_item["price"] if lh_item.get("label") == "HH" else None
    res["lh"] = lh_item["price"] if lh_item.get("label") == "LH" else None
    res["hl"] = ll_item["price"] if ll_item.get("label") == "HL" else None
    res["ll"] = ll_item["price"] if ll_item.get("label") == "LL" else None

    if lh_item.get("label") == "HH" and ll_item.get("label") == "HL":
        res["trend"] = "UPTREND"
        entry = ll_item["price"]
        sl = entry * (1 - SL_BUFFER_PERCENT/100)
        target = entry + ((entry - sl) * RISK_REWARD)
        res.update({"entry": entry, "sl": sl, "target": target, "signal": "BUY VALID", "status": "Uptrend Pullback Confirmed"})
    elif ll_item.get("label") == "LL" and lh_item.get("label") == "LH":
        res["trend"] = "DOWNTREND"
        entry = lh_item["price"]
        sl = entry * (1 + SL_BUFFER_PERCENT/100)
        target = entry - ((sl - entry) * RISK_REWARD)
        res.update({"entry": entry, "sl": sl, "target": target, "signal": "SELL VALID", "status": "Downtrend Pullback Confirmed"})
    return res

def scan_all_stocks():
    with LOCK:
        if STATE["running"]: return
        STATE["running"] = True
        stocks, source = get_nse_futures_list()
        STATE["stocks"] = stocks
        STATE["source"] = source
        STATE["status"] = "SCANNING"
        STATE["completed"] = 0
        STATE["errors"] = 0
        STATE["progress"] = 0
        STATE["results"] = []
        STATE["confirmed"] = []

    all_res = []
    total = len(stocks)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as exe:
        futures = {exe.submit(analyze_futures_stock, s): s for s in stocks}
        for fut in as_completed(futures):
            s = futures[fut]
            try:
                all_res.append(fut.result())
            except Exception:
                all_res.append({"stock": s, "current": 0, "p_change": 0, "daily_res":[], "daily_sup":[], "hourly_res":[], "hourly_sup":[], "trend":"ERROR", "status":"डेटा त्रुटि"})
            with LOCK:
                STATE["completed"] += 1
                STATE["progress"] = round((STATE["completed"]/total)*100, 1)

    valid = [r for r in all_res if r["trend"] != "ERROR"]
    valid.sort(key=lambda x: x["p_change"], reverse=True)
    gainers = valid[:5]
    losers = valid[-5:]
    losers.reverse()

    with LOCK:
        STATE["results"] = all_res
        STATE["confirmed"] = [r for r in all_res if r["signal"] in ["BUY VALID", "SELL VALID"]]
        STATE["gainers"] = gainers
        STATE["losers"] = losers
        STATE["status"] = "COMPLETED"
        STATE["message"] = "स्कैन सफलताપूर्वक संपन्न हुआ।"
        STATE["last_scan"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
        STATE["running"] = False

@app.route("/")
def home():
    return render_template_string(PAGE)

@app.route("/api/analyze")
def api_analyze():
    symbol = request.args.get("symbol", "")
    try: return safe_jsonify(analyze_single_stock(symbol))
    except Exception as e: return safe_jsonify({"error": str(e)}), 400

@app.route("/api/gainers-losers")
def api_gainers_losers():
    with LOCK:
        return safe_jsonify({"gainers": STATE["gainers"], "losers": STATE["losers"]})

@app.route("/api/status")
def api_status():
    with LOCK:
        return safe_jsonify(STATE)

@app.route("/api/rescan", methods=["POST"])
def api_rescan():
    with LOCK:
        if STATE["running"]: return jsonify({"success": False, "message": "स्कैन पहले से चल रहा है।"})
    threading.Thread(target=scan_all_stocks, daemon=True).start()
    return jsonify({"success": True, "message": "नया स्कैन शुरू हो गया है।"})

@app.route("/download")
def download():
    if not os.path.exists(REPORT_FILE): return "रिपोर्ट उपलब्ध नहीं है।", 404
    return send_file(REPORT_FILE, as_attachment=True)

if __name__ == "__main__":
    threading.Thread(target=scan_all_stocks, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)