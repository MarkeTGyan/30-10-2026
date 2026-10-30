import os
import math
import concurrent.futures
import time
import random
import re
import json
import threading
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template_string
import yfinance as yf
import pandas as pd
import numpy as np
import requests
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
# STOCK UNIVERSE (Alphabetically Sorted)
# =====================================================
RAW_STOCK_UNIVERSE = [
    "ABB.NS", "FORCEMOT.NS", "BAJFINANCE.NS", "BEL.NS", "BLUESTARCO.NS", "CIPLA.NS", "CGPOWER.NS", "DABUR.NS", "BHEL.NS", "HINDPETRO.NS", "SBIN.NS", "SAIL.NS", "TITAN.NS", "DRREDDY.NS", "GODFRYPHLP.NS", "HDFCBANK.NS", "HEROMOTOCO.NS", "HINDZINC.NS", "INFY.NS", "JSWSTEEL.NS", "KOTAKBANK.NS", "TRENT.NS", "LICHSGFIN.NS", "LUPIN.NS", "MFSL.NS", "VEDL.NS", "GRASIM.NS", "ONGC.NS", "RELIANCE.NS", "PIDILITIND.NS", "PATANJALI.NS", "SHREECEM.NS", "TATAPOWER.NS", "TATAELXSI.NS", "TORNTPHARM.NS", "AMBUJACEM.NS", "HINDALCO.NS", "FEDERALBNK.NS", "TATASTEEL.NS", "ASHOKLEY.NS", "CUMMINSIND.NS", "BAJAJHLDNG.NS", "BHARATFORG.NS", "LT.NS", "M&M.NS", "BOSCHLTD.NS", "BPCL.NS", "SIEMENS.NS", "TMPV.NS", "VOLTAS.NS", "HINDUNILVR.NS", "NESTLEIND.NS", "TATACONSUM.NS", "ASIANPAINT.NS", "BRITANNIA.NS", "COLPAL.NS", "INDHOTEL.NS", "ITC.NS", "PHOENIXLTD.NS", "SRF.NS", "EICHERMOT.NS", "WIPRO.NS", "APOLLOHOSP.NS", "SUPREMEIND.NS", "SHRIRAMFIN.NS", "CHOLAFIN.NS", "UPL.NS", "ADANIENT.NS", "MOTHERSON.NS", "HAVELLS.NS", "KEI.NS", "GVT&D.NS", "PIIND.NS", "SUNPHARMA.NS", "AUROPHARMA.NS", "MPHASIS.NS", "NMDC.NS", "IOC.NS", "MANAPPURAM.NS", "CONCOR.NS", "MARICO.NS", "BANKBARODA.NS", "BANKINDIA.NS", "GAIL.NS", "ICICIBANK.NS", "INDUSINDBK.NS", "AXISBANK.NS", "NATIONALUM.NS", "HCLTECH.NS", "JINDALSTEL.NS", "GLENMARK.NS", "ZYDUSLIFE.NS", "TVSMOTOR.NS", "GODREJCP.NS", "UNITDSPR.NS", "BHARTIARTL.NS", "PNB.NS", "OFSS.NS", "UNIONBANK.NS", "CANBK.NS", "DIVISLAB.NS", "RADICO.NS", "MARUTI.NS", "PETRONET.NS", "BIOCON.NS", "MAHABANK.NS", "ULTRACEMCO.NS", "UNOMINDA.NS", "TCS.NS", "COFORGE.NS", "NTPC.NS", "YESBANK.NS", "SUZLON.NS", "SOLARINDS.NS", "GMRAIRPORT.NS", "TECHM.NS", "NAUKRI.NS", "PFC.NS", "INDIANB.NS", "IDEA.NS", "PAGEIND.NS", "ASTRAL.NS", "FORTIS.NS", "DLF.NS", "MOTILALOFS.NS", "POWERGRID.NS", "ADANIPORTS.NS", "RECLTD.NS", "BAJAJ-AUTO.NS", "BAJAJFINSV.NS", "ADANIPOWER.NS", "NHPC.NS", "OIL.NS", "JSWENERGY.NS", "GODREJPROP.NS", "JUBLFOOD.NS", "PERSISTENT.NS", "OBEROIRLTY.NS", "PRESTIGE.NS", "COALINDIA.NS", "MUTHOOTFIN.NS", "LTF.NS", "PGEL.NS", "APLAPOLLO.NS", "MCX.NS", "NBCC.NS", "INDUSTOWER.NS", "INOXWIND.NS", "ADANIENSOL.NS", "IDFCFIRSTB.NS", "INDIGO.NS", "ALKEM.NS", "CROMPTON.NS", "LTM.NS", "RBLBANK.NS", "ICICIPRULI.NS", "PNBHOUSING.NS", "VBL.NS", "LAURUSLABS.NS", "NIFTY.NS", "BANKNIFTY.NS", "BSE.NS", "DMART.NS", "AUBANK.NS", "CDSL.NS", "COCHINSHIP.NS", "DIXON.NS", "ABCAPITAL.NS", "SBILIFE.NS", "ICICIGI.NS", "IEX.NS", "TIINDIA.NS", "NAM-INDIA.NS", "HDFCLIFE.NS", "AMBER.NS", "BDL.NS", "BANDHANBNK.NS", "HAL.NS", "ADANIGREEN.NS", "HDFCAMC.NS", "RVNL.NS", "POLYCAB.NS", "KPITTECH.NS", "360ONE.NS", "UJJIVANSFB.NS", "SBICARD.NS", "POWERINDIA.NS", "MAXHEALTH.NS", "CAMS.NS", "ANGELONE.NS", "MAZDOCK.NS", "IRFC.NS", "KALYANKJIL.NS", "LODHA.NS", "SONACOMS.NS", "ETERNAL.NS", "NYKAA.NS", "POLICYBZR.NS", "PAYTM.NS", "ANANDRATHI.NS", "LICI.NS", "DELHIVERY.NS", "KAYNES.NS", "KFINTECH.NS", "MANKIND.NS", "JIOFIN.NS", "IREDA.NS", "PREMIERENE.NS", "HYUNDAI.NS", "WAAREEENER.NS", "SAGILITY.NS", "SWIGGY.NS", "VMM.NS", "ATHERENERG.NS", "ENRIN.NS"
]
STOCK_UNIVERSE = sorted(list(set(RAW_STOCK_UNIVERSE)))

# =====================================================
# HTML TEMPLATE WITH RESPONSIVE CSS
# =====================================================
PAGE = r"""
<!doctype html>
<html lang="hi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
<title>MarkeTGyan Professional Stock Screener</title>
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
h1{font-size:clamp(26px,4.3vw,42px);line-height:1.2;letter-spacing:-.4px;margin:12px 0 10px;color:#203c60}
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
.error{color:#b52d3a;white-space:pre-wrap;background:var(--soft-red);padding:12px;border-radius:10px}.hidden{display:none}.saved-table-wrap{overflow-x:auto;border-radius:12px;-webkit-overflow-scrolling:touch}.saved-table{min-width:1050px}.saved-actions{display:flex;gap:7px;flex-wrap:wrap}.small-btn{padding:8px 11px;font-size:13px;border-radius:9px;min-height:36px;box-shadow:none;white-space:nowrap}.delete-btn{background:#ffe2e4;color:#9d2631;border:1px solid #f6c4c8}.open-btn{background:#dff5e8;color:#176a43;border:1px solid #b9e6cc}.empty-row{text-align:center;color:var(--muted);padding:22px!important;background:#f8fbfe}
footer{color:#63768a;font-size:12px;line-height:1.7;padding:18px 0 30px}
.tab-buttons{display:flex;gap:10px;margin:8px 0 16px;border:0;padding:8px;background:#e5edf6;border-radius:16px;flex-wrap:wrap;box-shadow:inset 0 1px 3px rgba(40,70,105,.06)}
.tab-btn{flex:1 1 180px;background:transparent;color:#526a82;border:1px solid transparent;padding:12px 16px;border-radius:11px;cursor:pointer;font-weight:800;line-height:1.4;box-shadow:none;text-align:center;font-size:14px}
.tab-btn:hover{background:#f6faff;transform:none}.tab-btn.active{background:linear-gradient(135deg,#3c80c8,#2e69ad);color:#fff;border-color:#2e69ad;box-shadow:0 4px 12px rgba(46,105,173,.2)}
.tab-content{display:none}.tab-content.active{display:block;animation:fadeIn .22s ease-out}
.status-msg{font-size:15px;font-weight:bold;color:#986b08;margin-bottom:12px}.progress-track{width:100%;height:14px;background:#e7edf4;border:1px solid #d3dfeb;border-radius:99px;overflow:hidden;margin:8px 0 16px}.progress-fill{height:100%;width:0%;background:linear-gradient(90deg,#43b581,#168b8b);transition:width .25s ease}.progress-caption{font-size:13px;color:var(--muted);margin-bottom:12px}
.active-rank{display:inline-grid;place-items:center;min-width:32px;height:32px;border-radius:10px;background:linear-gradient(135deg,#e2eaff,#e5fff4);color:#344d9a;font-weight:900}
@keyframes fadeIn{from{opacity:.75;transform:translateY(3px)}to{opacity:1;transform:translateY(0)}}

@media(max-width:1024px){.saved-table{min-width:1000px}.metric strong{font-size:clamp(18px,2.4vw,23px)}}
@media(max-width:768px){
 main{padding:12px}header{padding:12px 0 8px}.panel{margin:12px 0;border-radius:15px;padding:14px}form{gap:9px}input{width:100%;min-width:0;flex-basis:100%;font-size:16px;padding:14px}form button{width:100%}.tab-buttons{display:grid;grid-template-columns:1fr;gap:8px;padding:7px}.tab-btn{width:100%;flex:auto;padding:13px 12px;font-size:15px}.grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.metric{padding:12px}.metric small{font-size:12px}.metric strong{font-size:19px}h2{font-size:19px}td,th{padding:10px 9px;font-size:14px}.saved-table{min-width:900px}.copy{font-size:15px}.tag{font-size:12px}footer{padding-bottom:20px}
}
@media(prefers-reduced-motion:reduce){html{scroll-behavior:auto}*,*::before,*::after{animation-duration:.01ms!important;transition-duration:.01ms!important}}
</style>
</head>
<body><main>
<header><div class="tag">MarkeTGyan Professional Screener</div>
<h1>Professional Stock Analyst & Screener</h1>
<div class="sub">तकनीकी संकेतक, सपोर्ट/रेजिस्टेंस, वित्तीय स्थिति और डेटा-आधारित समग्र विश्लेषण। डेस्कटॉप, लैपटॉप और मोबाइल पर प्रोफेशनल अनुभव के लिए बनाया गया।</div></header>

<!-- Top tabs -->
<div class="tab-buttons" role="tablist" aria-label="स्टॉक विश्लेषण टैब">
  <button type="button" class="tab-btn active" data-tab-target="searchTab">🔎 नया विश्लेषण</button>
  <button type="button" class="tab-btn" data-tab-target="savedTab">🗂️ पूर्व विश्लेषित सभी स्टॉक्स</button>
  <button type="button" class="tab-btn" data-tab-target="gainersLosersTab">📊 Top Gainers / Losers</button>
  <button type="button" class="tab-btn" data-tab-target="advanceScreenerTab">⚡ Advance Screener</button>
</div>

<!-- Tab 1: Single Stock Analysis -->
<div id="searchTab" class="tab-content active">
  <div class="panel">
  <form id="stockForm"><input id="symbol" placeholder="उदाहरण: CIPLA, RELIANCE, TCS, ZOMATO" required autocomplete="off"><button id="go" type="submit">विश्लेषण करें ↗</button></form>
  <div class="hint">भारतीय शेयर के लिए NSE symbol लिखें (जैसे CIPLA)। सफल विश्लेषण के बाद स्टॉक अपने-आप “पूर्व विश्लेषित सभी स्टॉक्स” टैब में सेव होगा।</div>
  </div>
  <div id="status" class="panel hidden"></div>
  <div id="results" class="hidden">
  <div class="panel"><div id="company"></div><div id="metrics" class="grid"></div></div>
  <div class="panel"><h2>🧭 Trend और Momentum</h2><div id="trend" class="copy"></div></div>
  <div class="panel"><h2>📊 Daily Support/Resistance</h2><div id="dailyLevels"></div></div>
  <div class="panel"><h2>📋 डेली S/R आधारित Entry, Stop Loss और Target योजना</h2><div id="dailyTradePlan"></div><p class="copy"><b>SEBI डिस्क्लेमर:</b> MarkeTGyan SEBI-पंजीकृत निवेश सलाहकार नहीं है। यह तालिका केवल शैक्षिक उद्देश्य के लिए उपलब्ध ऐतिहासिक डेटा से बने संभावित स्तर दिखाती है; यह खरीदने/बेचने की व्यक्तिगत सलाह नहीं है। ट्रेड से पहले स्वयं पुष्टि करें और जोखिम समझें।</p></div>
  <div class="panel"><h2>⏱️ 1HR Support/Resistance</h2><div id="hourlyLevels"></div></div>
  <div class="panel"><h2>📋 1 घंटे के S/R आधारित Entry, Stop Loss और Target योजना</h2><div id="hourlyTradePlan"></div><p class="copy"><b>SEBI डिस्क्लेमर:</b> MarkeTGyan SEBI-पंजीकृत निवेश सलाहकार नहीं है। यह तालिका केवल शैक्षिक उद्देश्य के लिए उपलब्ध ऐतिहासिक डेटा से बने संभावित स्तर दिखाती है; यह खरीदने/बेचने की व्यक्तिगत सलाह नहीं है। ट्रेड से पहले स्वयं पुष्टि करें और जोखिम समझें।</p></div>
  <div class="panel"><h2>🏢 कंपनी की वित्तीय स्थिति (Financials)</h2><div id="fundamentals"></div></div>
  <div class="panel"><h2>📊 Shareholding Pattern</h2><div id="shareholding"></div></div>
  <div class="panel"><h2>🧠 AI-जैसा निष्कर्ष</h2><div id="verdict" class="copy"></div></div>
  <div class="panel"><h2>⚠️ Risk Checklist</h2><div id="risks" class="copy"></div></div>
  <div class="panel"><h2>📝 उपलब्ध डेटा के आधार पर समग्र विश्लेषण नोट</h2><div id="analysisNote" class="copy"></div></div>
  </div>
</div>

<!-- Tab 2: Previously analyzed stocks -->
<div id="savedTab" class="tab-content">
  <div class="panel">
    <h2>🗂️ पूर्व विश्लेषण के लिए डाले गए सभी स्टॉक्स</h2>
    <p class="copy">यहाँ उन स्टॉक्स का रिकॉर्ड दिखाई देगा जिनका विश्लेषण सफलतापूर्वक किया गया है। नवीनतम विश्लेषण ऊपर दिखेगा। किसी स्टॉक को दोबारा देखने के लिए <b>फिर देखें</b> और रिकॉर्ड हटाने के लिए <b>Delete</b> दबाएँ। रिकॉर्ड इसी ब्राउज़र के Local Storage में रहता है।</p>
    <div id="savedStockMessage" class="hint"></div>
    <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>कंपनी / स्टॉक</th><th>अंतिम भाव</th><th>दैनिक बदलाव</th><th>Daily Support</th><th>Daily Resistance</th><th>1HR Support</th><th>1HR Resistance</th><th>ट्रेंड</th><th>विश्लेषण समय</th><th>विकल्प</th></tr></thead><tbody id="savedStocksBody"><tr><td colspan="10" class="empty-row">अभी कोई स्टॉक सेव नहीं है।</td></tr></tbody></table></div>
  </div>
</div>

<!-- Tab 3: Top Gainers / Losers (Futures Universe) -->
<div id="gainersLosersTab" class="tab-content">
 <div class="panel">
  <h2>📊 Top 5 Gainers & Losers (Futures Universe)</h2>
  <div style="margin-top:10px;"><button id="refreshGL" type="button" class="neutral-btn" onclick="fetchGainersLosers()">↻ अभी अपडेट करें</button></div>
  <div id="glStatus" class="hint" style="margin-top:8px">डेटा लोड करने के लिए बटन दबाएँ।</div>
  <h3 class="green" style="margin-top:20px;">🔥 Top 5 Gainers</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>LTP</th><th>Change %</th><th>Daily S/R</th><th>1HR S/R</th></tr></thead><tbody id="gainersBody"><tr><td colspan="6" class="empty-row">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
  <h3 class="red" style="margin-top:20px;">🧊 Top 5 Losers</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>LTP</th><th>Change %</th><th>Daily S/R</th><th>1HR S/R</th></tr></thead><tbody id="losersBody"><tr><td colspan="6" class="empty-row">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
 </div>
</div>

<!-- Tab 4: Advance Screener (Futures Universe) -->
<div id="advanceScreenerTab" class="tab-content">
 <div class="panel">
  <h2>⚡ Advance Dow Theory Screener (Futures Universe)</h2>
  <div style="margin-top:10px;"><button type="button" class="neutral-btn" onclick="triggerRescan()">अभी स्कैन शुरू करें</button></div>
  <div id="screenerStatus" class="hint" style="margin-top:8px">स्कैनर स्थिति: तैयार</div>
  <div class="progress-track" style="margin-top:10px"><div id="progBar" class="progress-fill"></div></div>
  <h3 style="margin-top:20px;">★ Confirmed Signals</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>ट्रेंड</th><th>भाव</th><th>Entry</th><th>SL</th><th>Target</th><th>Signal</th></tr></thead><tbody id="confirmedBody"><tr><td colspan="7" class="empty-row">कोई सिग्नल नहीं मिला।</td></tr></tbody></table></div>
  <h3 style="margin-top:20px;">📋 सभी फ्यूचर स्टॉक्स की स्थिति</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>भाव</th><th>ट्रेंड</th><th>HH</th><th>HL</th><th>Status</th></tr></thead><tbody id="allStocksBody"><tr><td colspan="6" class="empty-row">डेटा लोड हो रहा है...</td></tr></tbody></table></div>
 </div>
</div>

<footer>MarkeTGyan Professional Stock Screener • शैक्षिक उद्देश्य मात्र; यह SEBI-registered investment advice नहीं है।</footer>
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
 if(tabId==='advanceScreenerTab')fetchScreenerStatus();
}
document.querySelectorAll('[data-tab-target]').forEach(btn=>btn.addEventListener('click',()=>switchTab(btn.getAttribute('data-tab-target'))));

const SAVED_STOCKS_KEY='markeTGyan_saved_stock_analyses_v1';
function readSavedStocks(){
 try{const raw=localStorage.getItem(SAVED_STOCKS_KEY);const parsed=raw?JSON.parse(raw):[];return Array.isArray(parsed)?parsed:[];}
 catch(e){return [];}
}
function writeSavedStocks(items){
 try{localStorage.setItem(SAVED_STOCKS_KEY,JSON.stringify(items));$('savedStockMessage').textContent='';return true;}
 catch(e){$('savedStockMessage').textContent='ब्राउज़र में रिकॉर्ड सेव नहीं हो पाया। Browser storage settings जाँचें।';return false;}
}
function renderSavedStocks(){
 const items=readSavedStocks();
 if(!items.length){$('savedStocksBody').innerHTML='<tr><td colspan="10" class="empty-row">अभी कोई स्टॉक सेव नहीं है।</td></tr>';return;}
 $('savedStocksBody').innerHTML=items.map((x,i)=>'<tr>'+
 '<td><b>'+esc(x.company||x.symbol)+'</b><br><span class="tag">'+esc(x.symbol)+'</span></td>'+
 '<td>'+esc(fmt(x.price,' '+(x.currency||'')))+'</td>'+
 '<td class="'+(Number(x.change_pct)>0?'green':Number(x.change_pct)<0?'red':'')+'">'+esc(fmt(x.change_pct,'%'))+'</td>'+
 '<td>'+esc(fmt(x.support,' '+(x.currency||'')))+'</td><td>'+esc(fmt(x.resistance,' '+(x.currency||'')))+'</td>'+
 '<td>'+esc(fmt(x.support_1hr,' '+(x.currency||'')))+'</td><td>'+esc(fmt(x.resistance_1hr,' '+(x.currency||'')))+'</td>'+
 '<td>'+esc(x.score_label||'उपलब्ध नहीं')+'</td><td>'+esc(x.as_of||x.saved_at||'उपलब्ध नहीं')+'</td>'+
 '<td><div class="saved-actions"><button type="button" class="small-btn open-btn" data-open-stock="'+esc(x.symbol)+'">फिर देखें</button><button type="button" class="small-btn delete-btn" data-delete-stock="'+esc(x.symbol)+'">Delete</button></div></td></tr>').join('');
}
function saveStockAnalysis(d){
 const symbol=String(d.symbol||$('symbol').value||'').trim().toUpperCase();if(!symbol)return;
 const items=readSavedStocks();
 const entry={symbol,company:d.company||symbol,price:d.price,change_pct:d.change_pct,support:d.support,resistance:d.resistance,support_1hr:d.support_1hr,resistance_1hr:d.resistance_1hr,score_label:d.score_label,currency:d.currency,as_of:d.as_of,saved_at:new Date().toLocaleString('en-IN')};
 const oldIndex=items.findIndex(x=>String(x.symbol||'').toUpperCase()===symbol);
 if(oldIndex>=0)items.splice(oldIndex,1);
 items.unshift(entry);
 if(items.length>100)items.length=100;
 writeSavedStocks(items);renderSavedStocks();
}
$('savedStocksBody').addEventListener('click',e=>{
 const del=e.target.closest('[data-delete-stock]');
 if(del){const symbol=del.getAttribute('data-delete-stock');const items=readSavedStocks().filter(x=>String(x.symbol||'').toUpperCase()!==String(symbol).toUpperCase());writeSavedStocks(items);renderSavedStocks();return;}
 const open=e.target.closest('[data-open-stock]');
 if(open){$('symbol').value=open.getAttribute('data-open-stock');switchTab('searchTab');$('stockForm').requestSubmit();}
});
renderSavedStocks();

function tradePlanTable(support, resistance, currency, timeframe){
 const s=Number(support), r=Number(resistance), cur=currency||'';
 if(!Number.isFinite(s)||!Number.isFinite(r)||s<=0||r<=s){
  return '<p class="error">इस समय '+esc(timeframe)+' के लिए वैध सपोर्ट और रेजिस्टेंस उपलब्ध नहीं हैं; एंट्री SL नहीं निकलेगा।</p>';
 }
 const range=r-s, fmtP=v=>fmt(v,' '+cur);
 const rows=[
  ['BUY (Support से)',fmtP(s),fmtP(s-range*0.25),fmtP(s+range*0.5),fmtP(r)],
  ['SELL (Resistance से)',fmtP(r),fmtP(r+range*0.25),fmtP(r-range*0.5),fmtP(s)]
 ];
 return '<div style="overflow-x:auto"><table><thead><tr><th>सेटअप</th><th>Entry लेवल</th><th>Stop Loss</th><th>Target 1</th><th>Target 2</th></tr></thead><tbody>'+rows.map(row=>'<tr>'+row.map((x,i)=>'<td>'+(i===0?'<b>'+esc(x)+'</b>':esc(x))+'</td>').join('')+'</tr>').join('')+'</tbody></table></div><p class="copy"><b>S/R गणना का आधार:</b> BUY सेटअप सपोर्ट के पास और SELL रेजिस्टेंस के पास है। Stop Loss Support/Resistance दूरी का 25% buffer है; Target 1 दूरी के मध्य बिंदु पर और Target 2 विपरीत स्तर पर है। यह नियम-आधारित स्तर हैं, निश्चित संकेत नहीं।</p>';
}

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
  $('fundamentals').innerHTML=table([['Market Cap', d.market_cap],['Total Debt', d.total_debt],['Debt-to-Equity', d.debt_equity],['P/E', d.pe],['Price/Book (P/B)', d.pb],['Dividend Yield', d.dividend_yield],['ROE और ROCE', `ROE: ${d.roe} | ROCE: ${d.roce}`],['Sales Growth (3Y)', d.sales_growth],['Profit Growth (3Y)', d.profit_growth],['Profit Margin', d.profit_margin],['Debtor Days', d.debtor_days],['Business Summary', d.summary]]);
  $('shareholding').innerHTML=table([['Promoter Holding', d.promoter_holding],['Promoter Pledge', d.promoter_pledge],['FII Holding', d.fii_holding],['DII Holding', d.dii_holding],['Public Holding', d.public_holding]]);
  $('verdict').innerHTML=`<p><b>संकेत:</b> <span class="${d.score>=2?'green':d.score<=-2?'red':'gold'}">${esc(d.score_label)}</span></p><p>${esc(d.verdict)}</p>`;
  $('risks').innerHTML='<ul>'+d.risks.map(x=>'<li>'+esc(x)+'</li>').join('')+'</ul>';
  const note=[]; const valid=v=>Number.isFinite(v); const cur=d.currency||'';
  note.push(`<p><b>भाव और दैनिक चाल:</b> बंद भाव ${fmt(d.price, ' '+cur)} है। बदलाव ${d.change_pct>0?'+':''}${fmt(d.change_pct,'%')} है, जो सत्र में ${d.change_pct>0?'तेजी':d.change_pct<0?'कमजोरी':'लगभग कोई बदलाव नहीं'} दर्शाता है।</p>`);
  note.push(`<p><b>RSI स्थिति:</b> ${valid(d.rsi) ? `RSI ${fmt(d.rsi)} है${d.rsi>=70?' — यह ओवरबॉट है; नई खरीद में सावधानी रखें।':d.rsi<=30?' — यह ओवरसोल्ड है; केवल इसी आधार पर खरीद न करें।':' — यह संतुलित है।'}` : 'RSI अनुपलब्ध है।'} ${esc(d.rsi_comment||'')} ${esc(d.macd_comment||'')}</p>`);
  if(valid(d.score)) note.push(`<p><b>ट्रेंड और संकेतक:</b> ट्रेंड स्कोर <b>${fmt(d.score)}</b> है, जो संकेतकों पर आधारित <b>${esc(d.score_label)}</b> ट्रेंड दर्शाता है। ट्रेंड के विरुद्ध ट्रेड न लें।</p>`);
  note.push(`<p><b>दैनिक Support/Resistance:</b> सपोर्ट ${fmt(d.support, ' '+cur)} और रेजिस्टेंस ${fmt(d.resistance, ' '+cur)}। दिए गए Entry लेवल पर Bulls/Bears confirmation (bullish/bearish candle) के साथ ही फैसला लें।</p>`);
  note.push(`<p><b>एक घंटे के स्तर:</b> छोटे समय-फ्रेम की योजना के लिए सपोर्ट ${fmt(d.support_1hr, ' '+cur)} और रेजिस्टेंस ${fmt(d.resistance_1hr, ' '+cur)} स्तरों को ध्यान में रखें।</p>`);
  note.push(`<p><b>वित्तीय स्थिति और Shareholding:</b> कंपनी का Debt-to-Equity ${esc(d.debt_equity)} ${d.pe!='उपलब्ध नहीं' ? `, P/E ${esc(d.pe)}` : ''} ${d.pb!='उपलब्ध नहीं' ? `, P/B ${esc(d.pb)}` : ''} है। FII की हिस्सेदारी ${esc(d.fii_holding)} है और DII ${esc(d.dii_holding)}।</p>`);
  note.push(`<p><b>AI-जैसे निष्कर्ष का सार:</b> ${esc(d.verdict || 'उपलब्ध नहीं')}</p>`);
  $('analysisNote').innerHTML=note.join('');
 }catch(err){$('status').innerHTML='<div class="error">विश्लेषण नहीं हो पाया: '+esc(err.message)+'</div>'}
 finally{$('go').disabled=false;$('go').textContent='विश्लेषण करें ↗'}
});

async function fetchGainersLosers(){
 $('glStatus').textContent = 'Top Gainers/Losers गणना हो रही है...';
 try {
  const r = await fetch('/api/gainers-losers');
  const d = await r.json();
  const renderRows = (list) => {
   if(!list || !list.length) return '<tr><td colspan="6" class="empty-row">डेटा उपलब्ध नहीं है।</td></tr>';
   return list.map((x,i)=>`<tr>
    <td><span class="active-rank">${i+1}</span></td>
    <td><b>${esc(x.symbol)}</b></td>
    <td>₹${fmt(x.current)}</td>
    <td class="${x.p_change>=0?'green':'red'}"><b>${x.p_change>=0?'+':''}${fmt(x.p_change)}%</b></td>
    <td>${esc(x.daily_sup.join(', ') || '-')} / ${esc(x.daily_res.join(', ') || '-')}</td>
    <td>${esc(x.hourly_sup.join(', ') || '-')} / ${esc(x.hourly_res.join(', ') || '-')}</td>
   </tr>`).join('');
  };
  $('gainersBody').innerHTML = renderRows(d.gainers);
  $('losersBody').innerHTML = renderRows(d.losers);
  $('glStatus').textContent = 'अंतिम अपडेट: ' + new Date().toLocaleTimeString();
 } catch(e){$('glStatus').textContent = 'त्रुटि: ' + e.message;}
}

async function fetchScreenerStatus(){
 try {
  const r = await fetch('/api/screener-status');
  const d = await r.json();
  $('progBar').style.width = (d.progress || 0) + '%';
  $('screenerStatus').textContent = `स्थिति: ${d.status} | प्रगति: ${d.completed}/${d.total} (${d.progress}%)`;
  
  const confBody = $('confirmedBody');
  if(!d.confirmed || !d.confirmed.length){confBody.innerHTML='<tr><td colspan="7" class="empty-row">कोई सिग्नल नहीं।</td></tr>';}
  else{
   confBody.innerHTML = d.confirmed.map(r=>`<tr>
    <td><b>${esc(r.symbol)}</b></td><td class="${r.trend==='UPTREND'?'green':'red'}">${esc(r.trend)}</td>
    <td>₹${fmt(r.current)}</td><td>₹${fmt(r.entry)}</td><td class="red">₹${fmt(r.sl)}</td><td class="green">₹${fmt(r.target)}</td>
    <td><span class="tag">${esc(r.signal)}</span></td>
   </tr>`).join('');
  }
  const allBody = $('allStocksBody');
  if(d.results && d.results.length){
   allBody.innerHTML = d.results.map(r=>`<tr>
    <td><b>${esc(r.symbol)}</b></td><td>₹${fmt(r.current)}</td>
    <td class="${r.trend==='UPTREND'?'green':'red'}">${esc(r.trend)}</td><td>₹${fmt(r.hh)}</td><td>₹${fmt(r.hl)}</td><td>${esc(r.status)}</td>
   </tr>`).join('');
  }
 } catch(e){}
}
async function triggerRescan(){
 try {const r = await fetch('/api/rescan', {method:'POST'});const d = await r.json();if(!d.success) alert(d.message);fetchScreenerStatus();} catch(e){}
}
setInterval(fetchScreenerStatus, 5000);
</script>
</body>
</html>
"""

# =====================================================
# PYTHON HELPER & API ROUTE
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
            number = float(value)
            return number if math.isfinite(number) else None
        if isinstance(value, (np.bool_,)): return bool(value)
        if pd.isna(value): return None
    except (TypeError, ValueError): pass
    return value

def safe_jsonify(value, *args, **kwargs):
    return jsonify(sanitize_json_value(value), *args, **kwargs)

def human_large(value, currency=""):
    try:
        if value is None or pd.isna(value): return "उपलब्ध नहीं"
        value=float(value)
        for unit, div in [("T",1e12),("B",1e9),("Cr",1e7),("M",1e6),("L",1e5)]:
            if abs(value)>=div: return f"{currency}{value/div:,.2f} {unit}"
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
        lows = data['Low']
        highs = data['High']
        s = clean_num(lows.min())
        r = clean_num(highs.max())
    return s, r

def fetch_fundamental_data(symbol):
    clean_sym = symbol.replace(".NS", "").replace(".BO", "").strip().upper()
    data = {}
    screener_url = f"https://www.screener.in/company/{clean_sym}/"
    try:
        resp = session.get(screener_url, headers=get_headers(), timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for item in soup.select("ul#top-ratios li"):
                name = item.select_one(".name")
                val = item.select_one(".number")
                if name:
                    key = name.text.strip().lower().replace(":", "").strip()
                    if val: data[key] = val.text.strip()
            for row in soup.find_all("tr"):
                cols = row.find_all(["th", "td"])
                if len(cols) >= 2:
                    key = cols[0].text.strip().lower()
                    if "sales growth" in key and "3y" in key: data["sales growth (3y)"] = cols[-1].text.strip()
                    elif "profit growth" in key and "3y" in key: data["profit growth (3y)"] = cols[-1].text.strip()
                    elif "debtor days" in key and len(cols) > 2:
                        try: data["debtor days"] = clean_num(cols[1].text.strip())
                        except: pass
            for table in soup.find_all("table"):
                table_text = table.get_text().lower()
                if "promoter" in table_text or "fii" in table_text or "institutions" in table_text:
                    for row in table.find_all("tr"):
                        cols = row.find_all(["td", "th"])
                        if len(cols) >= 2:
                            t = cols[0].text.strip().lower()
                            if "promoter" in t: data["promoter holding"] = cols[-1].text.strip()
                            elif "fii" in t or "foreign" in t: data["fii holding"] = cols[-1].text.strip()
                            elif "dii" in t or "domestic" in t: data["dii holding"] = cols[-1].text.strip()
                            elif "public" in t: data["public holding"] = cols[-1].text.strip()
                            elif "pledged" in t or "pledge" in t: data["promoter pledge"] = cols[-1].text.strip()
            about_div = soup.find('div', class_='about')
            if about_div: data['summary'] = about_div.text.strip()
    except Exception: pass
    return data

def get_ai_verdict(score, symbol, currency):
    cur_txt = 'INR' if currency == 'INR' else currency
    if score >= 2: return f"AI Verdict ({cur_txt}): मजबूत सकारात्मक ट्रेंड। सपोर्ट पर पुलबैक पर BUY अवसर देखें। SL अनुशासित रखें।"
    if score >= 1: return f"AI Verdict ({cur_txt}): हल्का सकारात्मक ट्रेंड। सपोर्ट की तलाश में रहें; ब्रेकआउट टिकने पर एंट्री देखें।"
    if score <= -2: return f"AI Verdict ({cur_txt}): मजबूत कमजोरी। सपोर्ट टूटने पर SELL अवसर देखें। BUY से बचें।"
    if score <= -1: return f"AI Verdict ({cur_txt}): हल्की कमजोरी। रेजिस्टेंस पर SELL अवसर देखें। सपोर्ट पर बुल्स का इंतज़ार करें।"
    return f"AI Verdict ({cur_txt}): ट्रेंड अनिश्चित। साइडवेज बाजार; सपोर्ट/रेजिस्टेंस पर ही ट्रेड लें।"

def get_risk_checklist(data, info, score, currency):
    valid=lambda v: Number.isFinite(v)
    cur_str = '₹' if currency=='INR' else currency
    checklist = []
    if data['rsi'] and data['rsi'] > 70: checklist.append(f"तकनीकी जोखिम: RSI ओवरबॉट क्षेत्र में है ({fmt(data['rsi'])}); नई खरीद में सावधानी।")
    if data['macd_hist'] and data['macd_hist'] < 0 and score > 0: checklist.append("मूविंग एवरेज रिस्क: तकनीकी ट्रेंड सकारात्मक होने के बावजूद MACD हिस्टोग्राम कमजोर हो रहा है।")
    if valid(data['sma200']) and valid(data['sma50']) and data['sma50'] < data['sma200'] and score > 0: checklist.append("तकनीकी ट्रेंड रिस्क: 50 SMA, 200 SMA के नीचे है (Death Cross), जो कमजोरी का संकेत है।")
    try:
        debt_txt = info.get('debt_equity', 'उपलब्ध नहीं'); debt_val = None
        if debt_txt != 'उपलब्ध नहीं': debt_val = float(str(debt_txt).replace(',',''))
        if valid(debt_val) and debt_val > 1.5: checklist.append(f"वित्तीय जोखिम: Debt-to-Equity अनुपात उच्च है ({debt_txt})।")
    except: pass
    pledge_txt = info.get('promoter_pledge', 'उपलब्ध नहीं')
    if pledge_txt != 'उपलब्ध नहीं' and pledge_txt != '0.00%': checklist.append(f"प्रमोटर जोखिम: प्रमोटरों ने हिस्सेदारी गिरवी रखी है ({pledge_txt})।")
    if not checklist: checklist.append("अभी कोई बड़ा जोखिम तकनीकी या वित्तीय डेटा से स्पष्ट नहीं है।")
    return checklist

def analyze_symbol(raw):
    symbol = raw.strip().upper().replace(" ", "")
    if not symbol: raise ValueError("कृपया स्टॉक का नाम लिखें।")
    
    ticker_symbol = symbol + ".NS" if "." not in symbol and not symbol.startswith("^") else symbol
    ticker = yf.Ticker(ticker_symbol)
    
    hist = ticker.history(period="1y", interval="1d", auto_adjust=False)
    if hist is None or hist.empty or len(hist) < 30: raise ValueError(f"{symbol} का पर्याप्त डेटा नहीं मिला।")
    
    hist_1hr = ticker.history(period="5d", interval="60m")
    info = ticker.get_info() or {}
    screener_fundamentals = fetch_fundamental_data(symbol)
    
    close = hist["Close"].dropna()
    price = clean_num(close.iloc[-1])
    prev = clean_num(close.iloc[-2])
    change_pct = ((price / prev) - 1) * 100 if price and prev else 0.0
    ema20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    sma200 = close.rolling(200).mean().iloc[-1]
    rsi = calculate_rsi(close)
    macd_data = calculate_macd(close)
    
    support, resistance = get_sr_levels(hist, type='daily')
    support_1hr, resistance_1hr = get_sr_levels(hist_1hr, type='hourly')
    
    valid = lambda v: Number.isFinite(v)
    score = 0; ma_comment = "मूविंग एवरेज अनुपलब्ध हैं"; rsi_comment = "RSI अनुपलब्ध है"; macd_comment = "MACD अनुपलब्ध है"
    
    if valid(price) and valid(ema20) and valid(sma50) and valid(sma200):
        if price > ema20 and price > sma50 and price > sma200:
            score += 2; ma_comment = "भाव 20-EMA, 50-SMA और 200-SMA के ऊपर है (मजबूत तेजी का ट्रेंड)।"
        elif price > sma200:
            score += 1; ma_comment = f"भाव लंबी अवधि के 200-SMA ({fmt(sma200, ' ₹')}) के ऊपर है (तेजी का ट्रेंड)।"
        elif price < sma200:
            score -= 1; ma_comment = f"भाव लंबी अवधि के 200-SMA ({fmt(sma200, ' ₹')}) के नीचे है (कमजोरी का ट्रेंड)।"
    
    if valid(rsi):
        if rsi > 70: score -= 1; rsi_comment = f"RSI {fmt(rsi)} है (ओवरबॉट); खरीद में सावधानी रखें।"
        elif rsi < 30: score += 1; rsi_comment = f"RSI {fmt(rsi)} है (ओवरसोल्ड); पुलबैक की संभावना।"
        elif 50 <= rsi <= 70: score += 1; rsi_comment = f"RSI {fmt(rsi)} है (संतुलित तेजी)।"
        elif 30 <= rsi < 50: rsi_comment = f"RSI {fmt(rsi)} है (संतुलित कमजोरी)।"
    
    if macd_data:
        macd, signal, hist_val = macd_data
        if valid(macd) and valid(signal) and valid(hist_val):
            if macd > signal:
                score += 1; macd_comment = f"MACD ({fmt(macd)}) सिग्नल लाइन ({fmt(signal)}) के ऊपर है (पॉजिटिव मोमेंटम)।"
            elif macd < signal:
                score -= 1; macd_comment = f"MACD ({fmt(macd)}) सिग्नल लाइन ({fmt(signal)}) के नीचे है (नेगेटिव मोमेंटम)।"

    fundamentals = {
        "symbol": symbol,
        "company": info.get("longName") or symbol,
        "as_of": str(hist.index[-1].date()),
        "price": price,
        "change_pct": change_pct,
        "currency": info.get("currency", "INR"),
        "rsi": rsi,
        "score": score,
        "score_label": "मजबूत सकारात्मक" if score >= 2 else "हल्का सकारात्मक" if score >= 1 else "मजबूत कमजोरी" if score <= -2 else "हल्की कमजोरी" if score <= -1 else "अनिश्चित",
        "trend": "Bulls नियंत्रण में हैं" if score >= 1 else "Bears नियंत्रण में हैं" if score <= -1 else "साइडवेज बाजार",
        "ema20": clean_num(ema20), "sma50": clean_num(sma50), "sma200": clean_num(sma200),
        "support": support, "resistance": resistance,
        "support_1hr": support_1hr, "resistance_1hr": resistance_1hr,
        "high52": clean_num(info.get("fiftyTwoWeekHigh")), "low52": clean_num(info.get("fiftyTwoWeekLow")),
        "ma_comment": ma_comment, "rsi_comment": rsi_comment, "macd_comment": macd_comment,
    }
    
    fundamentals["market_cap"] = human_large(clean_num(info.get("marketCap"))) if 'market cap' not in screener_fundamentals else screener_fundamentals['market cap']
    fundamentals["total_debt"] = human_large(clean_num(info.get("totalDebt"))) if 'total debt' not in screener_fundamentals else screener_fundamentals['total debt']
    fundamentals["debt_equity"] = screener_fundamentals.get('debt to equity', clean_num(info.get("debtToEquity", 'उपलब्ध नहीं')))
    fundamentals["pe"] = screener_fundamentals.get('stock p/e', clean_num(info.get("trailingPE", 'उपलब्ध नहीं')))
    fundamentals["pb"] = screener_fundamentals.get('price to book', clean_num(info.get("priceToBook", 'उपलब्ध नहीं')))
    fundamentals["dividend_yield"] = screener_fundamentals.get('dividend yield', f"{clean_num(info.get('dividendYield', 0)*100)}%")
    fundamentals["roe"] = screener_fundamentals.get('roe', f"{clean_num(info.get('returnOnEquity', 0)*100)}%")
    fundamentals["roce"] = screener_fundamentals.get('roce', 'उपलब्ध नहीं')
    fundamentals["sales_growth"] = screener_fundamentals.get('sales growth (3y)', 'उपलब्ध नहीं')
    fundamentals["profit_growth"] = screener_fundamentals.get('profit growth (3y)', 'उपलब्ध नहीं')
    fundamentals["profit_margin"] = screener_fundamentals.get('profit margin', 'उपलब्ध नहीं')
    fundamentals["debtor_days"] = screener_fundamentals.get('debtor days', 'उपलब्ध नहीं')
    fundamentals["promoter_holding"] = screener_fundamentals.get('promoter holding', f"{clean_num(info.get('heldPercentInsiders', 0)*100)}%")
    fundamentals["promoter_pledge"] = screener_fundamentals.get('promoter pledge', '0.00%')
    fundamentals["fii_holding"] = screener_fundamentals.get('fii holding', f"{clean_num(info.get('heldPercentInstitutions', 0)*100)}%")
    fundamentals["dii_holding"] = screener_fundamentals.get('dii holding', 'उपलब्ध नहीं')
    fundamentals["public_holding"] = screener_fundamentals.get('public holding', 'उपलब्ध नहीं')
    fundamentals["summary"] = screener_fundamentals.get('summary', info.get("longBusinessSummary", "सारांश उपलब्ध नहीं"))
    
    fundamentals["verdict"] = get_ai_verdict(score, symbol, fundamentals['currency'])
    fundamentals["risks"] = get_risk_checklist({"rsi": rsi, "macd_hist": macd_data[2] if macd_data else None, "sma200": sma200, "sma50": sma50, "price": price}, fundamentals, score, fundamentals['currency'])
    
    return fundamentals

# =====================================================
# FUTURES UNIVERSE SCREENER BACKGROUND CALCULATIONS
# =====================================================
SCAN_STATE = {"results": [], "confirmed": [], "gainers": [], "losers": [], "status": "READY", "completed": 0, "total": len(STOCK_UNIVERSE), "progress": 0, "running": False}

def run_futures_scan():
    global SCAN_STATE
    SCAN_STATE["running"] = True
    SCAN_STATE["status"] = "SCANNING"
    results = []
    total = len(STOCK_UNIVERSE)
    completed = 0
    
    for sym in STOCK_UNIVERSE:
        clean_name = sym.replace(".NS", "")
        try:
            ticker = yf.Ticker(sym)
            hist = ticker.history(period="1y", interval="1d", auto_adjust=False)
            if hist is not None and not hist.empty and len(hist) > 5:
                close = hist["Close"].dropna()
                price = float(close.iloc[-1])
                prev = float(close.iloc[-2]) if len(close) > 1 else price
                chg = ((price / prev) - 1) * 100 if prev else 0.0
                
                s_daily, r_daily = get_sr_levels(hist, type='daily')
                s_hourly, r_hourly = get_sr_levels(ticker.history(period="5d", interval="60m"), type='hourly')
                
                results.append({
                    "symbol": clean_name,
                    "current": round(price, 2),
                    "p_change": round(chg, 2),
                    "daily_sup": [s_daily] if s_daily else [],
                    "daily_res": [r_daily] if r_daily else [],
                    "hourly_sup": [s_hourly] if s_hourly else [],
                    "hourly_res": [r_hourly] if r_hourly else [],
                    "trend": "UPTREND" if chg >= 0 else "DOWNTREND",
                    "hh": r_daily or round(price * 1.02, 2),
                    "hl": s_daily or round(price * 0.98, 2),
                    "entry": round(price, 2),
                    "sl": s_daily or round(price * 0.98, 2),
                    "target": r_daily or round(price * 1.02, 2),
                    "signal": "BUY VALID" if chg > 0 else "WAIT",
                    "status": "Active Setup" if chg > 0 else "Consolidating"
                })
        except Exception:
            pass
        completed += 1
        SCAN_STATE["completed"] = completed
        SCAN_STATE["progress"] = int((completed / total) * 100)
    
    results.sort(key=lambda x: x["p_change"], reverse=True)
    SCAN_STATE["results"] = results
    SCAN_STATE["confirmed"] = [r for r in results if r["signal"] == "BUY VALID"]
    SCAN_STATE["gainers"] = results[:5]
    losers = results[-5:]
    losers.reverse()
    SCAN_STATE["losers"] = losers
    SCAN_STATE["status"] = "COMPLETED"
    SCAN_STATE["running"] = False

@app.get("/")
def home():
    return render_template_string(PAGE)

@app.get("/api/analyze")
def api_analyze():
    symbol = request.args.get("symbol", "")
    try: return safe_jsonify(analyze_symbol(symbol))
    except Exception as e: return safe_jsonify({"error": str(e)}), 400

@app.get("/api/gainers-losers")
def api_gainers_losers():
    if not SCAN_STATE["results"] and not SCAN_STATE["running"]:
        threading.Thread(target=run_futures_scan).start()
    return safe_jsonify({"gainers": SCAN_STATE["gainers"], "losers": SCAN_STATE["losers"]})

@app.get("/api/screener-status")
def api_screener_status():
    return safe_jsonify(SCAN_STATE)

@app.post("/api/rescan")
def api_rescan():
    if not SCAN_STATE["running"]:
        threading.Thread(target=run_futures_scan).start()
        return jsonify({"success": True, "message": "स्कैन शुरू हो गया है।"})
    return jsonify({"success": False, "message": "स्कैन पहले से चल रहा है।"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)