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
from flask import Flask, request, jsonify, render_template_string, send_file
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
# GLOBAL STATE FOR ADVANCED SCREENER & GAINERS/LOSERS
# =====================================================
SCAN_STATE = {
    "stocks": [],
    "results": [],
    "confirmed": [],
    "gainers": [],
    "losers": [],
    "status": "IDLE",
    "message": "Scanner ready",
    "total": 0,
    "completed": 0,
    "progress": 0,
    "batch_number": 0,
    "total_batches": 0,
    "current_batch": "",
    "running": False,
    "last_scan": None,
    "source": "Yahoo Finance Universe"
}
SCAN_LOCK = threading.Lock()

STOCK_UNIVERSE = [
    "RELIANCE", "TCS", "INFY", "HDFCBANK", "ICICIBANK", "SBIN", "BHARTIARTL", "KOTAKBANK", "LT", "ITC",
    "AXISBANK", "ASIANPAINT", "BAJFINANCE", "MARUTI", "SUNPHARMA", "TITAN", "ULTRACEMCO", "WIPRO", "HCLTECH",
    "POWERGRID", "NTPC", "TATAMOTORS", "TATASTEEL", "JSWSTEEL", "GRASIM", "TECHM", "ONGC", "COALINDIA", "ADANIENT",
    "ADANIPORTS", "BAJAJ-AUTO", "BRITANNIA", "CIPLA", "DIVISLAB", "EICHERMOT", "HINDALCO", "HEROMOTOCO", "INDUSINDBK",
    "M&M", "NESTLEIND", "BPCL", "SBILIFE", "HDFCLIFE", "DRREDDY", "IOC", "SBICARD", "MUTHOOTFIN", "HAL", "BEL",
    "ABB.NS", "FORCEMOT.NS", "CGPOWER.NS", "DABUR.NS", "BHEL.NS", "HINDPETRO.NS", "SAIL.NS", "GODFRYPHLP.NS",
    "LICHSGFIN.NS", "LUPIN.NS", "MFSL.NS", "VEDL.NS", "PIDILITIND.NS", "SHREECEM.NS", "TATAPOWER.NS", "TATAELXSI.NS",
    "TORNTPHARM.NS", "AMBUJACEM.NS", "FEDERALBNK.NS", "ASHOKLEY.NS", "CUMMINSIND.NS", "BAJAJHLDNG.NS", "BHARATFORG.NS",
    "BOSCHLTD.NS", "SIEMENS.NS", "VOLTAS.NS", "HINDUNILVR.NS", "TATACONSUM.NS", "COLPAL.NS", "INDHOTEL.NS", "PHOENIXLTD.NS",
    "SRF.NS", "APOLLOHOSP.NS", "SUPREMEIND.NS", "SHRIRAMFIN.NS", "CHOLAFIN.NS", "UPL.NS", "MOTHERSON.NS", "HAVELLS.NS",
    "KEI.NS", "PIIND.NS", "AUROPHARMA.NS", "MPHASIS.NS", "NMDC.NS", "MANAPPURAM.NS", "CONCOR.NS", "MARICO.NS",
    "BANKBARODA.NS", "BANKINDIA.NS", "GAIL.NS", "NATIONALUM.NS", "JINDALSTEL.NS", "GLENMARK.NS", "ZYDUSLIFE.NS",
    "TVSMOTOR.NS", "GODREJCP.NS", "UNITDSPR.NS", "PNB.NS", "OFSS.NS", "UNIONBANK.NS", "CANBK.NS", "RADICO.NS",
    "PETRONET.NS", "BIOCON.NS", "MAHABANK.NS", "UNOMINDA.NS", "COFORGE.NS", "YESBANK.NS", "SUZLON.NS", "SOLARINDS.NS",
    "GMRAIRPORT.NS", "NAUKRI.NS", "PFC.NS", "INDIANB.NS", "IDEA.NS", "PAGEIND.NS", "ASTRAL.NS", "FORTIS.NS", "DLF.NS",
    "MOTILALOFS.NS", "RECLTD.NS", "BAJAJFINSV.NS", "ADANIPOWER.NS", "NHPC.NS", "OIL.NS", "JSWENERGY.NS", "GODREJPROP.NS",
    "JUBLFOOD.NS", "PERSISTENT.NS", "OBEROIRLTY.NS", "PRESTIGE.NS", "LTF.NS", "PGEL.NS", "APLAPOLLO.NS", "MCX.NS",
    "NBCC.NS", "INDUSTOWER.NS", "INOXWIND.NS", "ADANIENSOL.NS", "IDFCFIRSTB.NS", "INDIGO.NS", "ALKEM.NS", "CROMPTON.NS",
    "LTM.NS", "RBLBANK.NS", "ICICIPRULI.NS", "PNBHOUSING.NS", "VBL.NS", "LAURUSLABS.NS", "BSE.NS", "AUBANK.NS", "CDSL.NS",
    "COCHINSHIP.NS", "DIXON.NS", "ABCAPITAL.NS", "IEX.NS", "TIINDIA.NS", "NAM-INDIA.NS", "AMBER.NS", "BDL.NS",
    "BANDHANBNK.NS", "ADANIGREEN.NS", "HDFCAMC.NS", "RVNL.NS", "POLYCAB.NS", "KPITTECH.NS", "360ONE.NS", "UJJIVANSFB.NS",
    "POWERINDIA.NS", "MAXHEALTH.NS", "CAMS.NS", "ANGELONE.NS", "MAZDOCK.NS", "IRFC.NS", "KALYANKJIL.NS", "LODHA.NS",
    "SONACOMS.NS", "NYKAA.NS", "POLICYBZR.NS", "PAYTM.NS", "ANANDRATHI.NS", "LICI.NS", "DELHIVERY.NS", "KAYNES.NS",
    "KFINTECH.NS", "MANKIND.NS", "JIOFIN.NS", "IREDA.NS", "HYUNDAI.NS", "WAAREEENER.NS", "SWIGGY.NS"
]

# =====================================================
# HTML TEMPLATE WITH ALL TABS & RESPONSIVE DESIGN
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
input,select{flex:1;min-width:min(210px,100%);background:#fbfdff;color:#26364a;border:1.5px solid #c6d6e7;border-radius:12px;padding:15px;font-size:17px;min-height:50px;outline:none;transition:border-color .2s,box-shadow .2s}
input:focus,select:focus{border-color:#4b8ed8;box-shadow:0 0 0 3px rgba(75,142,216,.15)}
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
.tab-buttons{display:flex;gap:8px;margin:8px 0 16px;border:0;padding:8px;background:#e5edf6;border-radius:16px;flex-wrap:wrap;box-shadow:inset 0 1px 3px rgba(40,70,105,.06)}
.tab-btn{flex:1 1 180px;background:transparent;color:#526a82;border:1px solid transparent;padding:12px 14px;border-radius:11px;cursor:pointer;font-weight:800;font-size:14px;line-height:1.4;box-shadow:none;text-align:center}
.tab-btn:hover{background:#f6faff;transform:none}.tab-btn.active{background:linear-gradient(135deg,#3c80c8,#2e69ad);color:#fff;border-color:#2e69ad;box-shadow:0 4px 12px rgba(46,105,173,.2)}
.tab-content{display:none}.tab-content.active{display:block;animation:fadeIn .22s ease-out}
.progress-track{width:100%;height:14px;background:#e7edf4;border:1px solid #d3dfeb;border-radius:99px;overflow:hidden;margin:8px 0 16px}.progress-fill{height:100%;width:0%;background:linear-gradient(90deg,#43b581,#168b8b);transition:width .25s ease}
@keyframes fadeIn{from{opacity:.75;transform:translateY(3px)}to{opacity:1;transform:translateY(0)}}

/* Demo Trading & Active Stocks extra classes */
.trade-toolbar{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;align-items:end}
.trade-field label{display:block;font-size:13px;font-weight:800;color:#435b91;margin-bottom:6px}
.trade-actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}
.buy-btn{background:linear-gradient(120deg,#12a875,#087c61);color:white;border:0}
.sell-btn{background:linear-gradient(120deg,#ee6375,#c42f4d);color:white;border:0}
.neutral-btn{background:linear-gradient(120deg,#4368de,#7654d0);color:#fff}
.trade-metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}
.trade-metric{padding:14px;border-radius:14px;border:1px solid #d9e4f6;background:linear-gradient(135deg,#eef3ff,#f0fffb)}
.trade-metric small{display:block;color:#60718a;font-weight:700;margin-bottom:6px}.trade-metric strong{font-size:21px}
.trade-status{padding:12px;border-radius:11px;background:#eaf7f0;color:#176a43;font-weight:700;white-space:pre-wrap}
.trade-status.error{background:#fff0f2;color:#a3273a}
.order-chip{display:inline-block;border-radius:999px;padding:4px 8px;font-size:12px;font-weight:800;background:#e5efff;color:#3559ad}
.active-rank{display:inline-grid;place-items:center;min-width:32px;height:32px;border-radius:10px;background:linear-gradient(135deg,#e2eaff,#e5fff4);color:#344d9a;font-weight:900}

@media(max-width:1024px){.saved-table{min-width:1000px}}
@media(max-width:768px){
 main{padding:12px}header{padding:12px 0 8px}.panel{margin:12px 0;border-radius:15px;padding:14px}form{gap:9px}input{width:100%;font-size:16px;padding:14px}form button{width:100%}.tab-buttons{display:grid;grid-template-columns:1fr;gap:8px;padding:7px}.tab-btn{width:100%;padding:13px 12px;font-size:15px}.grid{grid-template-columns:repeat(2,minmax(0,1fr));gap:9px}.metric{padding:12px}.metric small{font-size:12px}.metric strong{font-size:19px}h2{font-size:19px}td,th{padding:10px 9px;font-size:14px}.saved-table{min-width:900px}
}
</style>
</head>
<body><main>
<header><div class="tag">MarkeTGyan Professional Screener</div>
<h1>Professional Stock Analyst & Screener</h1>
<div class="sub">तकनीकी संकेतक, सपोर्ट/रेजिस्टेंस, वित्तीय स्थिति, Active Stocks, Demo Trading और Top Gainers/Losers विश्लेषण।</div></header>

<!-- ALL TABS NAVIGATION -->
<div class="tab-buttons" role="tablist" aria-label="स्टॉक विश्लेषण टैब">
  <button type="button" class="tab-btn active" data-tab-target="searchTab">🔎 नया विश्लेषण</button>
  <button type="button" class="tab-btn" data-tab-target="savedTab">🗂️ पूर्व विश्लेषित सभी स्टॉक्स</button>
  <button type="button" class="tab-btn" data-tab-target="active20Tab">📈 Active Stocks</button>
  <button type="button" class="tab-btn" data-tab-target="demoTradeTab">💹 Demo Trade</button>
  <button type="button" class="tab-btn" data-tab-target="gainersLosersTab">📊 Top Gainers/Losers</button>
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
  <div class="panel"><h2>📋 डेली S/R आधारित Entry, Stop Loss और Target योजना</h2><div id="dailyTradePlan"></div><p class="copy"><b>SEBI डिस्क्लेमर:</b> MarkeTGyan SEBI-पंजीकृत निवेश सलाहकार नहीं है। यह तालिका केवल शैक्षिक उद्देश्य के लिए उपलब्ध ऐतिहासिक डेटा से बने संभावित स्तर दिखाती है।</p></div>
  <div class="panel"><h2>⏱️ 1HR Support/Resistance</h2><div id="hourlyLevels"></div></div>
  <div class="panel"><h2>📋 1 घंटे के S/R आधारित Entry, Stop Loss और Target योजना</h2><div id="hourlyTradePlan"></div><p class="copy"><b>SEBI डिस्क्लेमर:</b> MarkeTGyan SEBI-पंजीकृत निवेश सलाहकार नहीं है। यह तालिका केवल शैक्षिक उद्देश्य के लिए है।</p></div>
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
    <p class="copy">यहाँ उन स्टॉक्स का रिकॉर्ड दिखाई देगा जिनका विश्लेषण सफलतापूर्वक किया गया है। रिकॉर्ड इसी ब्राउज़र के Local Storage में रहता है।</p>
    <div id="savedStockMessage" class="hint"></div>
    <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>कंपनी / स्टॉक</th><th>अंतिम भाव</th><th>दैनिक बदलाव</th><th>Daily Support</th><th>Daily Resistance</th><th>1HR Support</th><th>1HR Resistance</th><th>ट्रेंड</th><th>विश्लेषण समय</th><th>विकल्प</th></tr></thead><tbody id="savedStocksBody"><tr><td colspan="10" class="empty-row">अभी कोई स्टॉक सेव नहीं है।</td></tr></tbody></table></div>
  </div>
</div>

<!-- Tab 3: Active Stocks -->
<div id="active20Tab" class="tab-content">
 <div class="panel">
  <h2>📈 Active Stocks Screener</h2>
  <p class="copy">बाजार के सबसे सक्रिय स्टॉक्स की सूची Volume के घटते क्रम में।</p>
  <div class="trade-actions"><button id="refreshActive20" type="button" class="neutral-btn" onclick="fetchActiveStocks()">↻ अभी सूची अपडेट करें</button></div>
  <div id="active20Status" class="trade-status" style="margin-top:12px">सूची लोड करने के लिए अपडेट बटन दबाएँ।</div>
  <div class="saved-table-wrap" style="margin-top:14px"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक / कंपनी</th><th>Close ₹</th><th>Change %</th><th>Volume</th></tr></thead><tbody id="active20Body"><tr><td colspan="5" class="empty-row">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
 </div>
</div>

<!-- Tab 4: Demo Trade -->
<div id="demoTradeTab" class="tab-content">
 <div class="panel">
  <h2>💹 Professional Demo Trading Terminal</h2>
  <div class="trade-note" style="padding:12px;background:#fff8df;border:1px solid #f0dfa5;border-radius:10px;color:#735817;margin-bottom:14px"><b>DEMO / PAPER TRADING ONLY:</b> यहाँ कोई वास्तविक ऑर्डर एक्सचेंज को नहीं भेजा जाता। शुरुआती वर्चुअल कैश ₹1,00,000 है।</div>
  <div class="trade-metrics">
   <div class="trade-metric"><small>Available Cash</small><strong id="demoCash">₹1,00,000</strong></div>
   <div class="trade-metric"><small>Portfolio Value</small><strong id="demoPortfolioValue">₹0</strong></div>
   <div class="trade-metric"><small>Unrealized P&L</small><strong id="demoUnrealized">₹0</strong></div>
   <div class="trade-metric"><small>Realized P&L</small><strong id="demoRealized">₹0</strong></div>
  </div>
 </div>
 <div class="panel">
  <h2>🧾 Place Order</h2>
  <div class="trade-toolbar">
   <div class="trade-field"><label for="demoSymbol">NSE Symbol / Stock Name</label><input id="demoSymbol" value="RELIANCE" autocomplete="off"></div>
   <div class="trade-field"><label for="demoSide">Side</label><select id="demoSide"><option value="BUY">BUY</option><option value="SELL">SELL</option></select></div>
   <div class="trade-field"><label for="demoOrderType">Order Type</label><select id="demoOrderType"><option value="MARKET">Market</option><option value="LIMIT">Limit</option></select></div>
   <div class="trade-field"><label for="demoQty">Quantity</label><input id="demoQty" type="number" min="1" step="1" value="1"></div>
   <div class="trade-field"><label for="demoLimitPrice">Limit Price (₹)</label><input id="demoLimitPrice" type="number" min="0.01" step="0.05" placeholder="Limit Price"></div>
   <div class="trade-field"><label>Live Quote</label><div id="demoQuote" class="trade-status">भाव प्राप्त नहीं हुआ</div></div>
  </div>
  <div class="trade-actions"><button type="button" id="demoGetQuote" class="neutral-btn" onclick="fetchDemoQuote()">↻ भाव प्राप्त करें</button><button type="button" id="demoBuy" class="buy-btn" onclick="submitDemoOrder('BUY')">BUY</button><button type="button" id="demoSell" class="sell-btn" onclick="submitDemoOrder('SELL')">SELL</button></div>
  <div id="demoMessage" class="hint" aria-live="polite"></div>
 </div>
 <div class="panel"><h2>📍 Today's Positions</h2><div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>Symbol</th><th>Qty</th><th>Avg. Price</th><th>LTP</th><th>Market Value</th><th>Unrealized P&amp;L</th><th>Action</th></tr></thead><tbody id="demoTodayBody"><tr><td colspan="7" class="empty-row">कोई पोजीशन नहीं है।</td></tr></tbody></table></div></div>
 <div class="panel"><h2>📚 Order &amp; Trade Book</h2><div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>समय</th><th>Symbol</th><th>Side</th><th>Type</th><th>Qty</th><th>Price</th><th>Status</th><th>Realized P&amp;L</th></tr></thead><tbody id="demoBookBody"><tr><td colspan="8" class="empty-row">कोई ऑर्डर नहीं है।</td></tr></tbody></table></div><div class="trade-actions"><button id="demoReset" type="button" class="sell-btn" onclick="resetDemo()">Demo Data Reset करें</button></div></div>
</div>

<!-- Tab 5: Top Gainers / Losers -->
<div id="gainersLosersTab" class="tab-content">
 <div class="panel">
  <h2>📊 Top 5 Gainers & Losers (Watchlist Universe)</h2>
  <p class="copy">दिए गए स्टॉक्स की सूची में से आज के सबसे ज़्यादा बढ़ने और गिरने वाले स्टॉक्स Daily व 1HR सपोर्ट/रेजिस्टेंस स्तरों के साथ।</p>
  <div class="trade-actions"><button id="refreshGL" type="button" class="neutral-btn" onclick="fetchGainersLosers()">↻ अभी अपडेट करें</button></div>
  <div id="glStatus" class="trade-status" style="margin-top:12px">डेटा लोड करने के लिए अपडेट बटन दबाएँ।</div>
  
  <h3 class="green" style="margin-top:20px;">🔥 Top 5 Gainers (सबसे ज्यादा बढ़ने वाले)</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>LTP</th><th>Change %</th><th>Daily S/R</th><th>1HR S/R</th></tr></thead><tbody id="gainersBody"><tr><td colspan="6" class="empty-row">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>

  <h3 class="red" style="margin-top:20px;">🧊 Top 5 Losers (सबसे ज्यादा गिरने वाले)</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>LTP</th><th>Change %</th><th>Daily S/R</th><th>1HR S/R</th></tr></thead><tbody id="losersBody"><tr><td colspan="6" class="empty-row">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
 </div>
</div>

<!-- Tab 6: Advance Screener -->
<div id="advanceScreenerTab" class="tab-content">
 <div class="panel">
  <h2>⚡ Advance Dow Theory Futures Screener</h2>
  <p class="copy">स्वचालित रूप से मार्केट डेटा स्कैन करके Confirmed Buy/Sell सेटअप ढूँढता है।</p>
  <div class="trade-actions"><button type="button" class="neutral-btn" onclick="triggerRescan()">अभी स्कैन शुरू करें</button></div>
  <div id="screenerStatus" class="trade-status" style="margin-top:12px">स्कैनर स्थिति: तैयार</div>
  <div class="progress-track" style="margin-top:10px"><div id="progBar" class="progress-fill"></div></div>

  <h3 style="margin-top:20px;">★ Confirmed Signals</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>ट्रेंड</th><th>भाव</th><th>Entry</th><th>SL</th><th>Target</th><th>Signal</th></tr></thead><tbody id="confirmedBody"><tr><td colspan="7" class="empty-row">कोई सिग्नल नहीं मिला।</td></tr></tbody></table></div>

  <h3 style="margin-top:20px;">📋 सभी स्टॉक्स की स्थिति</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>भाव</th><th>ट्रेंड</th><th>HH</th><th>HL</th><th>Status</th></tr></thead><tbody id="allStocksBody"><tr><td colspan="6" class="empty-row">डेटा लोड हो रहा है...</td></tr></tbody></table></div>
 </div>
</div>

<footer>MarkeTGyan Professional Stock Screener • शैक्षिक उद्देश्य मात्र।</footer>
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
 if(tabId==='active20Tab')fetchActiveStocks();
 if(tabId==='demoTradeTab')renderDemoTrading();
 if(tabId==='gainersLosersTab')fetchGainersLosers();
 if(tabId==='advanceScreenerTab')fetchScreenerStatus();
}
document.querySelectorAll('[data-tab-target]').forEach(btn=>btn.addEventListener('click',()=>switchTab(btn.getAttribute('data-tab-target'))));

// Saved Stocks Logic
const SAVED_STOCKS_KEY='markeTGyan_saved_stock_analyses_v1';
function readSavedStocks(){try{const raw=localStorage.getItem(SAVED_STOCKS_KEY);return raw?JSON.parse(raw):[];}catch(e){return [];}}
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
 if(del){writeSavedStocks(readSavedStocks().filter(x=>String(x.symbol||'').toUpperCase()!==del.getAttribute('data-delete-stock').toUpperCase()));renderSavedStocks();return;}
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
  $('fundamentals').innerHTML=table([['Market Cap', d.market_cap],['Total Debt', d.total_debt],['Debt-to-Equity', d.debt_equity],['P/E', d.pe],['Price/Book (P/B)', d.pb],['Dividend Yield', d.dividend_yield],['ROE और ROCE', `ROE: ${d.roe} | ROCE: ${d.roce}`],['Sales Growth (3Y)', d.sales_growth],['Profit Growth (3Y)', d.profit_growth],['Profit Margin', d.profit_margin],['Debtor Days', d.debtor_days],['Business Summary', d.summary]]);
  $('shareholding').innerHTML=table([['Promoter Holding', d.promoter_holding],['Promoter Pledge', d.promoter_pledge],['FII Holding', d.fii_holding],['DII Holding', d.dii_holding],['Public Holding', d.public_holding]]);
  $('verdict').innerHTML=`<p><b>संकेत:</b> <span class="${d.score>=2?'green':d.score<=-2?'red':'gold'}">${esc(d.score_label)}</span></p><p>${esc(d.verdict)}</p>`;
  $('risks').innerHTML='<ul>'+d.risks.map(x=>'<li>'+esc(x)+'</li>').join('')+'</ul>';
  $('analysisNote').innerHTML=`<p><b>समग्र सारांश:</b> बंद भाव ${fmt(d.price)} है। ${esc(d.verdict)}</p>`;
 }catch(err){$('status').innerHTML='<div class="error">विश्लेषण नहीं हो पाया: '+esc(err.message)+'</div>'}
 finally{$('go').disabled=false;$('go').textContent='विश्लेषण करें ↗'}
});

// Active Stocks Fetcher
async function fetchActiveStocks(){
 $('active20Status').textContent = 'डेटा लोड हो रहा है...';
 try {
  const r = await fetch('/api/active-stocks');
  const d = await r.json();
  if(!r.ok || d.error) throw new Error(d.error || 'डेटा नहीं मिला');
  const list = d.data || [];
  if(!list.length){$('active20Body').innerHTML='<tr><td colspan="5" class="empty-row">कोई डेटा नहीं मिला।</td></tr>';return;}
  $('active20Body').innerHTML = list.map((x,i)=>`<tr>
   <td><span class="active-rank">${i+1}</span></td>
   <td><b>${esc(x.name||x.symbol)}</b><br><span class="tag">${esc(x.symbol)}</span></td>
   <td>₹${Number(x.close||0).toLocaleString('en-IN')}</td>
   <td class="${x.change_pct>=0?'green':'red'}">${x.change_pct>=0?'+':''}${Number(x.change_pct||0).toFixed(2)}%</td>
   <td><b>${Number(x.volume||0).toLocaleString('en-IN')}</b></td>
  </tr>`).join('');
  $('active20Status').textContent = `सफल: ${list.length} स्टॉक्स लोड हुए।`;
 } catch(e){
  $('active20Status').className = 'trade-status error';
  $('active20Status').textContent = 'त्रुटि: ' + e.message;
 }
}

// Demo Trading Logic
const DEMO_KEY='markeTGyan_demo_trade_v1';
function demoLoad(){try{const d=JSON.parse(localStorage.getItem(DEMO_KEY)||'null');return d||{cash:100000,positions:{},orders:[],trades:[],realized:0};}catch(e){return {cash:100000,positions:{},orders:[],trades:[],realized:0};}}
function demoSave(d){try{localStorage.setItem(DEMO_KEY,JSON.stringify(d));}catch(e){}}
async function fetchDemoQuote(){
 const sym = $('demoSymbol').value.trim();
 if(!sym) return;
 $('demoQuote').textContent = 'भाव प्राप्त हो रहा है...';
 try {
  const r = await fetch('/api/demo/quote?symbol='+encodeURIComponent(sym));
  const d = await r.json();
  if(!r.ok || d.error) throw new Error(d.error || 'भाव नहीं मिला');
  window.lastDemoPrice = d.price;
  $('demoQuote').textContent = `₹${d.price} (${d.as_of})`;
  renderDemoTrading();
 } catch(e){
  $('demoQuote').textContent = 'त्रुटि: ' + e.message;
 }
}
function submitDemoOrder(side){
 const sym = $('demoSymbol').value.trim().toUpperCase();
 const qty = parseInt($('demoQty').value);
 const price = window.lastDemoPrice || 100;
 if(!sym || isNaN(qty) || qty <= 0) { alert('मान्य symbol और quantity भरें'); return; }
 
 const d = demoLoad();
 const totalCost = qty * price;
 if(side === 'BUY' && d.cash < totalCost) { alert('पर्याप्त Cash नहीं है!'); return; }
 
 if(side === 'BUY'){
  d.cash -= totalCost;
  let p = d.positions[sym] || {qty:0, avg:0};
  p.avg = ((p.qty * p.avg) + totalCost) / (p.qty + qty);
  p.qty += qty;
  d.positions[sym] = p;
 } else {
  let p = d.positions[sym];
  if(!p || p.qty < qty) { alert('बेचने के लिए पर्याप्त शेयर नहीं हैं!'); return; }
  d.cash += qty * price;
  d.realized += (price - p.avg) * qty;
  p.qty -= qty;
  if(p.qty === 0) delete d.positions[sym];
  else d.positions[sym] = p;
 }
 
 d.orders.unshift({time: new Date().toLocaleString(), symbol: sym, side, type: 'MARKET', qty, price, status: 'EXECUTED'});
 demoSave(d);
 renderDemoTrading();
 alert('Order Executed Successfully!');
}
function renderDemoTrading(){
 const d = demoLoad();
 $('demoCash').textContent = '₹' + d.cash.toLocaleString('en-IN', {maximumFractionDigits:2});
 $('demoRealized').textContent = (d.realized>=0?'+':'-') + '₹' + Math.abs(d.realized).toLocaleString('en-IN', {maximumFractionDigits:2});
 $('demoRealized').className = d.realized >= 0 ? 'green' : 'red';
 
 let posRows = Object.keys(d.positions).map(sym => {
  let p = d.positions[sym];
  let ltp = window.lastDemoPrice || p.avg;
  let pnl = (ltp - p.avg) * p.qty;
  return `<tr><td><b>${sym}</b></td><td>${p.qty}</td><td>₹${p.avg.toFixed(2)}</td><td>₹${ltp.toFixed(2)}</td><td>₹${(p.qty*ltp).toFixed(2)}</td><td class="${pnl>=0?'green':'red'}">${pnl>=0?'+':''}${pnl.toFixed(2)}</td><td>-</td></tr>`;
 }).join('');
 $('demoTodayBody').innerHTML = posRows || '<tr><td colspan="7" class="empty-row">कोई खुली पोजीशन नहीं है।</td></tr>';
 
 $('demoBookBody').innerHTML = d.orders.map(o => `<tr>
  <td>${o.time}</td><td><b>${o.symbol}</b></td><td class="${o.side==='BUY'?'green':'red'}"><b>${o.side}</b></td><td>${o.type}</td><td>${o.qty}</td><td>₹${o.price}</td><td><span class="order-chip">${o.status}</span></td><td>-</td>
 </tr>`).join('') || '<tr><td colspan="8" class="empty-row">कोई ऑर्डर नहीं है।</td></tr>';
}
function resetDemo(){
 if(confirm('Demo account reset करना चाहते हैं?')){
  localStorage.removeItem(DEMO_KEY);
  renderDemoTrading();
 }
}
renderDemoTrading();

// Top Gainers / Losers Fetcher
async function fetchGainersLosers(){
 $('glStatus').textContent = 'डेटा लोड हो रहा है...';
 try {
  const r = await fetch('/api/gainers-losers');
  const d = await r.json();
  if(!r.ok || d.error) throw new Error(d.error || 'डेटा प्राप्त नहीं हुआ');
  
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
 } catch(e){
  $('glStatus').textContent = 'त्रुटि: ' + e.message;
 }
}

// Advance Screener Fetcher
async function fetchScreenerStatus(){
 try {
  const r = await fetch('/api/screener-status');
  const d = await r.json();
  $('progBar').style.width = (d.progress || 0) + '%';
  $('screenerStatus').textContent = `स्थिति: ${d.status} | प्रगति: ${d.completed}/${d.total} (${d.progress}%)`;
  
  const confBody = $('confirmedBody');
  if(!d.confirmed || !d.confirmed.length){
   confBody.innerHTML='<tr><td colspan="7" class="empty-row">कोई कन्फर्म सिग्नल नहीं मिला।</td></tr>';
  } else {
   confBody.innerHTML = d.confirmed.map(r=>`<tr>
    <td><b>${esc(r.symbol)}</b></td>
    <td class="${r.trend==='UPTREND'?'green':'red'}">${esc(r.trend)}</td>
    <td>₹${fmt(r.current)}</td>
    <td>₹${fmt(r.entry)}</td>
    <td class="red">₹${fmt(r.sl)}</td>
    <td class="green">₹${fmt(r.target)}</td>
    <td><span class="tag" style="background:${r.signal==='BUY VALID'?'#e7f6ee':'#fff0f0'};color:${r.signal==='BUY VALID'?'#18794e':'#c2414b'}">${esc(r.signal)}</span></td>
   </tr>`).join('');
  }

  const allBody = $('allStocksBody');
  if(!d.results || !d.results.length){
   allBody.innerHTML='<tr><td colspan="6" class="empty-row">डेटा उपलब्ध नहीं है।</td></tr>';
  } else {
   allBody.innerHTML = d.results.map(r=>`<tr>
    <td><b>${esc(r.symbol)}</b></td>
    <td>₹${fmt(r.current)}</td>
    <td class="${r.trend==='UPTREND'?'green':r.trend==='DOWNTREND'?'red':'sideways'}">${esc(r.trend)}</td>
    <td>₹${fmt(r.hh)}</td>
    <td>₹${fmt(r.hl)}</td>
    <td>${esc(r.status)}</td>
   </tr>`).join('');
  }
 } catch(e){}
}
async function triggerRescan(){
 try {
  const r = await fetch('/api/rescan', {method:'POST'});
  const d = await r.json();
  if(!d.success) alert(d.message);
  fetchScreenerStatus();
 } catch(e){alert('स्कैन शुरू करने में विफल: '+e); }
}
setInterval(fetchScreenerStatus, 5000);
</script>
</body>
</html>
"""

# =====================================================
# BACKEND PYTHON API ROUTES & LOGIC
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
        resp = session.get(f"https://www.screener.in/company/{clean_sym}/", headers=get_headers(), timeout=8)
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

def get_ai_verdict(score, symbol, currency):
    cur_txt = 'INR' if currency == 'INR' else currency
    if score >= 2: return f"AI Verdict ({cur_txt}): मजबूत सकारात्मक ट्रेंड। सपोर्ट पर पुलबैक पर BUY अवसर देखें।"
    if score >= 1: return f"AI Verdict ({cur_txt}): हल्का सकारात्मक ट्रेंड। सपोर्ट की तलाश में रहें।"
    if score <= -2: return f"AI Verdict ({cur_txt}): मजबूत कमजोरी। सपोर्ट टूटने पर SELL अवसर देखें।"
    if score <= -1: return f"AI Verdict ({cur_txt}): हल्की कमजोरी। रेजिस्टेंस पर SELL अवसर देखें।"
    return f"AI Verdict ({cur_txt}): ट्रेंड अनिश्चित। सपोर्ट/रेजिस्टेंस पर ही ट्रेड लें।"

def get_risk_checklist(data, info, score, currency):
    checklist = []
    if data.get('rsi') and data['rsi'] > 70: checklist.append(f"तकनीकी जोखिम: RSI ओवरबॉट क्षेत्र में है ({fmt(data['rsi'])}); नई खरीद में सावधानी।")
    if not checklist: checklist.append("अभी कोई बड़ा जोखिम तकनीकी या वित्तीय डेटा से स्पष्ट नहीं है।")
    return checklist

def analyze_symbol(raw):
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

    fundamentals = {
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
        "debt_equity": sc.get("debt to equity", clean_num(info.get("debtToEquity", 'उपलब्ध नहीं'))),
        "pe": sc.get("stock p/e", clean_num(info.get("trailingPE", 'उपलब्ध नहीं'))),
        "pb": sc.get("price to book", clean_num(info.get("priceToBook", 'उपलब्ध नहीं'))),
        "dividend_yield": f"{clean_num(info.get('dividendYield', 0)*100)}%",
        "roe": f"{clean_num(info.get('returnOnEquity', 0)*100)}%",
        "roce": 'उपलब्ध नहीं',
        "sales_growth": 'उपलब्ध नहीं',
        "profit_growth": 'उपलब्ध नहीं',
        "profit_margin": 'उपलब्ध नहीं',
        "debtor_days": 'उपलब्ध नहीं',
        "promoter_holding": f"{clean_num(info.get('heldPercentInsiders', 0)*100)}%",
        "promoter_pledge": '0.00%',
        "fii_holding": f"{clean_num(info.get('heldPercentInstitutions', 0)*100)}%",
        "dii_holding": 'उपलब्ध नहीं',
        "public_holding": 'उपलब्ध नहीं',
        "summary": sc.get("summary", info.get("longBusinessSummary", "")),
        "verdict": get_ai_verdict(score, symbol, info.get("currency", "INR")),
        "risks": get_risk_checklist({"rsi": rsi}, info, score, info.get("currency", "INR"))
    }
    return fundamentals

# Active Stocks API Route
@app.get("/api/active-stocks")
def api_active_stocks():
    results = []
    sample_symbols = STOCK_UNIVERSE[:30]
    for sym in sample_symbols:
        try:
            ticker_sym = sym if sym.endswith(".NS") else sym + ".NS"
            t = yf.Ticker(ticker_sym)
            hist = t.history(period="2d", interval="1d")
            if hist is not None and len(hist) >= 2:
                close = float(hist["Close"].iloc[-1])
                prev = float(hist["Close"].iloc[-2])
                vol = int(hist["Volume"].iloc[-1])
                chg = ((close / prev) - 1) * 100
                results.append({
                    "symbol": sym.replace(".NS", ""),
                    "name": t.info.get("longName", sym),
                    "close": round(close, 2),
                    "change_pct": round(chg, 2),
                    "volume": vol
                })
        except Exception:
            continue
    results.sort(key=lambda x: x["volume"], reverse=True)
    return safe_jsonify({"data": results[:15]})

# Demo Quote API Route
@app.get("/api/demo/quote")
def api_demo_quote():
    sym = request.args.get("symbol", "").strip().upper()
    if not sym: return safe_jsonify({"error": "Symbol आवश्यक है"}), 400
    ticker_sym = sym if sym.endswith(".NS") else sym + ".NS"
    try:
        t = yf.Ticker(ticker_sym)
        hist = t.history(period="1d")
        if hist.empty: raise ValueError("भाव नहीं मिला")
        price = float(hist["Close"].iloc[-1])
        return safe_jsonify({
            "symbol": sym.replace(".NS", ""),
            "price": round(price, 2),
            "as_of": datetime.now().strftime("%H:%M:%S")
        })
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 400

# Advanced Screener & Gainer/Loser Scan Logic
def run_background_scan():
    with SCAN_LOCK:
        if SCAN_STATE["running"]: return
        SCAN_STATE["running"] = True
        SCAN_STATE["status"] = "SCANNING"
        SCAN_STATE["completed"] = 0
        SCAN_STATE["total"] = len(STOCK_UNIVERSE[:50])

    all_res = []
    for idx, sym in enumerate(STOCK_UNIVERSE[:50]):
        try:
            ticker_sym = sym if sym.endswith(".NS") else sym + ".NS"
            t = yf.Ticker(ticker_sym)
            hist = t.history(period="30d", interval="1h")
            if hist is None or hist.empty or len(hist) < 10: continue
            
            current = float(hist["Close"].iloc[-1])
            prev_close = float(hist["Close"].iloc[0])
            p_change = ((current - prev_close) / prev_close) * 100
            
            d_sup = clean_num(hist["Low"].min())
            d_res = clean_num(hist["High"].max())
            
            all_res.append({
                "symbol": sym.replace(".NS", ""),
                "current": round(current, 2),
                "p_change": round(p_change, 2),
                "daily_sup": [d_sup],
                "daily_res": [d_res],
                "hourly_sup": [clean_num(hist["Low"].tail(10).min())],
                "hourly_res": [clean_num(hist["High"].tail(10).max())],
                "trend": "UPTREND" if p_change >= 0 else "DOWNTREND",
                "hh": d_res, "hl": d_sup,
                "entry": d_sup, "sl": d_sup * 0.98, "target": d_res * 1.02,
                "signal": "BUY VALID" if p_change > 1 else ("SELL VALID" if p_change < -1 else "WAIT"),
                "status": "Active Setup"
            })
        except Exception:
            pass
        
        with SCAN_LOCK:
            SCAN_STATE["completed"] = idx + 1
            SCAN_STATE["progress"] = round(((idx + 1) / SCAN_STATE["total"]) * 100, 1)

    all_res.sort(key=lambda x: x["p_change"], reverse=True)
    gainers = all_res[:5]
    losers = all_res[-5:]
    losers.reverse()

    with SCAN_LOCK:
        SCAN_STATE["results"] = all_res
        SCAN_STATE["confirmed"] = [r for r in all_res if r["signal"] in ["BUY VALID", "SELL VALID"]]
        SCAN_STATE["gainers"] = gainers
        SCAN_STATE["losers"] = losers
        SCAN_STATE["status"] = "COMPLETED"
        SCAN_STATE["running"] = False
        SCAN_STATE["last_scan"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

@app.get("/api/gainers-losers")
def api_gainers_losers():
    with SCAN_LOCK:
        return safe_jsonify({"gainers": SCAN_STATE["gainers"], "losers": SCAN_STATE["losers"]})

@app.get("/api/screener-status")
def api_screener_status():
    with SCAN_LOCK:
        return safe_jsonify(SCAN_STATE)

@app.post("/api/rescan")
def api_rescan():
    with SCAN_LOCK:
        if SCAN_STATE["running"]: return jsonify({"success": False, "message": "स्कैन पहले से चल रहा है।"})
    threading.Thread(target=run_background_scan, daemon=True).start()
    return jsonify({"success": True, "message": "नया स्कैन शुरू हो गया है।"})

@app.get("/")
def home():
    return render_template_string(PAGE)

@app.get("/api/analyze")
def api_analyze():
    symbol = request.args.get("symbol", "")
    try: return safe_jsonify(analyze_symbol(symbol))
    except Exception as e: return safe_jsonify({"error": str(e)}), 400

if __name__ == "__main__":
    threading.Thread(target=run_background_scan, daemon=True).start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)