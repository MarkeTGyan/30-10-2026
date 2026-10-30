import os
import math
import concurrent.futures
import time
import random
import re
import json
import threading
from auth_system import init_auth
from zoneinfo import ZoneInfo
from datetime import datetime, time as dtime, timedelta
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

PAGE = r"""
<!doctype html>
<html lang="hi">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no">
<title>MarkeTGyan, Technical Analysis Scanner</title>
<style>
:root{
 --bg:#f4f7fc;--panel:#ffffff;--line:#d8e2f0;--text:#1e293b;--muted:#64748b;
 --gold:#f59e0b;--green:#10b981;--red:#ef4444;--blue:#3b82f6;--teal:#0d9488;
 --soft-blue:#eff6ff;--soft-green:#ecfdf5;--soft-red:#fef2f2;--soft-gold:#fffbeb;
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{scroll-behavior:smooth}
body{margin:0;background:linear-gradient(135deg,#f8fafc 0%,#eef2f6 100%);font-family:system-ui,-apple-system,"Segoe UI",Roboto,Helvetica,Arial,"Noto Sans Devanagari",sans-serif;color:var(--text);font-size:15px;line-height:1.6}
main{width:100%;max-width:1300px;margin:0 auto;padding:clamp(8px, 2vw, 24px)}

/* PROFESSIONAL HEADER */
header{background:linear-gradient(135deg,#1e293b,#0f172a);color:#fff;padding:clamp(16px, 3vw, 28px) clamp(16px, 3vw, 32px);border-radius:16px;margin-bottom:16px;box-shadow:0 10px 25px -5px rgba(15,23,42,0.15)}
header .tag{display:inline-block;padding:4px 12px;background:rgba(255,255,255,0.1);color:#38bdf8;border-radius:99px;font-size:12px;font-weight:700;margin-bottom:8px;letter-spacing:0.5px}
h1{font-size:clamp(22px, 4vw, 36px);line-height:1.2;margin:4px 0 8px;color:#fff;font-weight:800}
.sub{color:#94a3b8;font-size:clamp(13px, 1.5vw, 15px);line-height:1.5}

/* PANELS & CARDS */
.panel{background:var(--panel);border:1px solid var(--line);border-radius:14px;padding:clamp(12px, 2vw, 22px);margin:14px 0;box-shadow:0 4px 12px rgba(0,0,0,0.03)}
.panel h2{color:#1e293b;font-size:clamp(17px, 2vw, 20px);margin:0 0 12px;display:flex;align-items:center;gap:8px;font-weight:700}
.panel h3{font-size:16px;font-weight:700;margin:12px 0 8px}

/* FORM & INPUTS */
form{display:flex;gap:10px;flex-wrap:wrap}
input,select{flex:1;min-width:min(200px, 100%);background:#f8fafc;color:var(--text);border:1.5px solid var(--line);border-radius:10px;padding:12px 14px;font-size:15px;outline:none;transition:all .2s}
input:focus,select:focus{border-color:var(--blue);background:#fff;box-shadow:0 0 0 3px rgba(59,130,246,0.12)}
button{border:0;border-radius:10px;padding:12px 20px;background:linear-gradient(135deg,#3b82f6,#2563eb);color:#fff;font-weight:700;font-size:15px;cursor:pointer;box-shadow:0 4px 12px rgba(37,99,235,0.2);transition:all .2s;white-space:nowrap}
button:hover{filter:brightness(1.05);transform:translateY(-1px)}button:active{transform:translateY(0)}button:disabled{opacity:.6;cursor:wait;transform:none}

/* METRICS GRID */
.grid{display:grid;grid-template-columns:repeat(auto-fit, minmax(150px, 1fr));gap:10px}
.metric{background:var(--soft-blue);border:1px solid #dbeafe;border-radius:12px;padding:12px 14px}
.metric small{display:block;color:var(--muted);font-size:12px;font-weight:600;margin-bottom:4px}
.metric strong{font-size:clamp(16px, 2vw, 21px);color:#1e3a8a;font-weight:800;overflow-wrap:anywhere}

/* UTILITY CLASSES */
.green{color:var(--green)!important}.red{color:var(--red)!important}.gold{color:var(--gold)!important}
.hint{font-size:12px;color:var(--muted);margin-top:8px;line-height:1.5}
.copy{line-height:1.7;color:#334155;font-size:14px}
.tag{display:inline-block;padding:3px 8px;border-radius:6px;background:var(--soft-blue);color:#1d4ed8;font-size:12px;font-weight:700}
.error{color:#991b1b;white-space:pre-wrap;background:var(--soft-red);padding:12px;border-radius:8px;font-size:14px}
.hidden{display:none}

/* PROFESSIONAL TABLES */
.table-wrap{width:100%;overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:10px;border:1px solid var(--line);margin:8px 0}
table{width:100%;border-collapse:collapse;white-space:nowrap;background:#fff}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid #f1f5f9;font-size:14px}
th{background:#f8fafc;color:#475569;font-weight:700;position:sticky;top:0;z-index:1}
tbody tr:hover{background:#f8fafc}
tbody tr:last-child td{border-bottom:0}

/* TABS CONTAINER - DESKTOP & MOBILE RESPONSIVE */
.tab-buttons{display:flex;gap:6px;padding:6px;background:#e2e8f0;border-radius:12px;overflow-x:auto;-webkit-overflow-scrolling:touch;margin-bottom:16px;box-shadow:inset 0 1px 2px rgba(0,0,0,0.05);white-space:nowrap}
.tab-btn{flex:0 0 auto;background:transparent;color:#475569;border:0;padding:10px 14px;border-radius:8px;cursor:pointer;font-weight:700;font-size:13px;transition:all .2s;box-shadow:none}
.tab-btn:hover{color:#1e293b;background:rgba(255,255,255,0.5)}
.tab-btn.active{background:#fff;color:#2563eb;box-shadow:0 2px 8px rgba(0,0,0,0.08)}
.tab-content{display:none}.tab-content.active{display:block;animation:fadeIn .2s ease-out}
@keyframes fadeIn{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}

/* SEBI DISCLAIMER */
.sebi-disclaimer{background:var(--soft-gold);border:1px solid #fde68a;border-left:4px solid var(--gold);padding:10px 14px;border-radius:8px;font-size:12px;color:#78350f;line-height:1.5;margin:12px 0}
.sebi-disclaimer b{color:#92400e}

/* TRADE TERMINAL SPECIFIC */
.trade-toolbar{display:grid;grid-template-columns:repeat(auto-fit, minmax(140px, 1fr));gap:10px;align-items:end}
.trade-field label{display:block;font-size:12px;font-weight:700;color:#475569;margin-bottom:4px}
.trade-actions{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.buy-btn{background:linear-gradient(135deg,#10b981,#059669)!important}
.sell-btn{background:linear-gradient(135deg,#ef4444,#dc2626)!important}
.neutral-btn{background:linear-gradient(135deg,#6366f1,#4f46e5)!important}
.trade-status{padding:10px;border-radius:8px;background:var(--soft-green);color:#065f46;font-weight:600;font-size:13px;word-break:break-all}
.trade-status.error{background:var(--soft-red);color:#991b1b}
.order-chip{display:inline-block;padding:2px 8px;border-radius:99px;font-size:11px;font-weight:700;background:var(--soft-blue);color:#1d4ed8}

/* ACTIVE RANK BADGE */
.active-rank{display:inline-grid;place-items:center;width:26px;height:26px;border-radius:8px;background:var(--soft-blue);color:#2563eb;font-weight:800;font-size:12px}
</style>
</head>
<body>
<main>
<header>
  <div class="tag">MarkeTGyan Professional Terminal</div>
  <h1>AI Stock Analyst & Screener</h1>
  <div class="sub">उन्नत तकनीकी विश्लेषण, सपोर्ट/रेजिस्टेंस, ऑटोमेटेड स्कैनर और वर्चुअल ट्रेडिंग टर्मिनल।</div>
</header>

<!-- RESPONSIVE TABS -->
<div class="tab-buttons" role="tablist">
  <button type="button" class="tab-btn active" data-tab-target="searchTab">🔎 विश्लेषण</button>
  <button type="button" class="tab-btn" data-tab-target="savedTab">🗂️ सेव्ड स्टॉक्स</button>
  <button type="button" class="tab-btn" data-tab-target="active20Tab">📈 Active Stocks</button>
  <button type="button" class="tab-btn" data-tab-target="demoTradeTab">💹 Demo Trade</button>
  <button type="button" class="tab-btn" data-tab-target="gainersLosersTab">📊 Gainers/Losers</button>
  <button type="button" class="tab-btn" data-tab-target="advanceScreenerTab">⚡ Screener</button>
</div>

<!-- TAB 1: Single Stock Analysis -->
<div id="searchTab" class="tab-content active">
  <div class="panel">
    <form id="stockForm"><input id="symbol" placeholder="NSE Symbol (जैसे: CIPLA, RELIANCE, TCS)" required autocomplete="off"><button id="go" type="submit">विश्लेषण करें ↗</button></form>
    <div class="hint">भारतीय शेयरों के लिए सही NSE Symbol दर्ज करें। खोजे गए स्टॉक्स ऑटोमैटिकली "सेव्ड स्टॉक्स" में सुरक्षित हो जाते हैं।</div>
  </div>
  <div id="status" class="panel hidden"></div>
  <div id="results" class="hidden">
    <div class="panel"><div id="company"></div><div id="metrics" class="grid" style="margin-top:12px"></div></div>
    <div class="panel"><h2>🧭 Trend और Momentum</h2><div id="trend" class="copy"></div></div>
    
    <div class="panel">
      <h2>📊 Daily Support/Resistance</h2>
      <div id="dailyLevels" class="table-wrap"></div>
      <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> MarkeTGyan SEBI-पंजीकृत सलाहकार नहीं है। स्तर केवल शैक्षिक अध्ययन के लिए हैं।</div>
    </div>
    
    <div class="panel">
      <h2>📋 डेली S/R आधारित Entry, Stop Loss और Target योजना</h2>
      <div id="dailyTradePlan" class="table-wrap"></div>
      <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> MarkeTGyan SEBI-पंजीकृत सलाहकार नहीं है। ट्रेड से पहले स्वयं जोखिम समझें।</div>
    </div>
    
    <div class="panel">
      <h2>⏱️ 1HR Support/Resistance</h2>
      <div id="hourlyLevels" class="table-wrap"></div>
      <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> MarkeTGyan SEBI-पंजीकृत सलाहकार नहीं है।</div>
    </div>
    
    <div class="panel">
      <h2>📋 1 घंटे के S/R आधारित Entry, Stop Loss और Target योजना</h2>
      <div id="hourlyTradePlan" class="table-wrap"></div>
      <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> MarkeTGyan SEBI-पंजीकृत सलाहकार नहीं है।</div>
    </div>

    <div class="panel">
      <h2>🏢 कंपनी की वित्तीय स्थिति (Financials)</h2>
      <div id="fundamentals" class="table-wrap"></div>
      <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> वित्तीय आँकड़े सार्वजनिक स्रोतों से हैं।</div>
    </div>
    
    <div class="panel">
      <h2>📊 Shareholding Pattern</h2>
      <div id="shareholding" class="table-wrap"></div>
      <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> शेयरहोल्डिंग डेटा केवल संदर्भ हेतु है।</div>
    </div>
    
    <div class="panel"><h2>🧠 AI- जैसा निष्कर्ष</h2><div id="verdict" class="copy"></div></div>
    <div class="panel"><h2>⚠️ Risk Checklist</h2><div id="risks" class="copy"></div></div>
    <div class="panel"><h2>📝 समग्र विश्लेषण नोट</h2><div id="analysisNote" class="copy"></div></div>
  </div>
</div>

<!-- TAB 2: Previously Analyzed Stocks -->
<div id="savedTab" class="tab-content">
  <div class="panel">
    <h2>🗂️ सहेजे गए स्टॉक्स (Saved Stocks)</h2>
    <p class="copy">आपके द्वारा पूर्व में विश्लेषण किए गए स्टॉक्स की सूची।</p>
    <div id="savedStockMessage" class="hint"></div>
    <div class="table-wrap"><table class="saved-table"><thead><tr><th>कंपनी / स्टॉक</th><th>अंतिम भाव</th><th>बदलाव %</th><th>Daily S/R</th><th>1HR S/R</th><th>ट्रेंड</th><th>समय</th><th>विकल्प</th></tr></thead><tbody id="savedStocksBody"><tr><td colspan="8" class="empty-row" style="text-align:center;padding:20px;color:var(--muted)">कोई स्टॉक सेव नहीं है।</td></tr></tbody></table></div>
    <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> व्यक्तिगत समीक्षा हेतु रिकॉर्ड।</div>
  </div>
</div>

<!-- TAB 3: Active Stocks -->
<div id="active20Tab" class="tab-content">
 <div class="panel">
  <h2>📈 Active Stocks Screener</h2>
  <p class="copy">बाजार के मानदंडों के आधार पर सक्रिय स्टॉक्स।</p>
  <div class="demo-subtabs tab-buttons" style="margin:10px 0">
   <button type="button" class="tab-btn active" data-active-subtab="activeCurrentPanel">आज के Active Stocks</button>
   <button type="button" class="tab-btn" data-active-subtab="activeHistoryPanel">📚 History</button>
  </div>
  <div id="activeCurrentPanel">
   <div class="trade-actions"><button id="refreshActive20" type="button" class="neutral-btn">↻ सूची अपडेट करें</button></div>
   <div id="active20Status" class="trade-status" style="margin-top:10px">सूची लोड करने के लिए बटन दबाएँ।</div>
   <div id="active20Time" class="hint" style="margin-top:6px"></div>
   <div class="table-wrap" style="margin-top:10px"><table class="active-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>Close ₹</th><th>Change %</th><th>Volume</th></tr></thead><tbody id="active20Body"><tr><td colspan="5" style="text-align:center;padding:20px;color:var(--muted)">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
   <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> स्क्रीनर परिणाम शैक्षिक अध्ययन के लिए हैं।</div>
  </div>
  <div id="activeHistoryPanel" style="display:none">
   <div class="trade-toolbar" style="margin-bottom:10px;">
    <div class="trade-field"><label>From Date</label><input type="date" id="historyFromDate"></div>
    <div class="trade-field"><label>To Date</label><input type="date" id="historyToDate"></div>
    <div class="trade-field"><label>Stock Filter</label><input type="text" id="historyStockSearch" placeholder="जैसे RELIANCE"></div>
    <div class="trade-actions">
      <button id="filterHistoryBtn" type="button" class="neutral-btn">🔍 Filter</button>
      <button id="clearHistoryFilterBtn" type="button" class="sell-btn">❌ Clear</button>
      <button id="refreshActiveHistory" type="button" class="neutral-btn">↻ Refresh</button>
    </div>
   </div>
   <div id="activeHistoryStatus" class="trade-status" style="margin-top:10px">तारीख के अनुसार लोड हो रहा है…</div>
   <div id="activeHistoryBody" style="margin-top:10px"><div style="text-align:center;padding:20px;color:var(--muted)">History उपलब्ध नहीं है।</div></div>
  </div>
 </div>
</div>

<!-- TAB 4: Paper Trading / Demo Trade -->
<div id="demoTradeTab" class="tab-content">
 <div class="panel">
  <h2>💹 Professional Demo Trading Terminal</h2>
  <div class="sebi-disclaimer" style="margin:4px 0 12px 0"><b>वर्चुअल ट्रेडिंग:</b> शुरुआती कैश ₹1,00,000। बाजार समय 9:15 से 3:30 IST। बाहर के समय AMO/PMO आर्डर मान्य हैं। भाव हर 3 सेकंड में अपडेट होते हैं।</div>
  <div class="grid" style="margin-top:12px">
   <div class="metric"><small>Available Cash</small><strong id="demoCash">₹1,00,000</strong></div>
   <div class="metric"><small>Portfolio Value</small><strong id="demoPortfolioValue">₹0</strong></div>
   <div class="metric"><small>Unrealized P&L</small><strong id="demoUnrealized">₹0</strong></div>
   <div class="metric"><small>Realized P&L</small><strong id="demoRealized">₹0</strong></div>
  </div>
 </div>
 <div class="panel">
  <h2>🧾 Place Order</h2>
  <div class="trade-toolbar">
   <div class="trade-field"><label>Symbol</label><input id="demoSymbol" placeholder="RELIANCE" value="RELIANCE" autocomplete="off"></div>
   <div class="trade-field"><label>Side</label><select id="demoSide"><option value="BUY">BUY</option><option value="SELL">SELL</option></select></div>
   <div class="trade-field"><label>Order Type</label><select id="demoOrderType"><option value="MARKET">Market</option><option value="LIMIT">Limit</option><option value="AMO">AMO (After Market)</option><option value="PMO">PMO (Post Market)</option></select></div>
   <div class="trade-field"><label>Quantity</label><input id="demoQty" type="number" min="1" value="1"></div>
   <div class="trade-field"><label>Limit Price (₹)</label><input id="demoLimitPrice" type="number" step="0.05" placeholder="मूल्य"></div>
   <div class="trade-field"><label>Live Quote</label><div id="demoQuote" class="trade-status" style="background:#f8fafc;color:#1e293b">भाव प्रतीक्षारत</div></div>
  </div>
  <div class="trade-actions">
    <button type="button" id="demoGetQuote" class="neutral-btn">↻ भाव देखें</button>
    <button type="button" id="demoBuy" class="buy-btn">BUY Order</button>
    <button type="button" id="demoSell" class="sell-btn">SELL Order</button>
    <button type="button" id="demoRefreshOrders" class="neutral-btn">Pending Orders जाँचें</button>
  </div>
  <div id="demoMessage" class="hint" style="margin-top:10px" aria-live="polite"></div>
 </div>
 
 <div class="panel">
   <h2>📍 Today's Positions</h2>
   <div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Qty</th><th>Avg. Price</th><th>LTP</th><th>Market Value</th><th>P&amp;L</th><th>Action</th></tr></thead><tbody id="demoTodayBody"><tr><td colspan="7" style="text-align:center;padding:15px;color:var(--muted)">कोई पोजीशन नहीं है।</td></tr></tbody></table></div>
 </div>
 
 <div class="panel">
   <h2>📊 Portfolio Open Holdings</h2>
   <div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Qty</th><th>Avg. Price</th><th>LTP</th><th>Market Value</th><th>P&amp;L</th><th>आज ट्रेड?</th></tr></thead><tbody id="demoPortfolioBody"><tr><td colspan="7" style="text-align:center;padding:15px;color:var(--muted)">Portfolio खाली है।</td></tr></tbody></table></div>
 </div>
 
 <div class="panel">
   <h2>📚 Order &amp; Trade Book</h2>
   <div class="table-wrap"><table><thead><tr><th>समय</th><th>Symbol</th><th>Side</th><th>Type</th><th>Qty</th><th>Price</th><th>Status</th><th>Realized P&amp;L</th></tr></thead><tbody id="demoBookBody"><tr><td colspan="8" style="text-align:center;padding:15px;color:var(--muted)">कोई ऑर्डर नहीं है।</td></tr></tbody></table></div>
   <div class="trade-actions" style="margin-top:12px"><button id="demoReset" type="button" class="sell-btn">Demo Reset करें</button></div>
 </div>
</div>

<!-- TAB 5: Top Gainers / Losers -->
<div id="gainersLosersTab" class="tab-content">
 <div class="panel">
  <h2>📊 Top 5 Gainers & Losers (Futures Universe)</h2>
  <div class="trade-actions"><button id="refreshGL" type="button" class="neutral-btn" onclick="fetchGainersLosers()">↻ अपडेट करें</button></div>
  <div id="glStatus" class="trade-status" style="margin-top:10px;background:#f8fafc;color:#1e293b">डेटा लोड करने के लिए बटन दबाएँ।</div>
  
  <h3 class="green" style="margin-top:16px;">🔥 Top 5 Gainers</h3>
  <div class="table-wrap"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>LTP</th><th>Change %</th><th>Daily S/R</th><th>1HR S/R</th></tr></thead><tbody id="gainersBody"><tr><td colspan="6" style="text-align:center;padding:15px;color:var(--muted)">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>

  <h3 class="red" style="margin-top:16px;">🧊 Top 5 Losers</h3>
  <div class="table-wrap"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>LTP</th><th>Change %</th><th>Daily S/R</th><th>1HR S/R</th></tr></thead><tbody id="losersBody"><tr><td colspan="6" style="text-align:center;padding:15px;color:var(--muted)">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
  <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> सूची केवल शैक्षिक अध्ययन हेतु है।</div>
 </div>
</div>

<!-- TAB 6: Advance Screener -->
<div id="advanceScreenerTab" class="tab-content">
 <div class="panel">
  <h2>⚡ Advance Dow Theory Screener</h2>
  <div class="trade-actions"><button type="button" class="neutral-btn" onclick="triggerRescan()">स्कैन शुरू करें</button></div>
  <div id="screenerStatus" class="trade-status" style="margin-top:10px;background:#f8fafc;color:#1e293b">स्थिति: तैयार</div>
  <div style="width:100%;height:10px;background:#e2e8f0;border-radius:99px;overflow:hidden;margin:10px 0"><div id="progBar" style="height:100%;width:0%;background:var(--blue);transition:width .3s"></div></div>
  
  <h3 class="green" style="margin-top:16px;">📈 Uptrend Confirmed Signals</h3>
  <div class="table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>ट्रेंड</th><th>भाव</th><th>Entry</th><th>SL</th><th>Target</th><th>Signal</th></tr></thead><tbody id="uptrendConfirmedBody"><tr><td colspan="7" style="text-align:center;padding:15px;color:var(--muted)">कोई सिग्नल नहीं मिला।</td></tr></tbody></table></div>

  <h3 class="red" style="margin-top:16px;">📉 Downtrend Confirmed Signals</h3>
  <div class="table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>ट्रेंड</th><th>भाव</th><th>Entry</th><th>SL</th><th>Target</th><th>Signal</th></tr></thead><tbody id="downtrendConfirmedBody"><tr><td colspan="7" style="text-align:center;padding:15px;color:var(--muted)">कोई सिग्नल नहीं मिला।</td></tr></tbody></table></div>

  <h3 style="margin-top:16px;">📋 सभी फ्यूचर स्टॉक्स की स्थिति</h3>
  <div class="table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>भाव</th><th>ट्रेंड</th><th>HH</th><th>HL</th><th>Status</th></tr></thead><tbody id="allStocksBody"><tr><td colspan="6" style="text-align:center;padding:15px;color:var(--muted)">डेटा लोड हो रहा है...</td></tr></tbody></table></div>
  <div class="sebi-disclaimer"><b>SEBI डिस्क्लेमर:</b> डेटा केवल शोध उद्देश्य के लिए है।</div>
 </div>
</div>

<footer>MarkeTGyan Professional Terminal • शैक्षिक उद्देश्य मात्र; यह SEBI-registered investment advice नहीं है।</footer>
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
 if(tabId==='active20Tab' && !window.active20Loaded)fetchActive20Stocks();
 if(tabId==='demoTradeTab')renderDemoTrading();
 if(tabId==='gainersLosersTab')fetchGainersLosers();
 if(tabId==='advanceScreenerTab')fetchScreenerStatus();
}
document.querySelectorAll('[data-tab-target]').forEach(btn=>btn.addEventListener('click',()=>switchTab(btn.getAttribute('data-tab-target'))));

const SAVED_STOCKS_KEY='markeTGyan_saved_stock_analyses_v1';
function readSavedStocks(){try{const raw=localStorage.getItem(SAVED_STOCKS_KEY);const parsed=raw?JSON.parse(raw):[];return Array.isArray(parsed)?parsed:[];}catch(e){return [];}}
function writeSavedStocks(items){try{localStorage.setItem(SAVED_STOCKS_KEY,JSON.stringify(items));$('savedStockMessage').textContent='';return true;}catch(e){return false;}}
function renderSavedStocks(){
 const items=readSavedStocks();
 if(!items.length){$('savedStocksBody').innerHTML='<tr><td colspan="8" class="empty-row" style="text-align:center;padding:20px;color:var(--muted)">अभी कोई स्टॉक सेव नहीं है।</td></tr>';return;}
 $('savedStocksBody').innerHTML=items.map((x,i)=>'<tr>'+
 '<td><b>'+esc(x.company||x.symbol)+'</b><br><span class="tag">'+esc(x.symbol)+'</span></td>'+
 '<td>'+esc(fmt(x.price,' '+(x.currency||'')))+'</td>'+
 '<td class="'+(Number(x.change_pct)>0?'green':Number(x.change_pct)<0?'red':'')+'">'+esc(fmt(x.change_pct,'%'))+'</td>'+
 '<td>'+esc(fmt(x.support,' '+(x.currency||'')))+' / '+esc(fmt(x.resistance,' '+(x.currency||'')))+'</td>'+
 '<td>'+esc(fmt(x.support_1hr,' '+(x.currency||'')))+' / '+esc(fmt(x.resistance_1hr,' '+(x.currency||'')))+'</td>'+
 '<td>'+esc(x.score_label||'उपलब्ध नहीं')+'</td><td>'+esc(x.as_of||x.saved_at||'उपलब्ध नहीं')+'</td>'+
 '<td><div style="display:flex;gap:4px"><button type="button" class="tab-btn" style="padding:4px 8px;background:#e0f2fe;color:#0369a1" data-open-stock="'+esc(x.symbol)+'">देखें</button><button type="button" class="tab-btn" style="padding:4px 8px;background:#fee2e2;color:#991b1b" data-delete-stock="'+esc(x.symbol)+'">Del</button></div></td></tr>').join('');
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
  return '<p class="error"> '+esc(timeframe)+' के लिए वैध सपोर्ट और रेजिस्टेंस उपलब्ध नहीं हैं।</p>';
 }
 const range=r-s, fmtP=v=>fmt(v,' '+cur);
 const buySL=s-range*0.25, buyT1=s+range*0.5, buyT2=r;
 const sellSL=r+range*0.25, sellT1=r-range*0.5, sellT2=s;
 const rows=[
  ['सेटअप','एंट्री ट्रिगर','Entry स्तर','Stop Loss','Target 1','Target 2'],
  ['BUY (Support से)','कीमत सपोर्ट के पास आए और कन्फर्मेशन मिले',fmtP(s),fmtP(buySL),fmtP(buyT1),fmtP(buyT2)],
  ['SELL (Resistance से)','कीमत रेजिस्टेंस के पास आए और कन्फर्मेशन मिले',fmtP(r),fmtP(sellSL),fmtP(sellT1),fmtP(sellT2)]
 ];
 return '<table><thead><tr>'+rows[0].map(x=>'<th>'+esc(x)+'</th>').join('')+'</tr></thead><tbody>'+rows.slice(1).map(row=>'<tr>'+row.map((x,i)=>'<td>'+(i===0?'<b>'+esc(x)+'</b>':esc(x))+'</td>').join('')+'</tr>').join('')+'</tbody></table>';
}

// Single Stock Search
$('stockForm').addEventListener('submit',async e=>{
 e.preventDefault(); const sym=$('symbol').value.trim(); if(!sym)return;
 $('go').disabled=true;$('go').textContent='जाँच रहा है…';$('results').classList.add('hidden');$('status').classList.remove('hidden');$('status').innerHTML='DATA FETCHING...';
 try{
  const r=await fetch('/api/analyze?symbol='+encodeURIComponent(sym));const d=await r.json();
  if(!r.ok)throw new Error(d.error||'डेटा प्राप्त नहीं हुआ');
  saveStockAnalysis(d);
  $('status').classList.add('hidden');$('results').classList.remove('hidden');
  $('company').innerHTML=`<h2 style="margin:0">${esc(d.company)} <span class="tag">${esc(d.symbol)}</span></h2><div class="sub">डेटा समय: ${esc(d.as_of)}</div>`;
  $('metrics').innerHTML=[
   metric('अंतिम बंद भाव',fmt(d.price,d.currency==='INR'?' ₹':' '+(d.currency||''))),
   metric('आज का बदलाव',fmt(d.change_pct,'%'),d.change_pct>=0?'green':'red'),
   metric('RSI (14)',fmt(d.rsi),d.rsi<30?'gold':d.rsi>70?'red':''),
   metric('ट्रेंड स्कोर',esc(d.score_label),d.score>=2?'green':d.score<=-2?'red':'gold'),
   metric('52-सप्ताह उच्च',fmt(d.high52,' '+(d.currency||''))),
   metric('52-सप्ताह निम्न',fmt(d.low52,' '+(d.currency||'')))
  ].join('');
  $('trend').innerHTML=`<p><b>तकनीकी स्थिति:</b> ${esc(d.trend)}</p><p><b>मूविंग एवरेज:</b> ${esc(d.ma_comment)}</p><p><b>RSI:</b> ${esc(d.rsi_comment)}</p>`;
  
  $('dailyLevels').innerHTML=table([
    ['सपोर्ट (Daily)', fmt(d.support,' '+(d.currency||''))],
    ['रेजिस्टेंस (Daily)', fmt(d.resistance,' '+(d.currency||''))],
    ['20 EMA', fmt(d.ema20,' '+(d.currency||''))],
    ['50 SMA', fmt(d.sma50,' '+(d.currency||''))],
    ['200 SMA', fmt(d.sma200,' '+(d.currency||''))]
  ]);

  $('hourlyLevels').innerHTML=table([
    ['सपोर्ट (1HR)', fmt(d.support_1hr,' '+(d.currency||''))],
    ['रेजिस्टेंस (1HR)', fmt(d.resistance_1hr,' '+(d.currency||''))]
  ]);
  $('dailyTradePlan').innerHTML=tradePlanTable(d.support,d.resistance,d.currency,'Daily');
  $('hourlyTradePlan').innerHTML=tradePlanTable(d.support_1hr,d.resistance_1hr,d.currency,'1HR');
  
  $('fundamentals').innerHTML=table([
    ['Market Cap', d.market_cap],
    ['Reserves & Surplus', d.reserves],
    ['Total Debt', d.total_debt],
    ['Debt-to-Equity', d.debt_equity],
    ['P/E', d.pe],
    ['Price/Book (P/B)', d.pb],
    ['ROE / ROCE', `ROE: ${d.roe} | ROCE: ${d.roce}`],
    ['Profit Margin', d.profit_margin],
    ['Business Summary', d.summary]
  ]);

  $('shareholding').innerHTML=table([
    ['Promoter Holding', d.promoter_holding],
    ['FII Holding', d.fii_holding],
    ['DII Holding', d.dii_holding],
    ['Public Holding', d.public_holding],
    ['Promoter Pledge', d.promoter_pledge]
  ]);

  $('verdict').innerHTML=`<p><b>संकेत:</b> <span class="${d.score>=2?'green':d.score<=-2?'red':'gold'}">${esc(d.score_label)}</span></p><p>${esc(d.verdict)}</p>`;
  $('risks').innerHTML='<ul>'+d.risks.map(x=>'<li>'+esc(x)+'</li>').join('')+'</ul>';
  $('analysisNote').innerHTML=`<p><b>समग्र निष्कर्ष:</b> ${esc(d.verdict)}। तकनीकी संकेतकों और सपोर्ट-रेजिस्टेंस के आधार पर ट्रेड की योजना बनाएं।</p>`;
 }catch(err){$('status').innerHTML='<div class="error">विश्लेषण नहीं हो पाया: '+esc(err.message)+'</div>'}
 finally{$('go').disabled=false;$('go').textContent='विश्लेषण करें ↗'}
});

let active20Loaded=false;
let globalActiveHistory = [];

function fmtINR(v){return '₹'+Number(v||0).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2})}
function fmtVol(v){return Number(v||0).toLocaleString('en-IN',{maximumFractionDigits:0})}
function renderActive20(rows){
 const body=$('active20Body');
 if(!Array.isArray(rows)||!rows.length){body.innerHTML='<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--muted)">कोई परिणाम उपलब्ध नहीं है।</td></tr>';return;}
 body.innerHTML=rows.map((x,i)=>{const ch=Number(x.change_pct);return `<tr><td><span class="active-rank">${i+1}</span></td><td><b>${esc(x.name||x.symbol||'—')}</b><br><span class="tag">${esc(x.symbol||'')}</span></td><td>${fmtINR(x.close)}</td><td class="${ch>0?'green':ch<0?'red':''}">${Number.isFinite(ch)?(ch>0?'+':'')+ch.toFixed(2)+'%':'—'}</td><td><b>${fmtVol(x.volume)}</b></td></tr>`}).join('');
}
async function fetchActive20Stocks(){
 const b=$('refreshActive20');b.disabled=true;$('active20Status').className='trade-status';$('active20Status').textContent='सक्रिय सूची प्राप्त की जा रही है…';
 try{
  const r=await fetch('/api/screener/active20?refresh=1',{headers:{'Accept':'application/json'}});
  const raw=await r.text();let d;try{d=JSON.parse(raw)}catch(e){throw new Error('सर्वर का उत्तर JSON नहीं है।')}
  if(!r.ok||d.error)throw new Error(d.error||'डेटा प्राप्त नहीं हुआ');
  renderActive20(d.data||[]);
  $('active20Status').textContent=`सफल: ${(d.data||[]).length} स्टॉक मिले।`;
  $('active20Time').textContent='अंतिम अपडेट: '+(d.updated_at||new Date().toLocaleString('en-IN'));
  window.active20Loaded=true;
 }catch(e){
  $('active20Status').className='trade-status error';
  $('active20Status').textContent='त्रुटि: '+e.message;
  $('active20Body').innerHTML='<tr><td colspan="5" style="text-align:center;padding:20px;color:var(--muted)">डेटा उपलब्ध नहीं है।</td></tr>';
 }finally{b.disabled=false;}
}
$('refreshActive20').addEventListener('click',fetchActive20Stocks);

document.querySelectorAll('[data-active-subtab]').forEach(b=>b.addEventListener('click',()=>{
 const panelId=b.getAttribute('data-active-subtab');
 $('activeCurrentPanel').style.display=panelId==='activeCurrentPanel'?'block':'none';
 $('activeHistoryPanel').style.display=panelId==='activeHistoryPanel'?'block':'none';
 document.querySelectorAll('[data-active-subtab]').forEach(x=>x.classList.toggle('active',x===b));
 if(panelId==='activeHistoryPanel')fetchActiveHistory();
}));

function renderActiveHistory(items){
 const root=$('activeHistoryBody');
 const fromDate=$('historyFromDate').value, toDate=$('historyToDate').value, searchTerm=$('historyStockSearch').value.trim().toUpperCase();
 let filtered=items.map(day=>{
  let rows=Array.isArray(day.data)?day.data:[];
  if(searchTerm)rows=rows.filter(x=>String(x.symbol||'').toUpperCase().includes(searchTerm)||String(x.name||'').toUpperCase().includes(searchTerm));
  return {...day,data:rows};
 });
 if(fromDate)filtered=filtered.filter(day=>day.date>=fromDate);
 if(toDate)filtered=filtered.filter(day=>day.date<=toDate);
 if(!filtered.length){root.innerHTML='<div style="text-align:center;padding:20px;color:var(--muted)">कोई रिकॉर्ड नहीं मिला।</div>';return;}
 root.innerHTML=filtered.map(day=>{
  const rows=Array.isArray(day.data)?day.data:[];
  if(!rows.length && searchTerm)return '';
  const tableRows=rows.map((x,i)=>{const ch=Number(x.change_pct);return `<tr><td>${i+1}</td><td><b>${esc(x.name||x.symbol||'—')}</b><br><span class="tag">${esc(x.symbol||'')}</span></td><td>${fmtINR(x.close)}</td><td class="${ch>0?'green':ch<0?'red':''}">${Number.isFinite(ch)?(ch>0?'+':'')+ch.toFixed(2)+'%':'—'}</td><td>${fmtVol(x.volume)}</td></tr>`}).join('');
  return `<div class="panel" style="margin:10px 0"><h3>📅 ${esc(day.date)}</h3><div style="font-size:12px;color:var(--muted);margin-bottom:8px">सेव समय: ${esc(day.saved_at)}</div><div class="table-wrap"><table><thead><tr><th>Rank</th><th>स्टॉक</th><th>Close ₹</th><th>Change %</th><th>Volume</th></tr></thead><tbody>${tableRows||'<tr><td colspan="5" style="text-align:center;padding:10px;color:var(--muted)">डेटा नहीं है</td></tr>'}</tbody></table></div></div>`;
 }).join('');
}

async function fetchActiveHistory(){
 $('activeHistoryStatus').textContent='History लोड हो रही है…';
 try{
  const r=await fetch('/api/screener/active-stocks/history');const d=await r.json();
  if(!r.ok||d.error)throw new Error(d.error||'त्रुटि');
  globalActiveHistory=d.history||[];
  renderActiveHistory(globalActiveHistory);
  $('activeHistoryStatus').textContent=`History उपलब्ध: ${globalActiveHistory.length} तारीखें।`;
 }catch(e){$('activeHistoryStatus').className='trade-status error';$('activeHistoryStatus').textContent='त्रुटि: '+e.message;}
}
$('refreshActiveHistory')?.addEventListener('click',fetchActiveHistory);
$('filterHistoryBtn')?.addEventListener('click',()=>renderActiveHistory(globalActiveHistory));
$('clearHistoryFilterBtn')?.addEventListener('click',()=>{
 $('historyFromDate').value='';$('historyToDate').value='';$('historyStockSearch').value='';
 renderActiveHistory(globalActiveHistory);
});

const DEMO_KEY='markeTGyan_demo_trade_v1';
const DEMO_START_CASH=100000;
let demoQuotes={};
function demoLoad(){try{const d=JSON.parse(localStorage.getItem(DEMO_KEY)||'null');if(d&&typeof d==='object')return {...{cash:DEMO_START_CASH,positions:{},orders:[],trades:[],realized:0},...d};}catch(e){}return {cash:DEMO_START_CASH,positions:{},orders:[],trades:[],realized:0};}
function demoSave(d){try{localStorage.setItem(DEMO_KEY,JSON.stringify(d));}catch(e){}}
function demoDateKey(v){const d=new Date(v||Date.now());return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;}
function demoQuoteFor(sym){return demoQuotes[sym]||0;}
let demoQuoteTimer=null, demoQuoteRequest=0;
async function getDemoQuote(symbol,quiet=false){
 const query=String(symbol||$('demoSymbol').value||'').trim();if(query.length<2)throw new Error('कम-से-कम 2 अक्षर लिखें।');
 const requestId=++demoQuoteRequest;
 if(!quiet){$('demoQuote').textContent='भाव लोड हो रहा है…';$('demoQuote').className='trade-status';$('demoMessage').textContent='';}
 const r=await fetch('/api/demo/quote?symbol='+encodeURIComponent(query));const d=await r.json();if(requestId!==demoQuoteRequest)throw new Error('रद्द किया गया');if(!r.ok||d.error||!(Number(d.price)>0))throw new Error(d.error||'भाव नहीं मिला');
 const sym=String(d.symbol||query).toUpperCase().replace(/\.NS$/,'');demoQuotes[sym]=Number(d.price);$('demoSymbol').value=sym;$('demoQuote').textContent=fmtINR(d.price);$('demoQuote').className='trade-status';renderDemoTrading();return {symbol:sym,price:Number(d.price)};
}
$('demoSymbol')?.addEventListener('input',()=>{
 clearTimeout(demoQuoteTimer);demoQuoteRequest++;const q=$('demoSymbol').value.trim();
 if(q.length<2)return;
 demoQuoteTimer=setTimeout(async()=>{try{await getDemoQuote(q,true);}catch(e){}},800);
});
$('demoGetQuote')?.addEventListener('click',async()=>{try{await getDemoQuote();}catch(e){$('demoQuote').textContent=e.message;$('demoQuote').className='trade-status error';}});
$('demoOrderType')?.addEventListener('change',()=>{$('demoLimitPrice').disabled=$('demoOrderType').value==='MARKET';if($('demoOrderType').value==='MARKET')$('demoLimitPrice').value='';});

function demoExecute(d,order,price){
 const sym=order.symbol,qty=Number(order.qty);let p=d.positions[sym]||{qty:0,avg:0,openedAt:'',lastTradeDate:''};let realized=0;
 if(order.side==='BUY'){
  const cost=qty*price;if(cost>d.cash+0.00001)throw new Error('पर्याप्त Available Cash नहीं है।');
  p.avg=((p.qty*p.avg)+(qty*price))/(p.qty+qty);p.qty+=qty;p.openedAt=p.openedAt||new Date().toISOString();p.lastTradeDate=new Date().toISOString();d.cash-=cost;
 }else{
  if(p.qty<qty)throw new Error(`पर्याप्त शेयर नहीं हैं। उपलब्ध: ${p.qty}`);
  realized=(price-p.avg)*qty;d.cash+=qty*price;d.realized+=realized;p.qty-=qty;p.lastTradeDate=new Date().toISOString();if(p.qty===0){p.avg=0;p.openedAt='';}
 }
 if(p.qty>0)d.positions[sym]=p;else delete d.positions[sym];
 order.status='EXECUTED';order.executedPrice=price;order.executedAt=new Date().toISOString();order.realized=realized;
 d.trades.unshift({...order,price,realized,executedAt:order.executedAt});
}

async function submitDemoOrder(side){
 const btn=side==='BUY'?$('demoBuy'):$('demoSell');btn.disabled=true;
 try{
  const quote=await getDemoQuote($('demoSymbol').value,true);const qty=Number($('demoQty').value);if(!Number.isInteger(qty)||qty<1)throw new Error('अमान्य Quantity');
  const type=$('demoOrderType').value,limit=Number($('demoLimitPrice').value);
  if((type==='LIMIT'||type==='AMO'||type==='PMO')&&(!Number.isFinite(limit)||limit<=0))throw new Error('वैध Limit Price दर्ज करें।');
  const d=demoLoad();
  if(side==='SELL' && Number((d.positions[quote.symbol]||{}).qty||0)<qty)throw new Error('पर्याप्त शेयर नहीं हैं।');
  
  const order={id:'D'+Date.now(),symbol:quote.symbol,side,type,qty,price:type==='MARKET'?quote.price:limit,status:'PENDING',createdAt:new Date().toISOString(),realized:0};
  const nowIST=new Date(new Date().toLocaleString("en-US",{timeZone:"Asia/Kolkata"})), curVal=nowIST.getHours()*60+nowIST.getMinutes();
  const isMarketOpen = curVal>=555 && curVal<=930; // 9:15 AM to 3:30 PM

  if(type==='MARKET' && isMarketOpen){
   demoExecute(d,order,quote.price);d.orders.unshift(order);demoSave(d);
   $('demoMessage').className='trade-status';$('demoMessage').textContent=`DEMO ${side} EXECUTED @ ${fmtINR(quote.price)}`;
  } else {
   order.status='PENDING';d.orders.unshift(order);demoSave(d);
   $('demoMessage').className='trade-status';$('demoMessage').textContent=`${type} Order सफलतापूर्वक दर्ज हुआ।`;
  }
  renderDemoTrading();
 }catch(e){$('demoMessage').className='trade-status error';$('demoMessage').textContent=e.message;}
 finally{btn.disabled=false;}
}
$('demoBuy')?.addEventListener('click',()=>submitDemoOrder('BUY'));
$('demoSell')?.addEventListener('click',()=>submitDemoOrder('SELL'));

async function checkDemoLimits(){
 const d=demoLoad(), pending=d.orders.filter(o=>o.status==='PENDING');
 if(!pending.length){$('demoMessage').textContent='कोई Pending Order नहीं है।';return;}
 let executed=0;
 for(const o of pending){
  try{
   const q=await getDemoQuote(o.symbol,true);
   let ok=false;
   if(o.side==='BUY' && q.price<=Number(o.price))ok=true;
   if(o.side==='SELL' && q.price>=Number(o.price))ok=true;
   if(o.type==='MARKET')ok=true;
   if(ok){demoExecute(d,o,q.price);executed++;}
  }catch(e){}
 }
 demoSave(d);renderDemoTrading();
 $('demoMessage').textContent=`जाँचे गए। Execute हुए: ${executed}`;
}
$('demoRefreshOrders')?.addEventListener('click',checkDemoLimits);

setInterval(async()=>{
 const d=demoLoad();
 for(const sym of Object.keys(d.positions)){
  try{
   const r=await fetch('/api/demo/quote?symbol='+encodeURIComponent(sym));const res=await r.json();
   if(res&&Number(res.price)>0)demoQuotes[sym]=Number(res.price);
  }catch(e){}
 }
 renderDemoTrading();
},3000);

function demoRenderPositionRows(positions,todayOnly){
 const syms=Object.keys(positions).filter(s=>!todayOnly||demoDateKey(positions[s].lastTradeDate)===demoDateKey());
 if(!syms.length)return `<tr><td colspan="7" style="text-align:center;padding:15px;color:var(--muted)">${todayOnly?'आज कोई पोजीशन नहीं है।':'कोई holding नहीं है।'}</td></tr>`;
 return syms.map(sym=>{const p=positions[sym],ltp=demoQuoteFor(sym)||p.avg,mv=p.qty*ltp,pnl=(ltp-p.avg)*p.qty;return `<tr><td><b>${esc(sym)}</b></td><td>${p.qty}</td><td>${fmtINR(p.avg)}</td><td>${fmtINR(ltp)}</td><td>${fmtINR(mv)}</td><td class="${pnl>=0?'green':'red'}"><b>${pnl>=0?'+':''}${fmtINR(pnl)}</b></td><td>${todayOnly?`<button type="button" class="tab-btn" style="padding:4px 8px;background:#fee2e2;color:#991b1b" data-demo-sell="${esc(sym)}">SELL</button>`:'—'}</td></tr>`}).join('');
}
function renderDemoTrading(){
 const d=demoLoad();let mv=0,unrealized=0;
 Object.entries(d.positions).forEach(([sym,p])=>{const ltp=demoQuoteFor(sym)||p.avg;mv+=p.qty*ltp;unrealized+=(ltp-p.avg)*p.qty;});
 $('demoCash').textContent=fmtINR(d.cash);$('demoPortfolioValue').textContent=fmtINR(mv);
 $('demoUnrealized').textContent=(unrealized>=0?'+':'')+fmtINR(unrealized);$('demoUnrealized').className=unrealized>=0?'green':'red';
 $('demoRealized').textContent=(d.realized>=0?'+':'')+fmtINR(d.realized);$('demoRealized').className=d.realized>=0?'green':'red';
 $('demoTodayBody').innerHTML=demoRenderPositionRows(d.positions,true);
 $('demoPortfolioBody').innerHTML=demoRenderPositionRows(d.positions,false);
 $('demoBookBody').innerHTML=d.orders.length?d.orders.slice(0,50).map(o=>`<tr><td>${esc(new Date(o.executedAt||o.createdAt).toLocaleTimeString())}</td><td><b>${esc(o.symbol)}</b></td><td class="${o.side==='BUY'?'green':'red'}"><b>${o.side}</b></td><td>${o.type}</td><td>${o.qty}</td><td>${fmtINR(o.executedPrice||o.price)}</td><td><span class="order-chip">${o.status}</span></td><td class="${Number(o.realized)>=0?'green':'red'}">${o.status==='EXECUTED'?fmtINR(o.realized):'—'}</td></tr>`).join(''):'<tr><td colspan="8" style="text-align:center;padding:15px;color:var(--muted)">कोई ऑर्डर नहीं है।</td></tr>';
}
$('demoTodayBody')?.addEventListener('click',e=>{const b=e.target.closest('[data-demo-sell]');if(!b)return;$('demoSymbol').value=b.getAttribute('data-demo-sell');$('demoSide').value='SELL';});
$('demoReset')?.addEventListener('click',()=>{if(!confirm('रीसेट करें?'))return;localStorage.removeItem(DEMO_KEY);renderDemoTrading();});
renderDemoTrading();

async function fetchGainersLosers(){
 $('glStatus').textContent='लोड हो रहा है...';
 try{
  const r=await fetch('/api/gainers-losers');const d=await r.json();
  const renderRows=list=>(!list||!list.length)?'<tr><td colspan="6" style="text-align:center;padding:15px;color:var(--muted)">डेटा नहीं है</td></tr>':list.map((x,i)=>`<tr><td><span class="active-rank">${i+1}</span></td><td><b>${esc(x.symbol)}</b></td><td>₹${fmt(x.current)}</td><td class="${x.p_change>=0?'green':'red'}"><b>${x.p_change>=0?'+':''}${fmt(x.p_change)}%</b></td><td>${esc((x.daily_sup||[]).map(v=>Math.round(v)).join(', ')||'-')} / ${esc((x.daily_res||[]).map(v=>Math.round(v)).join(', ')||'-')}</td><td>${esc((x.hourly_sup||[]).map(v=>Math.round(v)).join(', ')||'-')} / ${esc((x.hourly_res||[]).map(v=>Math.round(v)).join(', ')||'-')}</td></tr>`).join('');
  $('gainersBody').innerHTML=renderRows(d.gainers);$('losersBody').innerHTML=renderRows(d.losers);
  $('glStatus').textContent='अपडेटेड: '+new Date().toLocaleTimeString();
 }catch(e){$('glStatus').textContent='त्रुटि: '+e.message;}
}

async function fetchScreenerStatus(){
 try{
  const r=await fetch('/api/screener-status');const d=await r.json();
  $('progBar').style.width=(d.progress||0)+'%';
  $('screenerStatus').textContent=`स्थिति: ${d.status} | प्रगति: ${d.completed}/${d.total} (${d.progress}%)`;
  const renderConf=list=>(!list||!list.length)?'<tr><td colspan="7" style="text-align:center;padding:15px;color:var(--muted)">कोई सिग्नल नहीं</td></tr>':list.map(r=>`<tr><td><b>${esc(r.symbol)}</b></td><td class="${r.trend==='UPTREND'?'green':'red'}">${esc(r.trend)}</td><td>₹${fmt(r.current)}</td><td>₹${fmt(r.entry)}</td><td class="red">₹${fmt(r.sl)}</td><td class="green">₹${fmt(r.target)}</td><td><span class="tag">${esc(r.signal)}</span></td></tr>`).join('');
  $('uptrendConfirmedBody').innerHTML=renderConf((d.confirmed||[]).filter(x=>x.trend==='UPTREND'));
  $('downtrendConfirmedBody').innerHTML=renderConf((d.confirmed||[]).filter(x=>x.trend==='DOWNTREND'));
  if(d.results&&d.results.length){
   $('allStocksBody').innerHTML=d.results.map(r=>`<tr><td><b>${esc(r.symbol)}</b></td><td>₹${fmt(r.current)}</td><td class="${r.trend==='UPTREND'?'green':'red'}">${esc(r.trend)}</td><td>₹${fmt(r.hh)}</td><td>₹${fmt(r.hl)}</td><td>${esc(r.status)}</td></tr>`).join('');
  }
 }catch(e){}
}
async function triggerRescan(){try{await fetch('/api/rescan',{method:'POST'});fetchScreenerStatus();}catch(e){}}
setInterval(fetchScreenerStatus,5000);
</script>
</body>
</html>
"""

# =====================================================
# PYTHON BACKEND & API ROUTES
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

def calculate_rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -1 * delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()
    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def extract_screener_complete_data(symbol):
    clean_sym = symbol.replace(".NS", "").replace(".BO", "").strip().upper()
    data = {}
    try:
        resp = session.get(f"https://www.screener.in/company/{clean_sym}/", headers=get_headers(), timeout=10)
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            for item in soup.select("ul#top-ratios li"):
                name = item.select_one(".name")
                val = item.select_one(".number")
                span_val = item.select_one("span:not(.name)")
                if name:
                    key_text = name.text.strip().lower().replace(":", "").strip()
                    val_text = val.text.strip() if val else (span_val.text.strip() if span_val else "")
                    if val_text: data[key_text] = val_text
            for row in soup.find_all("tr"):
                cols = row.find_all(["th", "td"])
                if len(cols) >= 2:
                    key = cols[0].text.strip().lower().replace(":", "").strip()
                    vals = [c.text.strip() for c in cols[1:] if c.text.strip()]
                    if vals:
                        val = vals[-1]
                        data[key] = val
                        if "reserves" in key: data["reserves"] = val
                        elif "borrowings" in key or "debt" in key: data["debt"] = val
                        elif "sales" in key or "revenue" in key: data["sales"] = val
                        elif "net profit" in key or "profit after tax" in key: data["net profit"] = val
            for table in soup.find_all("table"):
                table_text = table.get_text().lower()
                if "promoter" in table_text or "fii" in table_text or "institutions" in table_text:
                    for row in table.find_all("tr"):
                        cols = row.find_all(["td", "th"])
                        if len(cols) >= 2:
                            t = cols[0].text.strip().lower()
                            v = cols[-1].text.strip()
                            if "promoter" in t: data["promoter holding"] = v
                            elif "fii" in t or "foreign" in t: data["fii holding"] = v
                            elif "dii" in t or "domestic" in t: data["dii holding"] = v
                            elif "public" in t: data["public holding"] = v
                            elif "pledged" in t or "pledge" in t: data["pledged percentage"] = v
            about_div = soup.find('div', class_='about')
            if about_div: data['about'] = about_div.text.strip()
    except Exception: pass
    return data

def get_field(data, keys):
    for k in keys:
        k_lower = k.lower()
        if k_lower in data and data[k_lower] not in [None, "", "0"]:
            return data[k_lower]
    return None

def analyze_symbol(raw):
    symbol = raw.strip().upper().replace(" ", "")
    if not symbol: raise ValueError("कृपया स्टॉक का नाम या symbol लिखें।")
    ticker_symbol = symbol + ".NS" if "." not in symbol and not symbol.startswith("^") else symbol
    ticker = yf.Ticker(ticker_symbol)
    
    hist = ticker.history(period="1y", interval="1d", auto_adjust=True)
    if hist is None or hist.empty or len(hist) < 30: raise ValueError(f"{symbol} का पर्याप्त डेटा नहीं मिला।")
    
    hist_1hr = ticker.history(period="30d", interval="60m")
    close = hist["Close"].astype(float)
    price = float(close.iloc[-1])
    prev = float(close.iloc[-2]) if len(close)>1 else price
    change_pct = ((price/prev)-1)*100 if prev else 0
    
    ema20_series = close.ewm(span=20, adjust=False).mean()
    sma50_series = close.rolling(window=50).mean()
    sma200_series = close.rolling(window=200).mean()
    rsi_series = calculate_rsi(close, 14)
    
    val_ema20 = clean_num(ema20_series.iloc[-1]) if not ema20_series.empty else None
    val_sma50 = clean_num(sma50_series.iloc[-1]) if not sma50_series.empty else None
    val_sma200 = clean_num(sma200_series.iloc[-1]) if not sma200_series.empty else None
    val_rsi = clean_num(rsi_series.iloc[-1]) if not rsi_series.empty else 50.0
    
    info = ticker.get_info() or {}
    sc = extract_screener_complete_data(symbol)

    support = clean_num(hist["Low"].tail(20).min())
    resistance = clean_num(hist["High"].tail(20).max())

    if hist_1hr is not None and not hist_1hr.empty:
        support_1hr = clean_num(hist_1hr["Low"].tail(20).min())
        resistance_1hr = clean_num(hist_1hr["High"].tail(20).max())
    else:
        support_1hr = support
        resistance_1hr = resistance

    market_cap = get_field(sc, ["market capitalization", "market cap"]) or f"₹ {human_large(info.get('marketCap'))}"
    reserves = get_field(sc, ["reserves", "reserves & surplus"]) or "उपलब्ध नहीं"
    total_debt = get_field(sc, ["borrowings", "debt", "total debt", "net debt"]) or f"₹ {human_large(info.get('totalDebt'))}"
    debt_equity = get_field(sc, ["debt to equity", "debt/equity"]) or clean_num(info.get("debtToEquity")) or "उपलब्ध नहीं"
    pe = get_field(sc, ["stock p/e", "p/e", "price to earnings"]) or clean_num(info.get("trailingPE")) or "उपलब्ध नहीं"
    pb = get_field(sc, ["price to book value", "book value", "p/b"]) or clean_num(info.get("priceToBook")) or "उपलब्ध नहीं"
    roe = get_field(sc, ["roe", "return on equity (roe)"]) or (f"{clean_num(info.get('returnOnEquity', 0)*100)}%" if info.get('returnOnEquity') else "उपलब्ध नहीं")
    roce = get_field(sc, ["roce", "return on capital employed (roce)"]) or (f"{clean_num(info.get('returnOnCapitalEmployed', 0)*100)}%" if info.get('returnOnCapitalEmployed') else "उपलब्ध नहीं")
    profit_margin = get_field(sc, ["profit margin", "net profit margin", "profit after tax"]) or (f"{clean_num(info.get('profitMargins', 0)*100)}%" if info.get('profitMargins') else "उपलब्ध नहीं")
    summary = sc.get("about") or info.get("longBusinessSummary", "सारांश उपलब्ध नहीं")

    promoter_holding = get_field(sc, ["promoter holding", "promoters"]) or (f"{clean_num(info.get('heldPercentInsiders', 0)*100)}%" if info.get('heldPercentInsiders') else "उपलब्ध नहीं")
    fii_holding = get_field(sc, ["fii holding", "fiis", "foreign institutions"]) or (f"{clean_num(info.get('heldPercentInstitutions', 0)*100)}%" if info.get('heldPercentInstitutions') else "उपलब्ध नहीं")
    dii_holding = get_field(sc, ["dii holding", "diis", "domestic institutions"]) or "उपलब्ध नहीं"
    public_holding = get_field(sc, ["public holding", "public"]) or "उपलब्ध नहीं"
    promoter_pledge = get_field(sc, ["pledged percentage", "pledged %", "pledge"]) or "0.00%"

    rsi_comment = f"RSI मान {val_rsi} है।"
    if val_rsi > 70: rsi_comment += " (Overbought - अधिक खरीदारी की स्थिति)"
    elif val_rsi < 30: rsi_comment += " (Oversold - अधिक बिकवाली की स्थिति)"
    else: rsi_comment += " (संतुलित क्षेत्र में है)"

    ma_comment = f"20-EMA: {val_ema20}, 50-SMA: {val_sma50}, 200-SMA: {val_sma200}।"

    return {
        "symbol": ticker_symbol,
        "company": info.get("longName") or symbol,
        "as_of": str(hist.index[-1].date()),
        "price": clean_num(price),
        "change_pct": clean_num(change_pct),
        "currency": info.get("currency", "INR"),
        "rsi": val_rsi,
        "score": 2 if change_pct >= 0 else -2,
        "score_label": "सकारात्मक" if change_pct >= 0 else "नकारात्मक",
        "trend": "मजबूत अपट्रेंड" if change_pct >= 0 else "डाउनट्रेंड",
        "ema20": val_ema20,
        "sma50": val_sma50,
        "sma200": val_sma200,
        "support": support,
        "resistance": resistance,
        "support_1hr": support_1hr,
        "resistance_1hr": resistance_1hr,
        "high52": clean_num(info.get("fiftyTwoWeekHigh")),
        "low52": clean_num(info.get("fiftyTwoWeekLow")),
        "ma_comment": ma_comment,
        "rsi_comment": rsi_comment,
        "market_cap": str(market_cap),
        "reserves": str(reserves),
        "total_debt": str(total_debt),
        "debt_equity": str(debt_equity),
        "pe": str(pe),
        "pb": str(pb),
        "roe": str(roe),
        "roce": str(roce),
        "profit_margin": str(profit_margin),
        "summary": str(summary),
        "promoter_holding": str(promoter_holding),
        "fii_holding": str(fii_holding),
        "dii_holding": str(dii_holding),
        "public_holding": str(public_holding),
        "promoter_pledge": str(promoter_pledge),
        "verdict": "अनुशासित तरीके से ट्रेडिंग करें और जोखिम प्रबंधन का पालन करें।",
        "risks": ["मार्केट में वोलैटिलिटी और जोखिम हमेशा बने रहते हैं।"]
    }

@app.get("/")
def home(): return render_template_string(PAGE)

@app.get("/api/analyze")
def api_analyze():
    symbol=request.args.get("symbol","")
    try: return safe_jsonify(analyze_symbol(symbol))
    except Exception as e: return safe_jsonify({"error":str(e)}),400

_active20_cache = {"data": [], "time": 0, "error": ""}

def fetch_chartink_active20():
    url = "https://chartink.com/screener/copy-daily-close-above-previous-high-50"
    home_url = "https://chartink.com/"
    process_url = "https://chartink.com/screener/process"
    scan_clause = "( {cash} (  daily close >  1 day ago low and  daily rsi( 14 ) >  50 and  daily close >  1 day ago close *  1.04999 ) )"
    scan_payload = {
        "scan_clause": scan_clause,
        "debug_clause": "groupcount( 1 where  daily close >  1 day ago low),groupcount( 1 where  daily rsi( 14 ) >  50),groupcount( 1 where  daily close >  1 day ago close *  1.04999)",
        "column_clause": ' Daily Close as \'scan-column-default-close\',  Daily "close - 1 candle ago close / 1 candle ago close * 100" as \'scan-column-default-percent-change\', filternumber( daily close >  1 day ago close,1) as \'default-percent-change-conditional-filters-color\',  Daily Volume as \'scan-column-default-volume\''
    }
    sess = requests.Session()
    ua = random.choice(USER_AGENTS)
    base_headers = {
        "User-Agent": ua,
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": url,
        "Origin": "https://chartink.com",
        "X-Requested-With": "XMLHttpRequest",
    }
    sess.get(home_url, headers={**base_headers, "Accept": "text/html"}, timeout=25)
    page = sess.get(url, headers={**base_headers, "Accept": "text/html"}, timeout=25)
    soup = BeautifulSoup(page.text, "html.parser")
    csrf_el = soup.find("meta", attrs={"name": "csrf-token"})
    csrf_token = csrf_el.get("content", "").strip() if csrf_el else ""
    from urllib.parse import unquote
    xsrf_cookie = unquote(sess.cookies.get("XSRF-TOKEN", "")).strip()
    ajax_headers = {**base_headers, "Accept": "application/json, text/plain, */*", "Content-Type": "application/json"}
    if csrf_token: ajax_headers["X-CSRF-TOKEN"] = csrf_token
    if xsrf_cookie: ajax_headers["X-XSRF-TOKEN"] = xsrf_cookie

    resp = sess.post(process_url, json=scan_payload, headers=ajax_headers, timeout=40)
    candidate = resp.json()
    records = candidate.get("data", [])
    if not records: raise RuntimeError("कोई रिकॉर्ड नहीं मिला।")

    out = []
    for row in records:
        if not isinstance(row, dict): continue
        symbol = str(row.get("nsecode") or row.get("symbol") or "").strip().upper()
        if not symbol: continue
        name = str(row.get("name") or symbol).strip()
        try:
            volume = float(str(row.get("volume") or row.get("scan-column-default-volume") or 0).replace(",", ""))
            close = float(str(row.get("close") or row.get("scan-column-default-close") or 0).replace(",", ""))
            change = float(str(row.get("per_chg") or row.get("scan-column-default-percent-change") or 0).replace(",", "").replace("%", ""))
        except (TypeError, ValueError): continue
        out.append({"symbol": symbol, "name": name, "volume": volume, "close": close, "change_pct": change})
    out.sort(key=lambda x: x["volume"], reverse=True)
    return out[:20]

@app.get("/api/screener/active20")
def screener_active20():
    now = time.time()
    force = request.args.get("refresh", "0") == "1"
    if force or now - _active20_cache["time"] > 180 or not _active20_cache["time"]:
        try:
            _active20_cache["data"] = fetch_chartink_active20()
            _active20_cache["error"] = ""
            _active20_cache["time"] = now
        except Exception as exc:
            _active20_cache["error"] = str(exc)
            _active20_cache["time"] = now
    if _active20_cache["error"]:
        return safe_jsonify({"error": _active20_cache["error"], "data": [], "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S")}), 502
    return safe_jsonify({"data": _active20_cache["data"], "updated_at": datetime.now().strftime("%d-%m-%Y %H:%M:%S")})

IST = ZoneInfo("Asia/Kolkata")
_DEFAULT_HISTORY_DIR = "/var/data" if os.path.isdir("/var/data") else os.path.dirname(os.path.abspath(__file__))
ACTIVE_STOCKS_HISTORY_FILE = os.environ.get("ACTIVE_STOCKS_HISTORY_FILE", os.path.join(_DEFAULT_HISTORY_DIR, "active_stocks_history.json"))
_active_history_lock = threading.Lock()

def _read_active_history():
    try:
        with open(ACTIVE_STOCKS_HISTORY_FILE, "r", encoding="utf-8") as f:
            value = json.load(f)
        return value if isinstance(value, list) else []
    except Exception: return []

def _write_active_history(history):
    os.makedirs(os.path.dirname(os.path.abspath(ACTIVE_STOCKS_HISTORY_FILE)), exist_ok=True)
    temp_path = ACTIVE_STOCKS_HISTORY_FILE + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, ACTIVE_STOCKS_HISTORY_FILE)

def save_daily_active_history():
    now_ist = datetime.now(IST)
    day_key = now_ist.strftime("%Y-%m-%d")
    with _active_history_lock:
        history = _read_active_history()
        if any(item.get("date") == day_key for item in history if isinstance(item, dict)): return
        try:
            stocks = fetch_chartink_active20()
            if not stocks: return
            snapshot = {"date": day_key, "saved_at": now_ist.strftime("%d-%m-%Y %H:%M:%S IST"), "data": stocks[:20]}
            history.insert(0, snapshot)
            _write_active_history(history)
        except Exception: pass

@app.get("/api/screener/active-stocks/history")
def active_stocks_history():
    try: save_daily_active_history()
    except Exception: pass
    return safe_jsonify({"history": _read_active_history()})

@app.get("/api/demo/quote")
def demo_quote():
    raw_query = request.args.get("symbol", "").strip()
    if not raw_query: return safe_jsonify({"error": "Symbol आवश्यक है"}), 400
    ticker = raw_query if "." in raw_query else raw_query + ".NS"
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period="5d", interval="1d", auto_adjust=True)
        if hist.empty:
            ticker = raw_query + ".BO"
            t = yf.Ticker(ticker)
            hist = t.history(period="5d", interval="1d", auto_adjust=True)
        if hist.empty: raise ValueError("भाव नहीं मिला")
        price = float(hist["Close"].iloc[-1])
        return safe_jsonify({"symbol": raw_query.replace(".NS", "").replace(".BO", "").upper(), "price": round(price, 2)})
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 404

SCAN_STATE = {"results": [], "confirmed": [], "gainers": [], "losers": [], "status": "READY", "completed": 0, "total": len(STOCK_UNIVERSE), "progress": 0, "running": False}

def find_dow_pivots(series_high, series_low):
    highs, lows = [], []
    for i in range(2, len(series_high) - 2):
        if series_high.iloc[i] >= series_high.iloc[i-1] and series_high.iloc[i] >= series_high.iloc[i-2] and series_high.iloc[i] >= series_high.iloc[i+1] and series_high.iloc[i] >= series_high.iloc[i+2]:
            highs.append(series_high.iloc[i])
        if series_low.iloc[i] <= series_low.iloc[i-1] and series_low.iloc[i] <= series_low.iloc[i-2] and series_low.iloc[i] <= series_low.iloc[i+1] and series_low.iloc[i] <= series_low.iloc[i+2]:
            lows.append(series_low.iloc[i])
    return highs, lows

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
            hist = ticker.history(period="3mo", interval="1d", auto_adjust=False)
            if hist is not None and not hist.empty and len(hist) > 10:
                close = hist["Close"].dropna()
                price = float(close.iloc[-1])
                prev = float(close.iloc[-2]) if len(close) > 1 else price
                chg = ((price / prev) - 1) * 100 if prev else 0.0
                d_highs, d_lows = find_dow_pivots(hist["High"], hist["Low"])
                is_uptrend = chg >= 0
                entry = d_lows[-1] if d_lows else price * 0.98
                sl = round(entry * 0.99, 2) if is_uptrend else round(entry * 1.01, 2)
                target = d_highs[-1] if d_highs else price * 1.02
                results.append({
                    "symbol": clean_name, "current": round(price, 2), "p_change": round(chg, 2),
                    "daily_sup": [d_lows[-1]] if d_lows else [], "daily_res": [d_highs[-1]] if d_highs else [],
                    "hourly_sup": [entry], "hourly_res": [target], "trend": "UPTREND" if is_uptrend else "DOWNTREND",
                    "hh": d_highs[-1] if d_highs else price, "hl": d_lows[-1] if d_lows else price,
                    "entry": round(entry, 2), "sl": sl, "target": round(target, 2),
                    "signal": "BUY VALID" if is_uptrend else "SELL VALID", "status": "Active", "dist_to_entry": abs(price - entry)
                })
        except Exception: pass
        completed += 1
        SCAN_STATE["completed"] = completed
        SCAN_STATE["progress"] = int((completed / total) * 100)
    results.sort(key=lambda x: x["p_change"], reverse=True)
    SCAN_STATE["results"] = results
    confirmed = [r for r in results]
    confirmed.sort(key=lambda x: x["dist_to_entry"])
    SCAN_STATE["confirmed"] = confirmed
    SCAN_STATE["gainers"] = results[:5]
    losers = results[-5:]
    losers.reverse()
    SCAN_STATE["losers"] = losers
    SCAN_STATE["status"] = "COMPLETED"
    SCAN_STATE["running"] = False

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
        return jsonify({"success": True})
    return jsonify({"success": False})
init_auth(app)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="0.0.0.0", port=port, debug=False)
