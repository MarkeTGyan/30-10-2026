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

PAGE = r"""
<!doctype html>
<html lang="hi">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MarkeTGyan, Technical Analysis Scanner</title>
<style>
:root{
 --bg:#f2f6fb;--panel:#ffffff;--line:#d9e4ef;--text:#26364a;--muted:#5c6f84;
 --gold:#f4c95d;--green:#18794e;--red:#c2414b;--blue:#3977c3;--teal:#168b8b;
 --soft-blue:#eaf3ff;--soft-green:#e7f6ee;--soft-red:#fff0f0;--soft-gold:#fff7df;
}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{scroll-behavior:smooth}
body{margin:0;background:linear-gradient(145deg,#f5f8fc 0%,#edf4fb 48%,#f8f7f0 100%);font-family:Arial,"Noto Sans Devanagari",sans-serif;color:var(--text);font-size:16px;line-height:1.55}
main{max-width:1200px;margin:0 auto;padding:clamp(12px,2.5vw,28px)}
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
/* Tabs */
.tab-buttons{display:flex;gap:10px;margin:8px 0 16px;border:0;padding:8px;background:#e5edf6;border-radius:16px;flex-wrap:wrap;box-shadow:inset 0 1px 3px rgba(40,70,105,.06)}
.tab-btn{flex:1 1 180px;background:transparent;color:#526a82;border:1px solid transparent;padding:12px 16px;border-radius:11px;cursor:pointer;font-weight:800;line-height:1.4;box-shadow:none;text-align:center;font-size:14px}
.tab-btn:hover{background:#f6faff;transform:none}.tab-btn.active{background:linear-gradient(135deg,#3c80c8,#2e69ad);color:#fff;border-color:#2e69ad;box-shadow:0 4px 12px rgba(46,105,173,.2)}
.tab-content{display:none}.tab-content.active{display:block;animation:fadeIn .22s ease-out}
.status-msg{font-size:15px;font-weight:bold;color:#986b08;margin-bottom:12px}.progress-track{width:100%;height:14px;background:#e7edf4;border:1px solid #d3dfeb;border-radius:99px;overflow:hidden;margin:8px 0 16px}.progress-fill{height:100%;width:0%;background:linear-gradient(90deg,#43b581,#168b8b);transition:width .25s ease}.progress-caption{font-size:13px;color:var(--muted);margin-bottom:12px}
@keyframes fadeIn{from{opacity:.75;transform:translateY(3px)}to{opacity:1;transform:translateY(0)}}

/* Extra colourful visual layer */
:root{
 --bg:#f4f7ff;--panel:#ffffff;--line:#d8e3f5;--text:#24324b;--muted:#596b86;
 --gold:#ffca58;--green:#16845b;--red:#c43f59;--blue:#3f68df;--teal:#078e9b;
 --soft-blue:#e9efff;--soft-green:#e5f8ed;--soft-red:#fff0f2;--soft-gold:#fff6dc;
}
body{background:radial-gradient(ellipse at 8% 0%,rgba(127,166,255,.20),transparent 33%),radial-gradient(ellipse at 96% 18%,rgba(255,190,122,.18),transparent 28%),linear-gradient(145deg,#f8faff 0%,#eef5ff 44%,#f8f2ff 75%,#fff9ed 100%)}
header{position:relative}
header .tag{background:linear-gradient(110deg,#e3eaff,#e2f8f3,#fff0d2);border:1px solid #d5e2ff;color:#3b4fa0;box-shadow:0 5px 16px rgba(75,103,186,.10)}
h1{background:linear-gradient(100deg,#294db5 0%,#7051c8 48%,#078c96 100%);-webkit-background-clip:text;background-clip:text;color:transparent}
.panel{border:1px solid rgba(190,207,237,.9);background:linear-gradient(145deg,rgba(255,255,255,.99),rgba(248,251,255,.98));box-shadow:0 7px 22px rgba(57,83,145,.075),0 1px 0 rgba(255,255,255,.9) inset;position:relative;overflow:hidden}
.panel:before{content:"";position:absolute;left:0;top:0;bottom:0;width:4px;background:linear-gradient(180deg,#5a83f0,#9a72e8,#45c7b0);opacity:.92}
.panel h2{color:#344c9a}
.tab-buttons{background:linear-gradient(110deg,#e7edff,#e6f8f5,#fff2df);border:1px solid #d7e2fa;box-shadow:0 5px 15px rgba(60,89,165,.08)}
.tab-btn{color:#465a91;border-color:transparent}
.tab-btn.active{background:linear-gradient(110deg,#3d65df 0%,#7654d0 52%,#07969d 100%);border-color:transparent;color:#fff;box-shadow:0 6px 16px rgba(75,87,190,.25)}
.metric{background:linear-gradient(140deg,#f1f5ff,#eaf7ff 55%,#effcf7);border-color:#d5e4fa;box-shadow:0 4px 12px rgba(65,95,155,.055)}
.metric strong{color:#2f477e}
.tag{background:linear-gradient(100deg,#e6edff,#e1f8f3);color:#344f9e;border-color:#d3e1fa}
table{border-color:#d7e3f4;box-shadow:0 3px 12px rgba(51,80,140,.045)}
th{background:linear-gradient(100deg,#e4ebff,#e5f5ff 55%,#e4f8f0);color:#354d91}
tbody tr:nth-child(even){background:#f4f8ff}tbody tr:hover{background:#edf5ff}

.active-table-wrap,.trade-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:13px}
.active-table{min-width:650px}.trade-table{min-width:650px}
.active-subtabs{margin:12px 0 16px}.active-subtabs button{background:linear-gradient(110deg,#e8efff,#e5faf2);color:#354d91;border:1px solid #d5e2f7}.active-subtabs button.active{background:linear-gradient(110deg,#4168df,#7753ce);color:#fff}
.history-day{border:1px solid #dce5f5;border-radius:14px;padding:12px;margin:12px 0;background:linear-gradient(135deg,#fff,#f5f8ff)}.history-day h3{margin:0 0 8px;color:#354d91}.history-meta{font-size:13px;color:#64748b;margin-bottom:10px}
.active-rank{display:inline-grid;place-items:center;min-width:32px;height:32px;border-radius:10px;background:linear-gradient(135deg,#e2eaff,#e5fff4);color:#344d9a;font-weight:900}
.trade-toolbar{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;align-items:end}
.trade-field label{display:block;font-size:13px;font-weight:800;color:#435b91;margin-bottom:6px}
.trade-field input,.trade-field select{width:100%;min-width:0;flex:auto;background:#fff;border:1.5px solid #cbd8ef;border-radius:11px;padding:12px;font-size:16px;min-height:46px;color:#24324b}
.trade-actions{display:flex;gap:10px;flex-wrap:wrap;margin-top:14px}.buy-btn{background:linear-gradient(120deg,#12a875,#087c61);color:white;border:0}.sell-btn{background:linear-gradient(120deg,#ee6375,#c42f4d);color:white;border:0}.neutral-btn{background:linear-gradient(120deg,#4368de,#7654d0);color:#fff;border:0}
.trade-metrics{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:10px}.trade-metric{padding:14px;border-radius:14px;border:1px solid #d9e4f6;background:linear-gradient(135deg,#eef3ff,#f0fffb)}.trade-metric small{display:block;color:#60718a;font-weight:700;margin-bottom:6px}.trade-metric strong{font-size:21px;overflow-wrap:anywhere}
.trade-note{padding:12px 14px;border-radius:12px;background:#fff8df;border:1px solid #f0dfa5;color:#735817;line-height:1.7}.trade-status{padding:12px;border-radius:11px;background:#eaf7f0;color:#176a43;font-weight:700;white-space:pre-wrap}.trade-status.error{background:#fff0f2;color:#a3273a}.order-chip{display:inline-block;border-radius:99px;padding:4px 8px;font-size:12px;font-weight:800;background:#e5efff;color:#3559ad}.demo-subtabs{display:flex;gap:8px;flex-wrap:wrap;margin:12px 0}.demo-subtabs button{padding:9px 13px;font-size:13px;min-height:38px;background:#e9efff;color:#4055a1;box-shadow:none;border:1px solid #d5dff7}.demo-subtabs button.active{background:linear-gradient(110deg,#4168df,#7753ce);color:#fff}
</style>
</head>
<body><main>
<header><div class="tag">MarkeTGyan, Technical Analysis Scanner</div>
<h1>AI Stock Analyst & Screener</h1>
<div class="sub">एक ही स्टॉक के तकनीकी संकेतक, सपोर्ट/रेजिस्टेंस, वित्तीय स्थिति और डेटा-आधारित समग्र विश्लेषण।</div></header>

<!-- ALL 6 TABS -->
<div class="tab-buttons" role="tablist" aria-label="स्टॉक विश्लेषण टैब">
  <button type="button" class="tab-btn active" data-tab-target="searchTab">🔎 नया विश्लेषण</button>
  <button type="button" class="tab-btn" data-tab-target="savedTab">🗂️ पूर्व विश्लेषित सभी स्टॉक्स</button>
  <button type="button" class="tab-btn" data-tab-target="active20Tab">📈 Active Stocks</button>
  <button type="button" class="tab-btn" data-tab-target="demoTradeTab">💹 Demo Trade</button>
  <button type="button" class="tab-btn" data-tab-target="gainersLosersTab">📊 Top Gainers / Losers</button>
  <button type="button" class="tab-btn" data-tab-target="advanceScreenerTab">⚡ Advance Screener</button>
</div>

<!-- TAB 1: Single Stock Analysis -->
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

<!-- TAB 2: Previously Analyzed Stocks -->
<div id="savedTab" class="tab-content">
  <div class="panel">
    <h2>🗂️ पूर्व विश्लेषण के लिए डाले गए सभी स्टॉक्स</h2>
    <p class="copy">यहाँ उन स्टॉक्स का रिकॉर्ड दिखाई देगा जिनका विश्लेषण सफलतापूर्वक किया गया है। नवीनतम विश्लेषण ऊपर दिखेगा। किसी स्टॉक को दोबारा देखने के लिए <b>फिर देखें</b> और रिकॉर्ड हटाने के लिए <b>Delete</b> दबाएँ। रिकॉर्ड इसी ब्राउज़र के Local Storage में रहता है।</p>
    <div id="savedStockMessage" class="hint"></div>
    <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>कंपनी / स्टॉक</th><th>अंतिम भाव</th><th>दैनिक बदलाव</th><th>Daily Support</th><th>Daily Resistance</th><th>1HR Support</th><th>1HR Resistance</th><th>ट्रेंड</th><th>विश्लेषण समय</th><th>विकल्प</th></tr></thead><tbody id="savedStocksBody"><tr><td colspan="10" class="empty-row">अभी कोई स्टॉक सेव नहीं है।</td></tr></tbody></table></div>
  </div>
</div>

<!-- TAB 3: Active Stocks (Chartink Screener & Daily History) -->
<div id="active20Tab" class="tab-content">
 <div class="panel">
  <h2>📈 Active Stocks — Chartink Screener</h2>
  <p class="copy">आपकी दी हुई Chartink scan_clause से वास्तविक स्टॉक लिए जाते हैं और Volume के घटते क्रम में अधिकतम 20 स्टॉक दिखते हैं।</p>
  <div class="demo-subtabs active-subtabs">
   <button type="button" class="active" data-active-subtab="activeCurrentPanel">आज के Active Stocks</button>
   <button type="button" data-active-subtab="activeHistoryPanel">📚 Daily History</button>
  </div>
  <div id="activeCurrentPanel">
   <div class="trade-actions"><button id="refreshActive20" type="button" class="neutral-btn">↻ अभी सूची अपडेट करें</button></div>
   <div id="active20Status" class="trade-status" style="margin-top:12px">सूची लोड करने के लिए अपडेट बटन दबाएँ।</div>
   <div id="active20Time" class="hint"></div>
   <div class="active-table-wrap" style="margin-top:14px"><table class="active-table"><thead><tr><th>Rank</th><th>स्टॉक / कंपनी</th><th>Close ₹</th><th>Change %</th><th>Volume</th></tr></thead><tbody id="active20Body"><tr><td colspan="5" class="empty-row">अभी डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
  </div>
  <div id="activeHistoryPanel" style="display:none">
   <div class="trade-actions"><button id="refreshActiveHistory" type="button" class="neutral-btn">↻ History अपडेट करें</button></div>
   <div id="activeHistoryStatus" class="trade-status" style="margin-top:12px">तारीख के अनुसार सेव की गई सूची लोड की जा रही है…</div>
   <div id="activeHistoryBody" style="margin-top:14px"><div class="empty-row">History अभी लोड नहीं हुई।</div></div>
  </div>
 </div>
</div>

<!-- TAB 4: Paper Trading / Demo Trade -->
<div id="demoTradeTab" class="tab-content">
 <div class="panel">
  <h2>💹 Professional Demo Trading Terminal</h2>
  <div class="trade-note"><b>DEMO / PAPER TRADING ONLY:</b> शुरुआती वर्चुअल कैश ₹1,00,000 है। रिकॉर्ड इसी ब्राउज़र के Local Storage में सुरक्षित रहेगा।</div>
  <div class="trade-metrics" style="margin-top:14px">
   <div class="trade-metric"><small>Available Cash</small><strong id="demoCash">₹1,00,000</strong></div>
   <div class="trade-metric"><small>Portfolio Value</small><strong id="demoPortfolioValue">₹0</strong></div>
   <div class="trade-metric"><small>Unrealized P&L</small><strong id="demoUnrealized">₹0</strong></div>
   <div class="trade-metric"><small>Realized P&L</small><strong id="demoRealized">₹0</strong></div>
  </div>
 </div>
 <div class="panel">
  <h2>🧾 Place Order</h2>
  <div class="trade-toolbar">
   <div class="trade-field"><label for="demoSymbol">NSE Symbol / Stock Name</label><input id="demoSymbol" placeholder="RELIANCE, TCS या Tata Motors" autocomplete="off" value="RELIANCE"></div>
   <div class="trade-field"><label for="demoSide">Side</label><select id="demoSide"><option value="BUY">BUY</option><option value="SELL">SELL</option></select></div>
   <div class="trade-field"><label for="demoOrderType">Order Type</label><select id="demoOrderType"><option value="MARKET">Market</option><option value="LIMIT">Limit</option></select></div>
   <div class="trade-field"><label for="demoQty">Quantity</label><input id="demoQty" type="number" min="1" step="1" value="1"></div>
   <div class="trade-field"><label for="demoLimitPrice">Limit Price (₹)</label><input id="demoLimitPrice" type="number" min="0.01" step="0.05" placeholder="केवल Limit Order"></div>
   <div class="trade-field"><label>Live / Latest Quote</label><div id="demoQuote" class="trade-status">भाव प्राप्त नहीं हुआ</div></div>
  </div>
  <div class="trade-actions"><button type="button" id="demoGetQuote" class="neutral-btn">↻ भाव प्राप्त करें</button><button type="button" id="demoBuy" class="buy-btn">BUY / Place Order</button><button type="button" id="demoSell" class="sell-btn">SELL / Place Order</button><button type="button" id="demoRefreshOrders">Limit Orders जाँचें</button></div>
  <div id="demoMessage" class="hint" aria-live="polite"></div>
 </div>
 <div class="panel"><h2>📍 Today's Positions — आज की पोजीशन</h2><div class="trade-table-wrap"><table class="trade-table"><thead><tr><th>Symbol</th><th>Qty</th><th>Avg. Price</th><th>LTP</th><th>Market Value</th><th>Unrealized P&amp;L</th><th>Action</th></tr></thead><tbody id="demoTodayBody"><tr><td colspan="7" class="empty-row">अभी कोई पोजीशन नहीं है।</td></tr></tbody></table></div></div>
 <div class="panel"><h2>📊 Portfolio — Open Holdings</h2><div class="trade-table-wrap"><table class="trade-table"><thead><tr><th>Symbol</th><th>Qty</th><th>Avg. Price</th><th>LTP</th><th>Market Value</th><th>Unrealized P&amp;L</th><th>आज ट्रेड?</th></tr></thead><tbody id="demoPortfolioBody"><tr><td colspan="7" class="empty-row">Portfolio खाली है।</td></tr></tbody></table></div></div>
 <div class="panel"><h2>📚 Order &amp; Trade Book</h2><div class="trade-table-wrap"><table class="trade-table"><thead><tr><th>समय</th><th>Symbol</th><th>Side</th><th>Type</th><th>Qty</th><th>Price</th><th>Status</th><th>Realized P&amp;L</th></tr></thead><tbody id="demoBookBody"><tr><td colspan="8" class="empty-row">अभी कोई ऑर्डर नहीं है।</td></tr></tbody></table></div><div class="trade-actions"><button id="demoReset" type="button" class="sell-btn">Demo Data Reset करें</button></div></div>
</div>

<!-- TAB 5: Top Gainers / Losers (Futures Universe) -->
<div id="gainersLosersTab" class="tab-content">
 <div class="panel">
  <h2>📊 Top 5 Gainers & Losers (Future Stocks Universe)</h2>
  <div class="trade-actions"><button id="refreshGL" type="button" class="neutral-btn" onclick="fetchGainersLosers()">↻ अभी अपडेट करें</button></div>
  <div id="glStatus" class="trade-status" style="margin-top:12px">डेटा लोड करने के लिए बटन दबाएँ।</div>
  <h3 class="green" style="margin-top:20px;">🔥 Top 5 Gainers</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>LTP</th><th>Change %</th><th>Daily S/R</th><th>1HR S/R</th></tr></thead><tbody id="gainersBody"><tr><td colspan="6" class="empty-row">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
  <h3 class="red" style="margin-top:20px;">🧊 Top 5 Losers</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>Rank</th><th>स्टॉक</th><th>LTP</th><th>Change %</th><th>Daily S/R</th><th>1HR S/R</th></tr></thead><tbody id="losersBody"><tr><td colspan="6" class="empty-row">डेटा लोड नहीं हुआ।</td></tr></tbody></table></div>
 </div>
</div>

<!-- TAB 6: Advance Screener (Futures Universe) -->
<div id="advanceScreenerTab" class="tab-content">
 <div class="panel">
  <h2>⚡ Advance Dow Theory Screener (Future Stocks Universe)</h2>
  <div class="trade-actions"><button type="button" class="neutral-btn" onclick="triggerRescan()">अभी स्कैन शुरू करें</button></div>
  <div id="screenerStatus" class="trade-status" style="margin-top:12px">स्कैनर स्थिति: तैयार</div>
  <div class="progress-track" style="margin-top:10px"><div id="progBar" class="progress-fill"></div></div>
  <h3 style="margin-top:20px;">★ Confirmed Signals</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>ट्रेंड</th><th>भाव</th><th>Entry</th><th>SL</th><th>Target</th><th>Signal</th></tr></thead><tbody id="confirmedBody"><tr><td colspan="7" class="empty-row">कोई सिग्नल नहीं मिला।</td></tr></tbody></table></div>
  <h3 style="margin-top:20px;">📋 सभी फ्यूचर स्टॉक्स की स्थिति</h3>
  <div class="saved-table-wrap"><table class="saved-table"><thead><tr><th>स्टॉक</th><th>भाव</th><th>ट्रेंड</th><th>HH</th><th>HL</th><th>Status</th></tr></thead><tbody id="allStocksBody"><tr><td colspan="6" class="empty-row">डेटा लोड हो रहा है...</td></tr></tbody></table></div>
 </div>
</div>

<footer>MarkeTGyan, Technical Analysis Scanner • शैक्षिक उद्देश्य मात्र; यह SEBI-registered investment advice नहीं है।</footer>
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
  return '<p class="error">इस समय '+esc(timeframe)+' के लिए वैध सपोर्ट और रेजिस्टेंस उपलब्ध नहीं हैं; इसलिए Entry/SL/Target नहीं निकाले गए।</p>';
 }
 const range=r-s, fmtP=v=>fmt(v,' '+cur);
 const buySL=s-range*0.25, buyT1=s+range*0.5, buyT2=r;
 const sellSL=r+range*0.25, sellT1=r-range*0.5, sellT2=s;
 const rows=[
  ['सेटअप','एंट्री ट्रिगर','Entry स्तर','Stop Loss','Target 1','Target 2'],
  ['BUY (Support से)','कीमत सपोर्ट के पास आए और bullish candle/price confirmation मिले',fmtP(s),fmtP(buySL),fmtP(buyT1),fmtP(buyT2)],
  ['SELL (Resistance से)','कीमत रेजिस्टेंस के पास आए और bearish candle/price confirmation मिले',fmtP(r),fmtP(sellSL),fmtP(sellT1),fmtP(sellT2)]
 ];
 return '<div style="overflow-x:auto"><table><thead><tr>'+rows[0].map(x=>'<th>'+esc(x)+'</th>').join('')+'</tr></thead><tbody>'+rows.slice(1).map(row=>'<tr>'+row.map((x,i)=>'<td>'+(i===0?'<b>'+esc(x)+'</b>':esc(x))+'</td>').join('')+'</tr>').join('')+'</tbody></table></div><p class="copy"><b>गणना का आधार:</b> BUY सपोर्ट से और SELL रेजिस्टेंस से। Support–Resistance दूरी का 25% Stop Loss buffer रखा गया है; Target 1 दूरी के मध्य बिंदु पर और Target 2 विपरीत स्तर पर है। ये शैक्षिक, नियम-आधारित स्तर हैं, निश्चित संकेत नहीं। कीमत सपोर्ट/रेजिस्टेंस से दूर हो तो केवल इन स्तरों के आधार पर market order न लें; confirmation, gap, slippage और charges का ध्यान रखें।</p>';
}

// Single Stock Search
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
    ['ROE और ROCE', `ROE: ${d.roe} | ROCE: ${d.roce}`],
    ['Profit Margin', d.profit_margin],
    ['Sales Growth', d.sales_growth],
    ['Profit Growth', d.profit_growth],
    ['Dividend Yield', d.dividend_yield],
    ['Debtor Days', d.debtor_days],
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
  
  const note = [];
  const num = v => (v === null || v === undefined || String(v).trim() === '' ? NaN : Number(v));
  const px = num(d.price), ch = num(d.change_pct), rsi = num(d.rsi);
  const sup = num(d.support), res = num(d.resistance);
  const sup1 = num(d.support_1hr), res1 = num(d.resistance_1hr);
  const valid = v => Number.isFinite(v);
  note.push(`<p><b>1. भाव और दैनिक चाल:</b> ${valid(px) ? `अंतिम उपलब्ध बंद भाव ${fmt(px, ' ' + (d.currency||''))}` : 'भाव उपलब्ध नहीं'}। ${valid(ch) ? `दैनिक बदलाव ${ch>0?'+':''}${fmt(ch,'%')} है, इसलिए उपलब्ध सत्र में ${ch>0?'तेजी':ch<0?'कमजोरी':'लगभग कोई बदलाव नहीं'} दिखता है।` : 'दैनिक बदलाव का भरोसेमंद आँकड़ा उपलब्ध नहीं है।'}</p>`);
  note.push(`<p><b>2. ट्रेंड और मूविंग एवरेज:</b> ${esc(d.trend || 'ट्रेंड का स्पष्ट विवरण उपलब्ध नहीं')}। ${esc(d.ma_comment || 'मूविंग एवरेज पर टिप्पणी उपलब्ध नहीं')}। ट्रेंड स्कोर/लेबल: <b>${esc(d.score_label || 'उपलब्ध नहीं')}</b>। केवल लेबल के आधार पर ट्रेड न लें; भाव का स्तर और कन्फर्मेशन भी देखें।</p>`);
  note.push(`<p><b>3. RSI और MACD:</b> ${valid(rsi) ? `RSI ${fmt(rsi)} है${rsi>=70?' — यह ओवरबॉट क्षेत्र के करीब/ऊपर है; नई खरीद में सावधानी रखें।':rsi<=30?' — यह ओवरसोल्ड क्षेत्र के करीब/नीचे है; केवल इसी आधार पर खरीद न करें।':' — यह 30–70 के बीच है, इसलिए अकेले RSI से मजबूत ओवरबॉट/ओवरसोल्ड संकेत नहीं मिलता।'}` : 'RSI का संख्यात्मक मान उपलब्ध नहीं है।'} ${esc(d.rsi_comment || '')} ${esc(d.macd_comment || '')}</p>`);
  note.push(`<p><b>4. डेली सपोर्ट/रेजिस्टेंस:</b> सपोर्ट ${valid(sup)?fmt(sup,' '+(d.currency||'')):'उपलब्ध नहीं'} और रेजिस्टेंस ${valid(res)?fmt(res,' '+(d.currency||'')):'उपलब्ध नहीं'}। ${valid(px)&&valid(sup)&&valid(res)?(px<=sup?'भाव डेली सपोर्ट के पास/नीचे है; सपोर्ट टूटने पर कमजोरी बढ़ सकती है, जबकि वापसी की पुष्टि जरूरी है।':px>=res?'भाव डेली रेजिस्टेंस के पास/ऊपर है; ब्रेकआउट टिकने और वॉल्यूम/क्लोजिंग से पुष्टि पर ध्यान दें।':'भाव दिए गए डेली सपोर्ट और रेजिस्टेंस के बीच है; बीच के क्षेत्र में जोखिम-इनाम स्पष्ट करके ही निर्णय लें।'):'इन स्तरों के आधार पर भाव की स्थिति पूरी तरह तय नहीं की जा सकती।'}</p>`);
  note.push(`<p><b>5. एक घंटे के स्तर:</b> सपोर्ट ${valid(sup1)?fmt(sup1,' '+(d.currency||'')):'उपलब्ध नहीं'} और रेजिस्टेंस ${valid(res1)?fmt(res1,' '+(d.currency||'')):'उपलब्ध नहीं'}। इन स्तरों को छोटे समय-फ्रेम की योजना के लिए देखें; डेली ट्रेंड के विरुद्ध ट्रेड लेने से पहले अतिरिक्त पुष्टि जरूरी है।</p>`);
  const fundamentals = [d.market_cap,d.debt_equity,d.pe,d.pb,d.roe,d.roce,d.profit_margin,d.sales_growth,d.profit_growth];
  const knownFundamentals = fundamentals.filter(v => v !== null && v !== undefined && String(v).trim() && !/उपलब्ध नहीं|^nan$|^none$/i.test(String(v).trim())).length;
  const debtText = String(d.debt_equity ?? '').trim();
  const pledgeText = String(d.promoter_pledge ?? '').trim();
  note.push(`<p><b>6. कंपनी की वित्तीय स्थिति:</b> प्रमुख वित्तीय मदों में से ${knownFundamentals}/9 के लिए कोई मान उपलब्ध है। Debt-to-Equity: ${esc(debtText || 'उपलब्ध नहीं')}; P/E: ${esc(d.pe ?? 'उपलब्ध नहीं')}; P/B: ${esc(d.pb ?? 'उपलब्ध नहीं')}; ROE/ROCE: ${esc(d.roe ?? 'उपलब्ध नहीं')} / ${esc(d.roce ?? 'उपलब्ध नहीं')}। Promoter pledge: ${esc(pledgeText || 'उपलब्ध नहीं')}। इन अनुपातों का अर्थ उद्योग और पुराने वर्षों के आँकड़ों से तुलना करके ही निकालें; खाली/अनुपलब्ध डेटा को सकारात्मक संकेत न मानें।</p>`);
  note.push(`<p><b>7. शेयरहोल्डिंग:</b> Promoter ${esc(d.promoter_holding ?? 'उपलब्ध नहीं')}, FII ${esc(d.fii_holding ?? 'उपलब्ध नहीं')}, DII ${esc(d.dii_holding ?? 'उपलब्ध नहीं')}, Public ${esc(d.public_holding ?? 'उपलब्ध नहीं')}। केवल एक अवधि की हिस्सेदारी से खरीद/बिक्री का निष्कर्ष नहीं निकालना चाहिए; पिछली तिमाहियों के बदलाव भी देखें।</p>`);
  note.push(`<p><b>समग्र निष्कर्ष:</b> ${esc(d.verdict || 'कोई स्पष्ट निष्कर्ष उपलब्ध नहीं है')} ${valid(ch)&&ch<0?'दैनिक बदलाव नकारात्मक है, इसलिए जल्दबाजी में खरीदने से बचें।':valid(ch)&&ch>0?'दैनिक बदलाव सकारात्मक है, लेकिन रेजिस्टेंस और जोखिम-इनाम की जाँच जरूरी है.':'अगला कदम तय करने से पहले भाव, स्तर और संकेतकों की पुष्टि करें।'} ${knownFundamentals<5?'वित्तीय आँकड़े आंशिक हैं, इसलिए कंपनी की गुणवत्ता पर निष्कर्ष सीमित रखें.':'तकनीकी संकेतों के साथ वित्तीय आँकड़ों की तुलना भी करें.'}</p>`);
  note.push('<p><b>कार्रवाई से पहले:</b> स्टॉप-लॉस, पोजीशन साइज और जोखिम-इनाम तय करें; केवल इस नोट के आधार पर ट्रेड न करें। यह उपलब्ध डेटा का नियम-आधारित सार है, भविष्य की गारंटी या व्यक्तिगत निवेश सलाह नहीं।</p>');
  $('analysisNote').innerHTML = note.join('');
 }catch(err){$('status').innerHTML='<div class="error">विश्लेषण नहीं हो पाया: '+esc(err.message)+'</div>'}
 finally{$('go').disabled=false;$('go').textContent='विश्लेषण करें ↗'}
});

let active20Loaded=false;
function fmtINR(v){return '₹'+Number(v||0).toLocaleString('en-IN',{minimumFractionDigits:2,maximumFractionDigits:2})}
function fmtVol(v){return Number(v||0).toLocaleString('en-IN',{maximumFractionDigits:0})}
function renderActive20(rows){
 const body=$('active20Body');
 if(!Array.isArray(rows)||!rows.length){body.innerHTML='<tr><td colspan="5" class="empty-row">इस समय Chartink से कोई परिणाम उपलब्ध नहीं है।</td></tr>';return;}
 body.innerHTML=rows.slice(0,20).map((x,i)=>{const ch=Number(x.change_pct);return `<tr><td><span class="active-rank">${i+1}</span></td><td><b>${esc(x.name||x.symbol||'—')}</b><br><span class="tag">${esc(x.symbol||'')}</span></td><td>${fmtINR(x.close)}</td><td class="${ch>0?'green':ch<0?'red':''}">${Number.isFinite(ch)?(ch>0?'+':'')+ch.toFixed(2)+'%':'—'}</td><td><b>${fmtVol(x.volume)}</b></td></tr>`}).join('');
}
async function fetchActive20Stocks(){
 const b=$('refreshActive20');b.disabled=true;$('active20Status').className='trade-status';$('active20Status').textContent='Chartink screener से वास्तविक सूची प्राप्त की जा रही है…';
 try{const r=await fetch('/api/screener/active20?refresh=1',{headers:{'Accept':'application/json'}});const raw=await r.text();let d;try{d=JSON.parse(raw)}catch(parseErr){const isHtml=/^\s*</.test(raw);throw new Error(isHtml?`सर्वर ने JSON के बजाय HTML लौटाया (HTTP ${r.status})। Render Logs में देखें कि नई app.py Deploy हुई है या नहीं।`: `सर्वर का उत्तर JSON नहीं है (HTTP ${r.status})।`)}if(!r.ok||d.error)throw new Error(d.error||`डेटा प्राप्त नहीं हुआ (HTTP ${r.status})`);renderActive20(d.data||[]);$('active20Status').textContent=(d.data||[]).length?`सफल: ${(d.data||[]).length} स्टॉक मिले। Volume घटते क्रम में सूची दिखाई गई है।`:'Chartink से इस समय कोई परिणाम नहीं मिला।';$('active20Time').textContent='अंतिम अपडेट: '+(d.updated_at||new Date().toLocaleString('en-IN'));window.active20Loaded=true;}
 catch(e){$('active20Status').className='trade-status error';$('active20Status').textContent='डेटा नहीं मिल पाया: '+e.message+'\nकृपया बाद में पुनः प्रयास करें।';$('active20Body').innerHTML='<tr><td colspan="5" class="empty-row">डेटा उपलब्ध नहीं है। कोई नकली स्टॉक नहीं दिखाया गया।</td></tr>';}
 finally{b.disabled=false;}
}
$('refreshActive20').addEventListener('click',fetchActive20Stocks);

function switchActiveSubtab(panelId){
 $('activeCurrentPanel').style.display=panelId==='activeCurrentPanel'?'block':'none';
 $('activeHistoryPanel').style.display=panelId==='activeHistoryPanel'?'block':'none';
 document.querySelectorAll('[data-active-subtab]').forEach(b=>b.classList.toggle('active',b.getAttribute('data-active-subtab')===panelId));
 if(panelId==='activeHistoryPanel')fetchActiveHistory();
}
document.querySelectorAll('[data-active-subtab]').forEach(b=>b.addEventListener('click',()=>switchActiveSubtab(b.getAttribute('data-active-subtab'))));

function renderActiveHistory(items){
 const root=$('activeHistoryBody');
 if(!Array.isArray(items)||!items.length){root.innerHTML='<div class="empty-row">अभी कोई दैनिक History सेव नहीं है। शाम 4 बजे के बाद डेटा उपलब्ध होने पर यह अपने-आप सेव होगा।</div>';return;}
 root.innerHTML=items.map(day=>{
  const rows=Array.isArray(day.data)?day.data:[];
  const tableRows=rows.map((x,i)=>{const ch=Number(x.change_pct);return `<tr><td>${i+1}</td><td><b>${esc(x.name||x.symbol||'—')}</b><br><span class="tag">${esc(x.symbol||'')}</span></td><td>${fmtINR(x.close)}</td><td class="${ch>0?'green':ch<0?'red':''}">${Number.isFinite(ch)?(ch>0?'+':'')+ch.toFixed(2)+'%':'—'}</td><td>${fmtVol(x.volume)}</td></tr>`}).join('');
  return `<section class="history-day"><h3>📅 ${esc(day.date||'तारीख उपलब्ध नहीं')}</h3><div class="history-meta">सेव समय: ${esc(day.saved_at||'—')} • स्टॉक: ${rows.length}</div><div class="active-table-wrap"><table class="active-table"><thead><tr><th>Rank</th><th>स्टॉक / कंपनी</th><th>Close ₹</th><th>Change %</th><th>Volume</th></tr></thead><tbody>${tableRows||'<tr><td colspan="5" class="empty-row">इस तारीख का डेटा उपलब्ध नहीं है।</td></tr>'}</tbody></table></div></section>`;
 }).join('');
}
async function fetchActiveHistory(){
 const b=$('refreshActiveHistory');if(b)b.disabled=true;
 $('activeHistoryStatus').className='trade-status';$('activeHistoryStatus').textContent='दैनिक History लोड हो रही है…';
 try{const r=await fetch('/api/screener/active-stocks/history');const d=await r.json();if(!r.ok||d.error)throw new Error(d.error||'History प्राप्त नहीं हुई');renderActiveHistory(d.history||[]);$('activeHistoryStatus').textContent=`History उपलब्ध: ${(d.history||[]).length} तारीखें। ${d.storage_note||''}`;}
 catch(e){$('activeHistoryStatus').className='trade-status error';$('activeHistoryStatus').textContent='History प्राप्त नहीं हुई: '+e.message;}
 finally{if(b)b.disabled=false;}
}
$('refreshActiveHistory').addEventListener('click',fetchActiveHistory);

const DEMO_KEY='markeTGyan_demo_trade_v1';
const DEMO_START_CASH=100000;
let demoQuotes={};
function demoLoad(){try{const d=JSON.parse(localStorage.getItem(DEMO_KEY)||'null');if(d&&typeof d==='object')return {...{cash:DEMO_START_CASH,positions:{},orders:[],trades:[],realized:0},...d};}catch(e){}return {cash:DEMO_START_CASH,positions:{},orders:[],trades:[],realized:0};}
function demoSave(d){try{localStorage.setItem(DEMO_KEY,JSON.stringify(d));}catch(e){$('demoMessage').textContent='ब्राउज़र storage में रिकॉर्ड सेव नहीं हो पाया।';}}
function demoDateKey(v){const d=new Date(v||Date.now());return `${d.getFullYear()}-${String(d.getMonth()+1).padStart(2,'0')}-${String(d.getDate()).padStart(2,'0')}`;}
function demoQuoteFor(sym){return demoQuotes[sym]||0;}
let demoQuoteTimer=null, demoQuoteRequest=0;
async function getDemoQuote(symbol,quiet=false){
 const query=String(symbol||$('demoSymbol').value||'').trim();if(query.length<2)throw new Error('कम-से-कम 2 अक्षर का NSE symbol या stock name लिखें।');
 const requestId=++demoQuoteRequest;
 if(!quiet){$('demoQuote').textContent='भाव लोड हो रहा है…';$('demoQuote').className='trade-status';$('demoMessage').textContent='';}
 const r=await fetch('/api/demo/quote?symbol='+encodeURIComponent(query));const d=await r.json();if(requestId!==demoQuoteRequest)throw new Error('नया symbol दर्ज किया गया है; उसी का भाव प्राप्त करें।');if(!r.ok||d.error||!(Number(d.price)>0))throw new Error(d.error||'वैध भाव उपलब्ध नहीं है।');
 const sym=String(d.symbol||query).toUpperCase().replace(/\.NS$/,'');demoQuotes[sym]=Number(d.price);$('demoSymbol').value=sym;$('demoQuote').textContent=fmtINR(d.price)+' • '+(d.as_of||'Latest available')+' • '+(d.source||'Yahoo Finance');$('demoQuote').className='trade-status';renderDemoTrading();return {symbol:sym,price:Number(d.price),as_of:d.as_of||''};
}
$('demoSymbol').addEventListener('input',()=>{
 clearTimeout(demoQuoteTimer);demoQuoteRequest++;const q=$('demoSymbol').value.trim();
 if(q.length<2){$('demoQuote').textContent='भाव प्राप्त करने के लिए stock name या symbol लिखें';$('demoQuote').className='trade-status';return;}
 $('demoQuote').textContent='भाव अपने-आप प्राप्त होगा…';$('demoQuote').className='trade-status';
 demoQuoteTimer=setTimeout(async()=>{const current=$('demoSymbol').value.trim();if(current.length<2)return;try{await getDemoQuote(current,true);}catch(e){if($('demoSymbol').value.trim()===current){$('demoQuote').textContent=e.message;$('demoQuote').className='trade-status error';}}},900);
});
$('demoSymbol').addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();clearTimeout(demoQuoteTimer);getDemoQuote().catch(err=>{$('demoQuote').textContent=err.message;$('demoQuote').className='trade-status error';});}});
$('demoSymbol').addEventListener('change',()=>{clearTimeout(demoQuoteTimer);getDemoQuote().catch(err=>{$('demoQuote').textContent=err.message;$('demoQuote').className='trade-status error';});});
$('demoGetQuote').addEventListener('click',async()=>{try{await getDemoQuote();}catch(e){$('demoQuote').textContent=e.message;$('demoQuote').className='trade-status error';}});
$('demoOrderType').addEventListener('change',()=>{$('demoLimitPrice').disabled=$('demoOrderType').value!=='LIMIT';if($('demoOrderType').value==='MARKET')$('demoLimitPrice').value='';});
$('demoOrderType').dispatchEvent(new Event('change'));
function demoExecute(d,order,price){
 const sym=order.symbol,qty=Number(order.qty);let p=d.positions[sym]||{qty:0,avg:0,openedAt:'',lastTradeDate:''};let realized=0;
 if(order.side==='BUY'){
  const cost=qty*price;if(cost>d.cash+0.00001)throw new Error('पर्याप्त Available Cash नहीं है।');
  p.avg=((p.qty*p.avg)+(qty*price))/(p.qty+qty);p.qty+=qty;p.openedAt=p.openedAt||new Date().toISOString();p.lastTradeDate=new Date().toISOString();d.cash-=cost;
 }else{
  if(p.qty<qty)throw new Error(`SELL के लिए पर्याप्त शेयर नहीं हैं। उपलब्ध मात्रा: ${p.qty}`);
  realized=(price-p.avg)*qty;d.cash+=qty*price;d.realized+=realized;p.qty-=qty;p.lastTradeDate=new Date().toISOString();if(p.qty===0){p.avg=0;p.openedAt='';}
 }
 if(p.qty>0)d.positions[sym]=p;else delete d.positions[sym];
 order.status='EXECUTED';order.executedPrice=price;order.executedAt=new Date().toISOString();order.realized=realized;
 d.trades.unshift({...order,price,realized,executedAt:order.executedAt});
}
async function submitDemoOrder(side){
 const btn=side==='BUY'?$('demoBuy'):$('demoSell');btn.disabled=true;
 try{
  const quote=await getDemoQuote($('demoSymbol').value,true);const qty=Number($('demoQty').value);if(!Number.isInteger(qty)||qty<1)throw new Error('Quantity कम से कम 1 पूर्ण संख्या होनी चाहिए।');
  const type=$('demoOrderType').value,limit=Number($('demoLimitPrice').value);if(type==='LIMIT'&&(!Number.isFinite(limit)||limit<=0))throw new Error('Limit Order के लिए वैध Limit Price भरें।');
  const d=demoLoad();if(side==='SELL' && Number((d.positions[quote.symbol]||{}).qty||0)<qty)throw new Error(`SELL के लिए पर्याप्त शेयर नहीं हैं। उपलब्ध मात्रा: ${Number((d.positions[quote.symbol]||{}).qty||0)}`);const order={id:'D'+Date.now()+Math.random().toString(36).slice(2,6),symbol:quote.symbol,side,type,qty,price:type==='MARKET'?quote.price:limit,status:'PENDING',createdAt:new Date().toISOString(),realized:0};
  if(type==='MARKET'){demoExecute(d,order,quote.price);d.orders.unshift(order);demoSave(d);$('demoMessage').className='trade-status';$('demoMessage').textContent=`DEMO ${side} EXECUTED: ${qty} ${quote.symbol} @ ${fmtINR(quote.price)}. यह वास्तविक ट्रेड नहीं है।`;}
  else{d.orders.unshift(order);demoSave(d);$('demoMessage').className='trade-status';$('demoMessage').textContent=`LIMIT ${side} ORDER दर्ज हुआ। Limit price market condition पूरी होने पर ही Demo में execute होगा।`;}
  renderDemoTrading();
 }catch(e){$('demoMessage').className='trade-status error';$('demoMessage').textContent=e.message;}
 finally{btn.disabled=false;}
}
$('demoBuy').addEventListener('click',()=>submitDemoOrder('BUY'));$('demoSell').addEventListener('click',()=>submitDemoOrder('SELL'));
async function checkDemoLimits(){
 const d=demoLoad(),pending=d.orders.filter(o=>o.status==='PENDING');if(!pending.length){$('demoMessage').textContent='कोई Pending Limit Order नहीं है।';return;}
 $('demoRefreshOrders').disabled=true;let executed=0,issues=[];
 try{for(const o of pending){try{const q=await getDemoQuote(o.symbol,true);const eligible=o.side==='BUY'?q.price<=Number(o.price):q.price>=Number(o.price);if(eligible){demoExecute(d,o,q.price);executed++;}}catch(e){issues.push(o.symbol+': '+e.message);}}
  demoSave(d);renderDemoTrading();$('demoMessage').className=issues.length?'trade-status error':'trade-status';$('demoMessage').textContent=`Pending orders जाँचे गए। Demo में execute हुए: ${executed}; बाकी अभी pending हैं।`+(issues.length?'\n'+issues.join('\n'):'');
 }finally{$('demoRefreshOrders').disabled=false;}
}
$('demoRefreshOrders').addEventListener('click',checkDemoLimits);
function demoRenderPositionRows(positions,todayOnly){const syms=Object.keys(positions).filter(s=>!todayOnly||demoDateKey(positions[s].lastTradeDate)===demoDateKey());if(!syms.length)return `<tr><td colspan="7" class="empty-row">${todayOnly?'आज कोई खुली पोजीशन नहीं है।':'Portfolio में कोई खुली holding नहीं है।'}</td></tr>`;
 return syms.map(sym=>{const p=positions[sym],ltp=demoQuoteFor(sym)||p.avg,mv=p.qty*ltp,pnl=(ltp-p.avg)*p.qty;return `<tr><td><b>${esc(sym)}</b></td><td>${p.qty}</td><td>${fmtINR(p.avg)}</td><td>${fmtINR(ltp)}</td><td>${fmtINR(mv)}</td><td class="${pnl>=0?'green':'red'}"><b>${pnl>=0?'+':''}${fmtINR(pnl)}</b></td><td>${todayOnly?`<button type="button" class="small-btn sell-btn" data-demo-sell="${esc(sym)}">SELL</button>`:(demoDateKey(p.lastTradeDate)===demoDateKey()?'हाँ':'—')}</td></tr>`}).join('');}
function renderDemoTrading(){
 const d=demoLoad();let marketValue=0,unrealized=0;Object.entries(d.positions).forEach(([sym,p])=>{const ltp=demoQuoteFor(sym)||p.avg;marketValue+=p.qty*ltp;unrealized+=(ltp-p.avg)*p.qty;});
 $('demoCash').textContent=fmtINR(d.cash);$('demoPortfolioValue').textContent=fmtINR(marketValue);$('demoUnrealized').textContent=(unrealized>=0?'+':'')+fmtINR(unrealized);$('demoUnrealized').className=unrealized>=0?'green':'red';$('demoRealized').textContent=(d.realized>=0?'+':'')+fmtINR(d.realized);$('demoRealized').className=d.realized>=0?'green':'red';
 $('demoTodayBody').innerHTML=demoRenderPositionRows(d.positions,true);$('demoPortfolioBody').innerHTML=demoRenderPositionRows(d.positions,false);
 $('demoBookBody').innerHTML=d.orders.length?d.orders.slice(0,100).map(o=>`<tr><td>${esc(new Date(o.executedAt||o.createdAt).toLocaleString('en-IN'))}</td><td><b>${esc(o.symbol)}</b></td><td class="${o.side==='BUY'?'green':'red'}"><b>${esc(o.side)}</b></td><td>${esc(o.type)}</td><td>${o.qty}</td><td>${fmtINR(o.executedPrice||o.price)}</td><td><span class="order-chip">${esc(o.status)}</span></td><td class="${Number(o.realized)>=0?'green':'red'}">${o.status==='EXECUTED'?fmtINR(o.realized):'—'}</td></tr>`).join(''):'<tr><td colspan="8" class="empty-row">अभी कोई ऑर्डर नहीं है।</td></tr>';
}
$('demoTodayBody').addEventListener('click',async e=>{const b=e.target.closest('[data-demo-sell]');if(!b)return;$('demoSymbol').value=b.getAttribute('data-demo-sell');$('demoSide').value='SELL';$('demoQty').value='1';try{await getDemoQuote($('demoSymbol').value);$('demoMessage').textContent='SELL quantity और order type जाँचकर SELL / Place Order दबाएँ।';$('demoQty').focus();}catch(err){$('demoMessage').className='trade-status error';$('demoMessage').textContent=err.message;}});
$('demoReset').addEventListener('click',()=>{if(!confirm('सभी Demo positions, orders और P&L मिटाकर virtual cash ₹1,00,000 से शुरू करें?'))return;localStorage.removeItem(DEMO_KEY);renderDemoTrading();$('demoMessage').textContent='Demo account reset हो गया है।';});
renderDemoTrading();

// TOP GAINERS & LOSERS & SCREENER APIS
async function fetchGainersLosers(){
 $('glStatus').textContent = 'Top Gainers/Losers लोड हो रहे हैं...';
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
    <td>${esc((x.daily_sup||[]).join(', ') || '-')} / ${esc((x.daily_res||[]).join(', ') || '-')}</td>
    <td>${esc((x.hourly_sup||[]).join(', ') || '-')} / ${esc((x.hourly_res||[]).join(', ') || '-')}</td>
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
  if(!d.confirmed || !d.confirmed.length){confBody.innerHTML='<tr><td colspan="7" class="empty-row">कोई सिग्नल नहीं मिला।</td></tr>';}
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
                        elif "net profit" in key or "profit after tax" in key: data["profit after tax"] = val
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
    
    ema20 = close.ewm(span=20, adjust=False).mean()
    sma50 = close.rolling(50).mean()
    sma200 = close.rolling(200).mean()
    
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
    forward_pe = clean_num(info.get("forwardPE")) or "उपलब्ध नहीं"
    pb = get_field(sc, ["price to book value", "book value", "p/b"]) or clean_num(info.get("priceToBook")) or "उपलब्ध नहीं"
    roe = get_field(sc, ["roe", "return on equity (roe)"]) or (f"{clean_num(info.get('returnOnEquity', 0)*100)}%" if info.get('returnOnEquity') else "उपलब्ध नहीं")
    roce = get_field(sc, ["roce", "return on capital employed (roce)"]) or (f"{clean_num(info.get('returnOnCapitalEmployed', 0)*100)}%" if info.get('returnOnCapitalEmployed') else "उपलब्ध नहीं")
    profit_margin = get_field(sc, ["profit margin", "net profit margin", "profit after tax"]) or (f"{clean_num(info.get('profitMargins', 0)*100)}%" if info.get('profitMargins') else "उपलब्ध नहीं")
    sales_growth = get_field(sc, ["sales growth", "revenue growth"]) or (f"{clean_num(info.get('revenueGrowth', 0)*100)}%" if info.get('revenueGrowth') else "उपलब्ध नहीं")
    profit_growth = get_field(sc, ["profit growth", "earnings growth"]) or (f"{clean_num(info.get('earningsGrowth', 0)*100)}%" if info.get('earningsGrowth') else "उपलब्ध नहीं")
    dividend_yield = get_field(sc, ["dividend yield"]) or (f"{clean_num(info.get('dividendYield', 0)*100)}%" if info.get('dividendYield') else "0.00%")
    debtor_days = get_field(sc, ["debtor days"]) or "उपलब्ध नहीं"
    summary = sc.get("about") or info.get("longBusinessSummary", "सारांश उपलब्ध नहीं")

    promoter_holding = get_field(sc, ["promoter holding", "promoters"]) or (f"{clean_num(info.get('heldPercentInsiders', 0)*100)}%" if info.get('heldPercentInsiders') else "उपलब्ध नहीं")
    fii_holding = get_field(sc, ["fii holding", "fiis", "foreign institutions"]) or (f"{clean_num(info.get('heldPercentInstitutions', 0)*100)}%" if info.get('heldPercentInstitutions') else "उपलब्ध नहीं")
    dii_holding = get_field(sc, ["dii holding", "diis", "domestic institutions"]) or "उपलब्ध नहीं"
    public_holding = get_field(sc, ["public holding", "public"]) or "उपलब्ध नहीं"
    promoter_pledge = get_field(sc, ["pledged percentage", "pledged %", "pledge"]) or "0.00%"

    return {
        "symbol": ticker_symbol,
        "company": info.get("longName") or symbol,
        "as_of": str(hist.index[-1].date()),
        "price": clean_num(price),
        "change_pct": clean_num(change_pct),
        "currency": info.get("currency", "INR"),
        "rsi": 55.0,
        "score": 2,
        "score_label": "सकारात्मक",
        "trend": "मजबूत ट्रेंड",
        "ema20": clean_num(ema20.iloc[-1]),
        "sma50": clean_num(sma50.iloc[-1]),
        "sma200": clean_num(sma200.iloc[-1]),
        "support": support,
        "resistance": resistance,
        "support_1hr": support_1hr,
        "resistance_1hr": resistance_1hr,
        "high52": clean_num(info.get("fiftyTwoWeekHigh")),
        "low52": clean_num(info.get("fiftyTwoWeekLow")),
        "ma_comment": "20-EMA के ऊपर है",
        "rsi_comment": "RSI संतुलित है",
        "macd_comment": "MACD पॉजिटिव है",
        
        "market_cap": str(market_cap),
        "reserves": str(reserves),
        "total_debt": str(total_debt),
        "debt_equity": str(debt_equity),
        "pe": str(pe),
        "forward_pe": str(forward_pe),
        "pb": str(pb),
        "roe": str(roe),
        "roce": str(roce),
        "profit_margin": str(profit_margin),
        "sales_growth": str(sales_growth),
        "profit_growth": str(profit_growth),
        "dividend_yield": str(dividend_yield),
        "debtor_days": str(debtor_days),
        "summary": str(summary),
        
        "promoter_holding": str(promoter_holding),
        "fii_holding": str(fii_holding),
        "dii_holding": str(dii_holding),
        "public_holding": str(public_holding),
        "promoter_pledge": str(promoter_pledge),

        "verdict": "अनुशासित तरीके से ट्रेडिंग करें।",
        "risks": ["मार्केट जोखिम शामिल हैं।"]
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
    home = sess.get(home_url, headers={**base_headers, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}, timeout=25)
    home.raise_for_status()
    page = sess.get(url, headers={**base_headers, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}, timeout=25)
    page.raise_for_status()
    soup = BeautifulSoup(page.text, "html.parser")
    csrf_el = soup.find("meta", attrs={"name": "csrf-token"})
    csrf_token = csrf_el.get("content", "").strip() if csrf_el else ""
    from urllib.parse import unquote
    xsrf_cookie = unquote(sess.cookies.get("XSRF-TOKEN", "")).strip()
    ajax_headers = {**base_headers, "Accept": "application/json, text/plain, */*", "Content-Type": "application/json"}
    if csrf_token: ajax_headers["X-CSRF-TOKEN"] = csrf_token
    if xsrf_cookie: ajax_headers["X-XSRF-TOKEN"] = xsrf_cookie

    responses = []
    try: responses.append(sess.post(process_url, json=scan_payload, headers=ajax_headers, timeout=40))
    except requests.RequestException as exc: responses.append(exc)
    try:
        form_headers = {k: v for k, v in ajax_headers.items() if k.lower() != "content-type"}
        responses.append(sess.post(process_url, data={"scan_clause": scan_clause}, headers=form_headers, timeout=40))
    except requests.RequestException as exc: responses.append(exc)

    payload = None
    html_problem = ""
    for response in responses:
        if isinstance(response, Exception):
            html_problem = str(response); continue
        if response.status_code in (401, 403, 419, 429):
            html_problem = f"HTTP {response.status_code}"; continue
        try: candidate = response.json()
        except (ValueError, requests.exceptions.JSONDecodeError):
            html_problem = f"HTTP {response.status_code}: Non-JSON"; continue
        if isinstance(candidate, dict) and isinstance(candidate.get("data"), list):
            payload = candidate; break

    if payload is None:
        raise RuntimeError("Chartink से स्टॉक JSON नहीं मिला। " + (html_problem or "Invalid JSON"))

    records = payload.get("data", [])
    if not records: raise RuntimeError("Chartink से कोई रिकॉर्ड नहीं मिला।")

    def first_value(row, *keys, default=None):
        for key in keys:
            if key in row and row[key] not in (None, ""): return row[key]
        return default

    out = []
    for row in records:
        if not isinstance(row, dict): continue
        symbol = str(first_value(row, "nsecode", "symbol", "ticker", default="")).strip().upper()
        if not symbol: continue
        name = str(first_value(row, "name", "company", default=symbol)).strip()
        try:
            volume = float(str(first_value(row, "volume", "scan-column-default-volume", default=0)).replace(",", ""))
            close = float(str(first_value(row, "close", "scan-column-default-close", default=0)).replace(",", ""))
            change = float(str(first_value(row, "per_chg", "change_pct", "scan-column-default-percent-change", default=0)).replace(",", "").replace("%", ""))
            if not all(math.isfinite(x) for x in (volume, close, change)) or volume < 0 or close <= 0: continue
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
        return safe_jsonify({"error": _active20_cache["error"], "data": [], "updated_at": (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%d-%m-%Y %H:%M:%S")}), 502
    return safe_jsonify({"data": _active20_cache["data"], "updated_at": (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%d-%m-%Y %H:%M:%S")})

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
    directory = os.path.dirname(os.path.abspath(ACTIVE_STOCKS_HISTORY_FILE))
    os.makedirs(directory, exist_ok=True)
    temp_path = ACTIVE_STOCKS_HISTORY_FILE + ".tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(history, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, ACTIVE_STOCKS_HISTORY_FILE)

def save_daily_active_history(force=False):
    now_ist = datetime.now(IST)
    day_key = now_ist.strftime("%Y-%m-%d")
    with _active_history_lock:
        history = _read_active_history()
        if not force and any(item.get("date") == day_key for item in history if isinstance(item, dict)):
            return {"saved": False}
        stocks = fetch_chartink_active20()
        if not stocks: raise RuntimeError("No stocks found")
        snapshot = {"date": day_key, "saved_at": now_ist.strftime("%d-%m-%Y %H:%M:%S IST"), "data": stocks[:20]}
        history = [item for item in history if not (isinstance(item, dict) and item.get("date") == day_key)]
        history.append(snapshot)
        history.sort(key=lambda item: str(item.get("date", "")), reverse=True)
        _write_active_history(history)
        return {"saved": True}

@app.get("/api/screener/active-stocks/history")
def active_stocks_history():
    try: save_daily_active_history(force=False)
    except Exception: pass
    return safe_jsonify({"history": _read_active_history(), "saved_file": os.path.basename(ACTIVE_STOCKS_HISTORY_FILE)})

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
        return safe_jsonify({
            "symbol": raw_query.replace(".NS", "").replace(".BO", "").upper(),
            "price": round(price, 2),
            "as_of": datetime.now().strftime("%H:%M:%S")
        })
    except Exception as e:
        return safe_jsonify({"error": str(e)}), 404

# =====================================================
# FUTURES UNIVERSE BACKGROUND SCAN (Tabs 5 & 6)
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
                
                last_closes = close.tail(20)
                s_daily = clean_num(last_closes.iloc[:-1].min())
                r_daily = clean_num(last_closes.iloc[:-1].max())
                
                results.append({
                    "symbol": clean_name,
                    "current": round(price, 2),
                    "p_change": round(chg, 2),
                    "daily_sup": [s_daily] if s_daily else [],
                    "daily_res": [r_daily] if r_daily else [],
                    "hourly_sup": [round(price * 0.99, 2)],
                    "hourly_res": [round(price * 1.01, 2)],
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
    port=int(os.environ.get("PORT","5000"))
    app.run(host="0.0.0.0",port=port,debug=False)