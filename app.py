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

# ============================================================
# MarkeTGyan - NSE FUTURES DOW THEORY PRO & SCREENER
# ============================================================

app = Flask(__name__)

# ============================================================
# 1. CONFIGURATION (Combined from snippet & prev UI)
# ============================================================

TIMEFRAME = "1h"
PERIOD = "60d"  # Need enough data for swing/daily analysis

PIVOT_LEFT = 3
PIVOT_RIGHT = 3

ENTRY_ZONE_PERCENT = 0.35
SL_BUFFER_PERCENT = 0.20
RISK_REWARD = 2.0

# Batch settings from your provided code
BATCH_SIZE = 10
MAX_WORKERS = 2

REQUEST_GAP_SECONDS = 1.0  # Safe pacing for Yahoo
MAX_RETRIES = 5
RETRY_BASE_SECONDS = 5

CACHE_TTL_SECONDS = 600
SCAN_INTERVAL = 900  # 15 mins

REQUEST_TIMEOUT = 30

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPORT_DIR = os.path.join(BASE_DIR, "MarkeTGyan_Reports")
os.makedirs(REPORT_DIR, exist_ok=True)

CACHE_FILE = os.path.join(BASE_DIR, "nse_futures_symbols_cache.json")
REPORT_FILE = os.path.join(REPORT_DIR, "NSE_FUTURES_DOW_REPORT.csv")

NSE_HOME = "[https://www.nseindia.com](https://www.nseindia.com)"
NSE_CONTRACT_BASE = "[https://nsearchives.nseindia.com/content/fo/](https://nsearchives.nseindia.com/content/fo/)"

# ============================================================
# 2. GLOBAL LOCKS AND CACHE
# ============================================================

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
    # New keys for GL tab
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

# ============================================================
# 3. SAFE YAHOO REQUEST PACING
# ============================================================

def wait_for_request_slot():
    global LAST_REQUEST_TIME
    global GLOBAL_COOLDOWN_UNTIL

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

# ============================================================
# 4. NSE FUTURES LIST FETCHING (Snippet Logic)
# ============================================================

def get_nse_session():
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "*/*",
        "Referer": NSE_HOME,
        "Connection": "keep-alive"
    })
    try:
        session.get(NSE_HOME, timeout=15)
    except Exception: pass
    return session

def parse_contract_file(content):
    if content[:2] == b"\x1f\x8b":
        content = gzip.decompress(content)
    df = pd.read_csv(io.BytesIO(content), low_memory=False)
    
    if df.empty: raise ValueError("Empty contract file")

    columns = {str(c).strip().lower(): c for c in df.columns}
    
    instrument_col = None
    symbol_col = None

    for name in ["fininstrmtp", "instrument", "instrumenttype"]:
        if name in columns:
            instrument_col = columns[name]
            break
            
    for name in ["tckrsymb", "symbol", "tradingsymbol", "underlyingsymbol"]:
        if name in columns:
            symbol_col = columns[name]
            break

    if symbol_col is None: raise ValueError("NSE symbol column not identified")

    symbols = df[symbol_col].astype(str).str.strip().str.upper()

    if instrument_col is not None:
        instrument = df[instrument_col].astype(str).str.upper().str.strip()
        mask = instrument.isin(["STF", "FUTSTK", "FUTSTOCK"])
        symbols = symbols[mask]

    symbols = sorted(set(s for s in symbols.tolist() if s and s != "NAN" and s.isalnum()))
    if len(symbols) < 50: raise ValueError("Incomplete NSE Futures list")
    return symbols

def load_cached_symbols():
    if not os.path.exists(CACHE_FILE): return []
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data.get("symbols", [])
    except Exception: return []

def save_cached_symbols(symbols):
    try:
        temp_file = CACHE_FILE + ".tmp"
        with open(temp_file, "w", encoding="utf-8") as f:
            json.dump({"symbols": symbols, "saved_at": datetime.now().isoformat()}, f, indent=2)
        os.replace(temp_file, CACHE_FILE)
    except Exception as e: print(f"Error saving cache: {e}")

def get_nse_futures_list():
    session = get_nse_session()
    # Try last 15 calendar dates.
    for days_back in range(15):
        date = datetime.now() - timedelta(days=days_back)
        date_string = date.strftime("%d%m%Y")
        url = f"{NSE_CONTRACT_BASE}NSE_FO_contract_{date_string}.csv.gz"
        try:
            response = session.get(url, timeout=REQUEST_TIMEOUT)
            if response.status_code != 200: continue
            symbols = parse_contract_file(response.content)
            save_cached_symbols(symbols)
            return symbols, f"NSE OFFICIAL CONTRACT FILE {date_string}"
        except Exception: continue

    cached = load_cached_symbols()
    if cached: return cached, "CACHED NSE LIST - UPDATE REQUIRED"
    
    raise RuntimeError("NSE list unavailable and no valid cache found")

# ============================================================
# 5. YAHOO FINANCE DATA WITH CACHE AND RETRY
# ============================================================

def fetch_stock_data(stock):
    with CACHE_LOCK:
        cached = STOCK_DATA_CACHE.get(stock)
        if cached:
            saved_time, saved_df = cached
            if time.time() - saved_time < CACHE_TTL_SECONDS:
                return saved_df.copy()

    url = f"[https://query1.finance.yahoo.com/v8/finance/chart/](https://query1.finance.yahoo.com/v8/finance/chart/){stock}.NS"
    params = {"range": PERIOD, "interval": TIMEFRAME}
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"}

    last_error = None
    for attempt in range(MAX_RETRIES):
        try:
            wait_for_request_slot()
            response = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)

            if response.status_code == 429:
                retry_after = response.headers.get("Retry-After")
                try: delay = float(retry_after)
                except: delay = RETRY_BASE_SECONDS * (2 ** attempt)
                activate_global_cooldown(min(delay, 60))
                time.sleep(min(delay, 30))
                continue

            if response.status_code in [500, 502, 503, 504]:
                time.sleep(RETRY_BASE_SECONDS * (attempt + 1))
                continue

            response.raise_for_status()
            raw = response.json()["chart"]["result"]
            if not raw: raise ValueError("No Yahoo market data")
            
            data = raw[0]
            quote = data["indicators"]["quote"][0]
            
            df = pd.DataFrame({
                "Date": pd.to_datetime(data["timestamp"], unit="s", utc=True),
                "Open": quote["open"], "High": quote["high"], "Low": quote["low"],
                "Close": quote["close"], "Volume": quote["volume"]
            })
            
            # Form DataFrame safely ensuring all lists are same length
            df = df.dropna(subset=["Open", "High", "Low", "Close"]).reset_index(drop=True)
            
            # Localizing timezone to IST (Yahoo returns UTC)
            df['Date'] = df['Date'].dt.tz_convert('Asia/Kolkata')

            with CACHE_LOCK:
                STOCK_DATA_CACHE[stock] = (time.time(), df.copy())
            return df

        except Exception as e:
            last_error = e
            time.sleep(1)

    raise Exception(f"Yahoo fetch failed: {last_error}")

# ============================================================
# 6. TECHNICAL INDICATORS & SWINGS
# ============================================================

def identify_swings(df):
    highs = []
    lows = []
    for i in range(PIVOT_LEFT, len(df) - PIVOT_RIGHT):
        current_high = float(df.loc[i, "High"])
        current_low = float(df.loc[i, "Low"])
        
        # Check if High is max in window
        if current_high == df.loc[i-PIVOT_LEFT:i+PIVOT_RIGHT, "High"].max():
            highs.append({"index": i, "price": current_high, "Date": df.loc[i, "Date"]})
            
        # Check if Low is min in window
        if current_low == df.loc[i-PIVOT_LEFT:i+PIVOT_RIGHT, "Low"].min():
            lows.append({"index": i, "price": current_low, "Date": df.loc[i, "Date"]})

    # Label Highs
    for i in range(1, len(highs)):
        if highs[i]["price"] > highs[i-1]["price"]: highs[i]["label"] = "HH"
        elif highs[i]["price"] < highs[i-1]["price"]: highs[i]["label"] = "LH"
        else: highs[i]["label"] = "EQH"

    # Label Lows
    for i in range(1, len(lows)):
        if lows[i]["price"] > lows[i-1]["price"]: lows[i]["label"] = "HL"
        elif lows[i]["price"] < lows[i-1]["price"]: lows[i]["label"] = "LL"
        else: lows[i]["label"] = "EQL"

    return highs, lows

def get_pivot_levels(data, p_left, p_right, is_high):
    series = data.reset_index(drop=True)
    levels = []
    for i in range(p_left, len(series) - p_right):
        window = series.iloc[i - p_left : i + p_right + 1]
        center_val = series.iloc[i]
        if is_high:
            if center_val == window.max(): levels.append(center_val)
        else:
            if center_val == window.min(): levels.append(center_val)
    return sorted(list(set(levels)), reverse=is_high) # Distinct sorted levels

def get_sr_from_pivots(levels, current_price, is_resistance):
    # Extracts relevant S/R closest to current price
    filtered = []
    if is_resistance:
        filtered = [l for l in levels if l > current_price]
        filtered.sort() # Closest first
    else:
        filtered = [l for l in levels if l < current_price]
        filtered.sort(reverse=True) # Closest first
    return filtered[:2] # Return top 2 levels

# ============================================================
# 7. ANALYSIS LOGIC (Combined Snippet Logic + GL Tab Data)
# ============================================================

def analyze_stock(stock):
    df = fetch_stock_data(stock)
    last_candle = df.iloc[-1]
    current_close = float(last_candle["Close"])

    # --- 1. Top Gainer/Loser Data Calculation (Requirement) ---
    # We need Daily data for gainer/loser pct and daily S/R
    
    # 1a. Calculate Pct Change (Today Close vs Yesterday Close)
    # Group hourly data by date to find yesterday's close
    current_ist_date = last_candle['Date'].date()
    # Get data before today
    prev_days_data = df[df['Date'].dt.date < current_ist_date]
    
    if prev_days_data.empty:
        # Fallback to start of Period if not enough days fetched
        prev_close = float(df.iloc[0]['Open']) 
    else:
        # Get close of last hourly candle of the previous trading day
        prev_close = float(prev_days_data.iloc[-1]['Close'])
        
    p_change = ((current_close - prev_close) / prev_close) * 100 if prev_close != 0 else 0

    # 1b. Resample to Daily for Daily S/R
    df_daily = df.set_index('Date').resample('D').agg({
        'Open': 'first', 'High': 'max', 'Low': 'min', 'Close': 'last'
    }).dropna()
    
    # Daily S/R based on Daily Pivot Highs/Lows (L2, R2)
    d_pivot_h = get_pivot_levels(df_daily['High'], 2, 2, True)
    d_pivot_l = get_pivot_levels(df_daily['Low'], 2, 2, False)
    
    d_res = get_sr_from_pivots(d_pivot_h, current_close, True)
    d_sup = get_sr_from_pivots(d_pivot_l, current_close, False)

    # --- 2. Dow Theory Analysis (Snippet Logic) ---
    highs, lows = identify_swings(df)
    
    # Hourly S/R from hourly swings (for GL tab)
    h_pivot_h = [h['price'] for h in highs]
    h_pivot_l = [l['price'] for l in lows]
    h_res = get_sr_from_pivots(h_pivot_h, current_close, True)
    h_sup = get_sr_from_pivots(h_pivot_l, current_close, False)

    # Base result dict used in Advance Screener
    result = {
        "stock": stock,
        "current": current_close,
        # Data needed for Gainer/Loser Tab
        "p_change": p_change,
        "daily_res": d_res,
        "daily_sup": d_sup,
        "hourly_res": h_res,
        "hourly_sup": h_sup,

        # Data needed for Screener Tab (Snippet Logic)
        "trend": "SIDEWAYS",
        "hh": None, "hl": None, "ll": None, "lh": None,
        "entry": None, "sl": None, "target": None,
        "signal": "WAIT",
        "status": "INSUFFICIENT STRUCTURE",
        "updated": last_candle["Date"].strftime("%Y-%m-%d %H:%M")
    }

    if len(highs) < 2 or len(lows) < 2: return result

    prev_high = highs[-2]["price"]
    latest_high = highs[-1]["price"]
    prev_low = lows[-2]["price"]
    latest_low = lows[-1]["price"]

    is_hh = latest_high > prev_high
    is_hl = latest_low > prev_low
    is_ll = latest_low < prev_low
    is_lh = latest_high < prev_high

    result["hh"] = latest_high if is_hh else None
    result["hl"] = latest_low if is_hl else None
    result["ll"] = latest_low if is_ll else None
    result["lh"] = latest_high if is_lh else None

    c_high = float(last_candle["High"])
    c_low = float(last_candle["Low"])
    c_open = float(last_candle["Open"])

    # Uptrend Buy
    if is_hh and is_hl:
        result["trend"] = "UPTREND"
        entry = latest_low["price"]
        sl = entry * (1 - SL_BUFFER_PERCENT/100)
        target = entry + ((entry - sl) * RISK_REWARD)
        
        result.update({"entry": entry, "sl": sl, "target": target})

        upper_zone = entry * (1 + ENTRY_ZONE_PERCENT/100)
        # Touch in current candle?
        touched = c_low <= upper_zone and c_high >= entry
        
        if touched:
            if current_close > c_open: # Bullish close
                result["signal"] = "BUY VALID"
                result["status"] = "HL Pullback + Bullish Confirmation"
            else:
                result["status"] = "In Zone - Wait for Bullish Close"
        else:
            result["status"] = "Waiting for HL Pullback"

    # Downtrend Sell
    elif is_ll and is_lh:
        result["trend"] = "DOWNTREND"
        entry = latest_high["price"]
        sl = entry * (1 + SL_BUFFER_PERCENT/100)
        target = entry - ((sl - entry) * RISK_REWARD)

        result.update({"entry": entry, "sl": sl, "target": target})

        lower_zone = entry * (1 - ENTRY_ZONE_PERCENT/100)
        # Touch in current candle?
        touched = c_high >= lower_zone and c_low <= entry

        if touched:
            if current_close < c_open: # Bearish close
                result["signal"] = "SELL VALID"
                result["status"] = "LH Pullback + Bearish Confirmation"
            else:
                result["status"] = "In Zone - Wait for Bearish Close"
        else:
            result["status"] = "Waiting for LH Pullback"
    else:
        result["trend"] = "SIDEWAYS / TRANSITION"
        result["status"] = "Wait for Clear Structure"

    return result

# ============================================================
# 8. REPORT & UTILITIES
# ============================================================

def save_report(results):
    if not results: return
    try:
        temp_file = REPORT_FILE + ".tmp"
        # Filter fields for CSV to match snippet format
        csv_fields = ["stock", "updated", "current", "trend", "hh", "hl", "ll", "lh", "entry", "sl", "target", "signal", "status"]
        df = pd.DataFrame(results)[csv_fields]
        df.to_csv(temp_file, index=False, encoding="utf-8-sig")
        os.replace(temp_file, REPORT_FILE)
    except Exception as e: print(f"Error saving report: {e}")

# ============================================================
# 9. SCANNER CORE (Combined Snippet logic + Post-processing)
# ============================================================

def scan_all_stocks():
    with LOCK:
        if STATE["running"]: return
        STATE["running"] = True
        stocks = list(STATE["stocks"])
        STATE["status"] = "SCANNING"
        STATE["completed"] = 0
        STATE["errors"] = 0
        STATE["progress"] = 0
        STATE["results"] = []
        STATE["confirmed"] = []
        STATE["gainers"] = []
        STATE["losers"] = []

    if not stocks:
        with LOCK:
            STATE["status"] = "ERROR"
            STATE["message"] = "No NSE Futures stocks available"
            STATE["running"] = False
        return

    all_results = []
    total = len(stocks)
    total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

    try:
        # BATCH PROCESSING (from snippet)
        for batch_index in range(total_batches):
            start = batch_index * BATCH_SIZE
            end = min(start + BATCH_SIZE, total)
            batch_stocks = stocks[start:end]
            
            with LOCK:
                STATE["batch_number"] = batch_index + 1
                STATE["total_batches"] = total_batches
                STATE["current_batch"] = f"{start+1}-{end}"
                STATE["message"] = f"Scanning batch {batch_index+1}/{total_batches}"

            batch_results = []
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {executor.submit(analyze_stock, stock): stock for stock in batch_stocks}
                for future in as_completed(futures):
                    stock = futures[future]
                    try:
                        batch_results.append(future.result())
                    except Exception as e:
                        # Error result format ensuring GL keys exist
                        batch_results.append({
                            "stock": stock, "current": 0, "p_change": 0,
                            "daily_res":[], "daily_sup":[], "hourly_res":[], "hourly_sup":[],
                            "trend": "ERROR", "hh": None, "hl": None, "ll": None, "lh": None,
                            "entry": None, "sl": None, "target": None, "signal": "WAIT",
                            "status": str(e)[:100], "updated": "-"
                        })
                        with LOCK: STATE["errors"] += 1

            with LOCK:
                all_results.extend(batch_results)
                STATE["results"] = all_results
                STATE["completed"] = len(all_results)
                STATE["progress"] = round((STATE["completed"] / total) * 100, 1)

            save_report(batch_results) # Snippet saves partials

        # --- POST PROCESSING (Requirement) ---
        # 1. Confirmed signals
        df_final = pd.DataFrame(all_results)
        # Filter valid results for GL sorting (remove error rows)
        df_valid = df_final[df_final['trend'] != "ERROR"]
        
        # 2. Gainers/Losers
        top_gainers = df_valid.nlargest(5, 'p_change').to_dict('records')
        top_losers = df_valid.nsmallest(5, 'p_change').to_dict('records')

        with LOCK:
            STATE["confirmed"] = [r for r in all_results if r["signal"] in ["BUY VALID", "SELL VALID"]]
            STATE["gainers"] = top_gainers
            STATE["losers"] = top_losers
            STATE["status"] = "COMPLETED"
            STATE["message"] = "Scan finished."
            STATE["last_scan"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")

        # Final complete report save
        save_report(all_results)

    except Exception as e:
        with LOCK:
            STATE["status"] = "ERROR"
            STATE["message"] = str(e)
    finally:
        with LOCK: STATE["running"] = False

# ============================================================
# 10. BACKGROUND SERVICE
# ============================================================

def background_scanner():
    while True:
        try:
            # Update list (Snippet uses archives, retry logic)
            symbols, source = get_nse_futures_list()
            with LOCK:
                STATE["stocks"] = symbols
                STATE["total"] = len(symbols)
                STATE["source"] = source
                STATE["last_list_update"] = datetime.now().strftime("%d-%m-%Y %H:%M:%S")
            
            # Start Scan
            scan_all_stocks()
        except Exception as e:
            with LOCK:
                STATE["status"] = "ERROR"
                STATE["message"] = str(e)
        
        time.sleep(SCAN_INTERVAL)

# ============================================================
# 11. DASHBOARD HTML (NEW TABBED UI)
# ============================================================

HTML = r"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MarkeTGyan NSE Futures Pro</title>
    <style>
        :root {
            --bg-dark: #101820;
            --bg-panel: #1a2634;
            --text-main: #e0e6ed;
            --text-muted: #8899a6;
            --accent: #1da1f2;
            --border: #38444d;
            --green: #00ba7c;
            --red: #f4212e;
            --orange: #ffd200;
        }

        body {
            margin: 0; padding: 0;
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: var(--bg-dark);
            color: var(--text-main);
            font-size: 14px;
        }

        /* Tabs Container */
        .tabs {
            display: flex;
            background-color: var(--bg-panel);
            border-bottom: 1px solid var(--border);
            position: sticky;
            top: 0;
            z-index: 100;
        }

        .tab-link {
            background: none;
            border: none;
            color: var(--text-muted);
            padding: 15px 20px;
            cursor: pointer;
            font-size: 15px;
            font-weight: 600;
            transition: 0.2s;
            border-bottom: 3px solid transparent;
        }

        .tab-link:hover {
            color: var(--accent);
            background-color: rgba(29, 161, 242, 0.1);
        }

        .tab-link.active {
            color: var(--accent);
            border-bottom-color: var(--accent);
        }

        /* Tab Content */
        .tab-content {
            display: none;
            padding: 20px;
        }

        .tab-content.active {
            display: block;
            animation: fadeIn 0.3s;
        }

        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }

        /* Original & New UI Styling combined */
        .container { max-width: 1600px; margin: 0 auto; }
        
        .header-panel {
            background-color: var(--bg-panel);
            padding: 15px 20px;
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        h1 { margin: 0; font-size: 18px; color: var(--accent); }
        .source-info { font-size: 12px; color: var(--text-muted); text-align: right; }

        /* Widgets */
        .card-row { display: flex; gap: 15px; margin-bottom: 15px; flex-wrap: wrap; }
        .card {
            background-color: var(--bg-panel);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 15px;
            flex: 1;
            min-width: 200px;
        }
        .card-title { color: var(--text-muted); font-size: 12px; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 5px; }
        .card-value { font-size: 24px; font-weight: bold; }

        /* Progress Bar */
        .progress-container {
            background-color: var(--bg-panel);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 15px;
            margin-bottom: 15px;
        }
        .prog-bar-outer {
            background: #38444d;
            border-radius: 10px;
            height: 10px;
            overflow: hidden;
            margin: 10px 0;
        }
        .prog-bar-inner {
            background: var(--accent);
            height: 100%;
            width: 0%;
            transition: width 0.3s ease;
        }
        .prog-status {
            display: flex;
            justify-content: space-between;
            font-size: 12px;
            color: var(--text-muted);
        }

        /* Tables */
        .table-section {
            background-color: var(--bg-panel);
            border: 1px solid var(--border);
            border-radius: 8px;
            margin-bottom: 15px;
            overflow: hidden;
        }
        .table-header {
            padding: 10px 15px;
            border-bottom: 1px solid var(--border);
            font-weight: bold;
            color: var(--accent);
        }
        .table-responsive { overflow-x: auto; }
        
        table { width: 100%; border-collapse: collapse; min-width: 600px; }
        th { text-align: left; padding: 10px 15px; color: var(--text-muted); border-bottom: 1px solid var(--border); font-size: 12px; white-space: nowrap;}
        td { padding: 10px 15px; border-bottom: 1px solid #2f3336; white-space: nowrap;}
        tr:last-child td { border-bottom: none; }
        tr:hover { background-color: rgba(255,255,255,0.02); }

        /* Controls */
        .controls {
            display: flex; gap: 10px; margin-bottom: 15px; align-items: center; flex-wrap: wrap;
        }
        button.btn {
            background-color: var(--accent);
            color: white;
            border: none;
            padding: 8px 15px;
            border-radius: 20px;
            font-weight: bold;
            cursor: pointer;
            font-size: 13px;
            transition: 0.2s;
        }
        button.btn:hover { background-color: #1a91da; }
        button.btn:disabled { opacity: 0.5; cursor: not-allowed; }
        .search-input {
            background-color: var(--bg-dark);
            border: 1px solid var(--border);
            color: var(--text-main);
            padding: 8px 12px;
            border-radius: 20px;
            width: 250px;
        }

        /* Helpers */
        .uptrend { color: var(--green); }
        .downtrend { color: var(--red); }
        .sideways { color: var(--orange); }
        .val-buy { background-color: rgba(0, 186, 124, 0.15); color: var(--green); padding: 2px 6px; border-radius: 4px; font-weight: bold; }
        .val-sell { background-color: rgba(244, 33, 46, 0.15); color: var(--red); padding: 2px 6px; border-radius: 4px; font-weight: bold; }
        .sr-level { font-size: 11px; color: var(--text-muted); }

        @media (max-width: 768px) {
            .card-row { flex-direction: column; gap: 10px; }
            .tabs { overflow-x: auto; }
            .tab-link { padding: 10px 15px; font-size: 13px; white-space: nowrap;}
        }
    </style>
</head>
<body>

<div class="header-panel">
    <h1>MarkeTGyan NSE Futures Pro</h1>
    <div class="source-info" id="sourceInfo">Loading info...</div>
</div>

<!-- TABS NAVIGATION -->
<div class="tabs">
    <button class="tab-link active" onclick="openTab(event, 'tab-dashboard')">Dashboard</button>
    <button class="tab-link" onclick="openTab(event, 'tab-gl')">Top Gainer/Loser</button>
    <button class="tab-link" onclick="openTab(event, 'tab-screener')">Advance Screener</button>
</div>

<div class="container">
    
    <!-- 1. DASHBOARD TAB -->
    <div id="tab-dashboard" class="tab-content active">
        <div class="card-row">
            <div class="card">
                <div class="card-title">Scanner Status</div>
                <div class="card-value" id="statStatus">IDLE</div>
                <div class="prog-status" style="margin-top:5px"><span id="statMsg">Ready</span></div>
            </div>
            <div class="card">
                <div class="card-title">Total Stocks</div>
                <div class="card-value" id="statTotal">0</div>
            </div>
            <div class="card">
                <div class="card-title uptrend">BUY Alerts</div>
                <div class="card-value uptrend" id="statBuy">0</div>
            </div>
            <div class="card">
                <div class="card-title downtrend">SELL Alerts</div>
                <div class="card-value downtrend" id="statSell">0</div>
            </div>
        </div>

        <div class="progress-container">
            <div class="prog-status">
                <span style="font-weight:bold; color:var(--text-main)">Scanning Progress</span>
                <span id="progRate">0%</span>
            </div>
            <div class="prog-bar-outer">
                <div class="prog-bar-inner" id="progBar"></div>
            </div>
            <div class="prog-status">
                <span id="progCount">0 / 0</span>
                <span id="progBatch">Batch: -</span>
            </div>
        </div>
        
        <div style="font-size:12px; color:var(--text-muted); text-align:center; margin-top:20px;">
            Last Scan: <span id="lastScanTime">-</span> | Timeframe: 1 Hour | Period: 60 Days
        </div>
    </div>

    <!-- 2. GAINER/LOSER TAB -->
    <div id="tab-gl" class="tab-content">
        <!-- Gainers -->
        <div class="table-section">
            <div class="table-header uptrend">▲ TOP 5 GAINERS</div>
            <div class="table-responsive">
                <table>
                    <thead>
                        <tr>
                            <th>SYMBOL</th>
                            <th>LTP</th>
                            <th>CHANGE %</th>
                            <th>DAILY RES</th>
                            <th>DAILY SUP</th>
                            <th>HOURLY RES</th>
                            <th>HOURLY SUP</th>
                        </tr>
                    </thead>
                    <tbody id="gainerBody">
                        <tr><td colspan="7">Scanning in progress or not started...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- Losers -->
        <div class="table-section" style="margin-top:20px;">
            <div class="table-header downtrend">▼ TOP 5 LOSERS</div>
            <div class="table-responsive">
                <table>
                    <thead>
                        <tr>
                            <th>SYMBOL</th>
                            <th>LTP</th>
                            <th>CHANGE %</th>
                            <th>DAILY RES</th>
                            <th>DAILY SUP</th>
                            <th>HOURLY RES</th>
                            <th>HOURLY SUP</th>
                        </tr>
                    </thead>
                    <tbody id="loserBody">
                        <tr><td colspan="7">Scanning in progress or not started...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

    <!-- 3. SCREENER TAB (Original Logic Location) -->
    <div id="tab-screener" class="tab-content">
        <div class="controls">
            <button class="btn" id="btnScan" onclick="triggerRescan()">SCAN NOW</button>
            <input type="text" class="search-input" id="StockSearch" placeholder="Search stock symbol..." onkeyup="doSearch()">
            <button class="btn" style="background-color:#38444d;" onclick="downloadCsv()">Download CSV</button>
        </div>

        <!-- Confirmed -->
        <div class="table-section">
            <div class="table-header">★ CONFIRMED DOW THEORY SIGNALS</div>
            <div class="table-responsive">
                <table>
                    <thead>
                        <tr>
                            <th>STOCK</th>
                            <th>TREND</th>
                            <th>LTP</th>
                            <th>ENTRY</th>
                            <th>STOP LOSS</th>
                            <th>TARGET</th>
                            <th>SIGNAL</th>
                        </tr>
                    </thead>
                    <tbody id="confirmedBody">
                        <tr><td colspan="7">No valid signals.</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <!-- All Stocks -->
        <div class="table-section">
            <div class="table-header">ALL NSE FUTURES WATCHLIST</div>
            <div class="table-responsive">
                <table id="allStocksTable">
                    <thead>
                        <tr>
                            <th>SYMBOL</th>
                            <th>LTP</th>
                            <th>TREND</th>
                            <th>HH</th>
                            <th>HL</th>
                            <th>LL</th>
                            <th>LH</th>
                            <th>STATUS</th>
                        </tr>
                    </thead>
                    <tbody id="allStocksBody">
                        <tr><td colspan="8">Loading data...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>
    </div>

</div>

<script>
    let rawData = []; // Store unfiltered results

    // Tab Logic
    function openTab(evt, tabId) {
        let contents = document.getElementsByClassName("tab-content");
        for (let i = 0; i < contents.length; i++) contents[i].classList.remove("active");

        let links = document.getElementsByClassName("tab-link");
        for (let i = 0; i < links.length; i++) links[i].classList.remove("active");

        document.getElementById(tabId).classList.add("active");
        evt.currentTarget.classList.add("active");
    }

    // formatting utilities
    const num = (v) => (v === null || v === undefined) ? "-" : Number(v).toFixed(2);
    
    function getTrendClass(trend) {
        if(trend === "UPTREND") return "uptrend";
        if(trend === "DOWNTREND") return "downtrend";
        return "sideways";
    }

    function formatSr(levels) {
        if(!levels || levels.length === 0) return "-";
        return levels.map(l => Number(l).toFixed(2)).join(" , ");
    }

    // Rendering Data
    function updateDashboard(data) {
        // Source Info
        document.getElementById("sourceInfo").innerHTML = `List: ${data.source || '-'}<br>Updated: ${data.last_list_update || '-'}`;
        
        // Stats
        document.getElementById("statStatus").textContent = data.status;
        document.getElementById("statMsg").textContent = data.message;
        document.getElementById("statTotal").textContent = data.total;
        
        let buyCnt = 0, sellCnt = 0;
        if(data.confirmed) {
            buyCnt = data.confirmed.filter(r => r.signal === "BUY VALID").length;
            sellCnt = data.confirmed.filter(r => r.signal === "SELL VALID").length;
        }
        document.getElementById("statBuy").textContent = buyCnt;
        document.getElementById("statSell").textContent = sellCnt;

        // Progress
        document.getElementById("progRate").textContent = data.progress + "%";
        document.getElementById("progBar").style.width = data.progress + "%";
        document.getElementById("progCount").textContent = `${data.completed} / ${data.total}`;
        document.getElementById("progBatch").textContent = `Batch: ${data.batch_number}/${data.total_batches} (${data.current_batch})`;
        
        document.getElementById("lastScanTime").textContent = data.last_scan || "-";
        
        // Buttons
        document.getElementById("btnScan").disabled = data.running;
    }

    function renderGainerLoser(gainers, losers) {
        const createGLRows = (data) => {
            if(!data || data.length === 0) return '<tr><td colspan="7">Scanning in progress...</td></tr>';
            return data.map(r => {
                let changeCls = r.p_change >= 0 ? 'uptrend' : 'downtrend';
                return `<tr>
                    <td style="font-weight:bold">${r.stock}</td>
                    <td>${num(r.current)}</td>
                    <td class="${changeCls}" style="font-weight:bold">${r.p_change.toFixed(2)}%</td>
                    <td class="downtrend sr-level">${formatSr(r.daily_res)}</td>
                    <td class="uptrend sr-level">${formatSr(r.daily_sup)}</td>
                    <td class="downtrend sr-level">${formatSr(r.hourly_res)}</td>
                    <td class="uptrend sr-level">${formatSr(r.hourly_sup)}</td>
                </tr>`;
            }).join('');
        };

        document.getElementById("gainerBody").innerHTML = createGLRows(gainers);
        document.getElementById("loserBody").innerHTML = createGLRows(losers);
    }

    function renderScreenerTabs(data) {
        rawData = data.results || [];
        
        // Confirmed Table
        const confBody = document.getElementById("confirmedBody");
        if(!data.confirmed || data.confirmed.length === 0) {
            confBody.innerHTML = '<tr><td colspan="7">Waiting for scan or no signals found.</td></tr>';
        } else {
            confBody.innerHTML = data.confirmed.map(r => `
                <tr>
                    <td style="font-weight:bold">${r.stock}</td>
                    <td class="${getTrendClass(r.trend)}">${r.trend}</td>
                    <td>${num(r.current)}</td>
                    <td>${num(r.entry)}</td>
                    <td class="downtrend">${num(r.sl)}</td>
                    <td class="uptrend">${num(r.target)}</td>
                    <td><span class="${r.signal === 'BUY VALID' ? 'val-buy' : 'val-sell'}">${r.signal}</span></td>
                </tr>
            `).join('');
        }

        // All Stocks Table (initial render)
        doSearch(); 
    }

    function doSearch() {
        const query = document.getElementById("StockSearch").value.toUpperCase();
        const body = document.getElementById("allStocksBody");
        
        let filtered = rawData;
        if(query) {
            filtered = rawData.filter(r => r.stock.includes(query));
        }

        if(filtered.length === 0) {
            body.innerHTML = `<tr><td colspan="8">No stocks found matching "${query}"</td></tr>`;
            return;
        }

        body.innerHTML = filtered.map(r => `
            <tr>
                <td style="font-weight:bold">${r.stock}</td>
                <td>${num(r.current)}</td>
                <td class="${getTrendClass(r.trend)}">${r.trend}</td>
                <td style="font-size:12px; color:var(--text-muted)">
                    H:${num(r.hh)|num(r.lh)} | L:${num(r.hl)|num(r.ll)}
                </td>
                <td style="font-size:12px; max-width:250px; overflow:hidden; text-overflow:ellipsis;" class="${r.trend === 'ERROR' ? 'downtrend':''}">
                    ${r.status}
                </td>
            </tr>
        `).join('');
    }

    async function fetchData() {
        try {
            const response = await fetch('/api/status', { cache: 'no-store' });
            if (!response.ok) return;
            const data = await response.json();
            
            updateDashboard(data);
            renderGainerLoser(data.gainers, data.losers);
            renderScreenerTabs(data);
        } catch (error) { console.error('Error fetching data:', error); }
    }

    async function triggerRescan() {
        try {
            document.getElementById("btnScan").disabled = true;
            const response = await fetch('/api/rescan', { method: 'POST' });
            const result = await response.json();
            if(!result.success) alert(result.message);
            fetchData(); // Immediate update
        } catch (error) { 
            console.error('Error triggering scan:', error);
            document.getElementById("btnScan").disabled = false;
        }
    }

    function downloadCsv() { window.location.href = '/download'; }

    // Start auto-refresh
    fetchData();
    setInterval(fetchData, 5000); // UI updates every 5s

</script>
</body>
</html>
"""

# ============================================================
# 12. FLASK DASHBOARD ROUTES
# ============================================================

@app.route("/")
def dashboard():
    # Force state update in UI immediately upon loading page based on cache
    symbols_len = len(STATE["stocks"])
    if symbols_len == 0:
        threading.Thread(target=get_nse_futures_list, daemon=True).start()
    return render_template_string(HTML)

@app.route("/api/status")
def api_status():
    with LOCK:
        data = dict(STATE)
        data["results"] = list(STATE["results"])
        data["confirmed"] = list(STATE["confirmed"])
        data["gainers"] = list(STATE["gainers"])
        data["losers"] = list(STATE["losers"])
    return jsonify(data)

@app.route("/api/rescan", methods=["POST"])
def api_rescan():
    with LOCK:
        if STATE["running"]:
            return jsonify({"success": False, "message": "Scanner is already running"})
        if not STATE["stocks"]:
            return jsonify({"success": False, "message": "Wait for NSE stock list to load"})

    threading.Thread(target=scan_all_stocks, daemon=True).start()
    return jsonify({"success": True, "message": "Manual scan started"})

@app.route("/download")
def download():
    if not os.path.exists(REPORT_FILE):
        return "Report file not generated yet. Run a scan first.", 404
    return send_file(REPORT_FILE, as_attachment=True)

# ============================================================
# 13. START APPLICATION
# ============================================================

if __name__ == "__main__":
    print("\n" + "="*50)
    print("MarkeTGyan NSE Futures Pro Starting")
    print("Url: [http://127.0.0.1:8080](http://127.0.0.1:8080)")
    print("="*50 + "\n")

    # Initial symbols fetch
    threading.Thread(target=get_nse_futures_list, daemon=True).start()
    
    # Background scheduler
    threading.Thread(target=background_scanner, daemon=True).start()

    app.run(host="127.0.0.1", port=8080, debug=False, threaded=True, use_reloader=False)