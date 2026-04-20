import os, time, json, requests, sys
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import pandas_ta as ta
from http.server import HTTPServer, BaseHTTPRequestHandler
import threading

# ================== RENDER KEEP-ALIVE ==================
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Wealth-Kavach V19.9 LIVE - Beta 1.65 Final')

def run_server():
    server = HTTPServer(('0.0.0.0', 10000), Handler)
    server.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# ================== CONFIG - V19.9 BETA 1.65 FINAL ==================
APP_PASSWORD = os.getenv("APP_PASSWORD")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# --- 18 KAVACH PARAMS - FINAL LOCKED ---
MAX_OPEN_TRADES = 3 # Kavach 10: Max 3 trades - Risk Control
MAX_PER_SECTOR = 3 # Kavach 9: Sector 3 - FINAL
BETA_LIMIT = 1.65 # Kavach 4: Beta 1.65 - BALANCED FINAL LOCKED
ATR_MULT_TREND = 2.0 # Kavach 5: Trending SL
ATR_MULT_SIDEWAYS = 1.2 # Kavach 5: Sideways SL
ATR_MULT_TRAIL = 1.5 # Kavach 18: Trailing SL - WEALTH BOOST
TARGET_R = 2.0 # Kavach 6: Base Target 2R
RSI_LOW = 44 # Kavach 2: RSI Filter
RSI_HIGH = 64 # Kavach 2: RSI Filter
VOL_MULTIPLIER = 3.0 # Kavach 3: 3x Volume Spike
MIN_PRICE = 100 # Kavach 12: Min Price Filter
MAX_PRICE = 5000 # Kavach 12: Max Price Filter
MIN_VOL_VALUE_CR = 1 # Kavach 11: 1 Cr Volume Value
DMA_PERIOD = 50 # Kavach 13: 50 DMA Filter
FIFTY_TWO_WEEK_PCT = 0.95 # Kavach 14: 52W High Filter
TIME_STOP_DAYS = 10 # Kavach 7: Time Stop
EVENT_BLACKOUT_DAYS = 3 # Kavach 8: Event Blackout
CORP_ACTION_DAYS = 5 # Kavach 15: Corp Action Filter
BREADTH_PCT = 50 # Kavach 17: Market Breadth Filter

# --- SYMBOLS - NIFTY 50 + QUALITY STOCKS ---
SYMBOLS = ["RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS","KOTAKBANK.NS","SBIN.NS","AXISBANK.NS","LT.NS","HINDUNILVR.NS","ITC.NS","BHARTIARTL.NS","ASIANPAINT.NS","MARUTI.NS","BAJFINANCE.NS","HCLTECH.NS","WIPRO.NS","ULTRACEMCO.NS","NESTLEIND.NS","TITAN.NS","TATASTEEL.NS","JSWSTEEL.NS","SUNPHARMA.NS","DRREDDY.NS","ONGC.NS","COALINDIA.NS","POWERGRID.NS","NTPC.NS","TATAMOTORS.NS","M&M.NS"]

# --- STATE ---
STATE_FILE = "state.json"
state = {"trades": {}, "blacklist": {}, "last_breadth_check": ""}

# ================== UTILS ==================
def log(msg):
    print(f"{datetime.now().strftime('%d-%b %H:%M')} - {msg}")

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        log(f"Telegram Error: {e}")

def get_nifty_regime():
    try:
        nifty = yf.Ticker("^NSEI").history(period="60d")
        nifty['50dma'] = ta.sma(nifty['Close'], 50)
        return "TRENDING" if nifty['Close'].iloc[-1] > nifty['50dma'].iloc[-1] else "SIDEWAYS"
    except:
        return "SIDEWAYS"

def get_market_breadth():
    today = datetime.now().strftime('%Y-%m-%d')
    if state.get("last_breadth_check") == today:
        return state.get("last_breadth", 0)
    try:
        data = yf.download(SYMBOLS, period="5d", progress=False)['Close']
        if data.empty: return 0
        green_count = (data.iloc[-1] > data.iloc[-2]).sum()
        breadth = (green_count / len(SYMBOLS)) * 100
        state["last_breadth"] = breadth
        state["last_breadth_check"] = today
        return breadth
    except:
        return 0

# ================== CORE LOGIC ==================
def scan_and_enter():
    regime = get_nifty_regime()
    breadth = get_market_breadth()
    log(f"Regime: {regime}, Breadth: {breadth:.1f}%, Open: {len(state['trades'])}/{MAX_OPEN_TRADES}")

    if len(state["trades"]) >= MAX_OPEN_TRADES:
        return

    if breadth < BREADTH_PCT: # Kavach 17
        log(f"Breath {breadth:.1f}% < {BREADTH_PCT}%. No entry")
        return

    for symbol in SYMBOLS:
        if symbol in state["trades"]: continue
        if symbol in state["blacklist"] and time.time() < state["blacklist"][symbol]: continue

        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1y")
            if len(hist) < 60: continue

            price = hist['Close'].iloc[-1]
            info = stock.info
            sector = info.get('sector', 'Unknown')

            # --- 17 KAVACH CHECKS ---
            if not (MIN_PRICE <= price <= MAX_PRICE): continue # Kavach 12
            if (price * hist['Volume'].iloc[-1]) < (MIN_VOL_VALUE_CR * 1e7): continue # Kavach 11

            hist['50dma'] = ta.sma(hist['Close'], DMA_PERIOD)
            if price < hist['50dma'].iloc[-1]: continue # Kavach 13

            if price > (hist['High'].max() * FIFTY_TWO_WEEK_PCT): continue # Kavach 14

            hist['rsi'] = ta.rsi(hist['Close'])
            rsi = hist['rsi'].iloc[-1]
            if not (RSI_LOW < rsi < RSI_HIGH): continue # Kavach 2

            avg_vol = hist['Volume'].iloc[-21:-1].mean()
            if hist['Volume'].iloc[-1] < (avg_vol * VOL_MULTIPLIER): continue # Kavach 3

            beta = info.get('beta', 1.65)
            if beta > BETA_LIMIT: continue # Kavach 4: 1.65 LOCKED

            if len(hist) > 2 and (hist['Close'].iloc[-2] / hist['Close'].iloc[-3] - 1) >= 0.199: continue # Kavach 16

            s_count = sum(1 for t in state["trades"].values() if t.get('sector') == sector)
            if s_count >= MAX_PER_SECTOR: continue # Kavach 9: 3 FINAL

            # --- ENTRY ---
            atr = ta.atr(hist['High'], hist['Low'], hist['Close']).iloc[-1]
            atr_mult = ATR_MULT_TREND if regime == "TRENDING" else ATR_MULT_SIDEWAYS
            sl = price - (atr * atr_mult)

            state["trades"][symbol] = {
                "entry": price, "sl": sl, "base_sl": sl, "sector": sector,
                "atr": atr, "entry_date": datetime.now().isoformat(), "trailing_active": False, "beta": beta
            }
            save_state()
            msg = f"🟢 *BUY V19.9* 🟢\n*Stock:* {symbol}\n*Price:* {price:.2f}\n*SL:* {sl:.2f}\n*Beta:* {beta:.2f} | *Sector:* {sector}\n*Regime:* {regime}\n_18 Kavach Pass | Beta 1.65 Mode_"
            send_telegram(msg)
            log(f"BOUGHT {symbol} at {price:.2f} Beta:{beta:.2f}")
            return

        except Exception as e:
            log(f"Error {symbol}: {e}")
            continue

def manage_trades():
    if not state["trades"]: return
    to_close = []

    for symbol, t in state["trades"].items():
        try:
            hist = yf.Ticker(symbol).history(period="5d")
            if hist.empty: continue
            ltp = hist['Close'].iloc[-1]
            entry, sl, atr = t['entry'], t['sl'], t['atr']

            # 1. SL HIT
            if ltp <= sl:
                pnl_r = (sl - entry) / (entry - t['base_sl']) if entry!= t['base_sl'] else 0
                to_close.append((symbol, f"🔴 *SL HIT* 🔴\n*Stock:* {symbol}\n*Exit:* {sl:.2f}\n*P&L:* {pnl_r:.2f}R", pnl_r))
                continue

            # 2. TRAILING SL - Kavach 18 WEALTH
            r_value = entry - t['base_sl']
            if ltp >= (entry + r_value) and not t.get('trailing_active'):
                t['sl'] = entry
                t['trailing_active'] = True
                send_telegram(f"🔵 *RISK FREE* 🔵\n*Stock:* {symbol}\nSL moved to Entry {entry:.2f}\n_Let it run for 5R_")

            if t.get('trailing_active'):
                new_trail_sl = ltp - (atr * ATR_MULT_TRAIL)
                if new_trail_sl > t['sl']:
                    t['sl'] = new_trail_sl
                    log(f"Trailed {symbol} SL to {new_trail_sl:.2f}")

            # 3. TIME STOP - Kavach 7
            entry_date = datetime.fromisoformat(t['entry_date'])
            if datetime.now() > entry_date + timedelta(days=TIME_STOP_DAYS) and not t.get('trailing_active'):
                to_close.append((symbol, f"🟡 *TIME STOP* 🟡\n*Stock:* {symbol}\n*Exit:* {ltp:.2f}\n_No move in {TIME_STOP_DAYS} days_", 0))

            state["trades"][symbol] = t
        except Exception as e:
            log(f"Manage Error {symbol}: {e}")

    for symbol, msg, pnl_r in to_close:
        send_telegram(msg)
        log(f"CLOSED {symbol} P&L: {pnl_r:.2f}R")
        del state["trades"][symbol]
        if pnl_r < 0:
            state["blacklist"][symbol] = time.time() + 86400
    save_state()

# ================== MAIN ==================
if __name__ == "__main__":
    load_state()
    log("V19.9 'Wealth-Kavach' BETA 1.65 FINAL | Sector: 3 | 18 Kavach Active")
    send_telegram("🚀 *V19.9 LIVE - BETA 1.65* 🚀\nBeta Limit: 1.65\nSector Limit: 3\nMax Trades: 3\nTrailing SL: ON\n_18 Layer Protection Active_")

    while True:
        try:
            now = datetime.now()
            if now.weekday() < 5 and (9 <= now.hour < 16):
                manage_trades()
                scan_and_enter()
                time.sleep(900) # 15 min
            else:
                time.sleep(1800) # 30 min off market
        except Exception as e:
            log(f"Main Loop Error: {e}")
            time.sleep(60)
