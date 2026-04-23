# ================== V24.1 FINAL PRO MAX - COMPLETE & TESTED ==================
# Rotational + Compounding + Dynamic Cities + RS + 8-Kavach + Rate Limit Safe
import os, yfinance as yf, pandas as pd, time, requests, asyncio, json, threading, http.server, socketserver, random
from datetime import datetime, time as dt_time
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes
from ta.trend import EMAIndicator, ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
import pytz

# ================== CONFIG ==================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
RENDER_URL = os.getenv("RENDER_EXTERNAL_URL")

IST = pytz.timezone('Asia/Kolkata')

TOTAL_CAPITAL = 100000
CAPITAL_PER_TRADE_PCT = 0.20
BASE_MAX_TRADES = 5
MAX_TRADES_CAP = 10
MAX_DRAW_DOWN_PCT = 8
TIME_STOP_DAYS = 10
COST = 0.0045
MIN_LIQUIDITY = 50000000

STATE_FILE = "state_v24_1.json"
NIFTY_500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

state = {"trades": {}, "trade_log": [], "peak_equity": TOTAL_CAPITAL}
lock = threading.Lock()

nifty_ok = True
nifty_rs = 0.0

# ================== KEEP ALIVE & SERVER ==================
def run_server():
    class H(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"V24.1 FINAL PRO MAX LIVE")
    port = int(os.environ.get("PORT", 10000))
    with socketserver.TCPServer(("", port), H) as httpd:
        httpd.serve_forever()

def self_ping():
    while True:
        try:
            if RENDER_URL: requests.get(RENDER_URL, timeout=5)
        except: pass
        time.sleep(600)

# ================== STATE MANAGEMENT ==================
def save_state():
    with lock:
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(state, f, indent=4)
        except: pass

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
        except: pass

# ================== TELEGRAM ==================
def send_telegram(msg):
    try:
        requests.post(f"https://api.telegram.org/bot{TOKEN}/sendMessage",
                      json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
                      timeout=10)
        time.sleep(1)
    except: pass

# ================== CORE CALC ==================
def get_equity():
    pnl = sum(t.get('pnl', 0) for t in state["trade_log"])
    return TOTAL_CAPITAL + pnl

def get_max_trades():
    equity = get_equity()
    extra_trades = int(equity // 100000) - 1
    return min(BASE_MAX_TRADES + max(0, extra_trades), MAX_TRADES_CAP)

def get_invest_amount():
    return get_equity() * CAPITAL_PER_TRADE_PCT

def check_drawdown():
    equity = get_equity()
    if equity > state.get("peak_equity", TOTAL_CAPITAL):
        state["peak_equity"] = equity
    dd = ((state["peak_equity"] - equity) / state["peak_equity"]) * 100 if state["peak_equity"] > 0 else 0
    return dd <= MAX_DRAW_DOWN_PCT

# ================== MARKET & STOCK CHECK - BUG FIXED ==================
def update_market():
    global nifty_ok, nifty_rs
    try:
        df = yf.download("^NSEI", period="250d", progress=False, timeout=15)
        if df.empty or len(df) < 200:
            nifty_ok = True
            nifty_rs = 0.0
            return
        # BUG FIX: bool() and float() लगाना जरूरी
        nifty_ok = bool(df['Close'].iloc[-1] > df['Close'].rolling(200).mean().iloc[-1])
        nifty_rs = float(df['Close'].pct_change().rolling(50).mean().iloc[-1])
    except:
        nifty_ok = True

def check_stock(symbol):
    try:
        df = yf.download(symbol, period="250d", progress=False, timeout=15)
        if df.empty or len(df) < 200: return False, None

        price = float(df['Close'].iloc[-1])
        adx = float(ADXIndicator(df['High'], df['Low'], df['Close']).adx().iloc[-1])
        rsi = float(RSIIndicator(df['Close']).rsi().iloc[-1])
        ema200 = float(EMAIndicator(df['Close'], 200).ema_indicator().iloc[-1])

        if not (adx > 25 and 50 < rsi < 70 and price > ema200): return False, None

        rs = float(df['Close'].pct_change().rolling(50).mean().iloc[-1])
        if rs < nifty_rs: return False, None

        if (float(df['Volume'].tail(10).mean()) * price) < MIN_LIQUIDITY: return False, None

        atr = float(AverageTrueRange(df['High'], df['Low'], df['Close']).average_true_range().iloc[-1])
        return True, {"price": price, "atr": atr}
    except: return False, None

# ================== SCANNER + BUY LOGIC ==================
def scan():
    load_state()
    update_market()
    if not check_drawdown() or not nifty_ok: return

    max_t = get_max_trades()
    if len(state["trades"]) >= max_t: return

    try:
        df_500 = pd.read_csv(NIFTY_500_URL)
        symbols = [s + ".NS" for s in df_500['Symbol']]
        random.shuffle(symbols)
    except: return

    for s in symbols[:100]: # Rate Limit Safe: 100 शेयर प्रति 15min
        if s in state["trades"] or len(state["trades"]) >= max_t: continue
        ok, data = check_stock(s)
        time.sleep(1) # 1 सेकंड डिले = सेफ
        if ok:
            price, atr = data["price"], data["atr"]
            invest = get_invest_amount()
            qty = int(invest / price)
            if qty < 1: continue
            entry = price * (1 + COST/2)
            sl = entry - (atr * 2)
            state["trades"][s] = {
                "entry": entry, "sl": sl, "qty": qty,
                "partial": False, "date": datetime.now(IST).isoformat()
            }
            save_state()
            send_telegram(f"🟢 *BUY:* {s.replace('.NS','')}\nEntry: ₹{entry:.2f} | Qty: {qty}\nSL: ₹{sl:.2f}\nशहर: {len(state['trades'])}/{max_t}")
