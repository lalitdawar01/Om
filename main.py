
# ================== V24.1 FINAL PRO MAX (COMPLETE) ==================
# Rotational + Compounding + Dynamic Cities + RS + 8-Kavach
import os, yfinance as yf, pandas as pd, time, requests, asyncio, json, threading, http.server, socketserver
from datetime import datetime
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
nifty_rs = 0

# ================== KEEP ALIVE & SERVER ==================
def run_server():
    class H(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"V24.1 FINAL PRO MAX LIVE")
    socketserver.TCPServer(("", int(os.environ.get("PORT", 10000))), H).serve_forever()

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
    if extra_trades < 0: extra_trades = 0
    return min(BASE_MAX_TRADES + extra_trades, MAX_TRADES_CAP)

def get_invest_amount():
    return get_equity() * CAPITAL_PER_TRADE_PCT

def check_drawdown():
    equity = get_equity()
    if equity > state.get("peak_equity", TOTAL_CAPITAL):
        state["peak_equity"] = equity
    dd = ((state["peak_equity"] - equity) / state["peak_equity"]) * 100
    return dd <= MAX_DRAW_DOWN_PCT

# ================== MARKET & STOCK CHECK ==================
def update_market():
    global nifty_ok, nifty_rs
    try:
        df = yf.download("^NSEI", period="250d", progress=False)
        nifty_ok = df['Close'].iloc[-1] > df['Close'].rolling(200).mean().iloc[-1]
        nifty_rs = df['Close'].pct_change().rolling(50).mean().iloc[-1]
    except: nifty_ok = True

def check_stock(symbol):
    try:
        df = yf.download(symbol, period="250d", progress=False)
        if len(df) < 200: return False, None
        price = df['Close'].iloc[-1]
        adx = ADXIndicator(df['High'], df['Low'], df['Close']).adx().iloc[-1]
        rsi = RSIIndicator(df['Close']).rsi().iloc[-1]
        ema200 = EMAIndicator(df['Close'], 200).ema_indicator().iloc[-1]
        
        if not (adx > 25 and 50 < rsi < 70 and price > ema200): return False, None
        
        rs = df['Close'].pct_change().rolling(50).mean().iloc[-1]
        if rs < nifty_rs: return False, None
        
        if (df['Volume'].tail(10).mean() * price) < MIN_LIQUIDITY: return False, None
        
        atr = AverageTrueRange(df['High'], df['Low'], df['Close']).average_true_range().iloc[-1]
        return True, {"price": price, "atr": atr}
    except: return False, None

# ================== ROTATIONAL SCAN ==================
def get_symbols():
    try:
        df = pd.read_csv(NIFTY_500_URL)
        symbols = [s + ".NS" for s in df['Symbol']]
        batch = int(time.time() / 900) % 5
        return symbols[batch*100:(batch+1)*100]
    except: return []

def scan():
    load_state()
    if not check_drawdown() or not nifty_ok: return
    max_t = get_max_trades()
    if len(state["trades"]) >= max_t: return

    for s in get_symbols():
        if s in state["trades"] or len(state["trades"]) >= max_t: continue
        ok, data = check_stock(s)
        time.sleep(0.3)
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
            send_telegram(f"🟢 *BUY:* {s.replace('.NS','')}\nEntry: ₹{entry:.2f}\nशहर: {len(state['trades'])}/{max_t}")
            break

# ================== MANAGEMENT ==================
def manage():
    load_state()
    for s in list(state["trades"].keys()):
        t = state["trades"][s]
        try:
            ltp = yf.Ticker(s).history(period="1d")['Close'].iloc[-1]
            exit_p = ltp * (1 - COST/2)
            if not t["partial"] and ltp >= t["entry"] * 1.08:
                state["trades"][s]["sl"] = t["entry"]
                state["trades"][s]["partial"] = True
                send_telegram(f"💰 *PARTIAL:* {s.replace('.NS','')} (SL moved to Cost)")
            
            days = (datetime.now(IST) - datetime.fromisoformat(t["date"])).days
            if ltp <= t["sl"] or (days >= TIME_STOP_DAYS and ltp < t["entry"] * 1.02):
                pnl = (exit_p - t["entry"]) * t["qty"]
                state["trade_log"].append({"symbol": s, "pnl": pnl})
                del state["trades"][s]
                save_state()
                send_telegram(f"🔴 *EXIT:* {s.replace('.NS','')}\nPnL: ₹{pnl:.0f} | Days: {days}")
        except: continue

# ================== BOT COMMANDS ==================
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    eq = get_equity()
    msg = f"📊 *Status V24.1*\nEquity: ₹{eq:.0f}\nशहर: {len(state['trades'])}/{get_max_trades()}\nDrawdown: {(((state['peak_equity']-eq)/state['peak_equity'])*100):.1f}%"
    await update.message.reply_text(msg, parse_mode='Markdown')

# ================== MAIN ==================
async def main():
    load_state()
    threading.Thread(target=run_server, daemon=True).start()
    threading.Thread(target=self_ping, daemon=True).start()
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("status", status))
    send_telegram("🚀 *V24.1 FINAL PRO MAX LIVE*")
    while True:
        now = datetime.now(IST)
        if now.weekday() < 5 and (9*60+15 <= now.hour*60+now.minute <= 15*60+30):
            update_market()
            manage()
            if now.minute % 15 == 0: scan()
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
