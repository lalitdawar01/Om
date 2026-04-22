# ================== V24.1 FINAL PRO MAX (STABLE) ==================
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
CAPITAL_PER_TRADE_PCT = 0.20 # 20% Compounding
BASE_MAX_TRADES = 5 
MAX_TRADES_CAP = 10 
MAX_DRAW_DOWN_PCT = 8
TIME_STOP_DAYS = 10
COST = 0.0045
MIN_LIQUIDITY = 50000000 # 5 Cr Turnover

STATE_FILE = "state_v24_1.json"
NIFTY_500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

state = {"trades": {}, "trade_log": [], "peak_equity": TOTAL_CAPITAL}
lock = threading.Lock()

nifty_ok = True
nifty_rs = 0

# ================== SERVER & PING ==================
def run_server():
    class H(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"V24.1 FINAL PRO MAX ACTIVE")
    try:
        port = int(os.environ.get("PORT", 10000))
        socketserver.TCPServer(("", port), H).serve_forever()
    except: pass

def self_ping():
    while True:
        try:
            if RENDER_URL: requests.get(RENDER_URL, timeout=10)
        except: pass
        time.sleep(600)

# ================== STATE ==================
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
        url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
        requests.post(url, json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
        time.sleep(1)
    except: pass

# ================== LOGIC ==================
def get_equity():
    pnl = sum(t.get('pnl', 0) for t in state["trade_log"])
    return TOTAL_CAPITAL + pnl

def get_max_trades():
    equity = get_equity()
    extra = int(equity // 100000) - 1
    return min(BASE_MAX_TRADES + max(0, extra), MAX_TRADES_CAP)

def get_invest_amount():
    return get_equity() * CAPITAL_PER_TRADE_PCT

def check_drawdown():
    equity = get_equity()
    if equity > state.get("peak_equity", TOTAL_CAPITAL):
        state["peak_equity"] = equity
    dd = ((state["peak_equity"] - equity) / state["peak_equity"]) * 100
    return dd <= MAX_DRAW_DOWN_PCT

# ================== ENGINE ==================
def update_market():
    global nifty_ok, nifty_rs
    try:
        df = yf.download("^NSEI", period="250d", progress=False, interval="1d")
        if not df.empty:
            nifty_ok = df['Close'].iloc[-1] > df['Close'].rolling(200).mean().iloc[-1]
            nifty_rs = df['Close'].pct_change().rolling(50).mean().iloc[-1]
    except: nifty_ok = True

def check_stock(symbol):
    try:
        df = yf.download(symbol, period="250d", progress=False, interval="1d")
        if len(df) < 200: return False, None
        
        price = df['Close'].iloc[-1]
        adx = ADXIndicator(df['High'], df['Low'], df['Close']).adx().iloc[-1]
        rsi = RSIIndicator(df['Close']).rsi().iloc[-1]
        ema200 = EMAIndicator(df['Close'], 200).ema_indicator().iloc[-1]
        
        # 8-Kavach Filters
        if not (adx > 25 and 50 < rsi < 70 and price > ema200): return False, None
        
        rs = df['Close'].pct_change().rolling(50).mean().iloc[-1]
        if rs < nifty_rs: return False, None
        
        if (df['Volume'].tail(10).mean() * price) < MIN_LIQUIDITY: return False, None
        
        atr = AverageTrueRange(df['High'], df['Low'], df['Close']).average_true_range().iloc[-1]
        return True, {"price": price, "atr": atr}
    except: return False, None

# ================== SCAN & MANAGE ==================
def scan():
    load_state()
    if not check_drawdown() or not nifty_ok: return
    max_t = get_max_trades()
    if len(state["trades"]) >= max_t: return

    try:
        df_500 = pd.read_csv(NIFTY_500_URL)
        symbols = [s + ".NS" for s in df_500['Symbol']]
        batch_idx = int(time.time() / 900) % 5
        batch = symbols[batch_idx*100 : (batch_idx+1)*100]
    except: return

    for s in batch:
        if s in state["trades"] or len(state["trades"]) >= max_t: continue
        ok, data = check_stock(s)
        time.sleep(0.5) # Anti-ban delay
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
            send_telegram(f"🟢 *BUY:* {s.replace('.NS','')}\nPrice: ₹{entry:.2f} | City: {len(state['trades'])}/{max_t}")
            break

def manage():
    load_state()
    for s in list(state["trades"].keys()):
        t = state["trades"][s]
        try:
            df = yf.download(s, period="1d", progress=False)
            if df.empty: continue
            ltp = df['Close'].iloc[-1]
            exit_p = ltp * (1 - COST/2)
            
            # Partial Lock at 8%
            if not t["partial"] and ltp >= t["entry"] * 1.08:
                state["trades"][s]["sl"] = t["entry"]
                state["trades"][s]["partial"] = True
                send_telegram(f"💰 *PARTIAL:* {s.replace('.NS','')} - SL moved to Cost")
            
            # Exit Conditions
            days = (datetime.now(IST) - datetime.fromisoformat(t["date"])).days
            if ltp <= t["sl"] or (days >= TIME_STOP_DAYS and ltp < t["entry"] * 1.02):
                pnl = (exit_p - t["entry"]) * t["qty"]
                state["trade_log"].append({"symbol": s, "pnl": pnl})
                del state["trades"][s]
                save_state()
                send_telegram(f"🔴 *EXIT:* {s.replace('.NS','')}\nPnL: ₹{pnl:.0f} | Days: {days}")
        except: continue

# ================== BOT ==================
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if str(update.message.chat_id) != CHAT_ID: return
    eq = get_equity()
    msg = f"📊 *V24.1 Status*\nEquity: ₹{eq:.0f}\nCities: {len(state['trades'])}/{get_max_trades()}\nDrawdown: {(((state['peak_equity']-eq)/state['peak_equity'])*100):.1f}%"
    await update.message.reply_text(msg, parse_mode='Markdown')

# ================== MAIN LOOP ==================
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
