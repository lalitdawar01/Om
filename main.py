# ================== V24.1 FINAL PRO MAX - RENDER STABLE VERSION ==================
import os, yfinance as yf, pandas as pd, time, requests, asyncio, json, threading, http.server, socketserver, random
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
IST = pytz.timezone('Asia/Kolkata')

TOTAL_CAPITAL = 100000
CAPITAL_PER_TRADE_PCT = 0.20
BASE_MAX_TRADES = 5
MAX_TRADES_CAP = 10
MAX_DRAW_DOWN_PCT = 8
TIME_STOP_DAYS = 10
COST = 0.0045

STATE_FILE = "state_v24_1.json"
NIFTY_500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

state = {"trades": {}, "trade_log": [], "peak_equity": TOTAL_CAPITAL}
lock = threading.Lock()
nifty_ok = True
nifty_rs = 0.0

# ================== RENDER PORT FIX (The Most Important Part) ==================
def run_server():
    class HealthHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            self.wfile.write(b"V24.1 FINAL PRO MAX IS LIVE AND SCANNING")

    # रेंडर के लिए पोर्ट 10000 को बाइंड करना अनिवार्य है
    port = int(os.environ.get("PORT", 10000))
    print(f"Starting Health Check Server on Port {port}")
    with socketserver.TCPServer(("", port), HealthHandler) as httpd:
        httpd.serve_forever()

# ================== CORE LOGIC & STATE ==================
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

def get_equity():
    pnl = sum(t.get('pnl', 0) for t in state["trade_log"])
    return TOTAL_CAPITAL + pnl

def update_market():
    global nifty_ok, nifty_rs
    try:
        df = yf.download("^NSEI", period="250d", progress=False, timeout=15)
        if df.empty or len(df) < 200: return
        # BUG FIX: Added bool and float
        nifty_ok = bool(df['Close'].iloc[-1] > df['Close'].rolling(200).mean().iloc[-1])
        nifty_rs = float(df['Close'].pct_change().rolling(50).mean().iloc[-1])
    except: nifty_ok = True

def check_stock(symbol):
    try:
        df = yf.download(symbol, period="250d", progress=False, timeout=15)
        if df.empty or len(df) < 200: return False, None
        
        price = float(df['Close'].iloc[-1])
        adx = float(ADXIndicator(df['High'], df['Low'], df['Close']).adx().iloc[-1])
        rsi = float(RSIIndicator(df['Close']).rsi().iloc[-1])
        ema200 = float(EMAIndicator(df['Close'], 200).ema_indicator().iloc[-1])

        if adx > 25 and 50 < rsi < 70 and price > ema200:
            rs = float(df['Close'].pct_change().rolling(50).mean().iloc[-1])
            if rs > nifty_rs:
                atr = float(AverageTrueRange(df['High'], df['Low'], df['Close']).average_true_range().iloc[-1])
                return True, {"price": price, "atr": atr}
        return False, None
    except: return False, None

# ================== SCANNER EXECUTION ==================
def perform_scan():
    load_state()
    update_market()
    equity = get_equity()
    
    # Dynamic Max Trades based on Capital
    max_t = min(BASE_MAX_TRADES + max(0, int((equity - 100000) // 100000)), MAX_TRADES_CAP)
    
    if len(state["trades"]) >= max_t or not nifty_ok: return

    try:
        df_500 = pd.read_csv(NIFTY_500_URL)
        symbols = [s + ".NS" for s in df_500['Symbol']]
        random.shuffle(symbols)
        
        for s in symbols[:50]: # Safety Batch
            if s in state["trades"] or len(state["trades"]) >= max_t: continue
            ok, data = check_stock(s)
            time.sleep(1)
            if ok:
                # यहाँ BUY लॉजिक और टेलीग्राम मैसेज आएगा
                print(f"Found Signal: {s}")
                pass
    except: pass

# ================== TELEGRAM COMMANDS ==================
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    load_state()
    msg = f"📊 *V24.1 STATUS*\nEquity: ₹{get_equity():.0f}\nMarket: {'BULL' if nifty_ok else 'BEAR'}\nActive Cities: {len(state['trades'])}"
    await update.message.reply_text(msg, parse_mode='Markdown')

# ================== MAIN RUNNER ==================
async def main_loop():
    while True:
        now = datetime.now(IST)
        # Market Hours check (9:15 to 15:30)
        if 9 <= now.hour < 16:
            perform_scan()
        await asyncio.sleep(600) # Every 10 mins

async def start_bot():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("status", status))
    
    # Start Scanner Loop in background
    asyncio.create_task(main_loop())
    
    async with app:
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        # Keep the bot running
        while True: await asyncio.sleep(3600)

if __name__ == "__main__":
    # 1. सबसे पहले सर्वर को अलग थ्रेड में चलाएं (Render Fix)
    threading.Thread(target=run_server, daemon=True).start()
    
    # 2. फिर टेलीग्राम और स्कैनर चालू करें
    try:
        asyncio.run(start_bot())
    except KeyboardInterrupt:
        pass
