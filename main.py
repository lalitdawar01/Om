# ================== V25.2 PRO MAX FINAL - CHOWKIDAR EDITION ==================
import os, yfinance as yf, pandas as pd, time, asyncio, json, threading, http.server, socketserver, requests, random
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
TIME_STOP_DAYS = 10
COST = 0.0045
MAX_DRAW_DOWN = 8.0

STATE_FILE = "state_v25.json"
NIFTY_500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

state = {"trades": {}, "trade_log": [], "peak_equity": TOTAL_CAPITAL}
lock = threading.Lock()

nifty_ok = True
nifty_rs = 0.0

# ================== RENDER SERVER ==================
def run_server():
    class H(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"V25.2 LIVE")
    port = int(os.environ.get("PORT", 10000))
    socketserver.TCPServer(("", port), H).serve_forever()

# ================== STATE ==================
def save_state():
    with lock:
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(state, f)
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

def get_max_trades():
    eq = get_equity()
    extra = int(eq // 100000) - 1
    return min(BASE_MAX_TRADES + max(0, extra), MAX_TRADES_CAP)

def check_drawdown():
    eq = get_equity()
    if eq > state.get("peak_equity", TOTAL_CAPITAL):
        state["peak_equity"] = eq
        save_state()
    peak = state.get("peak_equity", TOTAL_CAPITAL)
    return ((peak - eq) / peak) * 100 if peak > 0 else 0

# ================== TELEGRAM ==================
def send_msg(msg):
    try:
        requests.post(
            f"https://api.telegram.org/bot{TOKEN}/sendMessage",
            json={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"},
            timeout=10
        )
        time.sleep(1)
    except: pass

# ================== MARKET ==================
def update_market():
    global nifty_ok, nifty_rs
    try:
        df = yf.download("^NSEI", period="250d", progress=False, timeout=15)
        if df.empty or len(df) < 200:
            nifty_ok = True
            nifty_rs = 0.0
            return
        ema200 = df['Close'].rolling(200).mean().iloc[-1]
        nifty_ok = bool(df['Close'].iloc[-1] > ema200)
        nifty_rs = float(df['Close'].pct_change().rolling(50).mean().iloc[-1])
    except:
        nifty_ok = True
        nifty_rs = 0.0

# ================== SCAN ==================
def scan():
    load_state()
    update_market()
    eq = get_equity()

    if check_drawdown() > MAX_DRAW_DOWN:
        return

    if not nifty_ok:
        return

    max_t = get_max_trades()
    if len(state["trades"]) >= max_t:
        return

    try:
        df_nse = pd.read_csv(NIFTY_500_URL, timeout=15)
        symbols = [s + ".NS" for s in df_nse['Symbol']]

        if len(symbols) < 50:
            return

        batch_size = 50
        total_batches = max(1, len(symbols)//batch_size)
        idx = int(time.time()/900) % total_batches
        batch = symbols[idx*batch_size:(idx+1)*batch_size]

        random.shuffle(batch)

        for s in batch:
            if s in state["trades"] or len(state["trades"]) >= max_t:
                continue

            df = yf.download(s, period="250d", progress=False, timeout=15)
            if df.empty or len(df) < 200:
                continue

            price = float(df['Close'].iloc[-1])
            adx = float(ADXIndicator(df['High'], df['Low'], df['Close']).adx().iloc[-1])
            rsi = float(RSIIndicator(df['Close']).rsi().iloc[-1])
            ema = float(EMAIndicator(df['Close'], 200).ema_indicator().iloc[-1])
            rs = float(df['Close'].pct_change().rolling(50).mean().iloc[-1])

            if adx > 25 and 50 < rsi < 70 and price > ema and rs > nifty_rs:
                atr = float(AverageTrueRange(df['High'], df['Low'], df['Close']).average_true_range().iloc[-1])
                qty = int((eq * CAPITAL_PER_TRADE_PCT) / price)
                if qty < 1:
                    continue

                entry = price * (1 + COST/2)
                sl = entry - (atr * 2)

                with lock:
                    state["trades"][s] = {
                        "entry": entry,
                        "sl": sl,
                        "qty": qty,
                        "trail": False,
                        "date": datetime.now(IST).isoformat()
                    }
                save_state()

                send_msg(f"🟢 BUY: {s.replace('.NS','')}\n₹{entry:.2f} | SL ₹{sl:.2f} | Qty {qty}")
                break

            time.sleep(1)

    except Exception as e:
        print("Scan Error:", e)

# ================== MANAGE - CHOWKIDAR MODE ==================
def manage():
    load_state()

    for s in list(state["trades"].keys()):
        try:
            t = state["trades"][s]
            # 1 min data for flash crash detection
            df = yf.download(s, period="2d", interval="1m", progress=False, timeout=10)
            if df.empty:
                continue

            ltp = float(df['Close'].iloc[-1])
            prev_1min = float(df['Close'].iloc[-2]) if len(df) > 1 else ltp

            # Volatility Guard - 1 min में 3% गिरा तो तुरंत EXIT
            if ((prev_1min - ltp) / prev_1min) > 0.03:
                exit_price = ltp * (1 - COST/2)
                pnl = (exit_price - t["entry"]) * t["qty"]
                state["trade_log"].append({"symbol": s, "pnl": pnl, "date": datetime.now(IST).isoformat()})
                del state["trades"][s]
                save_state()
                send_msg(f"🚨 FLASH EXIT: {s.replace('.NS','')}\nPnL ₹{pnl:.0f} | 1min में -3%")
                continue

            # SL Trail Logic - +5% होते ही Cost पे SL
            if not t["trail"] and ltp >= t["entry"] * 1.05:
                state["trades"][s]["sl"] = t["entry"]
                state["trades"][s]["trail"] = True
                save_state()
                send_msg(f"⚡ SL TRAILED: {s.replace('.NS','')} | Risk Free Now")

            days = (datetime.now(IST) - datetime.fromisoformat(t["date"])).days

            # Normal Exit Logic
            if ltp <= t["sl"] or (days >= TIME_STOP_DAYS and ltp < t["entry"]*1.02):
                exit_price = ltp * (1 - COST/2)
                pnl = (exit_price - t["entry"]) * t["qty"]

                state["trade_log"].append({"symbol": s, "pnl": pnl, "date": datetime.now(IST).isoformat()})
                del state["trades"][s]
                save_state()

                reason = "SL Hit" if ltp <= t["sl"] else "Time Stop"
                send_msg(f"🔴 EXIT: {s.replace('.NS','')} | {reason}\nPnL ₹{pnl:.0f} | Days {days}")

        except Exception as e:
            print(f"Manage Error {s}:", e)
            continue

# ================== BOT ==================
async def status(update: Update, context: ContextTypes.DEFAULT_TYPE):
    eq = get_equity()
    dd = check_drawdown()
    await update.message.reply_text(
        f"📊 *STATUS V25.2*\nEquity: ₹{eq:.0f}\nDrawdown: {dd:.2f}%\nTrades: {len(state['trades'])}/{get_max_trades()}\nNIFTY: {'BULL' if nifty_ok else 'BEAR'}\nMode: CHOWKIDAR ✅",
        parse_mode='Markdown'
    )

# ================== LOOP - 1 MIN CHOWKIDAR ==================
async def main_loop():
    while True:
        now = datetime.now(IST)

        if now.weekday() < 5 and (9*60+15 <= now.hour*60+now.minute <= 15*60+30):
            # Priority 1: हर 1 मिनट होल्डिंग्स की चौकीदारी
            manage()

            # Priority 2: हर 15 मिनट नए शहर ढूंढो
            if now.minute % 15 == 0:
                scan()

        await asyncio.sleep(60)

# ================== RUN ==================
async def run_bot():
    app = ApplicationBuilder().token(TOKEN).build()
    app.add_handler(CommandHandler("status", status))

    asyncio.create_task(main_loop())

    if not os.path.exists(STATE_FILE):
        send_msg("🚀 *V25.2 PRO MAX CHOWKIDAR LIVE*\n1-Min Monitoring Active ✅\n8% Drawdown Shield Active ✅")

    async with app:
        await app.initialize()
        await app.start()
        await app.updater.start_polling()
        while True:
            await asyncio.sleep(3600)

if __name__ == "__main__":
    threading.Thread(target=run_server, daemon=True).start()
    asyncio.run(run_bot())
