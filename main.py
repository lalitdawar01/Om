import os
import time
import sqlite3
import threading
import logging
import warnings
import random
from datetime import datetime, timedelta
import pandas as pd
import requests
import yfinance as yf
import ta
import pytz
from http.server import HTTPServer, BaseHTTPRequestHandler
from concurrent.futures import ThreadPoolExecutor, as_completed

warnings.filterwarnings("ignore")
IST = pytz.timezone("Asia/Kolkata")
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DB_FILE = "brahmand_kavach_v32.db"
NSE500_URL = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

BASE_CAPITAL = 100000
RISK_PER_TRADE = 0.015
SCAN_INTERVAL = 1800
MAX_WORKERS = 2

LAST_GREETING_DATE = None
LAST_REPORT_DATE = None
LAST_UPDATE_ID = 0

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def send_telegram(message):
    if not BOT_TOKEN or not CHAT_ID: return
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {"chat_id": CHAT_ID, "text": message, "parse_mode": "Markdown"}
        requests.post(url, data=payload, timeout=15)
    except Exception as e: logging.error(f"Telegram Error: {e}")

def get_pnl_report(days=1):
    conn = sqlite3.connect(DB_FILE)
    since_date = (datetime.now(IST) - timedelta(days=days)).strftime("%Y-%m-%d")
    rows = conn.execute("SELECT symbol, pnl FROM trades WHERE status='CLOSED' AND exit_time >=?", (since_date,)).fetchall()
    conn.close()
    if not rows: return "आज कोई ट्रेड क्लोज नहीं हुआ।"
    total_pnl = sum(r[1] for r in rows)
    detail = "\n".join([f"🔹 {r[0]}: ₹{r[1]:,.2f}" for r in rows])
    return f"{detail}\n\n💰 Total P&L: ₹{total_pnl:,.2f}"

def check_special_messages():
    global LAST_GREETING_DATE, LAST_REPORT_DATE
    now = datetime.now(IST)
    today = now.strftime("%Y-%m-%d")
    if now.hour == 9 and now.minute >= 30 and LAST_GREETING_DATE!= today:
        send_telegram("🚩 जय श्री राम, ललित जी! \nमार्केट खुल गया है। आपका दिन मंगलमय और प्रॉफिटेबल हो। 🙏")
        LAST_GREETING_DATE = today
    if now.hour == 15 and now.minute >= 35 and LAST_REPORT_DATE!= today:
        report = get_pnl_report(1)
        send_telegram(f"📉 आज की क्लोजिंग रिपोर्ट: \n\n{report}")
        LAST_REPORT_DATE = today

def get_dynamic_config():
    conn = sqlite3.connect(DB_FILE)
    total_pnl = conn.execute("SELECT SUM(pnl) FROM trades WHERE status='CLOSED'").fetchone()[0] or 0
    conn.close()
    current_total_cap = BASE_CAPITAL + total_pnl
    max_slots = 5 + max(0, int(total_pnl // 100000))
    return current_total_cap, max_slots

def get_available_capital():
    total_cap, _ = get_dynamic_config()
    conn = sqlite3.connect(DB_FILE)
    rows = conn.execute("SELECT entry, qty FROM trades WHERE status='OPEN'").fetchall()
    conn.close()
    invested = sum(entry * qty for entry, qty in rows)
    return max(0, total_cap - invested)

def init_db():
    conn = sqlite3.connect(DB_FILE)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("""CREATE TABLE IF NOT EXISTS trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, entry REAL NOT NULL,
    sl REAL NOT NULL, target REAL NOT NULL, qty INTEGER NOT NULL, status TEXT NOT NULL,
    highest_price REAL, entry_time TEXT, exit_time TEXT, pnl REAL DEFAULT 0)""")
    conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_open_symbol ON trades(symbol) WHERE status='OPEN'")
    conn.commit(); conn.close()

def nifty_bear_check():
    try:
        nifty = yf.Ticker("^NSEI")
        hist = nifty.history(period="100d")
        if hist.empty: return False
        ema50 = hist["Close"].ewm(span=50).mean().iloc[-1]
        price = hist["Close"].iloc[-1]
        return price < ema50
    except: return False

def one_min_confirmation(symbol):
    try:
        time.sleep(0.5)
        df = yf.download(symbol, period="1d", interval="1m", progress=False, auto_adjust=True)
        if df.empty: return False
        return df["Close"].iloc[-1] > df["High"].iloc[-10:-1].max()
    except: return False

def analyze_stock(symbol):
    try:
        # NIFTY चौकीदार V25.2
        if nifty_bear_check(): return None

        time.sleep(random.uniform(1.0, 2.0))
        df = yf.download(symbol, period="1y", interval="1d", progress=False, auto_adjust=True, threads=False)
        if df.empty or len(df) < 220: return None

        close, high, low, vol = df["Close"], df["High"], df["Low"], df["Volume"]
        ema50 = close.ewm(span=50).mean()
        ema200 = close.ewm(span=200).mean()
        rsi = ta.momentum.RSIIndicator(close, 14).rsi()
        adx = ta.trend.ADXIndicator(high, low, close, 14).adx()
        atr = ta.volatility.AverageTrueRange(high, low, close, 14).average_true_range()
        price = float(close.iloc[-1])
        ema_slope = (ema50.iloc[-1] - ema50.iloc[-5]) / ema50.iloc[-5] * 100
        vol_breakout = vol.iloc[-1] > vol.rolling(20).mean().iloc[-1] * 2.5
        bullish = price > ema50.iloc[-1] > ema200.iloc[-1]
        momentum = (50 < rsi.iloc[-1] < 66) and (adx.iloc[-1] > 25) and (ema_slope > 0.2)

        if bullish and momentum and vol_breakout:
            # 1-Min चौकीदार V25.2
            if not one_min_confirmation(symbol): return None

            current_cap, max_slots = get_dynamic_config()
            available_cap = get_available_capital()
            if available_cap < price: return None
            sl = round(price - (2 * atr.iloc[-1]), 2)
            target = round(price + (4 * atr.iloc[-1]), 2)
            risk_qty = int((current_cap * RISK_PER_TRADE) / (price - sl))
            capital_qty = int((available_cap / max_slots) / price)
            qty = min(risk_qty, capital_qty)
            if qty > 0: return {"symbol": symbol, "price": round(price, 2), "sl": sl, "target": target, "qty": qty}
    except: return None

def manage_exits():
    conn = sqlite3.connect(DB_FILE)
    trades = conn.execute("SELECT id, symbol, entry, sl, target, qty, highest_price FROM trades WHERE status='OPEN'").fetchall()
    for tid, sym, entry, current_sl, target, qty, high_price in trades:
        try:
            time.sleep(1)
            df = yf.download(sym, period="2d", interval="5m", progress=False, auto_adjust=True, threads=False)
            if df.empty: continue
            current = float(df["Close"].iloc[-1])
            if current > high_price:
                high_price = current
                new_sl = max(current_sl, high_price * 0.97)
                conn.execute("UPDATE trades SET sl=?, highest_price=? WHERE id=?", (new_sl, high_price, tid))
                current_sl = new_sl
            reason = "Trailing SL 🛑" if current <= current_sl else "Target 🎯" if current >= target else None
            if reason:
                pnl = round((current - entry) * qty, 2)
                conn.execute("UPDATE trades SET status='CLOSED', exit_time=?, pnl=? WHERE id=?", (datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S"), pnl, tid))
                send_telegram(f"🔴 EXIT: {sym} | {reason}\nPrice: ₹{current:.2f} | P&L: ₹{pnl:.2f}")
        except: continue
    conn.commit(); conn.close()

def check_telegram_commands():
    global LAST_UPDATE_ID
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
        res = requests.get(url, params={"offset": LAST_UPDATE_ID, "timeout": 1}, timeout=5).json()
        for up in res.get("result", []):
            LAST_UPDATE_ID = up["update_id"] + 1
            if str(up.get("message", {}).get("chat", {}).get("id"))!= str(CHAT_ID): continue
            text = up.get("message", {}).get("text", "").strip().lower()
            if text == "#status":
                cap, slots = get_dynamic_config()
                avail = get_available_capital()
                conn = sqlite3.connect(DB_FILE)
                net_pnl = conn.execute("SELECT SUM(pnl) FROM trades WHERE status='CLOSED'").fetchone()[0] or 0
                open_tr = conn.execute("SELECT symbol, entry FROM trades WHERE status='OPEN'").fetchall()
                conn.close()
                msg = f"📊 BRAHMAND KAVACH V32.0\n💰 Total Capital: ₹{cap:,.0f}\n💵 Available: ₹{avail:,.0f}\n📦 Slots: {len(open_tr)}/{slots}\n💹 Net P&L: ₹{net_pnl:,.2f}\n\n📦 Open Positions:"
                msg += "\n".join([f"\n- {t[0]} @ ₹{t[1]}" for t in open_tr]) if open_tr else " None"
                send_telegram(msg)
    except: pass

class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self): self.send_response(200); self.end_headers(); self.wfile.write(b"Active")
    def log_message(self, format, *args): return

def market_open():
    now = datetime.now(IST)
    curr = now.hour * 60 + now.minute
    return now.weekday() < 5 and 555 <= curr <= 930

def run():
    init_db()
    port = int(os.environ.get("PORT", 10000))
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    send_telegram("🚀 BRAHMAND KAVACH V32.0 BRAHMASTRA LIVE\nCompounding + NIFTY Shield + 1-Min Confirm = ON")

    try:
        df_nse = pd.read_csv(NSE500_URL)
        df_nse.to_csv("nifty500_backup.csv", index=False)
        symbols = (df_nse["Symbol"] + ".NS").tolist()
    except:
        symbols = (pd.read_csv("nifty500_backup.csv")["Symbol"] + ".NS").tolist()

    while True:
        try:
            check_telegram_commands()
            check_special_messages()
            manage_exits()
            if market_open():
                _, max_slots = get_dynamic_config()
                conn = sqlite3.connect(DB_FILE)
                current_open = conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()[0]
                conn.close()
                if current_open < max_slots:
                    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                        futures = {executor.submit(analyze_stock, s): s for s in symbols}
                        for f in as_completed(futures):
                            conn = sqlite3.connect(DB_FILE)
                            now_open = conn.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'").fetchone()[0]
                            if now_open >= max_slots:
                                conn.close(); executor.shutdown(wait=False, cancel_futures=True); break
                            conn.close()
                            res = f.result()
                            if res:
                                conn = sqlite3.connect(DB_FILE)
                                try:
                                    conn.execute("INSERT INTO trades (symbol, entry, sl, target, qty, status, highest_price, entry_time) VALUES (?,?,?,?,?,?,?,?)",
                                    (res['symbol'], res['price'], res['sl'], res['target'], res['qty'], "OPEN", res['price'], datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")))
                                    conn.commit()
                                    send_telegram(f"🟢 *BUY:* `{res['symbol']}`\n━━━━━━━━━━━━━━━\n💰 Entry: ₹{res['price']:,.2f}\n🛑 SL: ₹{res['sl']:,.2f}\n🎯 Target: ₹{res['target']:,.2f}\n📦 Qty: {res['qty']}")
                                except: pass
                                finally: conn.close()
            time.sleep(SCAN_INTERVAL)
        except Exception as e: logging.error(f"Loop: {e}"); time.sleep(30)

if __name__ == "__main__": run()
