import os, time, json, requests, sys, threading
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import ta
from http.server import HTTPServer, BaseHTTPRequestHandler
import pytz
from telegram.ext import Application, CommandHandler
from apscheduler.schedulers.background import BackgroundScheduler

class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Wealth-Kavach V20.7 Sentinel - 18 Kavach LIVE')

def run_server():
    server = HTTPServer(('0.0.0.0', 10000), Handler)
    server.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IST = pytz.timezone('Asia/Kolkata')

if not TOKEN or not CHAT_ID:
    print("ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")
    sys.exit(1)

MAX_OPEN_TRADES = 3
MAX_PER_SECTOR = 2
BETA_LIMIT = 1.65
ATR_MULT_TRAIL = 1.5
RSI_LOW, RSI_HIGH = 44, 64
VOL_MULTIPLIER = 3.0
DMA_PERIOD = 50
FIFTY_TWO_WEEK_PCT = 0.95
TIME_STOP_DAYS = 10
BREADTH_PCT = 50
MAX_DRAW_DOWN_PCT = 8
RSI_PERIOD = 14
ATR_PERIOD = 14
MIN_PRICE = 100
MAX_PRICE = 10000
SECTOR_BLACKLIST = []
EARNINGS_DAYS_AVOID = 7
PIVOT_LOOKBACK = 5

SYMBOLS = ["RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS","SBIN.NS","AXISBANK.NS","LT.NS","ITC.NS","BHARTIARTL.NS","TATAMOTORS.NS","M&M.NS","TATASTEEL.NS","JSWSTEEL.NS","SUNPHARMA.NS","TITAN.NS","ADANIENT.NS","HAL.NS","BEL.NS"]

STATE_FILE = "wealth_state.json"
state = {"trades": {}, "blacklist": {}, "last_greeting": "", "last_weekly_report": "", "last_daily_report": "", "trade_log": [], "daily_log": [], "market_status": "closed"}

def log(msg):
    print(f"{datetime.now(IST).strftime('%d-%b %H:%M')} - {msg}")

def save_state():
    with open(STATE_FILE, 'w') as f:
        json.dump(state, f, indent=4)

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, 'r') as f:
            state = json.load(f)

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        log(f"Telegram Error: {e}")

def market_open_alert():
    load_state()
    state["market_status"] = "open"
    save_state()
    msg = f"🟢 *Good Morning ललित जी!* 🙏\n\n*बाज़ार खुल गया है (9:15 AM)* 📈\n*18-Kavach स्कैनिंग सक्रिय है!*\n\n_जय श्री ॐ_"
    send_telegram(msg)

def market_close_alert():
    load_state()
    state["market_status"] = "closed"
    buy_count = len([t for t in state["daily_log"] if t["action"] == "BUY"])
    sell_count = len([t for t in state["daily_log"] if t["action"] == "SELL"])
    save_state()
    msg = f"🔴 *बाज़ार बंद (3:30 PM)* 🔕\n\n*आज की गतिविधि:*\n🟢 Buy: {buy_count} | 🔴 Sell: {sell_count}\n\n_कल मिलते हैं 👋_"
    send_telegram(msg)

def send_daily_report_v207():
    load_state()
    now = datetime.now(IST)
    buys = [t for t in state["daily_log"] if t["action"] == "BUY"]
    sells = [t for t in state["daily_log"] if t["action"] == "SELL"]
    total_pnl = sum(t.get('pnl_pct', 0) for t in sells)
    msg = f"📊 *Daily Report - {now.strftime('%d-%b')}* 📊\n\n"
    msg += f"*🟢 खरीदे:* {len(buys)}\n*🔴 बेचे:* {len(sells)}\n*💰 आज का PnL:* {total_pnl:.2f}%\n"
    msg += f"*Positions Open:* {len(state['trades'])}\n\n_V20.7 Sentinel Active_"
    send_telegram(msg)
    state["daily_log"] = []
    save_state()

def send_weekly_report():
    load_state()
    total_trades = len(state["trade_log"])
    wins = sum(1 for t in state["trade_log"] if t.get("pnl_pct", 0) > 0)
    msg = f"📈 *Weekly Report Card* 📈\n\n*Total Trades:* {total_trades}\n*Wins:* {wins}\n*Win Rate:* {(wins/total_trades*100) if total_trades else 0:.1f}%\n\n_Have a great weekend ललित जी!_"
    send_telegram(msg)
    state["trade_log"] = []
    save_state()

async def start_command(update, context):
    if str(update.message.chat_id)!= CHAT_ID: return
    await update.message.reply_text(f"🙏 *जय श्री ॐ ललित जी*\n\nWealth-Kavach V20.7 सक्रिय है।\nCommands: /status, /positions", parse_mode='Markdown')

async def status_command(update, context):
    if str(update.message.chat_id)!= CHAT_ID: return
    msg = f"📊 *Status:* {state.get('market_status', 'N/A')}\n*Open Trades:* {len(state['trades'])}/{MAX_OPEN_TRADES}"
    await update.message.reply_text(msg, parse_mode='Markdown')

async def positions_command(update, context):
    if str(update.message.chat_id)!= CHAT_ID: return
    if not state["trades"]:
        await update.message.reply_text("कोई ओपन पोजीशन नहीं है।")
        return
    msg = "📈 *Current Positions:*\n\n"
    for s, t in state["trades"].items():
        ltp = yf.Ticker(s).history(period="1d")['Close'].iloc[-1]
        pnl = ((ltp - t['entry']) / t['entry']) * 100
        msg += f"*{s}*: ₹{ltp:.2f} ({pnl:.2f}%)\n"
    await update.message.reply_text(msg, parse_mode='Markdown')

def scan_and_enter():
    if len(state["trades"]) >= MAX_OPEN_TRADES: return
    for symbol in SYMBOLS:
        if symbol in state["trades"]: continue
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1y")
            price = hist['Close'].iloc[-1]
            rsi = ta.momentum.rsi(hist['Close']).iloc[-1]
            if not (RSI_LOW <= rsi <= RSI_HIGH): continue
            beta = stock.info.get('beta', 1.2)
            if beta > BETA_LIMIT: continue
            atr = ta.volatility.average_true_range(hist['High'], hist['Low'], hist['Close']).iloc[-1]
            sl = price - (atr * 2.0)
            state["trades"][symbol] = {"entry": price, "sl": sl, "base_sl": sl, "atr": atr, "date": datetime.now(IST).isoformat(), "trail": False}
            state["daily_log"].append({"action": "BUY", "symbol": symbol, "price": price})
            save_state()
            send_telegram(f"🟢 *BUY:* {symbol} @ ₹{price:.2f}\nSL: ₹{sl:.2f}")
            if len(state["trades"]) >= MAX_OPEN_TRADES: break
        except: continue

def manage_trades():
    to_close = []
    for s, t in state["trades"].items():
        try:
            ltp = yf.Ticker(s).history(period="1d")['Close'].iloc[-1]
            if ltp <= t['sl']:
                pnl_pct = ((ltp - t['entry']) / t['entry']) * 100
                state["daily_log"].append({"action": "SELL", "symbol": s, "price": ltp, "pnl_pct": pnl_pct})
                state["trade_log"].append({"pnl_pct": pnl_pct})
                to_close.append(s)
                send_telegram(f"🔴 *EXIT:* {s} @ ₹{ltp:.2f} ({pnl_pct:.2f}%)")
                continue
            r_val = t['entry'] - t['base_sl']
            if ltp >= (t['entry'] + r_val):
                if not t['trail']: t['sl'] = t['entry']; t['trail'] = True; send_telegram(f"🔵 *Risk-Free:* {s}")
                new_sl = ltp - (t['atr'] * ATR_MULT_TRAIL)
                if new_sl > t['sl']: t['sl'] = new_sl
        except: continue
    for s in to_close: del state["trades"][s]
    save_state()

def run_scanner():
    scheduler = BackgroundScheduler(timezone=IST)
    scheduler.add_job(market_open_alert, 'cron', day_of_week='mon-fri', hour=9, minute=15)
    scheduler.add_job(market_close_alert, 'cron', day_of_week='mon-fri', hour=15, minute=30)
    scheduler.add_job(send_daily_report_v207, 'cron', day_of_week='mon-fri', hour=18, minute=0)
    scheduler.add_job(send_weekly_report, 'cron', day_of_week='fri', hour=16, minute=30)
    scheduler.start()
    while True:
        now = datetime.now(IST)
        if now.weekday() < 5 and (9 <= now.hour <= 15):
            manage_trades()
            scan_and_enter()
            time.sleep(900)
        else:
            time.sleep(1800)

if __name__ == "__main__":
    load_state()
    send_telegram("✅ *V20.7 Sentinel LIVE* ✅\n_18-Kavach + Auto Alerts Active_\n\n*जय श्री ॐ* 🙏")
    threading.Thread(target=run_scanner, daemon=True).start()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("positions", positions_command))
    log("Bot polling started - V20.7 Ready")
    try:
        app.run_polling()
    except Exception as e:
        send_telegram(f"❌ *FATAL CRASH!* ❌\n\n*Error:* {str(e)[:150]}\n\n_बॉट बंद! Logs चेक करो_")
