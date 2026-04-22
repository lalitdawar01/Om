import os, time, json, requests, sys, threading
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import ta
from http.server import HTTPServer, BaseHTTPRequestHandler
import pytz
from telegram.ext import Application, CommandHandler
from apscheduler.schedulers.background import BackgroundScheduler

# ================== RENDER KEEP-ALIVE SERVER ==================
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Wealth-Kavach V20.7 FULL 18-Kavach LIVE')

def run_server():
    server = HTTPServer(('0.0.0.0', 10000), Handler)
    server.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# ================== CONFIGURATION ==================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = str(os.getenv("TELEGRAM_CHAT_ID")).strip()
IST = pytz.timezone('Asia/Kolkata')

if not TOKEN or not CHAT_ID:
    print("ERROR: TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID missing")
    sys.exit(1)

# ================== 18 KAVACH PARAMETERS ==================
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
SECTOR_BLACKLIST = ["REALTY", "MEDIA"]
EARNINGS_DAYS_AVOID = 7
PIVOT_LOOKBACK = 5

SYMBOLS = ["RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS","SBIN.NS","AXISBANK.NS","LT.NS","ITC.NS","BHARTIARTL.NS","TATAMOTORS.NS","M&M.NS","TATASTEEL.NS","JSWSTEEL.NS","SUNPHARMA.NS","TITAN.NS","ADANIENT.NS","HAL.NS","BEL.NS"]

STATE_FILE = "wealth_state.json"
state = {"trades": {}, "blacklist": {}, "last_greeting": "", "trade_log": [], "daily_log": [], "market_status": "closed", "peak_equity": 100000}

# ================== UTILS ==================
def log(msg):
    print(f"{datetime.now(IST).strftime('%d-%b %H:%M')} - {msg}")

def save_state():
    try:
        with open(STATE_FILE, 'w') as f:
            json.dump(state, f, indent=4)
    except Exception as e:
        log(f"Save Error: {e}")

def load_state():
    global state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r') as f:
                state = json.load(f)
        except: pass

def send_telegram(msg):
    url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
    try:
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg, "parse_mode": "Markdown"}, timeout=10)
    except Exception as e:
        log(f"Telegram Error: {e}")

# ================== 18 KAVACH HELPERS ==================
def get_sector(symbol):
    try:
        return yf.Ticker(symbol).info.get('sector', 'UNKNOWN')
    except:
        return 'UNKNOWN'

def check_drawdown():
    equity = 100000 + sum(t.get('pnl_pct',0) for t in state["trade_log"])
    if equity > state.get("peak_equity", 100000):
        state["peak_equity"] = equity
    dd = ((state["peak_equity"] - equity) / state["peak_equity"]) * 100
    return dd <= MAX_DRAW_DOWN_PCT

# ================== SCHEDULED ALERTS ==================
def market_open_alert():
    load_state()
    state["market_status"] = "open"
    save_state()
    send_telegram(f"🟢 *Good Morning ललित जी!* 🙏\n\n*बाज़ार खुल गया है (9:15 AM)* 📈\n*18-Kavach FULL स्कैनिंग सक्रिय!*\n\n_जय श्री ॐ_")

def market_close_alert():
    load_state()
    state["market_status"] = "closed"
    buy_count = len([t for t in state["daily_log"] if t["action"] == "BUY"])
    sell_count = len([t for t in state["daily_log"] if t["action"] == "SELL"])
    save_state()
    send_telegram(f"🔴 *बाज़ार बंद (3:30 PM)* 🔕\n\n*आज की गतिविधि:*\n🟢 Buy: {buy_count} | 🔴 Sell: {sell_count}\n\n_कल मिलते हैं 👋_")

def send_daily_report_v207():
    load_state()
    now = datetime.now(IST)
    buys = [t for t in state["daily_log"] if t["action"] == "BUY"]
    sells = [t for t in state["daily_log"] if t["action"] == "SELL"]
    total_pnl = sum(t.get('pnl_pct', 0) for t in sells)
    msg = f"📊 *Daily Report - {now.strftime('%d-%b')}* 📊\n\n"
    msg += f"*🟢 खरीदे:* {len(buys)}\n*🔴 बेचे:* {len(sells)}\n*💰 आज का PnL:* {total_pnl:.2f}%\n"
    msg += f"*Positions Open:* {len(state['trades'])}\n\n_V20.7 FULL 18-Kavach Active_"
    send_telegram(msg)
    state["daily_log"] = []
    save_state()

def send_weekly_report():
    load_state()
    total_trades = len(state["trade_log"])
    wins = sum(1 for t in state["trade_log"] if t.get("pnl_pct", 0) > 0)
    win_rate = (wins/total_trades*100) if total_trades else 0
    msg = f"📈 *Weekly Report Card* 📈\n\n*Total Trades:* {total_trades}\n*Wins:* {wins}\n*Win Rate:* {win_rate:.1f}%\n\n_Have a great weekend ललित जी!_"
    send_telegram(msg)
    state["trade_log"] = []
    save_state()

# ================== TELEGRAM COMMANDS ==================
async def start_command(update, context):
    if str(update.message.chat_id)!= CHAT_ID: return
    await update.message.reply_text(f"🙏 *जय श्री ॐ ललित जी*\n\nWealth-Kavach V20.7 FULL 18-Kavach सक्रिय।\n\n*Commands:*\n/status\n/positions\n/scan", parse_mode='Markdown')

async def status_command(update, context):
    if str(update.message.chat_id)!= CHAT_ID: return
    equity = 100000 + sum(t.get('pnl_pct',0) for t in state["trade_log"])
    dd = ((state.get("peak_equity", 100000) - equity) / state.get("peak_equity", 100000)) * 100
    msg = f"📊 *Status:* {state.get('market_status', 'N/A')}\n*Open:* {len(state['trades'])}/{MAX_OPEN_TRADES}\n*Drawdown:* {dd:.1f}%/{MAX_DRAW_DOWN_PCT}%"
    await update.message.reply_text(msg, parse_mode='Markdown')

async def positions_command(update, context):
    if str(update.message.chat_id)!= CHAT_ID: return
    if not state["trades"]:
        await update.message.reply_text("कोई ओपन पोजीशन नहीं है।")
        return
    msg = "📈 *Current Positions:*\n\n"
    try:
        for s, t in state["trades"].items():
            ltp = yf.Ticker(s).history(period="1d")['Close'].iloc[-1]
            pnl = ((ltp - t['entry']) / t['entry']) * 100
            days = (datetime.now(IST) - datetime.fromisoformat(t['date'])).days
            msg += f"*{s}*: ₹{ltp:.2f} ({pnl:.2f}%) | {days} दिन | SL: ₹{t['sl']:.2f}\n"
        await update.message.reply_text(msg, parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"❌ Data Error: {str(e)[:100]}")

async def scan_command(update, context):
    if str(update.message.chat_id)!= CHAT_ID: return
    await update.message.reply_text("🔍 *18-Kavach FULL स्कैन शुरू...* \n\n_जय श्री ॐ_ 🙏", parse_mode='Markdown')
    try:
        scan_and_enter()
        await update.message.reply_text(f"✅ *Scan Complete*\n\n*Open:* {len(state['trades'])}/{MAX_OPEN_TRADES}", parse_mode='Markdown')
    except Exception as e:
        await update.message.reply_text(f"❌ Scan Error: {str(e)[:150]}")

# ================== TRADING LOGIC - FULL 18 KAVACH ==================
def scan_and_enter():
    if len(state["trades"]) >= MAX_OPEN_TRADES: return
    if not check_drawdown():
        send_telegram("⚠️ *MAX DRAWDOWN HIT* ⚠️\n\nनई एंट्री बंद। 8% लॉस लिमिट।")
        return

    sector_count = {}
    for s in state["trades"]:
        sec = state["trades"][s].get("sector", get_sector(s))
        sector_count[sec] = sector_count.get(sec, 0) + 1

    for symbol in SYMBOLS:
        if symbol in state["trades"]: continue
        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1y")
            if len(hist) < 60: continue
            price = hist['Close'].iloc[-1]

            if not (MIN_PRICE <= price <= MAX_PRICE): continue
            sector = get_sector(symbol)
            if sector in SECTOR_BLACKLIST: continue
            if sector_count.get(sector, 0) >= MAX_PER_SECTOR: continue
            rsi = ta.momentum.rsi(hist['Close'], window=RSI_PERIOD).iloc[-1]
            if not (RSI_LOW <= rsi <= RSI_HIGH): continue
            beta = stock.info.get('beta', None)
            if beta and beta > BETA_LIMIT: continue
            dma50 = hist['Close'].rolling(DMA_PERIOD).mean().iloc[-1]
            if price < dma50: continue
            week52_high = hist['High'].rolling(252).max().iloc[-1]
            if price < (week52_high * FIFTY_TWO_WEEK_PCT): continue
            avg_vol = hist['Volume'].rolling(20).mean().iloc[-1]
            if hist['Volume'].iloc[-1] < (avg_vol * VOL_MULTIPLIER): continue
            pivot_high = hist['High'].rolling(PIVOT_LOOKBACK).max().iloc[-2]
            if price < pivot_high: continue

            atr = ta.volatility.average_true_range(hist['High'], hist['Low'], hist['Close'], window=ATR_PERIOD).iloc[-1]
            sl = price - (atr * 2.0)
            state["trades"][symbol] = {"entry": price, "sl": sl, "base_sl": sl, "atr": atr, "date": datetime.now(IST).isoformat(), "trail": False, "sector": sector}
            state["daily_log"].append({"action": "BUY", "symbol": symbol, "price": price})
            sector_count[sector] = sector_count.get(sector, 0) + 1
            save_state()
            send_telegram(f"🟢 *BUY:* {symbol} @ ₹{price:.2f}\n*SL:* ₹{sl:.2f} | *Sector:* {sector}\n*18-Kavach PASS* ✅")
            if len(state["trades"]) >= MAX_OPEN_TRADES: break
        except Exception as e:
            log(f"Scan Error {symbol}: {e}")
            continue

def manage_trades():
    to_close = []
    for s, t in state["trades"].items():
        try:
            days_held = (datetime.now(IST) - datetime.fromisoformat(t['date'])).days
            if days_held >= TIME_STOP_DAYS:
                ltp = yf.Ticker(s).history(period="1d")['Close'].iloc[-1]
                pnl_pct = ((ltp - t['entry']) / t['entry']) * 100
                state["daily_log"].append({"action": "SELL", "symbol": s, "price": ltp, "pnl_pct": pnl_pct, "reason": "TIME"})
                state["trade_log"].append({"pnl_pct": pnl_pct})
                to_close.append(s)
                send_telegram(f"⏰ *TIME EXIT:* {s} @ ₹{ltp:.2f} ({pnl_pct:.2f}%)\n*{days_held} दिन हो गए*")
                continue

            ltp = yf.Ticker(s).history(period="1d")['Close'].iloc[-1]
            if ltp <= t['sl']:
                pnl_pct = ((ltp - t['entry']) / t['entry']) * 100
                state["daily_log"].append({"action": "SELL", "symbol": s, "price": ltp, "pnl_pct": pnl_pct, "reason": "SL"})
                state["trade_log"].append({"pnl_pct": pnl_pct})
                to_close.append(s)
                send_telegram(f"🔴 *SL EXIT:* {s} @ ₹{ltp:.2f} ({pnl_pct:.2f}%)")
                continue

            r_val = t['entry'] - t['base_sl']
            if ltp >= (t['entry'] + r_val):
                if not t['trail']:
                    t['sl'] = t['entry']
                    t['trail'] = True
                    send_telegram(f"🔵 *Risk-Free:* {s}\n_SL अब कॉस्ट पे_")
                new_sl = ltp - (t['atr'] * ATR_MULT_TRAIL)
                if new_sl > t['sl']:
                    t['sl'] = new_sl
        except Exception as e:
            log(f"Manage Error {s}: {e}")
            continue
    for s in to_close:
        del state["trades"][s]
    save_state()

# ================== RUNNER ==================
def run_scanner():
    scheduler = BackgroundScheduler(timezone=IST)
    scheduler.add_job(market_open_alert, 'cron', day_of_week='mon-fri', hour=9, minute=15)
    scheduler.add_job(market_close_alert, 'cron', day_of_week='mon-fri', hour=15, minute=30)
    scheduler.add_job(send_daily_report_v207, 'cron', day_of_week='mon-fri', hour=18, minute=0)
    scheduler.add_job(send_weekly_report, 'cron', day_of_week='fri', hour=16, minute=30)
    scheduler.start()

    while True:
        try:
            now = datetime.now(IST)
            if now.weekday() < 5 and (9 <= now.hour <= 15):
                manage_trades()
                scan_and_enter()
                time.sleep(900)
            else:
                time.sleep(1800)
        except Exception as e:
            log(f"Scanner Loop Error: {e}")
            send_telegram(f"⚠️ *Loop Error:* {str(e)[:100]}")
            time.sleep(300)

if __name__ == "__main__":
    load_state()
    log(f"Bot Starting with CHAT_ID: {CHAT_ID}")

    if state.get("last_greeting")!= datetime.now(IST).strftime('%Y-%m-%d'):
        send_telegram("✅ *V20.7 FULL 18-Kavach LIVE* ✅\n_All Filters + Auto Reports Active_\n\n*जय श्री ॐ* 🙏")
        state["last_greeting"] = datetime.now(IST).strftime('%Y-%m-%d')
        save_state()

    threading.Thread(target=run_scanner, daemon=True).start()
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("status", status_command))
    app.add_handler(CommandHandler("positions", positions_command))
    app.add_handler(CommandHandler("scan", scan_command))
    log("Bot polling started - V20.7 FULL Ready")
    try:
        app.run_polling()
    except Exception as e:
        log(f"FATAL: {e}")
        send_telegram(f"❌ *FATAL CRASH!* ❌\n\n*Error:* {str(e)[:150]}")
