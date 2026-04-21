
import os, time, json, requests, sys, threading
from datetime import datetime, timedelta
import yfinance as yf
import pandas as pd
import ta
from http.server import HTTPServer, BaseHTTPRequestHandler
import pytz
from telegram.ext import Updater, CommandHandler

# ================== RENDER KEEP-ALIVE SERVER ==================
class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b'Wealth-Kavach V19.9 Integrated - 18 Kavach LIVE')

def run_server():
    server = HTTPServer(('0.0.0.0', 10000), Handler)
    server.serve_forever()

threading.Thread(target=run_server, daemon=True).start()

# ================== CONFIGURATION ==================
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
IST = pytz.timezone('Asia/Kolkata')

if not TOKEN or not CHAT_ID:
    print("ERROR: TELEGRAM_BOT_TOKEN या TELEGRAM_CHAT_ID नहीं मिला")
    sys.exit(1)

# ================== पूरे 18 KAVACH PARAMETERS ==================
MAX_OPEN_TRADES = 3 # 1
MAX_PER_SECTOR = 2 # 2
BETA_LIMIT = 1.65 # 3
ATR_MULT_TRAIL = 1.5 # 4
RSI_LOW, RSI_HIGH = 44, 64 # 5
VOL_MULTIPLIER = 3.0 # 6
DMA_PERIOD = 50 # 7
FIFTY_TWO_WEEK_PCT = 0.95 # 8
TIME_STOP_DAYS = 10 # 9
BREADTH_PCT = 50 # 10
MAX_DRAW_DOWN_PCT = 8 # 11
RSI_PERIOD = 14 # 12
ATR_PERIOD = 14 # 13
MIN_PRICE = 100 # 14
MAX_PRICE = 10000 # 15
SECTOR_BLACKLIST = [] # 16
EARNINGS_DAYS_AVOID = 7 # 17
PIVOT_LOOKBACK = 5 # 18

SYMBOLS = ["RELIANCE.NS","TCS.NS","INFY.NS","HDFCBANK.NS","ICICIBANK.NS","SBIN.NS","AXISBANK.NS","LT.NS","ITC.NS","BHARTIARTL.NS","TATAMOTORS.NS","M&M.NS","TATASTEEL.NS","JSWSTEEL.NS","SUNPHARMA.NS","TITAN.NS","ADANIENT.NS","HAL.NS","BEL.NS"]

STATE_FILE = "wealth_state.json"
state = {"trades": {}, "blacklist": {}, "last_greeting": "", "last_weekly_report": "", "last_daily_report": "", "trade_log": [], "daily_log": []}

# ================== UTILS ==================
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
        log(f"Telegram Sent: {msg[:30]}...")
    except Exception as e:
        log(f"Telegram Error: {e}")

# ================== TELEGRAM COMMAND HANDLERS ==================
def start_command(update, context):
    user_id = update.message.chat_id
    log(f"Received /start from {user_id}")
    if str(user_id)!= CHAT_ID:
        update.message.reply_text("⛔ Unauthorized access")
        return
    
    open_count = len(state["trades"])
    update.message.reply_text(
        f"🙏 *जय श्री ॐ ललित जी*\n\n"
        f"🚀 *Wealth-Kavach V19.9 Active*\n"
        f"_18-Kavach System LIVE_\n\n"
        f"*Status:* Running ✅\n"
        f"*Open Trades:* {open_count}/{MAX_OPEN_TRADES}\n"
        f"*Mode:* {'Market Hours' if 9 <= datetime.now(IST).hour <= 15 else 'Standby'}\n\n"
        f"मैं मार्केट स्कैन कर रहा हूँ। BUY/SELL सिग्नल मिलते ही बताऊंगा।\n\n"
        f"Commands:\n"
        f"/status - बॉट स्टेटस\n"
        f"/positions - खुले ट्रेड",
        parse_mode='Markdown'
    )

def status_command(update, context):
    user_id = update.message.chat_id
    if str(user_id)!= CHAT_ID: return
    
    load_state()
    open_trades = "\n".join([f"• {s} @ ₹{t['entry']:.2f}" for s,t in state["trades"].items()])
    if not open_trades: open_trades = "कोई नहीं"
    
    update.message.reply_text(
        f"📊 *Bot Status* 📊\n\n"
        f"*Open Positions:* {len(state['trades'])}/{MAX_OPEN_TRADES}\n"
        f"{open_trades}\n\n"
        f"*Last Scan:* {datetime.now(IST).strftime('%H:%M')}\n"
        f"_18-Kavach Active_",
        parse_mode='Markdown'
    )

def positions_command(update, context):
    user_id = update.message.chat_id
    if str(user_id)!= CHAT_ID: return
    load_state()
    if not state["trades"]:
        update.message.reply_text("📭 कोई Open Position नहीं है")
        return
    
    msg = "📈 *Open Positions* 📈\n\n"
    for symbol, t in state["trades"].items():
        try:
            ltp = yf.Ticker(symbol).history(period="1d")['Close'].iloc[-1]
            pnl = ((ltp - t['entry']) / t['entry']) * 100
            msg += f"*{symbol}*\nEntry: ₹{t['entry']:.2f} | LTP: ₹{ltp:.2f}\nSL: ₹{t['sl']:.2f} | *PnL: {pnl:.2f}%*\n\n"
        except:
            msg += f"*{symbol}*\nEntry: ₹{t['entry']:.2f} | SL: ₹{t['sl']:.2f}\n\n"
    update.message.reply_text(msg, parse_mode='Markdown')

# ================== GREETING + DAILY + WEEKLY REPORT ==================
def send_greeting():
    load_state()
    today = datetime.now(IST).strftime('%Y-%m-%d')
    hour = datetime.now(IST).hour

    if state.get("last_greeting") == today: return

    if 5 <= hour < 12:
        greet = "🌅 *Good Morning ललित जी* 🙏"
    elif 12 <= hour < 17:
        greet = "☀️ *Good Afternoon ललित जी* 🙏"
    elif 17 <= hour < 21:
        greet = "🌆 *Good Evening ललित जी* 🙏"
    else:
        return

    open_count = len(state["trades"])
    msg = f"{greet}\n_Wealth-Kavach Active_\n*Open Trades:* {open_count}/{MAX_OPEN_TRADES}\nजय श्री ॐ"
    send_telegram(msg)
    state["last_greeting"] = today
    save_state()

def send_daily_report():
    load_state()
    now = datetime.now(IST)
    if now.hour!= 18: return
    if state.get("last_daily_report") == now.strftime('%Y-%m-%d'): return

    buys = [t for t in state["daily_log"] if t["action"] == "BUY"]
    sells = [t for t in state["daily_log"] if t["action"] == "SELL"]

    msg = f"📊 *Daily Report - {now.strftime('%d-%b')}* 📊\n\n"

    if buys:
        msg += "*🟢 आज खरीदे:*\n"
        for t in buys:
            msg += f"• {t['symbol']} @ ₹{t['price']:.2f}\n"
    else:
        msg += "*🟢 आज खरीदे:* कोई नहीं\n"

    if sells:
        msg += f"\n*🔴 आज बेचे:*\n"
        for t in sells:
            msg += f"• {t['symbol']} @ ₹{t['price']:.2f} | *PnL:* {t['pnl_pct']:.2f}%\n"
    else:
        msg += f"\n*🔴 आज बेचे:* कोई नहीं\n"

    msg += f"\n*Open Positions:* {len(state['trades'])}\n_18-Kavach System_"

    send_telegram(msg)
    state["last_daily_report"] = now.strftime('%Y-%m-%d')
    state["daily_log"] = []
    save_state()

def send_weekly_report():
    load_state()
    now = datetime.now(IST)
    if now.weekday()!= 4 or now.hour < 16: return
    if state.get("last_weekly_report") == now.strftime('%Y-W%U'): return

    total_trades = len(state["trade_log"])
    wins = sum(1 for t in state["trade_log"] if t.get("pnl", 0) > 0)
    total_pnl = sum(t.get("pnl_pct", 0) for t in state["trade_log"])

    msg = f"📈 *Weekly Report* 📈\n"
    msg += f"*Total Trades:* {total_trades}\n"
    msg += f"*Wins:* {wins} | *Loss:* {total_trades - wins}\n"
    msg += f"*Win Rate:* {(wins/total_trades*100) if total_trades else 0:.1f}%\n"
    msg += f"*Total PnL:* {total_pnl:.2f}%\n"
    msg += f"*Avg PnL/Trade:* {(total_pnl/total_trades) if total_trades else 0:.2f}%\n"
    msg += f"*Open Positions:* {len(state['trades'])}\n"
    msg += f"\n_Have a great weekend ललित जी!_\n_जय श्री ॐ_"

    send_telegram(msg)
    state["last_weekly_report"] = now.strftime('%Y-W%U')
    state["trade_log"] = []
    save_state()

# ================== 18 KAVACH CHECKS ==================
def get_market_breadth():
    try:
        data = yf.download(SYMBOLS[:10], period="2d", progress=False)['Close']
        breadth = (data.iloc[-1] > data.iloc[-2]).sum() / 10 * 100
        return breadth
    except: return 60

def check_drawdown():
    if len(state["trades"]) == 0: return False
    total_pnl = 0
    for symbol, t in state["trades"].items():
        try:
            ltp = yf.Ticker(symbol).history(period="1d")['Close'].iloc[-1]
            pnl_pct = ((ltp - t['entry']) / t['entry']) * 100
            total_pnl += pnl_pct
        except: continue
    if total_pnl < -MAX_DRAW_DOWN_PCT:
        send_telegram(f"🚨 *MAX DRAWDOWN HIT:* {total_pnl:.1f}%\n_All positions closing for capital protection_")
        return True
    return False

def scan_and_enter():
    load_state()
    if len(state["trades"]) >= MAX_OPEN_TRADES: return

    breadth = get_market_breadth()
    if breadth < BREADTH_PCT:
        log(f"Market Breadth Low: {breadth}%")
        return

    if check_drawdown():
        state["trades"] = {}
        save_state()
        return

    for symbol in SYMBOLS:
        if symbol in state["trades"]: continue

        try:
            stock = yf.Ticker(symbol)
            hist = stock.history(period="1y")
            if len(hist) < 60: continue

            # कवच 17: EARNINGS AVOID
            try:
                earnings_dates = stock.calendar
                if not earnings_dates.empty and 'Earnings Date' in earnings_dates.index:
                    next_earnings = earnings_dates.loc['Earnings Date'].iloc[0]
                    if pd.notna(next_earnings):
                        days_to_earnings = (next_earnings.date() - datetime.now(IST).date()).days
                        if 0 <= days_to_earnings <= EARNINGS_DAYS_AVOID:
                            log(f"Skipping {symbol}: Earnings in {days_to_earnings} days")
                            continue
            except:
                pass

            # कवच 2 & 16: SECTOR CHECK + BLACKLIST
            try:
                sector = stock.info.get('sector', 'Unknown')
                if sector in SECTOR_BLACKLIST:
                    log(f"Skipping {symbol}: Sector {sector} blacklisted")
                    continue
                if sector!= 'Unknown':
                    sector_count = 0
                    for open_symbol in state["trades"]:
                        open_sector = state["trades"][open_symbol].get('sector', 'Unknown')
                        if open_sector == sector:
                            sector_count += 1
                    if sector_count >= MAX_PER_SECTOR:
                        log(f"Skipping {symbol}: Sector {sector} limit {MAX_PER_SECTOR} reached")
                        continue
            except:
                sector = 'Unknown'

            price = hist['Close'].iloc[-1]
            if price < MIN_PRICE or price > MAX_PRICE: continue

            sma50 = ta.trend.sma_indicator(hist['Close'], window=DMA_PERIOD).iloc[-1]
            if price < sma50: continue

            if price < (hist['High'].max() * FIFTY_TWO_WEEK_PCT): continue

            avg_vol = hist['Volume'].rolling(20).mean().iloc[-1]
            if hist['Volume'].iloc[-1] < (avg_vol * VOL_MULTIPLIER): continue

            beta = stock.info.get('beta', 1.2)
            if beta > BETA_LIMIT: continue

            rsi = ta.momentum.rsi(hist['Close'], window=RSI_PERIOD).iloc[-1]
            if rsi < RSI_LOW or rsi > RSI_HIGH: continue

            pivot_high = hist['High'].rolling(PIVOT_LOOKBACK).max().iloc[-2]
            if price < pivot_high: continue

            atr = ta.volatility.average_true_range(hist['High'], hist['Low'], hist['Close'], window=ATR_PERIOD).iloc[-1]
            sl = price - (atr * 2.0)

            state["trades"][symbol] = {
                "entry": price, "sl": sl, "base_sl": sl, "atr": atr,
                "date": datetime.now(IST).isoformat(), "trail": False,
                "sector": sector
            }
            state["daily_log"].append({"action": "BUY", "symbol": symbol, "price": price, "time": datetime.now(IST).isoformat()})
            save_state()
            send_telegram(f"🟢 *BUY:* {symbol} @ ₹{price:.2f}\n*SL:* ₹{sl:.2f}\n*RSI:* {rsi:.1f} | *Beta:* {beta}\n*Sector:* {sector}\n_18-Kavach Protection Active_")
            return
        except Exception as e:
            log(f"Error scanning {symbol}: {e}")
            continue

def manage_trades():
    load_state()
    to_close = []

    if check_drawdown():
        for symbol in list(state["trades"].keys()):
            to_close.append(symbol)

    for symbol, t in state["trades"].items():
        if symbol in to_close: continue
        try:
            hist = yf.Ticker(symbol).history(period="5d")
            ltp = hist['Close'].iloc[-1]

            if ltp <= t['sl']:
                pnl = ltp - t['entry']
                pnl_pct = (pnl / t['entry']) * 100
                state["trade_log"].append({"symbol": symbol, "pnl": pnl, "pnl_pct": pnl_pct, "exit": "SL"})
                state["daily_log"].append({"action": "SELL", "symbol": symbol, "price": ltp, "pnl_pct": pnl_pct, "time": datetime.now(IST).isoformat()})
                to_close.append(symbol)
                send_telegram(f"🔴 *EXIT:* {symbol} @ ₹{ltp:.2f}\n*PnL:* {pnl_pct:.2f}%\n_Stop Loss Triggered_")
                continue

            entry_date = datetime.fromisoformat(t['date'])
            if (datetime.now(IST) - entry_date).days >= TIME_STOP_DAYS:
                pnl = ltp - t['entry']
                pnl_pct = (pnl / t['entry']) * 100
                if pnl_pct < 2:
                    state["trade_log"].append({"symbol": symbol, "pnl": pnl, "pnl_pct": pnl_pct, "exit": "TIME"})
                    state["daily_log"].append({"action": "SELL", "symbol": symbol, "price": ltp, "pnl_pct": pnl_pct, "time": datetime.now(IST).isoformat()})
                    to_close.append(symbol)
                    send_telegram(f"⏰ *TIME EXIT:* {symbol} @ ₹{ltp:.2f}\n*PnL:* {pnl_pct:.2f}%\n_No movement in {TIME_STOP_DAYS} days_")
                    continue

            r_val = t['entry'] - t['base_sl']
            if ltp >= (t['entry'] + r_val) and not t['trail']:
                t['sl'] = t['entry']
                t['trail'] = True
                send_telegram(f"🔵 *RISK-FREE:* {symbol}\nSL moved to Entry ₹{t['entry']:.2f}")

            if t['trail']:
                new_sl = ltp - (t['atr'] * ATR_MULT_TRAIL)
                if new_sl > t['sl']: t['sl'] = new_sl

            state["trades"][symbol] = t
        except: continue

    for s in to_close:
        if s in state["trades"]: del state["trades"][s]
    save_state()

# ================== MAIN ==================
def run_scanner():
    log("Scanner Thread Started")
    while True:
        try:
            now = datetime.now(IST)
            send_greeting()
            send_daily_report()
            send_weekly_report()

            if now.weekday() < 5 and (9 <= now.hour <= 15):
                manage_trades()
                scan_and_enter()
                time.sleep(900) # 15 मिनट
            else:
                time.sleep(1800) # 30 मिनट
        except Exception as e:
            log(f"Scanner Error: {e}")
            time.sleep(60)

if __name__ == "__main__":
    log("V19.9 Master Script Started | 18-Kavach Active | Telegram Bot + Scanner")
    load_state()
    send_telegram("🚀 *Wealth-Kavach V19.9 Integrated* 🚀\n_18-Kavach System LIVE on Render_")
    
    # Scanner को बैकग्राउंड में चलाओ
    threading.Thread(target=run_scanner, daemon=True).start()
    
    # Telegram Bot शुरू करो - /start सुनेगा
    updater = Updater(TOKEN, use_context=True)
    dp = updater.dispatcher
    dp.add_handler(CommandHandler("start", start_command))
    dp.add_handler(CommandHandler("status", status_command))
    dp.add_handler(CommandHandler("positions", positions_command))
    
    log("Bot polling started - Ready to receive /start")
    updater.start_polling()
    updater.idle()
