import os
import time
import telebot
import yfinance as yf
from datetime import datetime, timedelta
import pytz
from flask import Flask
import threading
from ta.trend import ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange
import json

# ========== CONFIG ==========
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
bot = telebot.TeleBot(BOT_TOKEN)

# V32.9.1 - TELEGRAM MEMORY EDITION + JAI SHREE RAM
TOTAL_CAPITAL = 100000
RISK_PER_TRADE = 0.02
MAX_OPEN_POSITIONS = 5
STOP_LOSS_ATR_MULT = 1.5
TARGET_ATR_MULT = 3.0
MAX_WORKERS = 10
DRAWDOWN_LIMIT = 0.08
TRAILING_SL_PCT = 0.03
IST = pytz.timezone('Asia/Kolkata')

# इन-मेमोरी डेटाबेस
OPEN_TRADES = {}
CURRENT_CAPITAL = TOTAL_CAPITAL
TODAY_PNL = 0

NIFTY250_SYMBOLS = list(set([
    'RELIANCE.NS','TCS.NS','HDFCBANK.NS','ICICIBANK.NS','INFY.NS','HINDUNILVR.NS','ITC.NS','SBIN.NS','BHARTIARTL.NS','KOTAKBANK.NS',
    'LT.NS','AXISBANK.NS','ASIANPAINT.NS','MARUTI.NS','BAJFINANCE.NS','HCLTECH.NS','ADANIENT.NS','SUNPHARMA.NS','TITAN.NS','ULTRACEMCO.NS',
    'BAJAJFINSV.NS','WIPRO.NS','ONGC.NS','NTPC.NS','POWERGRID.NS','TATAMOTORS.NS','M&M.NS','JSWSTEEL.NS','COALINDIA.NS','TECHM.NS',
    'ZOMATO.NS','DMART.NS','TRENT.NS','RVNL.NS','IRFC.NS','PAYTM.NS','NYKAA.NS','POLICYBZR.NS','DLF.NS','SIEMENS.NS','HAVELLS.NS','BEL.NS',
    'HAL.NS','ADANIGREEN.NS','ADANIPOWER.NS','IOC.NS','GAIL.NS','PIDILITIND.NS','DABUR.NS','GODREJCP.NS','INDIGO.NS','AMBUJACEM.NS'
]))

# ========== TELEGRAM DATABASE LOGIC - THE REAL FIX ==========
def sync_from_telegram():
    """रीस्टार्ट पे टेलीग्राम से आखिरी स्टेट रिकवर करना"""
    global CURRENT_CAPITAL, OPEN_TRADES, TODAY_PNL
    try:
        bot.send_message(CHAT_ID, "🔄 सिस्टम रीस्टार्ट: टेलीग्राम से डेटा रिकवर कर रहे...")
        # आखिरी 50 मैसेज चेक करो
        updates = bot.get_updates(limit=50, offset=-50)
        for update in reversed(updates):
            if update.message and str(update.message.chat.id) == str(CHAT_ID):
                text = update.message.text
                if text and text.startswith("💾 BACKUP_DATA:"):
                    try:
                        data = json.loads(text.split("💾 BACKUP_DATA:")[1])
                        CURRENT_CAPITAL = data['capital']
                        OPEN_TRADES = data['trades']
                        TODAY_PNL = data.get('today_pnl', 0)
                        bot.send_message(CHAT_ID, f"✅ रिकवरी सफल!\n💰 Capital: ₹{CURRENT_CAPITAL:,.2f}\n📊 Open: {len(OPEN_TRADES)} trades")
                        return
                    except: continue
        bot.send_message(CHAT_ID, "⚠️ कोई बैकअप नहीं मिला। फ्रेश स्टार्ट।")
    except Exception as e:
        print(f"Sync Error: {e}")

def save_state_to_telegram():
    """हर ट्रेड के बाद टेलीग्राम पे बैकअप भेजना"""
    data = {
        'capital': CURRENT_CAPITAL,
        'trades': OPEN_TRADES,
        'today_pnl': TODAY_PNL,
        'timestamp': datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    }
    backup_msg = f"💾 BACKUP_DATA:{json.dumps(data)}"
    try:
        bot.send_message(CHAT_ID, backup_msg)
    except: pass

# ========== STRATEGY ==========
def get_nifty_trend():
    try:
        nifty = yf.download("^NSEI", period="2d", interval="15m", progress=False)
        ema50 = nifty['Close'].ewm(span=50).mean().iloc[-1]
        return "BULLISH" if nifty['Close'].iloc[-1] > ema50 else "BEARISH"
    except: return "SIDEWAYS"

def scan_stocks():
    if get_nifty_trend()!= "BULLISH": return []
    selected = []
    for i in range(0, len(NIFTY250_SYMBOLS), MAX_WORKERS):
        batch = NIFTY250_SYMBOLS[i:i+MAX_WORKERS]
        try:
            data = yf.download(batch, period="60d", interval="1d", progress=False, group_by='ticker')
            for symbol in batch:
                if symbol in OPEN_TRADES: continue
                try:
                    df = data[symbol] if len(batch) > 1 else data
                    if len(df) < 30: continue
                    rsi = RSIIndicator(df['Close'], 14).rsi().iloc[-1]
                    adx = ADXIndicator(df['High'], df['Low'], df['Close'], 14).adx().iloc[-1]
                    atr = AverageTrueRange(df['High'], df['Low'], df['Close'], 14).average_true_range().iloc[-1]
                    ema20 = df['Close'].ewm(span=20).mean().iloc[-1]
                    price = df['Close'].iloc[-1]
                    if adx > 25 and rsi > 55 and price > ema20:
                        selected.append({'symbol': symbol, 'price': price, 'atr': atr})
                except: continue
        except: continue
        time.sleep(2)
    return selected[:MAX_OPEN_POSITIONS]

def execute_trades(stocks):
    global CURRENT_CAPITAL
    if CURRENT_CAPITAL < TOTAL_CAPITAL * (1 - DRAWDOWN_LIMIT):
        bot.send_message(CHAT_ID, f"⚠️ DRAWDOWN LIMIT HIT! Trading Stopped.")
        return

    for stock in stocks:
        if len(OPEN_TRADES) >= MAX_OPEN_POSITIONS: break
        risk_amount = TOTAL_CAPITAL * RISK_PER_TRADE
        sl = stock['price'] - (stock['atr'] * STOP_LOSS_ATR_MULT)
        if stock['price'] - sl <= 0: continue
        qty = int(risk_amount / (stock['price'] - sl))
        if qty == 0: continue
        investment = qty * stock['price']
        if investment > CURRENT_CAPITAL: continue
        target = stock['price'] + (stock['atr'] * TARGET_ATR_MULT)

        OPEN_TRADES[stock['symbol']] = {
            'entry': stock['price'], 'qty': qty, 'sl': sl, 'target': target,
            'high': stock['price'], 'time': datetime.now(IST).strftime("%Y-%m-%d %H:%M")
        }
        CURRENT_CAPITAL -= investment
        bot.send_message(CHAT_ID, f"📝 PAPER BUY: {stock['symbol']}\nQty: {qty}\nEntry: ₹{stock['price']:.2f}\nSL: ₹{sl:.2f}\nTGT: ₹{target:.2f}")
        save_state_to_telegram()

def manage_exits():
    global CURRENT_CAPITAL, TODAY_PNL
    to_close = []
    for symbol, trade in OPEN_TRADES.items():
        try:
            df = yf.download(symbol, period="1d", interval="5m", progress=False)
            if df.empty: continue
            current = float(df['Close'].iloc[-1])

            # Trailing SL
            if current > trade['high']:
                trade['high'] = current
                new_sl = max(trade['sl'], current * (1 - TRAILING_SL_PCT))
                trade['sl'] = new_sl

            reason = None
            if current <= trade['sl']: reason = "SL HIT 🛑"
            elif current >= trade['target']: reason = "TARGET HIT 🎯"

            if reason:
                pnl = (current - trade['entry']) * trade['qty']
                CURRENT_CAPITAL += (trade['qty'] * current)
                TODAY_PNL += pnl
                bot.send_message(CHAT_ID, f"📝 PAPER EXIT: {symbol}\n{reason}\nExit: ₹{current:.2f}\nP&L: ₹{pnl:,.2f}\nCapital: ₹{CURRENT_CAPITAL:,.2f}")
                to_close.append(symbol)
        except: continue

    for symbol in to_close:
        del OPEN_TRADES[symbol]
    if to_close: save_state_to_telegram()

def send_daily_report():
    today = datetime.now(IST).strftime("%Y-%m-%d")
    msg = f"📉 **शाम की रिपोर्ट - {today}** 📉\n\n"
    msg += f"💰 कुल कैपिटल: ₹{CURRENT_CAPITAL:,.2f}\n"
    msg += f"📊 आज का P&L: ₹{TODAY_PNL:,.2f}\n"
    msg += f"📌 ओपन पोजीशन: {len(OPEN_TRADES)}\n\n"
    msg += "जय श्री राम 🙏 कल फिर मिलते हैं"
    bot.send_message(CHAT_ID, msg)

# ========== TELEGRAM ==========
@bot.message_handler(commands=['status','report'])
def send_status(message):
    dd_pct = ((TOTAL_CAPITAL - CURRENT_CAPITAL) / TOTAL_CAPITAL) * 100 if CURRENT_CAPITAL < TOTAL_CAPITAL else 0
    msg = f"🚀 **V32.9.1 NIFTY-250**\n\n💰 Total: ₹{CURRENT_CAPITAL:,.2f}\n📊 Slots: {len(OPEN_TRADES)}/{MAX_OPEN_POSITIONS}\n💹 आज का P&L: ₹{TODAY_PNL:,.2f}\n📉 Drawdown: {dd_pct:.1f}%\n\n"
    if OPEN_TRADES:
        msg += "**Open Positions:**\n"
        for sym, t in OPEN_TRADES.items():
            msg += f"🔹 {sym}: {t['qty']} @ ₹{t['entry']:.2f}\n SL: ₹{t['sl']:.2f} | TGT: ₹{t['target']:.2f}\n"
    else: msg += "**Open Positions:**\nNone"
    bot.reply_to(message, msg)

# ========== SCHEDULER ==========
jai_shree_ram_sent = False
report_sent = False

def scheduler():
    global jai_shree_ram_sent, report_sent, TODAY_PNL
    sync_from_telegram()

    while True:
        now = datetime.now(IST)

        if now.hour == 9 and now.minute == 15 and not jai_shree_ram_sent and now.weekday() < 5:
            bot.send_message(CHAT_ID, "🚩 जय श्री राम 🙏\nशुभ प्रभात! आज का ट्रेडिंग सेशन शुरू")
            jai_shree_ram_sent = True

        if now.weekday() < 5 and (9*60+15) <= (now.hour*60+now.minute) <= (15*60+30):
            manage_exits()
            if now.minute % 15 == 0:
                stocks = scan_stocks()
                if stocks: execute_trades(stocks)

        if now.hour == 15 and now.minute == 35 and not report_sent and now.weekday() < 5:
            send_daily_report()
            report_sent = True

        if now.hour == 0 and now.minute == 1:
            jai_shree_ram_sent = False
            report_sent = False
            TODAY_PNL = 0

        time.sleep(60)

# ========== FLASK & START ==========
app = Flask('')
@app.route('/')
def home(): return "V32.9.1 Telegram Memory Active - Jai Shree Ram"

if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=10000)).start()
    threading.Thread(target=scheduler).start()
    bot.send_message(CHAT_ID, "🚀 V32.9.1 STARTED\nजय श्री राम 🙏\nटेलीग्राम मेमोरी एक्टिव")
    bot.infinity_polling()
