import os
import time
import telebot
import yfinance as yf
from datetime import datetime
import pytz
from flask import Flask
import threading
import json
from ta.trend import ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

# ========== CONFIGURATION ==========
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

if not BOT_TOKEN or not CHAT_ID:
    print("❌ ERROR: BOT_TOKEN or CHAT_ID not set in Environment Variables!")
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN)

TOTAL_CAPITAL = 100000
RISK_PER_TRADE = 0.02 # 2% Risk
MAX_OPEN_POSITIONS = 5
IST = pytz.timezone('Asia/Kolkata')

# Memory & Reporting
OPEN_TRADES = {}
CURRENT_CAPITAL = TOTAL_CAPITAL
DAILY_EXIT_COUNT = 0
DAILY_PNL = 0

# NIFTY 250 - टॉप लिक्विड स्टॉक्स की लिस्ट
NIFTY250_SYMBOLS = [
    'RELIANCE.NS','TCS.NS','HDFCBANK.NS','ICICIBANK.NS','INFY.NS','ITC.NS','SBIN.NS','BHARTIARTL.NS','KOTAKBANK.NS',
    'LT.NS','AXISBANK.NS','ASIANPAINT.NS','MARUTI.NS','BAJFINANCE.NS','HCLTECH.NS','TATAMOTORS.NS','M&M.NS','ZOMATO.NS',
    'SUNPHARMA.NS','TITAN.NS','ULTRACEMCO.NS','WIPRO.NS','ONGC.NS','NTPC.NS','POWERGRID.NS','JSWSTEEL.NS','COALINDIA.NS'
    # यहाँ आप अपनी पसंद के और स्टॉक जोड़ सकते हैं
]

# ========== DATA PERSISTENCE (TELEGRAM BACKUP) ==========
def recover_data():
    global CURRENT_CAPITAL, OPEN_TRADES
    try:
        updates = bot.get_updates(limit=50)
        for update in reversed(updates):
            if update.message and "💾 BACKUP_DATA:" in update.message.text:
                data = json.loads(update.message.text.split("💾 BACKUP_DATA:")[1])
                CURRENT_CAPITAL = data['capital']
                OPEN_TRADES = data['trades']
                bot.send_message(CHAT_ID, "✅ डेटा रिकवरी सफल! पिछली पोजीशन चालू हैं।")
                return
    except: pass

def save_state():
    data = {'capital': CURRENT_CAPITAL, 'trades': OPEN_TRADES}
    bot.send_message(CHAT_ID, f"💾 BACKUP_DATA:{json.dumps(data)}")

# ========== TRADING LOGIC ==========
def get_nifty_trend():
    try:
        n = yf.download("^NSEI", period="2d", interval="15m", progress=False)
        ema50 = n['Close'].ewm(span=50).mean().iloc[-1]
        return "BULLISH" if n['Close'].iloc[-1] > ema50 else "BEARISH"
    except: return "SIDEWAYS"

def scan_and_buy():
    global CURRENT_CAPITAL
    if get_nifty_trend() != "BULLISH" or len(OPEN_TRADES) >= MAX_OPEN_POSITIONS:
        return

    for symbol in NIFTY250_SYMBOLS:
        if symbol in OPEN_TRADES: continue
        try:
            df = yf.download(symbol, period="60d", interval="1d", progress=False)
            if len(df) < 30: continue
            
            # इंडिकेटर कैलकुलेशन
            rsi = RSIIndicator(df['Close'], 14).rsi().iloc[-1]
            adx = ADXIndicator(df['High'], df['Low'], df['Close'], 14).adx().iloc[-1]
            atr = AverageTrueRange(df['High'], df['Low'], df['Close'], 14).average_true_range().iloc[-1]
            ema20 = df['Close'].ewm(span=20).mean().iloc[-1]
            price = df['Close'].iloc[-1]

            # एंट्री कंडीशन
            if adx > 25 and rsi > 55 and price > ema20:
                risk_amt = TOTAL_CAPITAL * RISK_PER_TRADE
                sl = price - (atr * 1.5)
                target = price + (atr * 3.0)
                qty = int(risk_amt / (price - sl)) if (price - sl) > 0 else 0
                
                investment = qty * price
                if qty > 0 and investment <= CURRENT_CAPITAL:
                    OPEN_TRADES[symbol] = {
                        'entry': float(price), 'qty': int(qty), 'sl': float(sl), 
                        'target': float(target), 'high': float(price)
                    }
                    CURRENT_CAPITAL -= investment
                    bot.send_message(CHAT_ID, f"📝 **BUY ALERT**\n🚀 {symbol}\nQty: {qty}\nEntry: ₹{price:.2f}\nSL: ₹{sl:.2f}\nTarget: ₹{target:.2f}")
                    save_state()
                    if len(OPEN_TRADES) >= MAX_OPEN_POSITIONS: break
        except: continue

def manage_exits():
    global CURRENT_CAPITAL, DAILY_PNL, DAILY_EXIT_COUNT
    to_delete = []
    for s, t in OPEN_TRADES.items():
        try:
            df = yf.download(s, period="1d", interval="5m", progress=False)
            curr = df['Close'].iloc[-1]
            
            reason = None
            if curr <= t['sl']: reason = "SL HIT 🛑"
            elif curr >= t['target']: reason = "TARGET HIT 🎯"

            if reason:
                pnl = (curr - t['entry']) * t['qty']
                CURRENT_CAPITAL += (t['qty'] * curr)
                DAILY_PNL += pnl
                DAILY_EXIT_COUNT += 1
                bot.send_message(CHAT_ID, f"📉 **EXIT ALERT: {s}**\n{reason}\nExit: ₹{curr:.2f}\nP&L: ₹{pnl:.2f}")
                to_delete.append(s)
        except: continue
    
    for s in to_delete: del OPEN_TRADES[s]
    if to_delete: save_state()

# ========== REPORTING ==========
def send_evening_report():
    today = datetime.now(IST).strftime("%d-%b-%Y")
    msg = (f"🏁 **डेली क्लोजिंग रिपोर्ट - {today}**\n\n"
           f"💰 करंट कैपिटल: ₹{CURRENT_CAPITAL:,.2f}\n"
           f"📊 आज का P&L: ₹{DAILY_PNL:,.2f}\n"
           f"🔄 क्लोज्ड ट्रेड्स: {DAILY_EXIT_COUNT}\n"
           f"📌 ओपन पोजीशन: {len(OPEN_TRADES)}\n\n"
           f"जय श्री राम! कल मिलते हैं। 🙏")
    bot.send_message(CHAT_ID, msg)

# ========== SCHEDULER ==========
report_sent = False

def scheduler():
    global report_sent
    recover_data()
    while True:
        now = datetime.now(IST)
        # सोमवार-शुक्रवार, 9:15-3:30
        if now.weekday() < 5:
            if 9*60+15 <= now.hour*60+now.minute <= 15*60+30:
                manage_exits()
                if now.minute % 15 == 0:
                    scan_and_buy()
                report_sent = False # मार्केट के समय रीसेट
            
            # शाम की रिपोर्ट 3:35 पर
            if now.hour == 15 and now.minute == 35 and not report_sent:
                send_evening_report()
                report_sent = True
        
        time.sleep(60)

# ========== KEEP ALIVE & START ==========
app = Flask('')
@app.route('/')
def home(): return "V32.9 Sampoorna Active"

def run_flask(): app.run(host='0.0.0.0', port=10000)

if __name__ == "__main__":
    threading.Thread(target=run_flask).start()
    threading.Thread(target=scheduler).start()
    bot.send_message(CHAT_ID, "🚩 जय श्री राम! ब्रह्मास्त्र V32.9 पूर्ण रूप से चालू है।")
    bot.infinity_polling()

