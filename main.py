import os
import time
import telebot
import pandas as pd
import yfinance as yf
from datetime import datetime
import pytz
from flask import Flask
import threading
import sqlite3
from ta.trend import ADXIndicator
from ta.momentum import RSIIndicator
from ta.volatility import AverageTrueRange

# ========== CONFIG ==========
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID') 
bot = telebot.TeleBot(BOT_TOKEN)

# V32.2 SETTINGS
TOTAL_CAPITAL = 100000 # अपना कैपिटल
MAX_OPEN_POSITIONS = 5
RISK_PER_TRADE = 0.02 # 2% रिस्क
STOP_LOSS_ATR_MULT = 1.5
TARGET_ATR_MULT = 3.0
SCAN_INTERVAL = 300 # 5 मिनट = 300 सेकंड
MAX_WORKERS = 5 # V32.1 Rate Limit Fix

# NIFTY 50 LIST - CSV की जरूरत नहीं V32.2 में
NIFTY50_SYMBOLS = [
    'ADANIENT.NS', 'ADANIPORTS.NS', 'APOLLOHOSP.NS', 'ASIANPAINT.NS', 'AXISBANK.NS',
    'BAJAJ-AUTO.NS', 'BAJFINANCE.NS', 'BAJAJFINSV.NS', 'BPCL.NS', 'BHARTIARTL.NS',
    'BRITANNIA.NS', 'CIPLA.NS', 'COALINDIA.NS', 'DIVISLAB.NS', 'DRREDDY.NS',
    'EICHERMOT.NS', 'GRASIM.NS', 'HCLTECH.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS',
    'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDUNILVR.NS', 'ICICIBANK.NS', 'ITC.NS',
    'INDUSINDBK.NS', 'INFY.NS', 'JSWSTEEL.NS', 'KOTAKBANK.NS', 'LTIM.NS',
    'LT.NS', 'M&M.NS', 'MARUTI.NS', 'NTPC.NS', 'NESTLEIND.NS',
    'ONGC.NS', 'POWERGRID.NS', 'RELIANCE.NS', 'SBILIFE.NS', 'SBIN.NS',
    'SUNPHARMA.NS', 'TCS.NS', 'TATACONSUM.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS',
    'TECHM.NS', 'TITAN.NS', 'ULTRACEMCO.NS', 'UPL.NS', 'WIPRO.NS'
]

# ========== DATABASE ==========
def init_db():
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trades
                 (id INTEGER PRIMARY KEY, symbol TEXT, entry_price REAL, qty INTEGER, 
                  sl REAL, target REAL, status TEXT, entry_time TEXT, exit_time TEXT, pnl REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS capital
                 (id INTEGER PRIMARY KEY, total_capital REAL, available_capital REAL)''')
    c.execute("INSERT OR IGNORE INTO capital (id, total_capital, available_capital) VALUES (1,?,?)", 
              (TOTAL_CAPITAL, TOTAL_CAPITAL))
    conn.commit()
    conn.close()

# ========== CORE LOGIC ==========
def get_nifty_trend():
    try:
        nifty = yf.Ticker("^NSEI")
        hist = nifty.history(period="1d", interval="5m")
        if len(hist) < 20: return "SIDEWAYS"
        sma20 = hist['Close'].rolling(20).mean().iloc[-1]
        last_close = hist['Close'].iloc[-1]
        if last_close > sma20: return "BULLISH"
        elif last_close < sma20: return "BEARISH"
        return "SIDEWAYS"
    except: return "SIDEWAYS"

def scan_stocks():
    nifty_trend = get_nifty_trend()
    if nifty_trend!= "BULLISH": 
        print("NIFTY Shield: Market not bullish. Skipping scan.")
        return []
    
    selected = []
    # V32.1 FIX: 5-5 के बैच में चलाओ + 2 sec गैप
    for i in range(0, len(NIFTY50_SYMBOLS), MAX_WORKERS):
        batch = NIFTY50_SYMBOLS[i:i+MAX_WORKERS]
        for symbol in batch:
            try:
                df = yf.download(symbol, period="60d", interval="1d", progress=False)
                if len(df) < 30: continue
                
                df['ADX'] = ADXIndicator(df['High'], df['Low'], df['Close'], 14).adx()
                df['RSI'] = RSIIndicator(df['Close'], 14).rsi()
                df['ATR'] = AverageTrueRange(df['High'], df['Low'], df['Close'], 14).average_true_range()
                
                last = df.iloc[-1]
                if last['ADX'] > 25 and last['RSI'] > 55 and last['Close'] > df['Close'].rolling(20).mean().iloc[-1]:
                    selected.append({
                        'symbol': symbol, 
                        'price': last['Close'], 
                        'atr': last['ATR']
                    })
            except Exception as e:
                print(f"Error {symbol}: {e}")
                continue
        time.sleep(2) # V32.1 Rate Limit Fix
    
    return selected[:MAX_OPEN_POSITIONS]

def execute_trades(stocks):
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'")
    open_count = c.fetchone()[0]
    
    c.execute("SELECT available_capital FROM capital WHERE id=1")
    available = c.fetchone()[0]
    
    for stock in stocks:
        if open_count >= MAX_OPEN_POSITIONS: break
        
        risk_amount = TOTAL_CAPITAL * RISK_PER_TRADE
        sl = stock['price'] - (stock['atr'] * STOP_LOSS_ATR_MULT)
        qty = int(risk_amount / (stock['price'] - sl))
        if qty == 0: continue
        
        investment = qty * stock['price']
        if investment > available: continue
        
        target = stock['price'] + (stock['atr'] * TARGET_ATR_MULT)
        
        c.execute("INSERT INTO trades (symbol, entry_price, qty, sl, target, status, entry_time) VALUES (?,?,?,?,?, 'OPEN',?)",
                  (stock['symbol'], stock['price'], qty, sl, target, datetime.now(pytz.timezone('Asia/Kolkata')).strftime("%Y-%m-%d %H:%M")))
        
        available -= investment
        open_count += 1
        
        bot.send_message(CHAT_ID, f"🟢 BUY: {stock['symbol']}\nQty: {qty}\nEntry: ₹{stock['price']:.2f}\nSL: ₹{sl:.2f}\nTarget: ₹{target:.2f}")
    
    c.execute("UPDATE capital SET available_capital=? WHERE id=1", (available,))
    conn.commit()
    conn.close()

def run_scan():
    print("Starting scan...")
    stocks = scan_stocks()
    if stocks: execute_trades(stocks)
    else: print("No stocks selected")

# ========== TELEGRAM COMMANDS ==========
@bot.message_handler(commands=['status'])
def send_status(message):
    conn = sqlite3.connect('trades.db')
    c = conn.cursor()
    c.execute("SELECT available_capital FROM capital WHERE id=1")
    available = c.fetchone()[0]
    c.execute("SELECT COUNT(*) FROM trades WHERE status='OPEN'")
    open_pos = c.fetchone()[0]
    c.execute("SELECT SUM(pnl) FROM trades WHERE status='CLOSED'")
    net_pnl = c.fetchone()[0] or 0
    conn.close()
    
    msg = f"🚀 **BRAHMAND KAVACH V32.2**\n\n💰 Total Capital: ₹{TOTAL_CAPITAL:,}\n💵 Available: ₹{available:,.2f}\n📊 Slots: {open_pos}/{MAX_OPEN_POSITIONS}\n💹 Net P&L: ₹{net_pnl:,.2f}\n\n"
    
    if open_pos > 0:
        c = sqlite3.connect('trades.db').cursor()
        c.execute("SELECT symbol, entry_price, qty FROM trades WHERE status='OPEN'")
        msg += "**Open Positions:**\n"
        for row in c.fetchall():
            msg += f"🔹 {row[0]}: {row[2]} @ ₹{row[1]:.2f}\n"
    else:
        msg += "**Open Positions:**\nNone"
    
    bot.reply_to(message, msg)

# ========== SCHEDULER ==========
def scheduler():
    while True:
        now = datetime.now(pytz.timezone('Asia/Kolkata'))
        if now.weekday() < 5 and now.hour >= 9 and now.hour < 15: # Mon-Fri 9AM-3PM
            if now.minute % 30 == 0 or now.minute % 5 == 0: # 5-min या 30-min
                run_scan()
        if now.hour == 15 and now.minute == 35: # 3:35 PM क्लोजिंग रिपोर्ट
            bot.send_message(CHAT_ID, "📉 **आज की क्लोजिंग रिपोर्ट:**\nआज कोई ट्रेड क्लोज नहीं हुआ")
        time.sleep(60)

# ========== FLASK KEEP ALIVE ==========
app = Flask('')
@app.route('/')
def home():
    return "BRAHMAND KAVACH V32.2 Live"

def run_flask():
    app.run(host='0.0.0.0', port=10000)

# ========== START ==========
if __name__ == "__main__":
    init_db()
    threading.Thread(target=scheduler).start()
    threading.Thread(target=run_flask).start()
    print("BRAHMAND KAVACH V32.2 Started")
    bot.polling()
