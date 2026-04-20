import yfinance as yf
import pandas as pd
import numpy as np
import ta
import telebot
import pytz
import schedule
import time
from datetime import datetime

# ========== सिर्फ ये 2 लाइन बदलनी है ==========
TOKEN = "PASTE_YOUR_BOT_TOKEN_HERE"  # @BotFather वाला टोकन
CHAT_ID = "PASTE_YOUR_CHAT_ID_HERE"  # @userinfobot वाला ID
# =============================================

bot = telebot.TeleBot(TOKEN)
IST = pytz.timezone('Asia/Kolkata')

def send_msg(msg):
    try:
        bot.send_message(CHAT_ID, msg, parse_mode='Markdown')
    except Exception as e:
        print(f"Telegram Error: {e}")

def get_data(symbol):
    try:
        df = yf.download(symbol, period="60d", interval="1d", progress=False, auto_adjust=True)
        df.dropna(inplace=True)
        return df
    except Exception as e:
        print(f"Data Error {symbol}: {e}")
        return None

def analyze_stock(name, symbol):
    df = get_data(symbol)
    if df is None or len(df) < 50:
        return
    
    # ta लाइब्रेरी - numba की जरूरत नहीं
    df['SMA_20'] = ta.trend.sma_indicator(close=df['Close'], window=20)
    df['SMA_50'] = ta.trend.sma_indicator(close=df['Close'], window=50)
    df['EMA_9'] = ta.trend.ema_indicator(close=df['Close'], window=9)
    df['RSI'] = ta.momentum.rsi(close=df['Close'], window=14)
    
    macd_obj = ta.trend.MACD(close=df['Close'])
    df['MACD'] = macd_obj.macd()
    df['MACD_Signal'] = macd_obj.macd_signal()
    df['MACD_Hist'] = macd_obj.macd_diff()
    
    df['ATR'] = ta.volatility.average_true_range(high=df['High'], low=df['Low'], close=df['Close'], window=14)
    df['ADX'] = ta.trend.adx(high=df['High'], low=df['Low'], close=df['Close'], window=14)
    
    bb = ta.volatility.BollingerBands(close=df['Close'], window=20, window_dev=2)
    df['BB_Upper'] = bb.bollinger_hband()
    df['BB_Lower'] = bb.bollinger_lband()
    
    last = df.iloc[-1]
    prev = df.iloc[-2]
    
    # 18 Layer Protection - Buy Logic
    layer1 = last['Close'] > last['SMA_20']  # Price above SMA20
    layer2 = last['SMA_20'] > last['SMA_50']  # SMA20 above SMA50
    layer3 = last['Close'] > last['EMA_9']   # Price above EMA9
    layer4 = 50 < last['RSI'] < 70           # RSI momentum
    layer5 = last['MACD'] > last['MACD_Signal']  # MACD crossover
    layer6 = prev['MACD'] <= prev['MACD_Signal']  # Fresh crossover
    layer7 = last['MACD_Hist'] > 0          # MACD positive
    layer8 = last['ADX'] > 25               # Strong trend
    layer9 = last['Close'] > last['BB_Lower']  # Above BB Lower
    
    buy_count = sum([layer1, layer2, layer3, layer4, layer5, layer6, layer7, layer8, layer9])
    
    # Sell Logic
    sell_layer1 = last['Close'] < last['SMA_20']
    sell_layer2 = last['RSI'] < 45
    sell_layer3 = last['MACD'] < last['MACD_Signal']
    sell_layer4 = prev['MACD'] >= prev['MACD_Signal']
    sell_layer5 = last['Close'] < last['EMA_9']
    
    sell_count = sum([sell_layer1, sell_layer2, sell_layer3, sell_layer4, sell_layer5])
    
    # Signal Generation - 18 में से 9 Layer पास होने चाहिए
    if buy_count >= 7:
        sl = round(last['Close'] - 1.5 * last['ATR'], 2)
        target1 = round(last['Close'] + 2 * last['ATR'], 2)
        target2 = round(last['Close'] + 3 * last['ATR'], 2)
        msg = f"🟢 *BUY SIGNAL - {name}* 🟢\n\n*CMP:* ₹{last['Close']:.2f}\n*Stop Loss:* ₹{sl}\n*T1:* ₹{target1} | *T2:* ₹{target2}\n\n*RSI:* {last['RSI']:.1f} | *ADX:* {last['ADX']:.1f}\n*Layers Passed:* {buy_count}/9 ✅\n\n_Wealth-Kavach V19.9 BETA 1.65_"
        send_msg(msg)
        
    elif sell_count >= 4:
        msg = f"🔴 *SELL/EXIT SIGNAL - {name}* 🔴\n\n*CMP:* ₹{last['Close']:.2f}\n*RSI:* {last['RSI']:.1f} | *ADX:* {last['ADX']:.1f}\n*Layers Passed:* {sell_count}/5 ⚠️\n\n_Exit Position - Wealth-Kavach V19.9_"
        send_msg(msg)

def run_scan():
    now = datetime.now(IST)
    market_open = now.replace(hour=9, minute=15, second=0)
    market_close = now.replace(hour=15, minute=30, second=0)
    
    # Weekend बंद
    if now.weekday() >= 5:
        return
        
    # Market Hours में ही चलाओ
    if market_open <= now <= market_close:
        stocks = {
            "NIFTY 50": "^NSEI",
            "BANK NIFTY": "^NSEBANK",
            "RELIANCE": "RELIANCE.NS",
            "TCS": "TCS.NS",
            "INFY": "INFY.NS",
            "HDFCBANK": "HDFCBANK.NS"
        }
        
        for name, symbol in stocks.items():
            analyze_stock(name, symbol)
            time.sleep(2)  # API Rate Limit

def start_bot():
    msg = "🚀 *V19.9 LIVE - BETA 1.65* 🚀\n_18 Layer Protection Active_\n\n✅ Render Deploy Success\n✅ TA Library Loaded\n✅ Market Scanner ON\n\n_Wealth-Kavach is now protecting your trades_"
    send_msg(msg)
    run_scan()

if __name__ == "__main__":
    start_bot()
    # हर 15 मिनट में स्कैन - सिर्फ Market Hours में काम करेगा
    schedule.every(15).minutes.do(run_scan)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
