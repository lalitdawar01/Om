# 🚩🚩 जय श्री राम - V42.1 ULTIMATE BRAHMASTRA - FINAL EDITION 🚩🚩
import os
import time
import telebot
import yfinance as yf
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta
from threading import Thread
from flask import Flask

# ===== CONFIG =====
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
bot = telebot.TeleBot(BOT_TOKEN)

DATA_FILE = "v42_ultimate.json"
CAPITAL = 100000
MAX_POSITIONS = 4
DAILY_LOSS_LIMIT = -1500

# ===== V42.1 ULTIMATE SETTINGS =====
RR_RATIO = 2.7
ATR_SL_MULTIPLIER = 1.5
ATR_TGT_MULTIPLIER = 4.0
BREAK_EVEN_PCT = 0.02
ADX_THRESHOLD = 25
AUTO_EXIT_DAYS = 3
BATCH_SIZE = 50
RISK_PER_TRADE = 0.01
PARTIAL_BOOK_PCT = 0.06
PARTIAL_BOOK_QTY = 0.5

# ===== FLASK FOR RENDER =====
app = Flask(__name__)
@app.route('/')
def home(): return "🚩 जय श्री राम - V42.1 ULTIMATE BRAHMASTRA LIVE 🛡️💰"

# ===== DATABASE =====
def save_data():
    try:
        data = {"positions": POSITIONS, "daily_pnl": DAILY_PNL, "date": str(datetime.now().date())}
        with open(DATA_FILE, "w") as f: json.dump(data, f)
    except: pass

def load_data():
    if os.path.exists(DATA_FILE):
        try:
            with open(DATA_FILE, "r") as f:
                data = json.load(f)
                if data.get("date")!= str(datetime.now().date()):
                    return {}, 0
                else:
                    return data.get("positions", {}), data.get("daily_pnl", 0)
        except:
            return {}, 0
    return {}, 0

POSITIONS, DAILY_PNL = load_data()
TRADING_HALTED = False
MORNING_SENT = False
EVENING_SENT = False

# ===== SECTOR MAP NIFTY 250 =====
SECTOR_MAP = {
    'MARUTI.NS': 'AUTO', 'M&M.NS': 'AUTO', 'TATAMOTORS.NS': 'AUTO', 'BAJAJ-AUTO.NS': 'AUTO',
    'EICHERMOT.NS': 'AUTO', 'HEROMOTOCO.NS': 'AUTO', 'TVSMOTOR.NS': 'AUTO', 'ASHOKLEY.NS': 'AUTO',
    'BOSCHLTD.NS': 'AUTO', 'MOTHERSON.NS': 'AUTO', 'BALKRISIND.NS': 'AUTO', 'BHARATFORG.NS': 'AUTO',
    'TCS.NS': 'IT', 'INFY.NS': 'IT', 'HCLTECH.NS': 'IT', 'WIPRO.NS': 'IT', 'TECHM.NS': 'IT',
    'LTIM.NS': 'IT', 'COFORGE.NS': 'IT', 'PERSISTENT.NS': 'IT', 'MPHASIS.NS': 'IT', 'LTTS.NS': 'IT',
    'HDFCBANK.NS': 'BANK', 'ICICIBANK.NS': 'BANK', 'SBIN.NS': 'BANK', 'KOTAKBANK.NS': 'BANK',
    'AXISBANK.NS': 'BANK', 'INDUSINDBK.NS': 'BANK', 'BANDHANBNK.NS': 'BANK', 'FEDERALBNK.NS': 'BANK',
    'IDFCFIRSTB.NS': 'BANK', 'PNB.NS': 'BANK', 'BANKBARODA.NS': 'BANK', 'AUBANK.NS': 'BANK',
    'RELIANCE.NS': 'ENERGY', 'ONGC.NS': 'ENERGY', 'NTPC.NS': 'ENERGY', 'POWERGRID.NS': 'ENERGY',
    'IOC.NS': 'ENERGY', 'BPCL.NS': 'ENERGY', 'HINDPETRO.NS': 'ENERGY', 'GAIL.NS': 'ENERGY',
    'ADANIENT.NS': 'ENERGY', 'ADANIPOWER.NS': 'ENERGY', 'TATAPOWER.NS': 'ENERGY', 'JSWENERGY.NS': 'ENERGY',
    'TATASTEEL.NS': 'METAL', 'JSWSTEEL.NS': 'METAL', 'HINDALCO.NS': 'METAL', 'COALINDIA.NS': 'METAL',
    'NMDC.NS': 'METAL', 'VEDL.NS': 'METAL', 'NATIONALUM.NS': 'METAL', 'HINDCOPPER.NS': 'METAL',
    'JINDALSTEL.NS': 'METAL', 'SAIL.NS': 'METAL', 'SUNPHARMA.NS': 'PHARMA', 'DRREDDY.NS': 'PHARMA',
    'CIPLA.NS': 'PHARMA', 'DIVISLAB.NS': 'PHARMA', 'APOLLOHOSP.NS': 'PHARMA', 'MAXHEALTH.NS': 'PHARMA',
    'LUPIN.NS': 'PHARMA', 'AUROPHARMA.NS': 'PHARMA', 'TORNTPHARM.NS': 'PHARMA', 'ALKEM.NS': 'PHARMA',
    'ZYDUSLIFE.NS': 'PHARMA', 'LT.NS': 'INFRA', 'ULTRACEMCO.NS': 'INFRA', 'GRASIM.NS': 'INFRA',
    'ADANIPORTS.NS': 'INFRA', 'DLF.NS': 'INFRA', 'GODREJPROP.NS': 'INFRA', 'SHREECEM.NS': 'INFRA',
    'AMBUJACEM.NS': 'INFRA', 'ACC.NS': 'INFRA', 'SIEMENS.NS': 'INFRA', 'ABB.NS': 'INFRA',
    'HAVELLS.NS': 'INFRA', 'POLYCAB.NS': 'INFRA', 'HAL.NS': 'INFRA', 'BEL.NS': 'INFRA', 'BHEL.NS': 'INFRA'
}

SECTORS = ['^CNXAUTO', '^CNXIT', '^CNXBANK', '^CNXENERGY', '^CNXMETAL', '^CNXPHARMA', '^CNXINFRA']
def get_strong_sectors():
    strong = []
    for s in SECTORS:
        try:
            df = yf.download(s, period="2d", progress=False)
            if len(df) >= 2 and df['Close'].iloc[-1] > df['Close'].iloc[-2]:
                strong.append(s.replace('^CNX',''))
        except: pass
    return strong

def market_trend():
    try:
        df = yf.download("^NSEI", period="50d", interval="1d", progress=False)
        if len(df) < 50: return True
        df['EMA50'] = df['Close'].ewm(span=50).mean()
        is_uptrend = df['Close'].iloc[-1] > df['EMA50'].iloc[-1] and df['Close'].iloc[-1] > df['Close'].iloc[-2]
        if not is_uptrend:
            send_msg("🌧️ *BEAR MARKET* 🚩 जय श्री राम 🚩\nNifty Weak. नए सिग्नल बंद, कैश सेफ।")
        return is_uptrend
    except: return True

STOCKS = list(SECTOR_MAP.keys()) + [
    'ASIANPAINT.NS', 'BHARTIARTL.NS', 'BRITANNIA.NS', 'DABUR.NS', 'GODREJCP.NS', 'HINDUNILVR.NS',
    'ITC.NS', 'MARICO.NS', 'NESTLEIND.NS', 'TATACONSUM.NS', 'UNITEDSPR.NS', 'VBL.NS', 'COLPAL.NS',
    'PAGEIND.NS', 'PIDILITIND.NS', 'BERGEPAINT.NS', 'TRENT.NS', 'DMART.NS', 'JUBLFOOD.NS', 'NYKAA.NS',
    'ZOMATO.NS', 'PAYTM.NS', 'POLICYBZR.NS', 'DELHIVERY.NS', 'IRCTC.NS', 'INDIGO.NS', 'INDHOTEL.NS',
    'TITAN.NS', 'BAJFINANCE.NS', 'BAJAJFINSV.NS', 'CHOLAFIN.NS', 'HDFCLIFE.NS', 'SBILIFE.NS',
    'ICICIPRULI.NS', 'ICICIGI.NS', 'SBICARD.NS', 'SHRIRAMFIN.NS', 'M&MFIN.NS', 'LICHSGFIN.NS',
    'RECLTD.NS', 'PFC.NS', 'IRFC.NS', 'POONAWALLA.NS', 'MANAPPURAM.NS', 'MUTHOOTFIN.NS'
]

def send_msg(text):
    try: bot.send_message(CHAT_ID, text, parse_mode='Markdown')
    except: pass

def calculate_indicators(df):
    df['EMA20'] = df['Close'].ewm(span=20).mean()
    df['EMA50'] = df['Close'].ewm(span=50).mean()
    delta = df['Close'].diff()
    gain = delta.clip(lower=0).rolling(14).mean()
    loss = (-delta.clip(upper=0)).rolling(14).mean()
    df['RSI'] = 100 - (100/(1+(gain/loss)))
    tr = pd.concat([df['High']-df['Low'], abs(df['High']-df['Close'].shift()), abs(df['Low']-df['Close'].shift())], axis=1).max(axis=1)
    df['ATR'] = tr.rolling(14).mean()
    plus_dm = df['High'].diff().clip(lower=0)
    minus_dm = abs(df['Low'].diff().clip(upper=0))
    atr_smooth = df['ATR'].ewm(alpha=1/14).mean()
    plus_di = 100 * (plus_dm.ewm(alpha=1/14).mean() / atr_smooth)
    minus_di = 100 * (minus_dm.ewm(alpha=1/14).mean() / atr_smooth)
    df['ADX'] = 100 * (abs(plus_di - minus_di) / (plus_di + minus_di)).ewm(alpha=1/14).mean()
    return df

def scan_and_trade():
    global TRADING_HALTED
    if not market_trend(): return
    if TRADING_HALTED or len(POSITIONS) >= MAX_POSITIONS or DAILY_PNL <= DAILY_LOSS_LIMIT:
        if DAILY_PNL <= DAILY_LOSS_LIMIT and not TRADING_HALTED:
            TRADING_HALTED = True
            send_msg(f"🛑 *LOSS LIMIT HIT* 🚩 जय श्री राम 🚩\n₹{DAILY_PNL:.0f}\nBot आज के लिए बंद।")
        return

    strong_sectors = get_strong_sectors()
    found = []
    stocks_to_scan = [s for s in STOCKS if s not in POSITIONS][:BATCH_SIZE]
    if not stocks_to_scan: return

    try:
        data = yf.download(stocks_to_scan, period="60d", interval="1d", group_by='ticker', progress=False, threads=True)
    except: return

    for s in stocks_to_scan:
        try:
            df = data[s].copy() if len(stocks_to_scan) > 1 else data.copy()
            if len(df) < 50 or df.isnull().values.any(): continue
            df = calculate_indicators(df)
            l, p = df.iloc[-1], df.iloc[-2]
            if l['ADX'] < ADX_THRESHOLD or l['Close'] < l['EMA50']: continue
            pullback = (l['Close'] > l['EMA20'] and p['Close'] <= p['EMA20'] and l['Close'] > p['High'])
            if pullback:
                score = 50
                if 55 < l['RSI'] < 70: score += 20
                if l['Volume'] > df['Volume'].rolling(20).mean().iloc[-1] * 1.5: score += 15
                stock_sector = SECTOR_MAP.get(s)
                sector_boost = ""
                if stock_sector and stock_sector in strong_sectors:
                    score += 15
                    sector_boost = f"🔥{stock_sector}"
                if score >= 70:
                    found.append((s, score, l['Close'], l['ATR'], sector_boost))
        except: continue

    for s, score, price, atr, sector_boost in sorted(found, key=lambda x: x[1], reverse=True)[:MAX_POSITIONS]:
        if len(POSITIONS) >= MAX_POSITIONS: break
        risk_amount = CAPITAL * RISK_PER_TRADE
        sl_distance = atr * ATR_SL_MULTIPLIER
        qty = max(1, int(risk_amount / sl_distance))
        if qty > 0 and price * qty <= CAPITAL * 0.25:
            sl = price - sl_distance
            target = price + (atr * ATR_TGT_MULTIPLIER)
            POSITIONS[s] = {
                'buy': float(price), 'qty': qty, 'sl': float(sl), 'target': float(target),
                'score': score, 'time': datetime.now().isoformat(), 'be_guard': False, 'partial_booked': False
            }
            save_data()
            send_msg(f"🚀 *V42.1 BUY {s.replace('.NS','')}* {sector_boost} 🚩 जय श्री राम 🚩\nScore: {score} | Price: ₹{price:.2f} | Qty: {qty}\nRisk: ₹{risk_amount:.0f} | SL: ₹{sl:.2f} | TGT: ₹{target:.2f}")

def monitor():
    global DAILY_PNL
    if not POSITIONS: return
    remove = []
    try:
        data_1m = yf.download(list(POSITIONS.keys()), period="1d", interval="1m", group_by='ticker', progress=False)
    except: return

    for s, p in POSITIONS.items():
        try:
            df = data_1m[s] if len(POSITIONS) > 1 else data_1m
            curr = df['Close'].iloc[-1]

            if curr > p['buy'] * (1 + PARTIAL_BOOK_PCT) and not p.get('partial_booked', False):
                partial_qty = int(p['qty'] * PARTIAL_BOOK_QTY)
                if partial_qty > 0:
                    pnl = (curr - p['buy']) * partial_qty
                    DAILY_PNL += pnl
                    p['qty'] -= partial_qty
                    p['partial_booked'] = True
                    send_msg(f"💰 *6% PARTIAL 50% {s.replace('.NS','')}* 🚩 जय श्री राम 🚩\nQty: {partial_qty} | P&L: ₹{pnl:.0f}\nBaki: {p['qty']} | Daily: ₹{DAILY_PNL:.0f}")

            if curr >= p['buy'] * (1 + BREAK_EVEN_PCT) and not p['be_guard']:
                p['sl'] = p['buy']
                p['be_guard'] = True
                send_msg(f"🛡️ *SAFE MODE: {s.replace('.NS','')}* 🚩 जय श्री राम 🚩\nSL=Cost. रिस्क जीरो!")

            if curr > p['buy'] * 1.03:
                new_trail_sl = curr * 0.98
                if new_trail_sl > p['sl']:
                    p['sl'] = new_trail_sl
                    send_msg(f"📈 *TRAIL SL: {s.replace('.NS','')}* 🚩 जय श्री राम 🚩\nNew SL: ₹{p['sl']:.2f}")

            entry_time = datetime.fromisoformat(p['time'])
            if (datetime.now() - entry_time).days >= AUTO_EXIT_DAYS:
                pnl = (curr - p['buy']) * p['qty']
                DAILY_PNL += pnl
                send_msg(f"⏰ *3-DAY EXIT {s.replace('.NS','')}* 🚩 जय श्री राम 🚩\nExit: ₹{curr:.2f} | P&L: ₹{pnl:.0f}")
                remove.append(s)
                continue

            if curr >= p['target']:
                pnl = (curr - p['buy']) * p['qty']
                DAILY_PNL += pnl
                send_msg(f"🎯 *TARGET {s.replace('.NS','')}* 🚩 जय श्री राम 🚩\nExit: ₹{curr:.2f} | P&L: ₹{pnl:.0f} | Daily: ₹{DAILY_PNL:.0f}")
                remove.append(s)
            elif curr <= p['sl']:
                pnl = (curr - p['buy']) * p['qty']
                DAILY_PNL += pnl
                send_msg(f"🛑 *SL HIT {s.replace('.NS','')}* 🚩 जय श्री राम 🚩\nExit: ₹{curr:.2f} | P&L: ₹{pnl:.0f} | Daily: ₹{DAILY_PNL:.0f}")
                remove.append(s)
        except: continue
    for s in remove: del POSITIONS[s]
    if remove: save_data()

@bot.message_handler(commands=['start', 'status'])
def status(message):
    pos_count = len(POSITIONS)
    msg = f"📊 *V42.1 ULTIMATE BRAHMASTRA* 🚩 जय श्री राम 🚩\n\n💰 Daily P&L: ₹{DAILY_PNL:.0f}\n📈 Open: {pos_count}/{MAX_POSITIONS}\n🛡️ Risk/Trade: {RISK_PER_TRADE*100}%\n🎯 RR: 1:{RR_RATIO}\n\n"
    for s, p in POSITIONS.items():
        msg += f"• {s.replace('.NS','')}: ₹{p['buy']:.0f} | SL: ₹{p['sl']:.0f} | Qty: {p['qty']}\n"
    if pos_count == 0: msg += "No positions. Market scanner active..."
    bot.reply_to(message, msg, parse_mode='Markdown')

def main_loop():
    global MORNING_SENT, EVENING_SENT, DAILY_PNL, TRADING_HALTED
    while True:
        try:
            now = datetime.now()
            t = now.strftime("%H:%M")

            # सुबह 9:20 पे जय श्री राम
            if t == "09:20" and not MORNING_SENT and now.weekday() < 5:
                send_msg("🚩 *जय श्री राम, ललित जी!* 🚩\n*V42.1 ULTIMATE BRAHMASTRA चालू* 🛡️💰\n\n✅ Market Trend Filter\n✅ ADX + Sector Map\n✅ Batch Download 50 Stocks\n✅ 1% Risk Per Trade\n✅ 6% Partial Book 50%\n✅ Break-Even + Trailing SL\n✅ RR 1:2.7 + 3-Day Exit\n\n*आज का दिन शुभ हो* 👑\n🚩 *जय श्री राम* 🚩")
                MORNING_SENT = True
                EVENING_SENT = False

            # ट्रेडिंग टाइम
            if now.weekday() < 5 and "09:20" <= t < "15:25":
                if now.minute % 15 == 0: scan_and_trade()
                monitor()

            # शाम 3:30 पे रिपोर्ट
            if t == "15:30" and not EVENING_SENT and now.weekday() < 5:
                total_pnl = DAILY_PNL
                open_pos = len(POSITIONS)
                msg = f"📊 *DAILY REPORT* 🚩 जय श्री राम 🚩\n\n📅 Date: {now.strftime('%d-%b-%Y')}\n💰 Total P&L: ₹{total_pnl:.0f}\n📈 Open Positions: {open_pos}\n\n"
                if POSITIONS:
                    msg += "*HOLDING:*\n"
                    for s, p in POSITIONS.items():
                        msg += f"• {s.replace('.NS','')}: ₹{p['buy']:.0f} | Qty: {p['qty']}\n"
                else:
                    msg += "*No Open Positions*\n"
                msg += f"\n*कल फिर मिलेंगे* 👑\n🚩 *जय श्री राम* 🚩"
                send_msg(msg)
                DAILY_PNL = 0
                TRADING_HALTED = False
                save_data()
                EVENING_SENT = True
                MORNING_SENT = False

            time.sleep(30)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(30)

if __name__ == "__main__":
    Thread(target=lambda: app.run(host='0.0.0.0', port=10000), daemon=True).start()
    send_msg("🚩 *जय श्री राम* 🚩 *V42.1 ULTIMATE START* 🛡️💰\n✅ Market Trend Filter\n✅ ADX + Sector Map\n✅ Batch Download 50 Stocks\n✅ 1% Risk Per Trade\n✅ 6% Partial Book 50%\n✅ Break-Even + Trailing SL\n✅ RR 1:2.7 + 3-Day Exit\n\n*Bull + Bear दोनों में तैयार* 👑\n🚩 *जय श्री राम* 🚩")
    Thread(target=main_loop, daemon=True).start()
    bot.infinity_polling()
