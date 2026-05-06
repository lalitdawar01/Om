import telebot
import yfinance as yf
import pandas as pd
import numpy as np
import json
import os
import threading
import time
from datetime import datetime
from flask import Flask
from pytz import timezone

# ===== CONFIG =====
# रेंडर के Environment Variables से डेटा उठाना
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID_VAL = os.environ.get("CHAT_ID")
CHAT_ID = int(CHAT_ID_VAL) if CHAT_ID_VAL else 0

bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)
IST = timezone('Asia/Kolkata')

STATE_FILE = "v40_state.json"
DAILY_LOSS_LIMIT = -1500
MAX_POS = 6
RISK_PER_TRADE = 25000
LEVERAGE = 5
INTERVAL_MIN = 15
YF_DELAY = 2 

nifty250 = ["RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS","HINDUNILVR.NS","ITC.NS","SBIN.NS","BHARTIARTL.NS","KOTAKBANK.NS","LT.NS","ASIANPAINT.NS","AXISBANK.NS","MARUTI.NS","SUNPHARMA.NS","TITAN.NS","ULTRACEMCO.NS","BAJFINANCE.NS","WIPRO.NS","NESTLEIND.NS","POWERGRID.NS","NTPC.NS","ONGC.NS","TECHM.NS","TATAMOTORS.NS","M&M.NS","HCLTECH.NS","COALINDIA.NS","ADANIENT.NS","ADANIPORTS.NS","JSWSTEEL.NS","HINDALCO.NS","GRASIM.NS","CIPLA.NS","DRREDDY.NS","EICHERMOT.NS","HEROMOTOCO.NS","BAJAJ-AUTO.NS","TATASTEEL.NS","INDUSINDBK.NS","BAJAJFINSV.NS","DIVISLAB.NS","BRITANNIA.NS","APOLLOHOSP.NS","UPL.NS","TATACONSUM.NS","BPCL.NS","SHREECEM.NS","PIDILITIND.NS","DABUR.NS","SBILIFE.NS","HDFCLIFE.NS","ICICIPRULI.NS","VEDL.NS","GODREJCP.NS","SIEMENS.NS","AMBUJACEM.NS","BANDHANBNK.NS","BANKBARODA.NS","BERGEPAINT.NS","BIOCON.NS","BOSCHLTD.NS","CADILAHC.NS","CANBK.NS","CHOLAFIN.NS","COLPAL.NS","CONCOR.NS","DLF.NS","GAIL.NS","HAVELLS.NS","HINDPETRO.NS","IDFCFIRSTB.NS","IGL.NS","INDIGO.NS","IOC.NS","JINDALSTEL.NS","LICHSGFIN.NS","LUPIN.NS","MARICO.NS","MUTHOOTFIN.NS","NAUKRI.NS","NMDC.NS","PAGEIND.NS","PETRONET.NS","PFC.NS","PGHH.NS","PNB.NS","RECLTD.NS","SAIL.NS","SRF.NS","TORNTPHARM.NS","TORNTPOWER.NS","TVSMOTOR.NS","UBL.NS","VOLTAS.NS","ZEEL.NS","ZYDUSLIFE.NS","ABB.NS","ACC.NS","ALKEM.NS","ASHOKLEY.NS","ASTRAL.NS","ATUL.NS","AUBANK.NS","BALKRISIND.NS","BEL.NS","BHEL.NS","DALBHARAT.NS","DEEPAKNTR.NS","ESCORTS.NS","FEDERALBNK.NS","GUJGASLTD.NS","HAL.NS","HONAUT.NS","IDBI.NS","IDEA.NS","INDIANB.NS","IRCTC.NS","JUBLFOOD.NS","LALPATHLAB.NS","LAURUSLABS.NS","M&MFIN.NS","MANAPPURAM.NS","MFSL.NS","MPHASIS.NS","MRF.NS","MOTHERSON.NS","OBEROIRLTY.NS","OFSS.NS","PERSISTENT.NS","PIIND.NS","POLYCAB.NS","RBLBANK.NS","SUNDARMFIN.NS","TATACHEM.NS","TATACOMM.NS","TATAELXSI.NS","TATAPOWER.NS","TRENT.NS","UNIONBANK.NS","VBL.NS","YESBANK.NS","ABFRL.NS","AIAENG.NS","APLAPOLLO.NS","AARTIIND.NS","ADANIGREEN.NS","AFFLE.NS","AJANTPHARM.NS","AMARAJABAT.NS","APOLLOTYRE.NS","AUROPHARMA.NS","BAJAJHLDNG.NS","BALRAMCHIN.NS","BATAINDIA.NS","BHARATFORG.NS","CAMS.NS","CANFINHOME.NS","CDSL.NS","CHAMBLFERT.NS","CROMPTON.NS","CUMMINSIND.NS","DIXON.NS","FORTIS.NS","GLENMARK.NS","GODREJPROP.NS","GRANULES.NS","HAPPSTMNDS.NS","INDHOTEL.NS","INDIAMART.NS","IEX.NS","IPCALAB.NS","JUBLPHARM.NS","KAJARIACER.NS","KPITTECH.NS","LICI.NS","LTTS.NS","METROPOLIS.NS","NH.NS","OIL.NS","PEL.NS","PHOENIXLTD.NS","POONAWALLA.NS","PRESTIGE.NS","RADICO.NS","RAJESHEXPO.NS","RAMCOCEM.NS","RELAXO.NS","SCHAEFFLER.NS","SHRIRAMFIN.NS","SOBHA.NS","SOLARINDS.NS","SUNTV.NS","SUPREMEIND.NS","SYNGENE.NS","TANLA.NS","TIMKEN.NS","TRIDENT.NS","TTKPRESTIG.NS","UJJIVANSFB.NS","VGUARD.NS","WHIRLPOOL.NS","ZENSARTECH.NS","ZOMATO.NS"]

# ===== STATE MANAGEMENT =====
def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f: return json.load(f)
        except: return {"pos": {}, "daily_pnl": 0, "date": str(datetime.now(IST).date())}
    return {"pos": {}, "daily_pnl": 0, "date": str(datetime.now(IST).date())}

def save_state(s):
    with open(STATE_FILE, "w") as f: json.dump(s, f)

state = load_state()

# ===== FUNCTIONS =====
def send_msg(t):
    if CHAT_ID != 0:
        try: bot.send_message(CHAT_ID, t, parse_mode='Markdown')
        except Exception as e: print(f"TG Error: {e}")

def get_data(symbol, period="60d", interval="1d"):
    time.sleep(YF_DELAY)
    try:
        df = yf.download(symbol, period=period, interval=interval, progress=False)
        return df
    except: return pd.DataFrame()

def ema(s, n): return s.ewm(span=n, adjust=False).mean()

def scan():
    global state
    now = datetime.now(IST)
    for s in nifty250:
        if s in state["pos"] or len(state["pos"]) >= MAX_POS: continue
        df = get_data(s)
        if df.empty or len(df) < 55: continue

        df['EMA50'] = ema(df['Close'], 50)
        c = df.iloc[-1]
        
        # V40 फिल्टर लॉजिक
        if c['Close'] > c['EMA50'] * 1.02: # ट्रेंड कन्फर्मेशन
            entry = float(c['Close'])
            sl = entry * 0.97 # 3% स्टॉपलॉस
            tp = entry + (entry - sl) * 2.7
            qty = max(1, int(RISK_PER_TRADE / (entry - sl)))

            state["pos"][s] = {"buy": entry, "sl": sl, "tp": tp, "qty": qty, "time": str(now)}
            save_state(state)
            send_msg(f"✅ *BUY ALERT* - {s}\nPrice: {entry:.2f}\nSL: {sl:.2f}\nTP: {tp:.2f}")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    txt = f"🚩 *V40.1 Status*\nPos: {len(state['pos'])}/{MAX_POS}\nDaily PNL: {state['daily_pnl']}"
    bot.reply_to(m, txt)

# ===== WEB SERVER FOR RENDER =====
@app.route('/')
def home(): return "V40.1 IST Final Running"

def main_loop():
    while True:
        now = datetime.now(IST)
        # मार्केट टाइम: 9:15 AM से 3:30 PM (सोमवार से शुक्रवार)
        if now.weekday() < 5 and 915 <= now.hour*100+now.minute <= 1530:
            scan()
        time.sleep(900) # हर 15 मिनट में स्कैन

if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000))), daemon=True).start()
    threading.Thread(target=main_loop, daemon=True).start()
    bot.infinity_polling()

