# ========== RENDER FREE PLAN FIX - START ==========
import threading, os
from flask import Flask
app = Flask(__name__)

@app.route('/')
def home():
    return "Om Bot Running - V40.1"

def run_flask():
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)

threading.Thread(target=run_flask).start()
# ========== RENDER FREE PLAN FIX - END ==========

# 👇 इसके नीचे आपका पुराना बॉट का सारा कोड जैसा था वैसा रहने दो
import telegram
# ... बाकी सारा कोड ...import telebot
import yfinance as yf
import pandas as pd
import json
import os
import time
import logging
from datetime import datetime
from pytz import timezone

# ===== LOGGING SETUP =====
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# ===== CONFIG =====
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID_VAL = os.environ.get("CHAT_ID")

if not BOT_TOKEN or not CHAT_ID_VAL:
    logging.error("BOT_TOKEN या CHAT_ID Environment में नहीं मिला!")
    exit(1)

CHAT_ID = int(CHAT_ID_VAL)
bot = telebot.TeleBot(BOT_TOKEN)
IST = timezone('Asia/Kolkata')

STATE_FILE = "v40_state.json"
DAILY_LOSS_LIMIT = -1500
MAX_POS = 6
RISK_PER_TRADE = 25000
SCAN_INTERVAL = 900 # 15 मिनट
YF_DELAY = 2.5 # Rate limit से बचने के लिए

nifty250 = [
    "RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS","HINDUNILVR.NS",
    "ITC.NS","SBIN.NS","BHARTIARTL.NS","KOTAKBANK.NS","LT.NS","ASIANPAINT.NS",
    "AXISBANK.NS","MARUTI.NS","SUNPHARMA.NS","TITAN.NS","ULTRACEMCO.NS","BAJFINANCE.NS",
    "WIPRO.NS","NESTLEIND.NS","POWERGRID.NS","NTPC.NS","ONGC.NS","TECHM.NS",
    "TATAMOTORS.NS","M&M.NS","HCLTECH.NS","COALINDIA.NS","ADANIENT.NS","ADANIPORTS.NS",
    "JSWSTEEL.NS","HINDALCO.NS","GRASIM.NS","CIPLA.NS","DRREDDY.NS","EICHERMOT.NS",
    "HEROMOTOCO.NS","BAJAJ-AUTO.NS","TATASTEEL.NS","INDUSINDBK.NS","BAJAJFINSV.NS",
    "DIVISLAB.NS","BRITANNIA.NS","APOLLOHOSP.NS","UPL.NS","TATACONSUM.NS","BPCL.NS",
    "SHREECEM.NS","PIDILITIND.NS","DABUR.NS","SBILIFE.NS","HDFCLIFE.NS","ICICIPRULI.NS",
    "VEDL.NS","GODREJCP.NS","SIEMENS.NS","AMBUJACEM.NS","BANDHANBNK.NS","BANKBARODA.NS",
    "BERGEPAINT.NS","BIOCON.NS","BOSCHLTD.NS","CADILAHC.NS","CANBK.NS","CHOLAFIN.NS",
    "COLPAL.NS","CONCOR.NS","DLF.NS","GAIL.NS","HAVELLS.NS","HINDPETRO.NS",
    "IDFCFIRSTB.NS","IGL.NS","INDIGO.NS","IOC.NS","JINDALSTEL.NS","LICHSGFIN.NS",
    "LUPIN.NS","MARICO.NS","MUTHOOTFIN.NS","NAUKRI.NS","NMDC.NS","PAGEIND.NS",
    "PETRONET.NS","PFC.NS","PGHH.NS","PNB.NS","RECLTD.NS","SAIL.NS","SRF.NS",
    "TORNTPHARM.NS","TORNTPOWER.NS","TVSMOTOR.NS","UBL.NS","VOLTAS.NS","ZEEL.NS",
    "ZYDUSLIFE.NS","ABB.NS","ACC.NS","ALKEM.NS","ASHOKLEY.NS","ASTRAL.NS","ATUL.NS",
    "AUBANK.NS","BALKRISIND.NS","BEL.NS","BHEL.NS","DALBHARAT.NS","DEEPAKNTR.NS",
    "ESCORTS.NS","FEDERALBNK.NS","GUJGASLTD.NS","HAL.NS","HONAUT.NS","IDBI.NS",
    "IDEA.NS","INDIANB.NS","IRCTC.NS","JUBLFOOD.NS","LALPATHLAB.NS","LAURUSLABS.NS",
    "M&MFIN.NS","MANAPPURAM.NS","MFSL.NS","MPHASIS.NS","MRF.NS","MOTHERSON.NS",
    "OBEROIRLTY.NS","OFSS.NS","PERSISTENT.NS","PIIND.NS","POLYCAB.NS","RBLBANK.NS",
    "SUNDARMFIN.NS","TATACHEM.NS","TATACOMM.NS","TATAELXSI.NS","TATAPOWER.NS",
    "TRENT.NS","UNIONBANK.NS","VBL.NS","YESBANK.NS","ABFRL.NS","AIAENG.NS",
    "APLAPOLLO.NS","AARTIIND.NS","ADANIGREEN.NS","AFFLE.NS","AJANTPHARM.NS",
    "AMARAJABAT.NS","APOLLOTYRE.NS","AUROPHARMA.NS","BAJAJHLDNG.NS","BALRAMCHIN.NS",
    "BATAINDIA.NS","BHARATFORG.NS","CAMS.NS","CANFINHOME.NS","CDSL.NS","CHAMBLFERT.NS",
    "CROMPTON.NS","CUMMINSIND.NS","DIXON.NS","FORTIS.NS","GLENMARK.NS","GODREJPROP.NS",
    "GRANULES.NS","HAPPSTMNDS.NS","INDHOTEL.NS","INDIAMART.NS","IEX.NS","IPCALAB.NS",
    "JUBLPHARM.NS","KAJARIACER.NS","KPITTECH.NS","LICI.NS","LTTS.NS","METROPOLIS.NS",
    "NH.NS","OIL.NS","PEL.NS","PHOENIXLTD.NS","POONAWALLA.NS","PRESTIGE.NS",
    "RADICO.NS","RAJESHEXPO.NS","RAMCOCEM.NS","RELAXO.NS","SCHAEFFLER.NS",
    "SHRIRAMFIN.NS","SOBHA.NS","SOLARINDS.NS","SUNTV.NS","SUPREMEIND.NS",
    "SYNGENE.NS","TANLA.NS","TIMKEN.NS","TRIDENT.NS","TTKPRESTIG.NS","UJJIVANSFB.NS",
    "VGUARD.NS","WHIRLPOOL.NS","ZENSARTECH.NS","ZOMATO.NS"
]

# ===== STATE MANAGEMENT =====
def load_state():
    today = str(datetime.now(IST).date())
    default_state = {"pos": {}, "daily_pnl": 0, "date": today}

    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
                # नया दिन है तो PNL रीसेट
                if s.get("date")!= today:
                    s["daily_pnl"] = 0
                    s["date"] = today
                return s
        except Exception as e:
            logging.error(f"State load error: {e}")
    return default_state

def save_state(s):
    try:
        with open(STATE_FILE, "w") as f: json.dump(s, f)
    except Exception as e:
        logging.error(f"State save error: {e}")

state = load_state()

# ===== TELEGRAM FUNCTIONS =====
def send_msg(text):
    try:
        bot.send_message(CHAT_ID, text, parse_mode='Markdown')
        logging.info(f"Msg sent: {text[:50]}...")
    except Exception as e:
        logging.error(f"TG Error: {e}")

@bot.message_handler(commands=['start'])
def cmd_start(m):
    if m.chat.id == CHAT_ID:
        bot.reply_to(m, "🚩 *जय श्री राम*\nV40.1 ब्रह्मास्त्र चालू है\n/status भेजो स्टेटस के लिए")

@bot.message_handler(commands=['status'])
def cmd_status(m):
    if m.chat.id == CHAT_ID:
        txt = f"🚩 *V40.1 Status*\nPos: {len(state['pos'])}/{MAX_POS}\nDaily PNL: {state['daily_pnl']}\nDate: {state['date']}"
        bot.reply_to(m, txt)

@bot.message_handler(commands=['ping'])
def cmd_ping(m):
    if m.chat.id == CHAT_ID:
        bot.reply_to(m, "✅ Pong! Bot जिंदा है")

# ===== TRADING FUNCTIONS =====
def ema(series, period):
    return series.ewm(span=period, adjust=False).mean()

def get_data(symbol):
    time.sleep(YF_DELAY)
    try:
        df = yf.download(symbol, period="60d", interval="1d", progress=False)
        return df if not df.empty else pd.DataFrame()
    except Exception as e:
        logging.error(f"YF Error {symbol}: {e}")
        return pd.DataFrame()

def scan():
    global state

    # डेली लॉस लिमिट चेक
    if state["daily_pnl"] <= DAILY_LOSS_LIMIT:
        logging.info("Daily loss limit hit. Scanning stopped.")
        return

    for symbol in nifty250:
        if symbol in state["pos"] or len(state["pos"]) >= MAX_POS:
            continue

        df = get_data(symbol)
        if df.empty or len(df) < 55:
            continue

        df['EMA50'] = ema(df['Close'], 50)
        c = df.iloc[-1]

        # V40 लॉजिक: EMA50 से 2% ऊपर + ट्रेंड
        if c['Close'] > c['EMA50'] * 1.02:
            entry = float(c['Close'])
            sl = entry * 0.97 # 3% SL
            tp = entry + (entry - sl) * 2.7 # 1:2.7 RR
            qty = max(1, int(RISK_PER_TRADE / (entry - sl)))

            state["pos"][symbol] = {
                "buy": round(entry, 2),
                "sl": round(sl, 2),
                "tp": round(tp, 2),
                "qty": qty,
                "time": str(datetime.now(IST))
            }
            save_state(state)
            send_msg(
                f"✅ *BUY ALERT* - `{symbol}`\n"
                f"Entry: `{entry:.2f}`\n"
                f"SL: `{sl:.2f}` | TP: `{tp:.2f}`\n"
                f"Qty: `{qty}`"
            )
            logging.info(f"Signal generated: {symbol}")

# ===== MAIN LOOP =====
def main_loop():
    logging.info("Main scanning loop started")
    send_msg("🚩 *V40.1 ब्रह्मास्त्र चालू*\nमुनीम जी गद्दी पर बैठ गए")

    while True:
        now = datetime.now(IST)
        # सोमवार-शुक्रवार, 9:15 से 3:30 तक
        if now.weekday() < 5 and 915 <= now.hour*100+now.minute <= 1530:
            logging.info("Market open. Scanning...")
            scan()
        else:
            logging.info("Market closed. Sleeping...")

        time.sleep(SCAN_INTERVAL)

if __name__ == "__main__":
    import threading
    # स्कैनिंग अलग Thread में
    threading.Thread(target=main_loop, daemon=True).start()

    # Telegram Polling Main Thread में - 409 Fix
    logging.info("Starting Telegram polling...")
    bot.remove_webhook() # Webhook की गारंटी से सफाई
    time.sleep(1)
    bot.infinity_polling(timeout=60, long_polling_timeout=60)
