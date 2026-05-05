Import os
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
    print("❌ ERROR: BOT_TOKEN or CHAT_ID not set!")
    exit(1)

bot = telebot.TeleBot(BOT_TOKEN, threaded=True)

TOTAL_CAPITAL = 100000
RISK_PER_TRADE = 0.02
MAX_OPEN_POSITIONS = 5
IST = pytz.timezone('Asia/Kolkata')

# Memory & Reporting
OPEN_TRADES = {}
CURRENT_CAPITAL = TOTAL_CAPITAL
DAILY_EXIT_COUNT = 0
DAILY_PNL = 0

# ========== NIFTY 250 COMPLETE LIST ==========
NIFTY250_SYMBOLS = [
    'RELIANCE.NS','TCS.NS','HDFCBANK.NS','ICICIBANK.NS','INFY.NS','HINDUNILVR.NS','ITC.NS','SBIN.NS','BHARTIARTL.NS','KOTAKBANK.NS',
    'LT.NS','BAJFINANCE.NS','HCLTECH.NS','ASIANPAINT.NS','MARUTI.NS','AXISBANK.NS','TITAN.NS','SUNPHARMA.NS','ULTRACEMCO.NS','NESTLEIND.NS',
    'WIPRO.NS','ONGC.NS','NTPC.NS','POWERGRID.NS','TATAMOTORS.NS','JSWSTEEL.NS','TECHM.NS','M&M.NS','TATASTEEL.NS','HDFCLIFE.NS',
    'ADANIENT.NS','ADANIPORTS.NS','COALINDIA.NS','SBILIFE.NS','BAJAJFINSV.NS','HINDALCO.NS','GRASIM.NS','CIPLA.NS','DRREDDY.NS','BRITANNIA.NS',
    'EICHERMOT.NS','APOLLOHOSP.NS','DIVISLAB.NS','HEROMOTOCO.NS','BAJAJ-AUTO.NS','INDUSINDBK.NS','UPL.NS','BPCL.NS','SHREECEM.NS','TATACONSUM.NS',
    'PIDILITIND.NS','DABUR.NS','GODREJCP.NS','SIEMENS.NS','DLF.NS','SBICARD.NS','ICIPRULI.NS','ICICIGI.NS','MARICO.NS','AMBUJACEM.NS',
    'BANDHANBNK.NS','BANKBARODA.NS','BERGEPAINT.NS','BIOCON.NS','BOSCHLTD.NS','CADILAHC.NS','CANBK.NS','CHOLAFIN.NS','COLPAL.NS','CONCOR.NS',
    'CUMMINSIND.NS','DIXON.NS','DMART.NS','FEDERALBNK.NS','GAIL.NS','GLAND.NS','GLENMARK.NS','GODREJPROP.NS','HAVELLS.NS','HDFCAMC.NS',
    'HINDPETRO.NS','IDFCFIRSTB.NS','INDHOTEL.NS','INDUSTOWER.NS','IOC.NS','IRCTC.NS','JINDALSTEL.NS','JUBLFOOD.NS','L&TFH.NS','LALPATHLAB.NS',
    'LICHSGFIN.NS','LTIM.NS','LUPIN.NS','MANAPPURAM.NS','MFSL.NS','MOTHERSON.NS','MPHASIS.NS','MRF.NS','MUTHOOTFIN.NS','NAUKRI.NS',
    'NMDC.NS','OBEROIRLTY.NS','OFSS.NS','PAGEIND.NS','PERSISTENT.NS','PETRONET.NS','PFC.NS','PIIND.NS','PNB.NS','POLYCAB.NS',
    'PVRINOX.NS','RAMCOCEM.NS','RECLTD.NS','SAIL.NS','SRF.NS','SRTRANSFIN.NS','STARHEALTH.NS','TATACHEM.NS','TATACOMM.NS','TATAPOWER.NS',
    'TORNTPHARM.NS','TORNTPOWER.NS','TRENT.NS','TVSMOTOR.NS','UBL.NS','UNIONBANK.NS','VBL.NS','VEDL.NS','VOLTAS.NS','ZYDUSLIFE.NS',
    'ABB.NS','ACC.NS','ABCAPITAL.NS','AARTIIND.NS','ABFRL.NS','ALKEM.NS','ASHOKLEY.NS','ASTRAL.NS','ATUL.NS','AUROPHARMA.NS',
    'BALKRISIND.NS','BALRAMCHIN.NS','BATAINDIA.NS','BEL.NS','BHARATFORG.NS','BHEL.NS','COROMANDEL.NS','CROMPTON.NS','DEEPAKNTR.NS','ESCORTS.NS',
    'EXIDEIND.NS','FORTIS.NS','GUJGASLTD.NS','HAL.NS','HONAUT.NS','IDEA.NS','IDFC.NS','IEX.NS','IGL.NS','INDIAMART.NS',
    'INDIGO.NS','INTELLECT.NS','IPCALAB.NS','IRFC.NS','JKTYRE.NS','JSL.NS','KAJARIACER.NS','KPITTECH.NS','LTTS.NS','MAHABANK.NS',
    'MAHSEAMLES.NS','MAXHEALTH.NS','METROPOLIS.NS','NHPC.NS','OIL.NS','PEL.NS','POONAWALLA.NS','RAJESHEXPO.NS','RBLBANK.NS','RELAXO.NS',
    'RVNL.NS','SHRIRAMFIN.NS','SKFINDIA.NS','SOLARINDS.NS','SONACOMS.NS','SUMICHEM.NS','SUNDARMFIN.NS','SUNDRMFAST.NS','SUNTV.NS','SUPREMEIND.NS',
    'SYNGENE.NS','TATVA.NS','TIMKEN.NS','TTKPRESTIG.NS','UCOBANK.NS','UNOMINDA.NS','VGUARD.NS','VINATIORGA.NS','WHIRLPOOL.NS','YESBANK.NS',
    'ZEEL.NS','ZOMATO.NS','3MINDIA.NS','AIAENG.NS','APLAPOLLO.NS','APTUS.NS','ASAHIINDIA.NS','BSOFT.NS','CAMS.NS','CANFINHOME.NS',
    'CASTROLIND.NS','CGPOWER.NS','CIEINDIA.NS','CLEAN.NS','COFORGE.NS','CRISIL.NS','DELHIVERY.NS','EASEMYTRIP.NS','EMAMILTD.NS','ENDURANCE.NS',
    'FINEORG.NS','FSL.NS','GICRE.NS','GILLETTE.NS','GMRINFRA.NS','GPIL.NS','GSPL.NS','HAPPSTMNDS.NS','HATSUN.NS','HEMIPROP.NS',
    'HUDCO.NS','IIFL.NS','INDIGOPNTS.NS','INFIBEAM.NS','J&KBANK.NS','JINDALSAW.NS','JKLAKSHMI.NS','JSWENERGY.NS','KALYANKJIL.NS','KANSAINER.NS',
    'KEI.NS','KNRCON.NS','KPRMILL.NS','LAXMI.NS','LAURUSLABS.NS','LXCHEM.NS','MEDANTA.NS','MSUMI.NS','MGL.NS','NAVINFLUOR.NS'
]

# ========== DATA PERSISTENCE ==========
def recover_data():
    global CURRENT_CAPITAL, OPEN_TRADES, DAILY_PNL, DAILY_EXIT_COUNT
    try:
        updates = bot.get_updates(limit=50, timeout=5)
        for update in reversed(updates):
            if update.message and "💾 BACKUP_DATA:" in update.message.text:
                data = json.loads(update.message.text.split("💾 BACKUP_DATA:")[1])
                CURRENT_CAPITAL = data.get('capital', TOTAL_CAPITAL)
                OPEN_TRADES = data.get('trades', {})
                DAILY_PNL = data.get('daily_pnl', 0)
                DAILY_EXIT_COUNT = data.get('daily_exit_count', 0)
                bot.send_message(CHAT_ID, f"✅ रिकवरी सफल!\n💰 Capital: ₹{CURRENT_CAPITAL:,.2f}\n📊 Open: {len(OPEN_TRADES)}")
                return
        bot.send_message(CHAT_ID, "⚠️ कोई बैकअप नहीं मिला। फ्रेश स्टार्ट।")
    except Exception as e: print(f"Recovery Error: {e}")

def save_state():
    try:
        data = {
            'capital': CURRENT_CAPITAL,
            'trades': OPEN_TRADES,
            'daily_pnl': DAILY_PNL,
            'daily_exit_count': DAILY_EXIT_COUNT,
            'timestamp': datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
        }
        bot.send_message(CHAT_ID, f"💾 BACKUP_DATA:{json.dumps(data)}")
    except Exception as e: print(f"Save Error: {e}")

# ========== TRADING LOGIC ==========
def get_nifty_trend():
    try:
        n = yf.download("^NSEI", period="2d", interval="15m", progress=False)
        if n.empty or len(n) < 50: return "SIDEWAYS"
        ema50 = n['Close'].ewm(span=50).mean().iloc[-1]
        return "BULLISH" if n['Close'].iloc[-1] > ema50 else "BEARISH"
    except: return "SIDEWAYS"

def scan_and_buy():
    global CURRENT_CAPITAL
    if get_nifty_trend()!= "BULLISH" or len(OPEN_TRADES) >= MAX_OPEN_POSITIONS: return

    for symbol in NIFTY250_SYMBOLS:
        if symbol in OPEN_TRADES: continue
        try:
            df = yf.download(symbol, period="60d", interval="1d", progress=False)
            if len(df) < 30: continue

            close, high, low = df['Close'], df['High'], df['Low']
            rsi = RSIIndicator(close, 14).rsi().iloc[-1]
            adx = ADXIndicator(high, low, close, 14).adx().iloc[-1]
            atr = AverageTrueRange(high, low, close, 14).average_true_range().iloc[-1]
            ema20 = close.ewm(span=20).mean().iloc[-1]
            price = close.iloc[-1]

            if adx > 25 and rsi > 55 and price > ema20:
                risk_amt = TOTAL_CAPITAL * RISK_PER_TRADE
                sl = price - (atr * 1.5)
                target = price + (atr * 3.0)
                if price <= sl: continue
                qty = int(risk_amt / (price - sl))

                if qty > 0 and (qty * price) <= CURRENT_CAPITAL:
                    OPEN_TRADES[symbol] = {
                        'entry': round(float(price), 2),
                        'qty': int(qty),
                        'sl': round(float(sl), 2),
                        'target': round(float(target), 2)
                    }
                    CURRENT_CAPITAL -= (qty * price)
                    bot.send_message(CHAT_ID, f"📝 **BUY ALERT**\n🚀 {symbol}\nQty: {qty}\nEntry: ₹{price:.2f}\nSL: ₹{sl:.2f}\nTarget: ₹{target:.2f}")
                    save_state()
                    if len(OPEN_TRADES) >= MAX_OPEN_POSITIONS: break
        except: continue

def manage_exits():
    global CURRENT_CAPITAL, DAILY_PNL, DAILY_EXIT_COUNT
    to_delete = []
    for s, t in list(OPEN_TRADES.items()):
        try:
            df = yf.download(s, period="1d", interval="5m", progress=False)
            if df.empty: continue
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

    for s in to_delete:
        if s in OPEN_TRADES: del OPEN_TRADES[s]
    if to_delete: save_state()

# ========== TELEGRAM COMMANDS ==========
@bot.message_handler(commands=['start', 'status'])
def send_status(message):
    if str(message.chat.id)!= str(CHAT_ID): return
    nifty_trend = get_nifty_trend()
    status_msg = (
        f"🚀 **V32.9 SAMPURNA - NIFTY 250**\n\n"
        f"💰 Total Capital: ₹{CURRENT_CAPITAL:,.2f}\n"
        f"📊 Open Positions: {len(OPEN_TRADES)}/{MAX_OPEN_POSITIONS}\n"
        f"📈 आज का P&L: ₹{DAILY_PNL:,.2f}\n"
        f"🔄 आज क्लोज्ड: {DAILY_EXIT_COUNT}\n"
        f"🛡️ NIFTY Trend: {nifty_trend}\n"
        f"📋 Scan List: 250 Stocks\n\n"
        f"जय श्री राम! 🚩"
    )
    bot.send_message(CHAT_ID, status_msg)

@bot.message_handler(commands=['portfolio'])
def send_portfolio(message):
    if str(message.chat.id)!= str(CHAT_ID): return
    if not OPEN_TRADES:
        bot.send_message(CHAT_ID, "📊 **PORTFOLIO**\n\nकोई ओपन पोजीशन नहीं है।")
        return
    msg = "📊 **PORTFOLIO**\n\n"
    for s, t in OPEN_TRADES.items():
        msg += f"🚀 {s}\nQty: {t['qty']} | Entry: ₹{t['entry']:.2f}\nSL: ₹{t['sl']:.2f} | Target: ₹{t['target']:.2f}\n\n"
    bot.send_message(CHAT_ID, msg)

# ========== SCHEDULER ==========
report_sent = False

def scheduler():
    global report_sent
    recover_data()
    while True:
        try:
            now = datetime.now(IST)
            if now.weekday() < 5:
                if 9*60+15 <= now.hour*60+now.minute <= 15*60+30:
                    manage_exits()
                    if now.minute % 15 == 0:
                        scan_and_buy()
                    report_sent = False
                if now.hour == 15 and now.minute == 35 and not report_sent:
                    send_evening_report()
                    report_sent = True
        except Exception as e: print(f"Scheduler Error: {e}")
        time.sleep(60)

def send_evening_report():
    today = datetime.now(IST).strftime("%d-%b-%Y")
    msg = (f"🏁 **डेली क्लोजिंग रिपोर्ट - {today}**\n\n"
           f"💰 करंट कैपिटल: ₹{CURRENT_CAPITAL:,.2f}\n"
           f"📊 आज का P&L: ₹{DAILY_PNL:,.2f}\n"
           f"🔄 क्लोज्ड ट्रेड्स: {DAILY_EXIT_COUNT}\n"
           f"📌 ओपन पोजीशन: {len(OPEN_TRADES)}\n\n"
           f"जय श्री राम! कल मिलते हैं। 🙏")
    bot.send_message(CHAT_ID, msg)

# ========== KEEP ALIVE & START ==========
app = Flask('')
@app.route('/')
def home(): return "V32.9 Sampoorna Active"

def run_flask(): app.run(host='0.0.0.0', port=10000)

if __name__ == "__main__":
    threading.Thread(target=run_flask, daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()
    try:
        bot.send_message(CHAT_ID, "🚩 जय श्री राम! ब्रह्मास्त्र V32.9 NIFTY 250 चालू है।")
    except: pass
    bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=10)
