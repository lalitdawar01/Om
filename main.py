# ========== OM BOT V40.4 FINAL - 250 STOCKS + PORT FIX ==========
import os, time, threading
from flask import Flask
import telegram
import pandas as pd
import yfinance as yf
from datetime import datetime
import pytz

# ====== FLASK SERVER - RENDER के लिए Main Thread में ======
app = Flask(__name__)

@app.route('/')
def home():
    ist = pytz.timezone('Asia/Kolkata')
    current_time = datetime.now(ist).strftime('%d-%m-%Y %H:%M:%S')
    return f"Om Bot V40.4 Live ✅<br>Scanning 250 Stocks<br>Time: {current_time}<br>Port Fix OK 🛡️"

# ====== BOT CONFIG ======
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
IST = pytz.timezone('Asia/Kolkata')

if not BOT_TOKEN or not CHAT_ID:
    print("ERROR: BOT_TOKEN or CHAT_ID missing")
    exit()

bot = telegram.Bot(token=BOT_TOKEN)

# ====== NIFTY 250 FULL LIST - ढाई सौ शेर ======
NIFTY_250 = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "ICICIBANK.NS", "INFY.NS",
    "HINDUNILVR.NS", "ITC.NS", "SBIN.NS", "BHARTIARTL.NS", "KOTAKBANK.NS",
    "LT.NS", "AXISBANK.NS", "ASIANPAINT.NS", "MARUTI.NS", "SUNPHARMA.NS",
    "TITAN.NS", "ULTRACEMCO.NS", "BAJFINANCE.NS", "NESTLEIND.NS", "WIPRO.NS",
    "ONGC.NS", "NTPC.NS", "JSWSTEEL.NS", "POWERGRID.NS", "M&M.NS",
    "TATAMOTORS.NS", "HCLTECH.NS", "BAJAJFINSV.NS", "ADANIENT.NS", "COALINDIA.NS",
    "TECHM.NS", "INDUSINDBK.NS", "HINDALCO.NS", "TATASTEEL.NS", "GRASIM.NS",
    "CIPLA.NS", "DRREDDY.NS", "EICHERMOT.NS", "APOLLOHOSP.NS", "DIVISLAB.NS",
    "BAJAJ-AUTO.NS", "BRITANNIA.NS", "UPL.NS", "HEROMOTOCO.NS", "BPCL.NS",
    "SHREECEM.NS", "ADANIPORTS.NS", "TATACONSUM.NS", "DABUR.NS", "GODREJCP.NS",
    "HAVELLS.NS", "PIDILITIND.NS", "SIEMENS.NS", "AMBUJACEM.NS", "BANKBARODA.NS",
    "DLF.NS", "GAIL.NS", "IOC.NS", "VEDL.NS", "INDIGO.NS",
    "MCDOWELL-N.NS", "MARICO.NS", "MUTHOOTFIN.NS", "PEL.NS", "PGHH.NS",
    "ABB.NS", "ACC.NS", "ADANIGREEN.NS", "ADANITRANS.NS", "ALKEM.NS",
    "ASHOKLEY.NS", "ASTRAL.NS", "ATGL.NS", "AUBANK.NS", "AUROPHARMA.NS",
    "BALKRISIND.NS", "BANDHANBNK.NS", "BATAINDIA.NS", "BEL.NS", "BERGEPAINT.NS",
    "BIOCON.NS", "BOSCHLTD.NS", "CANBK.NS", "CHOLAFIN.NS", "COLPAL.NS",
    "CONCOR.NS", "COROMANDEL.NS", "CUMMINSIND.NS", "DALBHARAT.NS", "DEEPAKNTR.NS",
    "DIXON.NS", "ESCORTS.NS", "FEDERALBNK.NS", "GLAND.NS", "GNFC.NS",
    "GODREJPROP.NS", "GUJGASLTD.NS", "HAL.NS", "HDFCAMC.NS", "HDFCLIFE.NS",
    "HINDPETRO.NS", "ICIGI.NS", "ICICIPRULI.NS", "IDFCFIRSTB.NS", "IEX.NS",
    "IGL.NS", "INDHOTEL.NS", "INDUSTOWER.NS", "NAUKRI.NS", "IRCTC.NS",
    "JINDALSTEL.NS", "JUBLFOOD.NS", "L&TFH.NS", "LALPATHLAB.NS", "LICHSGFIN.NS",
    "LTIM.NS", "LUPIN.NS", "MFSL.NS", "MPHASIS.NS", "MRF.NS",
    "NAM-INDIA.NS", "NMDC.NS", "OBEROIRLTY.NS", "OFSS.NS", "PAGEIND.NS",
    "PERSISTENT.NS", "PETRONET.NS", "PFC.NS", "PIIND.NS", "POLYCAB.NS",
    "PVRINOX.NS", "RAMCOCEM.NS", "RECLTD.NS", "SAIL.NS", "SBILIFE.NS",
    "SRF.NS", "SRTRANSFIN.NS", "TATACHEM.NS", "TATACOMM.NS", "TATAPOWER.NS",
    "TORNTPHARM.NS", "TORNTPOWER.NS", "TRENT.NS", "TVSMOTOR.NS", "UBL.NS",
    "UNIONBANK.NS", "UNITDSPR.NS", "VBL.NS", "VOLTAS.NS", "ZYDUSLIFE.NS",
    "ABFRL.NS", "AIAENG.NS", "APLAPOLLO.NS", "AARTIIND.NS", "ABSLAMC.NS",
    "ADVENZYMES.NS", "AFFLE.NS", "AJANTPHARM.NS", "ALKYLAMINE.NS", "AMARAJABAT.NS",
    "ANURAS.NS", "APLLTD.NS", "ASAHIINDIA.NS", "ATUL.NS", "AVANTIFEED.NS",
    "BAJAJHLDNG.NS", "BALAMINES.NS", "BALRAMCHIN.NS", "BANKINDIA.NS", "BAYERCROP.NS",
    "BBTC.NS", "BEML.NS", "BHARATFORG.NS", "BHEL.NS", "BIRLACORPN.NS",
    "BLUEDART.NS", "CENTRALBK.NS", "CENTURYTEX.NS", "CESC.NS", "CGPOWER.NS",
    "CHAMBLFERT.NS", "CHENNPETRO.NS", "COFORGE.NS", "CREDITACC.NS", "CRISIL.NS",
    "CROMPTON.NS", "CYIENT.NS", "DCBBANK.NS", "DCMSHRIRAM.NS", "DELTACORP.NS",
    "ECLERX.NS", "EDELWEISS.NS", "EIDPARRY.NS", "ENDURANCE.NS", "ENGINERSIN.NS",
    "EQUITASBNK.NS", "EXIDEIND.NS", "FINEORG.NS", "FINCABLES.NS", "FORTIS.NS",
    "FSL.NS", "GALAXYSURF.NS", "GARFIBRES.NS", "GESHIP.NS", "GHCL.NS",
    "GILLETTE.NS", "GLAXO.NS", "GLENMARK.NS", "GMRINFRA.NS", "GODFRYPHLP.NS",
    "GODREJAGRO.NS", "GODREJIND.NS", "GRANULES.NS", "GRAPHITE.NS", "GRINDWELL.NS",
    "GSFC.NS", "GSPL.NS", "GUJALKALI.NS", "HEG.NS", "HINDCOPPER.NS",
    "HINDZINC.NS", "HONAUT.NS", "HSCL.NS", "HUDCO.NS", "IBULHSGFIN.NS",
    "IDBI.NS", "IDEA.NS", "IDFC.NS", "IIFL.NS", "INDIACEM.NS",
    "INDIANB.NS", "INDIAMART.NS", "IOLCP.NS", "IPCALAB.NS", "IRB.NS",
    "ISEC.NS", "ITI.NS", "J&KBANK.NS", "JBCHEPHARM.NS", "JKLAKSHMI.NS",
    "JKPAPER.NS", "JMFINANCIL.NS", "JSL.NS", "JSWENERGY.NS", "JUBLINGREA.NS",
    "JUSTDIAL.NS", "JYOTHYLAB.NS", "KAJARIACER.NS", "KALPATPOWR.NS", "KANSAINER.NS",
    "KARURVYSYA.NS", "KEC.NS", "KEI.NS", "KIRLOSENG.NS", "KNRCON.NS",
    "KOTARISUG.NS", "KPITTECH.NS", "KPRMILL.NS", "KRBL.NS", "KSB.NS",
    "LAOPALA.NS", "LAXMIMACH.NS", "LICI.NS", "LUXIND.NS", "MAHABANK.NS"
]

# ====== BOT FUNCTIONS ======
def send_msg(text):
    try:
        bot.send_message(chat_id=CHAT_ID, text=text, parse_mode='Markdown')
    except Exception as e:
        print(f"Telegram Error: {e}")

def check_stock(ticker):
    try:
        df = yf.download(ticker, period="5d", interval="5m", progress=False)
        if len(df) < 200: return None
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()
        last = df.iloc[-1]; prev = df.iloc[-2]
        
        # PANDAS FIX FINAL
        close = last['Close']
        ema50 = last['EMA50']
        ema200 = last['EMA200']
        prev_close = prev['Close']
        prev_ema50 = prev['EMA50']
        
        # BRAHMASTRA LOGIC
        if close > ema50 * 1.02 and ema50 > ema200 and prev_close <= prev_ema50 * 1.02:
            return {
                'ticker': ticker.replace('.NS', ''),
                'price': round(close, 2),
                'ema50': round(ema50, 2)
            }
        return None
    except Exception as e:
        print(f"Error {ticker}: {e}")
        return None

def run_bot():
    send_msg("✅ *V40.4 ब्रह्मास्त्र चालू* ✅\n\n`250 Stocks Scanner`\n`Render Port Fix`\n`Pandas Fixed`\n`UptimeRobot Ready` 🛡️")
    print("Bot thread started...")
    
    while True:
        try:
            now = datetime.now(IST)
            # Market Hours: 9:15 AM to 3:30 PM IST, Mon-Fri
            if now.weekday() < 5 and 9 <= now.hour < 16:
                if not (now.hour == 15 and now.minute > 30):
                    print(f"Scanning 250 stocks... {now.strftime('%H:%M:%S')}")
                    signals = []
                    for ticker in NIFTY_250:
                        result = check_stock(ticker)
                        if result: signals.append(result)
                        time.sleep(0.5)  # Rate limit से बचने के लिए
                    
                    if signals:
                        msg = f"🚀 *BRAHMASTRA SIGNALS* 🚀\n*{len(signals)} Stocks*\n\n"
                        for s in signals[:10]:  # Telegram पे Max 10 भेजो
                            msg += f"*{s['ticker']}* `{s['price']}`\n"
                        msg += f"\n_Time: {now.strftime('%H:%M:%S')}_"
                        send_msg(msg)
                    else:
                        print("No signals")
            
            time.sleep(300)  # 5 मिनट वेट
        except Exception as e:
            print(f"Bot Loop Error: {e}")
            time.sleep(60)

# ====== RENDER FIX: Flask Main में, Bot Background में ======
if __name__ == '__main__':
    threading.Thread(target=run_bot, daemon=True).start()
    port = int(os.environ.get('PORT', 10000))
    app.run(host='0.0.0.0', port=port)
