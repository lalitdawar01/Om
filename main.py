import os
import time
import requests
import json
from datetime import datetime, timedelta
from threading import Thread
from flask import Flask, request
import telebot
import yfinance as yf
import pandas as pd
import numpy as np

# ======== ENV VARIABLES ========
BOT_TOKEN = os.environ.get('BOT_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')
bot = telebot.TeleBot(BOT_TOKEN)

# ======== FLASK APP ========
app = Flask(__name__)

@app.route('/')
def home():
    return "🚩 जय श्री राम! ब्रह्मास्त्र V32.10 FINAL - Nifty 250 Live है।"

@app.route('/health')
def health():
    return "OK", 200

# ======== TRADING LOGIC ========
CAPITAL = 100000
POSITIONS = {}
MAX_POSITIONS = 5

# ======== NIFTY 250 FULL LIST ========
def get_nifty_250_stocks():
    return [
        'ADANIENT.NS', 'ADANIPORTS.NS', 'APOLLOHOSP.NS', 'ASIANPAINT.NS', 'AXISBANK.NS',
        'BAJAJ-AUTO.NS', 'BAJFINANCE.NS', 'BAJAJFINSV.NS', 'BPCL.NS', 'BHARTIARTL.NS',
        'BRITANNIA.NS', 'CIPLA.NS', 'COALINDIA.NS', 'DIVISLAB.NS', 'DRREDDY.NS',
        'EICHERMOT.NS', 'GRASIM.NS', 'HCLTECH.NS', 'HDFCBANK.NS', 'HDFCLIFE.NS',
        'HEROMOTOCO.NS', 'HINDALCO.NS', 'HINDUNILVR.NS', 'ICICIBANK.NS', 'ITC.NS',
        'INDUSINDBK.NS', 'INFY.NS', 'JSWSTEEL.NS', 'KOTAKBANK.NS', 'LT.NS',
        'M&M.NS', 'MARUTI.NS', 'NTPC.NS', 'NESTLEIND.NS', 'ONGC.NS',
        'POWERGRID.NS', 'RELIANCE.NS', 'SBILIFE.NS', 'SBIN.NS', 'SUNPHARMA.NS',
        'TCS.NS', 'TATACONSUM.NS', 'TATAMOTORS.NS', 'TATASTEEL.NS', 'TECHM.NS',
        'TITAN.NS', 'UPL.NS', 'ULTRACEMCO.NS', 'WIPRO.NS', 'VEDL.NS',
        'ABB.NS', 'ACC.NS', 'AIAENG.NS', 'APLAPOLLO.NS', 'AUBANK.NS',
        'AARTIIND.NS', 'ABBOTINDIA.NS', 'ABCAPITAL.NS', 'ABFRL.NS', 'ALKEM.NS',
        'AMBUJACEM.NS', 'ANGELONE.NS', 'APLLTD.NS', 'ASHOKLEY.NS', 'ASTRAL.NS',
        'ATUL.NS', 'AUROPHARMA.NS', 'DMART.NS', 'BALKRISIND.NS', 'BANDHANBNK.NS',
        'BANKBARODA.NS', 'BANKINDIA.NS', 'BATAINDIA.NS', 'BAYERCROP.NS', 'BERGEPAINT.NS',
        'BEL.NS', 'BHARATFORG.NS', 'BHEL.NS', 'BIOCON.NS', 'BOSCHLTD.NS',
        'BSE.NS', 'CANBK.NS', 'CDSL.NS', 'CESC.NS', 'CGPOWER.NS',
        'CHAMBLFERT.NS', 'CHOLAFIN.NS', 'CUB.NS', 'COFORGE.NS', 'COLPAL.NS',
        'CONCOR.NS', 'COROMANDEL.NS', 'CROMPTON.NS', 'CUMMINSIND.NS', 'DALBHARAT.NS',
        'DEEPAKNTR.NS', 'DELHIVERY.NS', 'DIXON.NS', 'LALPATHLAB.NS', 'EMAMILTD.NS',
        'ENDURANCE.NS', 'ESCORTS.NS', 'EXIDEIND.NS', 'FEDERALBNK.NS', 'FORTIS.NS',
        'GAIL.NS', 'GMRINFRA.NS', 'GLENMARK.NS', 'GODREJCP.NS', 'GODREJPROP.NS',
        'GRANULES.NS', 'GUJGASLTD.NS', 'GSPL.NS', 'HAL.NS', 'HAVELLS.NS',
        'HDFCAMC.NS', 'HINDPETRO.NS', 'HONAUT.NS', 'HUDCO.NS', 'ICIGI.NS',
        'ICICIPRULI.NS', 'IEX.NS', 'IGL.NS', 'IDFCFIRSTB.NS', 'INDHOTEL.NS',
        'INDIAMART.NS', 'INDIANB.NS', 'ISEC.NS', 'INDUSTOWER.NS', 'NAUKRI.NS',
        'INDIGO.NS', 'IPCALAB.NS', 'IRCTC.NS', 'IRFC.NS', 'JINDALSTEL.NS',
        'JKCEMENT.NS', 'JSL.NS', 'JUBLFOOD.NS', 'KAJARIACER.NS', 'KPITTECH.NS',
        'KPRMILL.NS', 'L&TFH.NS', 'LTTS.NS', 'LICHSGFIN.NS', 'LAURUSLABS.NS',
        'LICI.NS', 'LTIM.NS', 'LUPIN.NS', 'M&MFIN.NS', 'MANAPPURAM.NS',
        'MRF.NS', 'MGL.NS', 'MUTHOOTFIN.NS', 'NAM-INDIA.NS', 'NHPC.NS',
        'NMDC.NS', 'OBEROIRLTY.NS', 'OFSS.NS', 'OIL.NS', 'PAYTM.NS',
        'PAGEIND.NS', 'PERSISTENT.NS', 'PETRONET.NS', 'PFIZER.NS', 'PIDILITIND.NS',
        'PIIND.NS', 'PNB.NS', 'POLYCAB.NS', 'POONAWALLA.NS', 'PVRINOX.NS',
        'RAMCOCEM.NS', 'RBLBANK.NS', 'RECLTD.NS', 'SAIL.NS', 'SHREECEM.NS',
        'SRF.NS', 'MOTHERSON.NS', 'SHRIRAMFIN.NS', 'SIEMENS.NS', 'SONACOMS.NS',
        'SBICARD.NS', 'SUNDARMFIN.NS', 'SUNDRMFAST.NS', 'SYNGENE.NS', 'TATACOMM.NS',
        'TATAPOWER.NS', 'TORNTPHARM.NS', 'TORNTPOWER.NS', 'TRENT.NS', 'TRIDENT.NS',
        'TVSMOTOR.NS', 'UNIONBANK.NS', 'IDEA.NS', 'UCOBANK.NS', 'UBL.NS',
        'MCDOWELL-N.NS', 'UNITDSPR.NS', 'VGUARD.NS', 'VBL.NS', 'VOLTAS.NS',
        'WHIRLPOOL.NS', 'YESBANK.NS', 'ZEEL.NS', 'ZYDUSLIFE.NS', 'NYKAA.NS',
        'ZOMATO.NS', 'POLICYBZR.NS', 'AFFLE.NS', 'AMARAJABAT.NS', 'APL.NS',
        'AEGISCHEM.NS', 'BALAMINES.NS', 'BDL.NS', 'BHARATDYNAM.NS', 'BIRLACORPN.NS',
        'BLUESTARCO.NS', 'BRIGADE.NS', 'CARBORUNIV.NS', 'CCL.NS', 'CENTRALBK.NS',
        'CHALET.NS', 'COCHINSHIP.NS', 'CYIENT.NS', 'DCMSHRIRAM.NS', 'DELTACORP.NS',
        'EASEMYTRIP.NS', 'EIDPARRY.NS', 'ELECON.NS', 'ELGIEQUIP.NS', 'ENGINERSIN.NS',
        'EQUITAS.NS', 'ERIS.NS', 'FINEORG.NS', 'FINCABLES.NS', 'FINPIPE.NS',
        'FSL.NS', 'FIVESTAR.NS', 'GAEL.NS', 'GESHIP.NS', 'GLAXO.NS',
        'GNFC.NS', 'GPIL.NS', 'GODFRYPHLP.NS', 'GRAPHITE.NS', 'GREAVESCOT.NS',
        'GRINDWELL.NS', 'GULFOILLUB.NS', 'HATSUN.NS', 'HEG.NS', 'HINDCOPPER.NS',
        'HONASA.NS', 'HFCL.NS', 'IIFL.NS', 'INOXWIND.NS', 'INTELLECT.NS',
        'ITI.NS', 'J&KBANK.NS', 'JBCHEPHARM.NS', 'JKLAKSHMI.NS', 'JKTYRE.NS',
        'JUBLINGREA.NS', 'JUSTDIAL.NS', 'JYOTHYLAB.NS', 'KALYANKJIL.NS', 'KANSAINER.NS',
        'KEI.NS', 'KIRLOSENG.NS', 'KNRCON.NS', 'LAXMIMACH.NS', 'LXCHEM.NS',
        'LEMONTREE.NS', 'LINDEINDIA.NS', 'LLOYDSME.NS', 'MAHABANK.NS', 'MAHLIFE.NS',
        'MANKIND.NS', 'MARICO.NS', 'MAZDOCK.NS', 'METROPOLIS.NS', 'MFSL.NS',
        'MPHASIS.NS', 'MRPL.NS', 'NATCOPHARM.NS', 'NAVINFLUOR.NS', 'NCC.NS',
        'NESTLEIND.NS', 'NLCINDIA.NS', 'NSLNISP.NS', 'OBEROIRLTY.NS', 'OLECTRA.NS',
        'PATANJALI.NS', 'PCBL.NS', 'PEL.NS', 'PFC.NS', 'PNBHOUSING.NS',
        'POLYMED.NS', 'PRAJIND.NS', 'PRSMJOHNSN.NS', 'QUESS.NS', 'RADICO.NS',
        'RAILTEL.NS', 'RALLIS.NS', 'RAYMOND.NS', 'REDINGTON.NS', 'RENUKA.NS',
        'RHIM.NS', 'RTNPOWER.NS', 'SAFARI.NS', 'SANOFI.NS', 'SCHAEFFLER.NS',
        'SHARDACROP.NS', 'SJVN.NS', 'SKFINDIA.NS', 'SOLARINDS.NS', 'SOMANYCERA.NS',
        'SONATSOFTW.NS', 'STARHEALTH.NS', 'SUMICHEM.NS', 'SUNTECK.NS', 'SUPREMEIND.NS',
        'SURYAROSNI.NS', 'SUVENPHAR.NS', 'SUZLON.NS', 'SWANENERGY.NS', 'SYRMA.NS',
        'TANLA.NS', 'TEJASNET.NS', 'THYROCARE.NS', 'TIMKEN.NS', 'TRIVENI.NS',
        'TTKPRESTIG.NS', 'UNOMINDA.NS', 'UJJIVANSFB.NS', 'USHAMART.NS', 'VTL.NS',
        'WELCORP.NS', 'WELSPUNLIV.NS', 'WESTLIFE.NS', 'WOCKPHARMA.NS', 'ZENSARTECH.NS'
    ]

def send_telegram_message(text):
    try:
        bot.send_message(CHAT_ID, text, parse_mode='Markdown')
    except Exception as e:
        print(f"Telegram Error: {e}")

def calculate_ema(data, period):
    return data['Close'].ewm(span=period, adjust=False).mean()

def calculate_rsi(data, period=14):
    delta = data['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def check_buy_signal(symbol):
    try:
        data = yf.download(symbol, period='60d', interval='1d', progress=False)
        if len(data) < 50:
            return False, None
        data['EMA9'] = calculate_ema(data, 9)
        data['EMA21'] = calculate_ema(data, 21)
        data['RSI'] = calculate_rsi(data, 14)
        latest = data.iloc[-1]
        prev = data.iloc[-2]
        if (latest['EMA9'] > latest['EMA21'] and
            prev['EMA9'] <= prev['EMA21'] and
            latest['RSI'] > 50):
            return True, latest['Close']
        return False, None
    except Exception as e:
        print(f"Error checking {symbol}: {e}")
        return False, None

def scan_stocks():
    if len(POSITIONS) >= MAX_POSITIONS:
        return
    stocks = get_nifty_250_stocks()
    for stock in stocks:
        if len(POSITIONS) >= MAX_POSITIONS:
            break
        if stock in POSITIONS:
            continue
        buy_signal, price = check_buy_signal(stock)
        if buy_signal and price:
            qty = int((CAPITAL * 0.2) / price)
            if qty > 0:
                POSITIONS[stock] = {
                    'buy_price': price,
                    'qty': qty,
                    'sl': price * 0.98,
                    'target': price * 1.04,
                    'time': datetime.now()
                }
                msg = f"🚀 *BUY SIGNAL* 🚀\n\n*Stock:* `{stock}`\n*Price:* ₹{price:.2f}\n*Qty:* {qty}\n*SL:* ₹{price * 0.98:.2f}\n*Target:* ₹{price * 1.04:.2f}"
                send_telegram_message(msg)
        time.sleep(1) # API rate limit से बचने के लिए

def check_positions():
    stocks_to_remove = []
    for stock, pos in POSITIONS.items():
        try:
            data = yf.download(stock, period='1d', interval='5m', progress=False)
            if data.empty:
                continue
            ltp = data['Close'].iloc[-1]
            if ltp >= pos['target']:
                pnl = (ltp - pos['buy_price']) * pos['qty']
                msg = f"🎯 *TARGET HIT* 🎯\n\n*Stock:* `{stock}`\n*Buy:* ₹{pos['buy_price']:.2f}\n*Sell:* ₹{ltp:.2f}\n*Qty:* {pos['qty']}\n*P&L:* ₹{pnl:.2f}"
                send_telegram_message(msg)
                stocks_to_remove.append(stock)
            elif ltp <= pos['sl']:
                pnl = (ltp - pos['buy_price']) * pos['qty']
                msg = f"🛑 *STOP LOSS HIT* 🛑\n\n*Stock:* `{stock}`\n*Buy:* ₹{pos['buy_price']:.2f}\n*Sell:* ₹{ltp:.2f}\n*Qty:* {pos['qty']}\n*P&L:* ₹{pnl:.2f}"
                send_telegram_message(msg)
                stocks_to_remove.append(stock)
        except Exception as e:
            print(f"Error checking position {stock}: {e}")
    for stock in stocks_to_remove:
        del POSITIONS[stock]

def scheduler():
    while True:
        now = datetime.now()
        if now.weekday() < 5 and 9 <= now.hour < 16:
            if now.hour == 9 and now.minute < 20:
                time.sleep(60)
                continue
            scan_stocks()
            check_positions()
        time.sleep(300)

@bot.message_handler(commands=['start', 'status'])
def handle_start(message):
    if str(message.chat.id)!= CHAT_ID:
        return
    status_msg = f"🚀 *V32.10 FINAL - NIFTY 250*\n\n💰 *Total Capital:* ₹{CAPITAL:,.2f}\n📊 *Open Positions:* {len(POSITIONS)}/{MAX_POSITIONS}\n📈 *Scanning:* 250 Stocks\n\n"
    if POSITIONS:
        for stock, pos in POSITIONS.items():
            try:
                ltp = yf.Ticker(stock).history(period='1d')['Close'].iloc[-1]
                pnl = (ltp - pos['buy_price']) * pos['qty']
                status_msg += f"*{stock}*\nBuy: ₹{pos['buy_price']:.2f} | LTP: ₹{ltp:.2f}\nP&L: ₹{pnl:.2f}\n\n"
            except:
                status_msg += f"*{stock}*\nBuy: ₹{pos['buy_price']:.2f}\n\n"
    else:
        status_msg += "कोई पोजीशन नहीं है। मार्केट स्कैन चालू है..."
    bot.reply_to(message, status_msg, parse_mode='Markdown')

@bot.message_handler(func=lambda message: True)
def handle_all(message):
    if str(message.chat.id) == CHAT_ID and message.text == '#status':
        handle_start(message)

# ======== BOT START FUNCTION ========
def run_bot():
    print("🚩 जय श्री राम! ब्रह्मास्त्र V32.10 FINAL चालू है।")
    send_telegram_message("🚩 जय श्री राम! ब्रह्मास्त्र V32.10 FINAL चालू है।\n📈 *Nifty 250 Scanning Started*")
    Thread(target=scheduler).start()
    bot.infinity_polling()

# ======== START BOT IN BACKGROUND ========
Thread(target=run_bot).start()

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
