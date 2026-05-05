import telebot, yfinance as yf, pandas as pd, numpy as np, json, os, threading, time
from datetime import datetime, timedelta
from flask import Flask

# ===== CONFIG =====
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = int(os.environ.get("CHAT_ID"))
bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)

STATE_FILE = "v40_state.json"
DAILY_LOSS_LIMIT = -1500
MAX_POS = 6
RISK_PER_TRADE = 25000
LEVERAGE = 5
INTERVAL_MIN = 15

nifty250 = ["RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS","HINDUNILVR.NS","ITC.NS","SBIN.NS","BHARTIARTL.NS","KOTAKBANK.NS","LT.NS","ASIANPAINT.NS","AXISBANK.NS","MARUTI.NS","SUNPHARMA.NS","TITAN.NS","ULTRACEMCO.NS","BAJFINANCE.NS","WIPRO.NS","NESTLEIND.NS","POWERGRID.NS","NTPC.NS","ONGC.NS","TECHM.NS","TATAMOTORS.NS","M&M.NS","HCLTECH.NS","COALINDIA.NS","ADANIENT.NS","ADANIPORTS.NS","JSWSTEEL.NS","HINDALCO.NS","GRASIM.NS","CIPLA.NS","DRREDDY.NS","EICHERMOT.NS","HEROMOTOCO.NS","BAJAJ-AUTO.NS","TATASTEEL.NS","INDUSINDBK.NS","BAJAJFINSV.NS","DIVISLAB.NS","BRITANNIA.NS","APOLLOHOSP.NS","UPL.NS","TATACONSUM.NS","BPCL.NS","SHREECEM.NS","PIDILITIND.NS","DABUR.NS","SBILIFE.NS","HDFCLIFE.NS","ICIPRULI.NS","VEDL.NS","GODREJCP.NS","SIEMENS.NS","AMBUJACEM.NS","BANDHANBNK.NS","BANKBARODA.NS","BERGEPAINT.NS","BIOCON.NS","BOSCHLTD.NS","CADILAHC.NS","CANBK.NS","CHOLAFIN.NS","COLPAL.NS","CONCOR.NS","DLF.NS","GAIL.NS","HAVELLS.NS","HINDPETRO.NS","IBULHSGFIN.NS","IDFCFIRSTB.NS","IGL.NS","INDIGO.NS","IOC.NS","JINDALSTEL.NS","L&TFH.NS","LICHSGFIN.NS","LUPIN.NS","MARICO.NS","MCDOWELL-N.NS","MUTHOOTFIN.NS","NAUKRI.NS","NMDC.NS","PAGEIND.NS","PETRONET.NS","PFC.NS","PGHH.NS","PNB.NS","RECLTD.NS","SAIL.NS","SRF.NS","TORNTPHARM.NS","TORNTPOWER.NS","TVSMOTOR.NS","UBL.NS","VOLTAS.NS","ZEEL.NS","ZYDUSLIFE.NS","ABB.NS","ACC.NS","ALKEM.NS","ASHOKLEY.NS","ASTRAL.NS","ATUL.NS","AUBANK.NS","BALKRISIND.NS","BEL.NS","BHEL.NS","DALBHARAT.NS","DEEPAKNTR.NS","ESCORTS.NS","FEDERALBNK.NS","GLAND.NS","GUJGASLTD.NS","HAL.NS","HONAUT.NS","IDBI.NS","IDEA.NS","INDIANB.NS","IRCTC.NS","JUBLFOOD.NS","LALPATHLAB.NS","LAURUSLABS.NS","LINDEINDIA.NS","M&MFIN.NS","MANAPPURAM.NS","MFSL.NS","MPHASIS.NS","MRF.NS","MOTHERSON.NS","OBEROIRLTY.NS","OFSS.NS","PERSISTENT.NS","PIIND.NS","POLYCAB.NS","RBLBANK.NS","SUNDARMFIN.NS","TATACHEM.NS","TATACOMM.NS","TATAELXSI.NS","TATAPOWER.NS","TRENT.NS","UNIONBANK.NS","VBL.NS","YESBANK.NS","ABFRL.NS","AIAENG.NS","APLAPOLLO.NS","AARTIIND.NS","ABSLAMC.NS","ADANIGREEN.NS","ADANITRANS.NS","AFFLE.NS","AJANTPHARM.NS","AMARAJABAT.NS","ANURAS.NS","APOLLOTYRE.NS","ASAHIINDIA.NS","AUROPHARMA.NS","AVANTIFEED.NS","BAJAJHLDNG.NS","BALAMINES.NS","BALRAMCHIN.NS","BATAINDIA.NS","BAYERCROP.NS","BHARATFORG.NS","BLUEDART.NS","BSE.NS","CAMS.NS","CANFINHOME.NS","CDSL.NS","CEATLTD.NS","CENTRALBK.NS","CHAMBLFERT.NS","CREDITACC.NS","CRISIL.NS","CROMPTON.NS","CUMMINSIND.NS","CYIENT.NS","DCBBANK.NS","DCMSHRIRAM.NS","DELTACORP.NS","DIXON.NS","EIDPARRY.NS","EIHOTEL.NS","ENDURANCE.NS","EQUITASBNK.NS","EXIDEIND.NS","FINEORG.NS","FSL.NS","FORTIS.NS","FLUOROCHEM.NS","GICRE.NS","GILLETTE.NS","GLAXO.NS","GLENMARK.NS","GNFC.NS","GODREJAGRO.NS","GODREJIND.NS","GODREJPROP.NS","GRANULES.NS","GRAPHITE.NS","GSFC.NS","GSPL.NS","GUJALKALI.NS","HAPPSTMNDS.NS","HATSUN.NS","HEG.NS","HFCL.NS","HINDCOPPER.NS","HUDCO.NS","IIFL.NS","INDHOTEL.NS","INDIAMART.NS","IEX.NS","INDOCO.NS","INTELLECT.NS","IPCALAB.NS","IRB.NS","IRCON.NS","ITI.NS","J&KBANK.NS","JAMNAAUTO.NS","JBCHEPHARM.NS","JKTYRE.NS","JMFINANCIL.NS","JSL.NS","JUSTDIAL.NS","JYOTHYLAB.NS","KAJARIACER.NS","KALPATPOWR.NS","KANSAINER.NS","KARURVYSYA.NS","KEC.NS","KIRLOSENG.NS","KOTAKBANK.NS","KPITTECH.NS","KRBL.NS","LAXMIMACH.NS","LICI.NS","LXCHEM.NS","MAHABANK.NS","MAHINDCIE.NS","MAHLOG.NS","MAHSCOOTER.NS","MAHSEAMLES.NS","MANAPPURAM.NS","MCX.NS","METROPOLIS.NS","MINDACORP.NS","MOTILALOFS.NS","NATIONALUM.NS","NAVINFLUOR.NS","NBCC.NS","NCC.NS","NHPC.NS","NIACL.NS","NLCINDIA.NS","NAM-INDIA.NS","NATCOPHARM.NS","NAVINFLUOR.NS","NETWORK18.NS","NHPC.NS","NLCINDIA.NS","NMDC.NS","OIL.NS","ORIENTCEM.NS","ORIENTELEC.NS","PATANJALI.NS","PEL.NS","PERSISTENT.NS","PETRONET.NS","PFIZER.NS","PHOENIXLTD.NS","PNBHOUSING.NS","POLYMED.NS","POLYCAB.NS","POONAWALLA.NS","POWERINDIA.NS","PRESTIGE.NS","PRINCEPIPE.NS","PRSMJOHNSN.NS","PUNJABCHEM.NS","PVRINOX.NS","QUESS.NS","RADICO.NS","RAIN.NS","RAJESHEXPO.NS","RALLIS.NS","RAMCOCEM.NS","RATNAMANI.NS","RAYMOND.NS","REDINGTON.NS","RELAXO.NS","RELINFRA.NS","RESPONIND.NS","RITES.NS","ROUTE.NS","SANOFI.NS","SCHAEFFLER.NS","SCHNEIDER.NS","SCI.NS","SHILPAMED.NS","SHOPERSTOP.NS","SHRIRAMFIN.NS","SOBHA.NS","SOLARINDS.NS","SONACOMS.NS","SPANDANA.NS","SPICEJET.NS","STAR.NS","STARCEMENT.NS","SUDARSCHEM.NS","SUMICHEM.NS","SUNDRMFAST.NS","SUNTECK.NS","SUNTV.NS","SUPREMEIND.NS","SUVENPHAR.NS","SUZLON.NS","SYMPHONY.NS","SYNGENE.NS","TANLA.NS","TASTYBITE.NS","TATACOMM.NS","TATAMETALI.NS","TEAMLEASE.NS","TEJASNET.NS","THANGAMAYL.NS","THERMAX.NS","TIMKEN.NS","TORNTPOWER.NS","TRENT.NS","TRIDENT.NS","TRITURBINE.NS","TTKPRESTIG.NS","UCOBANK.NS","UFLEX.NS","UJJIVANSFB.NS","UNOMINDA.NS","VGUARD.NS","VAKRANGEE.NS","VARDHACEM.NS","VTL.NS","WELCORP.NS","WELSPUNIND.NS","WESTLIFE.NS","WHIRLPOOL.NS","WOCKPHARMA.NS","YESBANK.NS","ZENSARTECH.NS","ZOMATO.NS"]

# ===== STATE =====
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f: return json.load(f)
    return {"pos": {}, "daily_pnl": 0, "date": str(datetime.now().date()), "cycle": 0}

def save_state(s):
    with open(STATE_FILE, "w") as f: json.dump(s, f)

state = load_state()
if state["date"]!= str(datetime.now().date()):
    state = {"pos": {}, "daily_pnl": 0, "date": str(datetime.now().date()), "cycle": 0}
    save_state(state)

# ===== UTILS =====
def send_msg(t):
    try: bot.send_message(CHAT_ID, t, parse_mode='Markdown')
    except: pass

def atr(df, n=14):
    h, l, c = df['High'], df['Low'], df['Close']
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def rsi(s, n=14):
    d = s.diff(); g = d.where(d > 0, 0).rolling(n).mean(); l = -d.where(d < 0, 0).rolling(n).mean()
    rs = g / l; return 100 - (100 / (1 + rs))

# ===== MAIN SCAN =====
def scan():
    global state
    if state["daily_pnl"] <= DAILY_LOSS_LIMIT:
        send_msg(f"🛑 Daily Loss Limit {DAILY_LOSS_LIMIT} हिट। ट्रेडिंग बंद।")
        return

    for s in nifty250:
        if s in state["pos"] or len(state["pos"]) >= MAX_POS: continue
        try:
            time.sleep(1) # RATE LIMIT FIX: 1 सेकंड गैप
            df = yf.download(s, period="60d", interval="1d", progress=False)
            if len(df) < 55: continue

            df['EMA50'] = ema(df['Close'], 50)
            df['ATR'] = atr(df)
            df['RSI'] = rsi(df['Close'])
            df['ADX'] = 25 # Simplified
            df['VolAvg'] = df['Volume'].rolling(20).mean()

            c, p = df.iloc[-1], df.iloc[-2]
            if pd.isna(c['EMA50']) or pd.isna(c['ATR']): continue

            # V40 FILTERS: Strong Trend + Pullback Entry
            trend = c['Close'] > c['EMA50'] * 1.02 and c['ADX'] > 22
            pullback = p['Low'] <= p['EMA50'] and c['Close'] > p['High'] and c['Close'] > c['EMA50']
            vol_ok = c['Volume'] > c['VolAvg'] * 2
            rsi_ok = 55 <= c['RSI'] <= 65
            adx_ok = c['ADX'] > 25

            if not (trend and pullback and vol_ok and rsi_ok and adx_ok): continue

            # ENTRY + 4x ATR SL
            entry = float(c['Close'])
            sl = entry - 4 * float(c['ATR'])
            if sl <= 0 or entry - sl < entry * 0.01: continue
            risk_per_share = entry - sl
            qty = max(1, int((RISK_PER_TRADE * LEVERAGE) / entry))
            tp = entry + 2.7 * risk_per_share # 1:2.7 RR

            state["pos"][s] = {
                "buy": entry, "sl": sl, "tp": tp, "qty": qty, "peak": entry,
                "half": False, "time": str(datetime.now()), "day": 0
            }
            save_state(state)
            send_msg(f"✅ *BUY* {s.replace('.NS','')}\n₹{entry:.2f} Qty:{qty}\nSL:₹{sl:.2f} | TP:₹{tp:.2f}\nRR:1:2.7 | Strong Trend+Pullback")

        except Exception as e:
            if "YFRateLimitError" in str(e):
                time.sleep(5) # रेट लिमिट पे 5 सेकंड रुको
            continue

# ===== EXIT LOGIC =====
def manage():
    global state
    for s in list(state["pos"].keys()):
        try:
            time.sleep(1) # RATE LIMIT FIX
            df = yf.download(s, period="5d", interval="5m", progress=False)
            if df.empty: continue
            ltp = float(df['Close'].iloc[-1])
            p = state["pos"][s]

            # 1. SL Hit
            if ltp <= p['sl']:
                pnl = (ltp - p['buy']) * p['qty']
                state["daily_pnl"] += pnl
                send_msg(f"🛑 *SL* {s.replace('.NS','')}\nExit:₹{ltp:.2f}\nPNL:₹{pnl:.0f}")
                del state["pos"][s]
                continue

            # 2. ATR Trailing Update
            atr_val = float(atr(yf.download(s, period="20d", interval="1d", progress=False)).iloc[-1])
            new_sl = ltp - 2.5 * atr_val
            if new_sl > p['sl']: p['sl'] = new_sl

            # 3. Partial 50% @ 6%
            if not p['half'] and ltp >= p['buy'] * 1.06:
                pnl = (ltp - p['buy']) * (p['qty'] // 2)
                state["daily_pnl"] += pnl
                p['qty'] = p['qty'] - (p['qty'] // 2)
                p['half'] = True
                p['sl'] = p['buy'] # Breakeven
                send_msg(f"💰 *PARTIAL 50%* {s.replace('.NS','')}\n₹{ltp:.2f}\nBooked:₹{pnl:.0f}\nSL→Breakeven")

            # 4. TP Hit
            if ltp >= p['tp']:
                pnl = (ltp - p['buy']) * p['qty']
                state["daily_pnl"] += pnl
                send_msg(f"🎯 *TARGET* {s.replace('.NS','')}\nExit:₹{ltp:.2f}\nPNL:₹{pnl:.0f}\nRR:1:2.7 Achieved")
                del state["pos"][s]
                continue

            # 5. 3-Day Exit
            p['day'] = p.get('day', 0) + 1
            if p['day'] >= 3 * (390 // INTERVAL_MIN): # 3 दिन
                pnl = (ltp - p['buy']) * p['qty']
                state["daily_pnl"] += pnl
                send_msg(f"⏰ *3-DAY EXIT* {s.replace('.NS','')}\n₹{ltp:.2f}\nPNL:₹{pnl:.0f}")
                del state["pos"][s]

            save_state(state)
        except: continue

# ===== REPORTS =====
def send_report():
    open_pnl = 0
    for s, p in state["pos"].items():
        try:
            time.sleep(1)
            ltp = float(yf.download(s, period="1d", interval="1m", progress=False)['Close'].iloc[-1])
            open_pnl += (ltp - p['buy']) * p['qty']
        except: pass
    send_msg(f"📊 *Daily Report*\nRealized:₹{state['daily_pnl']:.0f}\nOpen:₹{open_pnl:.0f}\nPositions:{len(state['pos'])}\nStable Profit Engine 🛡️💰")

def morning():
    send_msg(f"🚩 जय श्री राम, ललित जी!\nV40.0 FINAL STABLE PROFIT चालू है।\n✅ 250 Stocks ✅ EMA50 Trend ✅ Pullback\n✅ 1:2.7 RR ✅ ATR Trailing ✅ 3-Day Exit\n✅ Rate Limit Safe ✅ JSON Memory\nशुभ दिन! 🙏")

# ===== COMMANDS =====
@bot.message_handler(commands=['start','status','#status'])
def status(m):
    open_pnl = 0
    txt = f"*V40.0 LIVE*\nDaily:₹{state['daily_pnl']:.0f}\n"
    for s, p in state["pos"].items():
        try:
            ltp = float(yf.download(s, period="1d", interval="1m", progress=False)['Close'].iloc[-1])
            pnl = (ltp - p['buy']) * p['qty']
            open_pnl += pnl
            txt += f"{s.replace('.NS','')}: ₹{pnl:.0f}\n"
        except: pass
    txt += f"Open:₹{open_pnl:.0f}\nStable:🛡️"
    bot.reply_to(m, txt)

# ===== LOOP =====
def loop():
    morning()
    while True:
        now = datetime.now()
        if now.weekday() < 5 and 915 <= now.hour*100+now.minute <= 1530:
            scan()
            manage()
        if now.hour == 15 and now.minute == 35 and state.get("last_report")!= str(now.date()):
            send_report()
            state["last_report"] = str(now.date())
            save_state(state)
        time.sleep(INTERVAL_MIN * 60)

@app.route('/')
def home(): return "V40.0 FINAL Running"
@app.route('/health')
def health(): return "OK", 200

if __name__ == "__main__":
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000))), daemon=True).start()
    threading.Thread(target=loop, daemon=True).start()
    bot.infinity_polling()
