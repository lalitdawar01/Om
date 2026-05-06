import telebot, yfinance as yf, pandas as pd, numpy as np, json, os, threading, time
from datetime import datetime, timedelta
from flask import Flask
from pytz import timezone

# ===== CONFIG =====
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHAT_ID = int(os.environ.get("CHAT_ID"))
bot = telebot.TeleBot(BOT_TOKEN)
app = Flask(__name__)
IST = timezone('Asia/Kolkata')

STATE_FILE = "v40_state.json"
DAILY_LOSS_LIMIT = -1500
MAX_POS = 6
RISK_PER_TRADE = 25000
LEVERAGE = 5
INTERVAL_MIN = 15
YF_DELAY = 2 # 2 सेकंड गैप = रेट लिमिट 0%

nifty250 = ["RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS","HINDUNILVR.NS","ITC.NS","SBIN.NS","BHARTIARTL.NS","KOTAKBANK.NS","LT.NS","ASIANPAINT.NS","AXISBANK.NS","MARUTI.NS","SUNPHARMA.NS","TITAN.NS","ULTRACEMCO.NS","BAJFINANCE.NS","WIPRO.NS","NESTLEIND.NS","POWERGRID.NS","NTPC.NS","ONGC.NS","TECHM.NS","TATAMOTORS.NS","M&M.NS","HCLTECH.NS","COALINDIA.NS","ADANIENT.NS","ADANIPORTS.NS","JSWSTEEL.NS","HINDALCO.NS","GRASIM.NS","CIPLA.NS","DRREDDY.NS","EICHERMOT.NS","HEROMOTOCO.NS","BAJAJ-AUTO.NS","TATASTEEL.NS","INDUSINDBK.NS","BAJAJFINSV.NS","DIVISLAB.NS","BRITANNIA.NS","APOLLOHOSP.NS","UPL.NS","TATACONSUM.NS","BPCL.NS","SHREECEM.NS","PIDILITIND.NS","DABUR.NS","SBILIFE.NS","HDFCLIFE.NS","ICICIPRULI.NS","VEDL.NS","GODREJCP.NS","SIEMENS.NS","AMBUJACEM.NS","BANDHANBNK.NS","BANKBARODA.NS","BERGEPAINT.NS","BIOCON.NS","BOSCHLTD.NS","CADILAHC.NS","CANBK.NS","CHOLAFIN.NS","COLPAL.NS","CONCOR.NS","DLF.NS","GAIL.NS","HAVELLS.NS","HINDPETRO.NS","IDFCFIRSTB.NS","IGL.NS","INDIGO.NS","IOC.NS","JINDALSTEL.NS","LICHSGFIN.NS","LUPIN.NS","MARICO.NS","MUTHOOTFIN.NS","NAUKRI.NS","NMDC.NS","PAGEIND.NS","PETRONET.NS","PFC.NS","PGHH.NS","PNB.NS","RECLTD.NS","SAIL.NS","SRF.NS","TORNTPHARM.NS","TORNTPOWER.NS","TVSMOTOR.NS","UBL.NS","VOLTAS.NS","ZEEL.NS","ZYDUSLIFE.NS","ABB.NS","ACC.NS","ALKEM.NS","ASHOKLEY.NS","ASTRAL.NS","ATUL.NS","AUBANK.NS","BALKRISIND.NS","BEL.NS","BHEL.NS","DALBHARAT.NS","DEEPAKNTR.NS","ESCORTS.NS","FEDERALBNK.NS","GUJGASLTD.NS","HAL.NS","HONAUT.NS","IDBI.NS","IDEA.NS","INDIANB.NS","IRCTC.NS","JUBLFOOD.NS","LALPATHLAB.NS","LAURUSLABS.NS","M&MFIN.NS","MANAPPURAM.NS","MFSL.NS","MPHASIS.NS","MRF.NS","MOTHERSON.NS","OBEROIRLTY.NS","OFSS.NS","PERSISTENT.NS","PIIND.NS","POLYCAB.NS","RBLBANK.NS","SUNDARMFIN.NS","TATACHEM.NS","TATACOMM.NS","TATAELXSI.NS","TATAPOWER.NS","TRENT.NS","UNIONBANK.NS","VBL.NS","YESBANK.NS","ABFRL.NS","AIAENG.NS","APLAPOLLO.NS","AARTIIND.NS","ADANIGREEN.NS","AFFLE.NS","AJANTPHARM.NS","AMARAJABAT.NS","APOLLOTYRE.NS","AUROPHARMA.NS","BAJAJHLDNG.NS","BALRAMCHIN.NS","BATAINDIA.NS","BHARATFORG.NS","CAMS.NS","CANFINHOME.NS","CDSL.NS","CHAMBLFERT.NS","CROMPTON.NS","CUMMINSIND.NS","DIXON.NS","FORTIS.NS","GLENMARK.NS","GODREJPROP.NS","GRANULES.NS","HAPPSTMNDS.NS","INDHOTEL.NS","INDIAMART.NS","IEX.NS","IPCALAB.NS","JUBLPHARM.NS","KAJARIACER.NS","KPITTECH.NS","LICI.NS","LTTS.NS","METROPOLIS.NS","NH.NS","OIL.NS","PEL.NS","PHOENIXLTD.NS","POONAWALLA.NS","PRESTIGE.NS","RADICO.NS","RAJESHEXPO.NS","RAMCOCEM.NS","RELAXO.NS","SCHAEFFLER.NS","SHRIRAMFIN.NS","SOBHA.NS","SOLARINDS.NS","SUNTV.NS","SUPREMEIND.NS","SYNGENE.NS","TANLA.NS","TIMKEN.NS","TRIDENT.NS","TTKPRESTIG.NS","UJJIVANSFB.NS","VGUARD.NS","WHIRLPOOL.NS","ZENSARTECH.NS","ZOMATO.NS"]

# ===== STATE =====
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE) as f: return json.load(f)
    return {"pos": {}, "daily_pnl": 0, "date": str(datetime.now(IST).date()), "morning": False}

def save_state(s):
    with open(STATE_FILE, "w") as f: json.dump(s, f)

state = load_state()
if state["date"]!= str(datetime.now(IST).date()):
    state = {"pos": {}, "daily_pnl": 0, "date": str(datetime.now(IST).date()), "morning": False}
    save_state(state)

# ===== UTILS =====
def send_msg(t):
    try:
        bot.send_message(CHAT_ID, t, parse_mode='Markdown')
        print(f"TG Sent: {t[:50]}")
    except Exception as e:
        print(f"TG Error: {e}")

def get_data(symbol, period="60d", interval="1d"):
    for i in range(2):
        try:
            time.sleep(YF_DELAY)
            df = yf.download(symbol, period=period, interval=interval, progress=False, timeout=10)
            if not df.empty: return df
        except Exception as e:
            print(f"YF Error {symbol}: {e}")
            time.sleep(3)
    return pd.DataFrame()

def atr(df, n=14):
    h, l, c = df['High'], df['Low'], df['Close']
    tr = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    return tr.rolling(n).mean()

def ema(s, n): return s.ewm(span=n, adjust=False).mean()
def rsi(s, n=14):
    d = s.diff(); g = d.where(d > 0, 0).rolling(n).mean(); l = -d.where(d < 0, 0).rolling(n).mean()
    rs = g / l; return 100 - (100 / (1 + rs))

# ===== COMMANDS =====
@bot.message_handler(commands=['start','status','#status'])
def cmd_status(m):
    try:
        now = datetime.now(IST)
        open_pnl = 0
        txt = f"*V40.1 LIVE*\n📅 {now.strftime('%d-%m %H:%M IST')}\nDaily: ₹{state['daily_pnl']:.0f}\nPos: {len(state['pos'])}/{MAX_POS}\n"
        for s, p in state["pos"].items():
            df = get_data(s, "1d", "5m")
            if not df.empty:
                ltp = float(df['Close'].iloc[-1])
                pnl = (ltp - p['buy']) * p['qty']
                open_pnl += pnl
                txt += f"{s.replace('.NS','')}: ₹{pnl:.0f} @₹{ltp:.1f}\n"
        txt += f"Open: ₹{open_pnl:.0f}\n🛡️ Stable"
        bot.reply_to(m, txt)
    except Exception as e:
        bot.reply_to(m, f"Error: {e}")

@bot.message_handler(commands=['exit'])
def cmd_exit(m):
    state["pos"] = {}
    save_state(state)
    bot.reply_to(m, "All positions cleared")

# ===== SCAN =====
def scan():
    global state
    if state["daily_pnl"] <= DAILY_LOSS_LIMIT:
        send_msg(f"🛑 Daily Loss Limit {DAILY_LOSS_LIMIT} हिट। आज बंद।")
        return

    now = datetime.now(IST)
    scan_count = 0
    for s in nifty250:
        if s in state["pos"] or len(state["pos"]) >= MAX_POS: continue
        df = get_data(s, "60d", "1d")
        if len(df) < 55: continue
        scan_count += 1

        df['EMA50'] = ema(df['Close'], 50)
        df['ATR'] = atr(df)
        df['RSI'] = rsi(df['Close'])
        df['VolAvg'] = df['Volume'].rolling(20).mean()

        c, p = df.iloc[-1], df.iloc[-2]
        if pd.isna(c['EMA50']) or pd.isna(c['ATR']): continue

        # V40 FILTERS
        trend = c['Close'] > c['EMA50'] * 1.02
        pullback = p['Low'] <= p['EMA50'] and c['Close'] > p['High'] and c['Close'] > c['EMA50']
        vol_ok = c['Volume'] > c['VolAvg'] * 2
        rsi_ok = 55 <= c['RSI'] <= 65

        if not (trend and pullback and vol_ok and rsi_ok): continue

        entry = float(c['Close'])
        sl = entry - 4 * float(c['ATR'])
        if sl <= 0 or entry - sl < entry * 0.01: continue
        risk_per_share = entry - sl
        qty = max(1, int((RISK_PER_TRADE * LEVERAGE) / entry))
        tp = entry + 2.7 * risk_per_share

        state["pos"][s] = {
            "buy": entry, "sl": sl, "tp": tp, "qty": qty,
            "time": str(now), "half": False, "bars": 0
        }
        save_state(state)
        send_msg(f"✅ *BUY* {s.replace('.NS','')}\n₹{entry:.2f} Qty:{qty}\nSL:₹{sl:.2f} | TP:₹{tp:.2f}\nRR:1:2.7")

    if scan_count > 0:
        send_msg(f"🔍 Scan Done: {scan_count} stocks, {len(state['pos'])} pos")

# ===== MANAGE =====
def manage():
    global state
    for s in list(state["pos"].keys()):
        df = get_data(s, "5d", "5m")
        if df.empty: continue
        ltp = float(df['Close'].iloc[-1])
        p = state["pos"][s]
        p["bars"] += 1

        # 1. SL
        if ltp <= p['sl']:
            pnl = (ltp - p['buy']) * p['qty']
            state["daily_pnl"] += pnl
            send_msg(f"🛑 *SL* {s.replace('.NS','')}\nExit:₹{ltp:.2f}\nPNL:₹{pnl:.0f}")
            del state["pos"][s]; continue

        # 2. ATR Trailing
        df_d = get_data(s, "20d", "1d")
        if not df_d.empty:
            atr_val = float(atr(df_d).iloc[-1])
            new_sl = ltp - 2.5 * atr_val
            if new_sl > p['sl']: p['sl'] = new_sl

        # 3. Partial 6%
        if not p['half'] and ltp >= p['buy'] * 1.06:
            sell_qty = p['qty'] // 2
            pnl = (ltp - p['buy']) * sell_qty
            state["daily_pnl"] += pnl
            p['qty'] -= sell_qty
            p['half'] = True
            p['sl'] = p['buy']
            send_msg(f"💰 *PARTIAL 50%* {s.replace('.NS','')}\n₹{ltp:.2f}\nBooked:₹{pnl:.0f}\nSL→BE")

        # 4. TP
        if ltp >= p['tp']:
            pnl = (ltp - p['buy']) * p['qty']
            state["daily_pnl"] += pnl
            send_msg(f"🎯 *TARGET* {s.replace('.NS','')}\nExit:₹{ltp:.2f}\nPNL:₹{pnl:.0f}")
            del state["pos"][s]; continue

        # 5. 3-Day Exit: 78 bars = 3 दिन * 26 बार/दिन 15min
        if p['bars'] >= 78:
            pnl = (ltp - p['buy']) * p['qty']
            state["daily_pnl"] += pnl
            send_msg(f"⏰ *3-DAY EXIT* {s.replace('.NS','')}\n₹{ltp:.2f}\nPNL:₹{pnl:.0f}")
            del state["pos"][s]

        save_state(state)

# ===== REPORTS =====
def morning():
    now = datetime.now(IST)
    send_msg(f"🚩 जय श्री राम, ललित जी!\nV40.1 IST FINAL चालू\n📅 {now.strftime('%d %b %Y, %H:%M IST')}\n✅ 250 Stocks ✅ EMA50 ✅ Pullback\n✅ 1:2.7 RR ✅ Rate Safe ✅ IST Time\nशुभ दिन! 🙏")

def eod_report():
    open_pnl = 0
    for s, p in state["pos"].items():
        df = get_data(s, "1d", "1m")
        if not df.empty: open_pnl += (float(df['Close'].iloc[-1]) - p['buy']) * p['qty']
    send_msg(f"📊 *EOD Report*\nRealized: ₹{state['daily_pnl']:.0f}\nOpen: ₹{open_pnl:.0f}\nPositions: {len(state['pos'])}\n🛡️ Stable Profit")

# ===== LOOP =====
def main_loop():
    while True:
        now = datetime.now(IST)

        # 9:30 AM Morning Msg - 1 बार
        if now.hour == 9 and now.minute == 30 and not state.get("morning"):
            morning()
            state["morning"] = True
            save_state(state)

        # 9:31 AM Reset flag for next day
        if now.hour == 9 and now.minute == 31:
            state["morning"] = False
            save_state(state)

        # Market Hours: 9:15-15:30, Mon-Fri
        if now.weekday() < 5 and 915 <= now.hour*100+now.minute <= 1530:
            if now.minute % INTERVAL_MIN == 0:
                scan()
                manage()

        # 3:35 PM EOD Report
        if now.hour == 15 and now.minute == 35 and state.get("eod")!= str(now.date()):
            eod_report()
            state["eod"] = str(now.date())
            save_state(state)

        time.sleep(60)

@app.route('/')
def home(): return "V40.1 IST Final Running"
@app.route('/health')
def health(): return "OK", 200

if __name__ == "__main__":
    print("=== V40.1 Starting ===")
    threading.Thread(target=lambda: app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000))), daemon=True).start()
    threading.Thread(target=main_loop, daemon=True).start()
    print("Starting Telegram Polling...")
    bot.infinity_polling(skip_pending=True, timeout=20)
