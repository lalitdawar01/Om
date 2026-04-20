import yfinance as yf
import pandas as pd
import sqlite3
import smtplib
import time
import requests
import logging
import warnings
import os
import ta
import pytz
from datetime import datetime, date, time as dt_time
from email.mime.text import MIMEText
from concurrent.futures import ThreadPoolExecutor

# बेसिक सेटिंग्स
warnings.filterwarnings('ignore')
logging.getLogger("yfinance").setLevel(logging.CRITICAL)
IST = pytz.timezone('Asia/Kolkata')

# ==========================================================
# ⚙️ V19.8 "संपूर्ण-कवच" - फाइनल प्रोडक्शन वर्जन
# ==========================================================
MY_EMAIL = "lalitdawar01@gmail.com"
APP_PASSWORD = os.environ.get('APP_PASSWORD')
DB_NAME = "brahmand_v19_sarv_kavach.db"

# === सेटिंग्स - फाइनल लॉक्ड ===
INITIAL_CAPITAL = 100000.0
TARGET_BOOK_1_TREND = 9.0
TARGET_BOOK_2_TREND = 18.0
TARGET_BOOK_1_SIDEWAYS = 5.0
TARGET_BOOK_2_SIDEWAYS = 10.0
BREAK_EVEN_LIMIT = 7.0
ATR_MULT_TREND = 2.0
ATR_MULT_SIDEWAYS = 1.2
MAX_PER_SECTOR = 2
VOL_MULTIPLIER = 3.0
BETA_LIMIT = 1.5
FIFTY_TWO_WEEK_BUFFER = 0.95

session = requests.Session()
session.headers.update({'User-Agent': 'Mozilla/5.0'})

def get_max_slots(current_wallet):
    if current_wallet < 200000: return 5
    elif current_wallet < 300000: return 8
    elif current_wallet < 500000: return 12
    else: return 15

def send_alert(subject, body):
    try:
        msg = MIMEText(body, 'html')
        msg['Subject'] = subject; msg['From'] = MY_EMAIL; msg['To'] = MY_EMAIL
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as server:
            server.login(MY_EMAIL, APP_PASSWORD)
            server.send_message(msg)
    except Exception as e: print(f"❌ ईमेल फेल: {e}")

def init_db():
    conn = sqlite3.connect(DB_NAME, timeout=20)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS wallet (id INTEGER PRIMARY KEY, balance REAL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS holdings (symbol TEXT PRIMARY KEY, buy_p REAL, peak REAL, qty INTEGER, b1 INTEGER, b2 INTEGER, sl REAL, sector TEXT, buy_date TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS trade_log (id INTEGER PRIMARY KEY AUTOINCREMENT, time TEXT, type TEXT, symbol TEXT, qty INTEGER, price REAL, pnl REAL, detail TEXT)''')
    if not c.execute("SELECT balance FROM wallet WHERE id=1").fetchone():
        c.execute("INSERT INTO wallet VALUES (1,?)", (INITIAL_CAPITAL,))
    conn.commit(); conn.close()

def log_trade(t_type, symbol, qty, price, pnl=0, detail=""):
    conn = sqlite3.connect(DB_NAME, timeout=20)
    conn.execute("INSERT INTO trade_log (time, type, symbol, qty, price, pnl, detail) VALUES (?,?,?,?,?,?,?)",
              (datetime.now(IST).strftime('%d-%b %H:%M'), t_type, symbol, qty, price, pnl, detail))
    conn.commit(); conn.close()

    if t_type in ["BUY", "SELL", "EXIT", "PART_BOOK", "PART_BOOK_2", "2:45_PANIC"]:
        emoji = "🟢" if t_type == "BUY" else "🔴"
        sub = f"{emoji} {t_type} अलर्ट: {symbol}"
        body = f"<b>ट्रेड:</b> {t_type}<br><b>शेयर:</b> {symbol}<br><b>क्वांटिटी:</b> {qty}<br>"
        body += f"<b>प्राइस:</b> ₹{price}<br><b>P&L:</b> ₹{pnl:.2f}<br><b>डिटेल:</b> {detail}<br>"
        body += f"<b>टाइम:</b> {datetime.now(IST).strftime('%d-%b %I:%M %p')}"
        send_alert(sub, body)

def get_market_regime():
    try:
        nifty = yf.Ticker("^NSEI", session=session).history(period="15d")
        if len(nifty) < 10: return "TRENDING"
        high_10d = nifty['High'].iloc[-10:].max()
        low_10d = nifty['Low'].iloc[-10:].min()
        range_pct = ((high_10d - low_10d) / low_10d) * 100
        return "SIDEWAYS" if range_pct < 2.5 else "TRENDING"
    except: return "TRENDING"

def send_daily_summary():
    try:
        conn = sqlite3.connect(DB_NAME, timeout=20)
        c = conn.cursor()
        today = datetime.now(IST).strftime('%d-%b')
        trades = c.execute("SELECT * FROM trade_log WHERE time LIKE?", (f'{today}%',)).fetchall()
        wallet = c.execute("SELECT balance FROM wallet WHERE id=1").fetchone()[0]
        holdings = c.execute("SELECT symbol, qty, buy_p FROM holdings").fetchall()
        conn.close()
        total_pnl = sum([t[6] for t in trades])
        body = f"<h3>📊 रोज का हिसाब - {today}</h3>"
        body += f"<b>💰 वॉलेट बैलेंस:</b> ₹{wallet:.2f}<br>"
        body += f"<b>📈 आज का कुल P&L:</b> ₹{total_pnl:.2f}<br><br>"
        body += "<b>🔄 आज के ट्रेड:</b><br>"
        if trades:
            for t in trades:
                body += f"➤ {t[2]} | {t[3]} | {t[4]} Qty @ ₹{t[5]} | P&L: ₹{t[6]:.2f} | {t[7]}<br>"
        else: body += "आज कोई ट्रेड नहीं हुआ।<br>"
        body += "<br><b>📦 मौजूदा होल्डिंग:</b><br>"
        if holdings:
            for h in holdings: body += f"🔹 {h[0]}: {h[1]} शेयर @ ₹{h[2]}<br>"
        else: body += "कोई होल्डिंग नहीं।<br>"
        send_alert(f"📈 रोज का हिसाब - {today}", body)
    except Exception as e: print(f"Daily Summary Error: {e}")

def perform_audit():
    try:
        conn = sqlite3.connect(DB_NAME, timeout=20)
        conn.execute("DELETE FROM trade_log WHERE time < datetime('now', '-30 days')")
        holdings = conn.execute("SELECT symbol, buy_p, qty FROM holdings").fetchall()
        wallet = conn.execute("SELECT balance FROM wallet WHERE id=1").fetchone()[0]
        report = f"📊 संडे ऑडिट रिपोर्ट - {datetime.now(IST).strftime('%d %b %Y')}<br>"
        report += f"💰 वॉलेट बैलेंस: ₹{wallet:.2f}<br><br>बची हुई होल्डिंग्स:<br>"
        if holdings:
            for h in holdings: report += f"🔹 {h[0]}: {h[2]} शेयर @ ₹{h[1]}<br>"
        else: report += "कोई होल्डिंग नहीं है।"
        send_alert("🧹 संडे ऑडिट रिपोर्ट", report)
        conn.commit(); conn.close()
    except Exception as e: print(f"Audit Error: {e}")

def get_nifty500():
    try:
        url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"
        df = pd.read_csv(url)
        return [s + ".NS" for s in df['Symbol'].tolist()]
    except:
        return ["RELIANCE.NS","TCS.NS","HDFCBANK.NS","INFY.NS","ICICIBANK.NS","HINDUNILVR.NS","ITC.NS","SBIN.NS","BHARTIARTL.NS","LT.NS"]

NIFTY_500_LIST = get_nifty500()

def process_stock(symbol, wallet, current_holdings, regime, MAX_SLOTS):
    try:
        df = yf.Ticker(symbol, session=session).history(period="1y")
        if len(df) < 50: return

        atr = ta.volatility.AverageTrueRange(df['High'], df['Low'], df['Close']).average_true_range().iloc[-1]
        if pd.isna(atr) or atr == 0: return

        last_p = round(df['Close'].iloc[-1], 2)
        high_52w = df['High'].max()

        if regime == "SIDEWAYS":
            TARGET_BOOK_1, TARGET_BOOK_2 = TARGET_BOOK_1_SIDEWAYS, TARGET_BOOK_2_SIDEWAYS
            ATR_MULT = ATR_MULT_SIDEWAYS
        else:
            TARGET_BOOK_1, TARGET_BOOK_2 = TARGET_BOOK_1_TREND, TARGET_BOOK_2_TREND
            ATR_MULT = ATR_MULT_TREND

        conn = sqlite3.connect(DB_NAME, timeout=20)
        cur = conn.cursor()
        holding = cur.execute("SELECT * FROM holdings WHERE symbol=?", (symbol,)).fetchone()

        if holding:
            _, b_p, peak, qty, b1, b2, sl, sector, b_date = holding
            new_peak = max(peak, last_p)
            p_pct = ((last_p - b_p)/b_p)*100
            temp_sl = max(sl, round(new_peak - (atr * ATR_MULT), 2))
            new_sl = max(temp_sl, b_p) if p_pct >= BREAK_EVEN_LIMIT else temp_sl

            if datetime.now(IST).hour == 14 and datetime.now(IST).minute >= 45:
                if last_p < df['Open'].iloc[-1] * 0.96:
                    s_qty = qty // 2
                    if s_qty > 0:
                        cur.execute("UPDATE holdings SET qty=? WHERE symbol=?", (qty-s_qty, symbol))
                        cur.execute("UPDATE wallet SET balance=balance+?", (s_qty*last_p*0.9975,))
                        log_trade("2:45_PANIC", symbol, s_qty, last_p, (last_p-b_p)*s_qty, "Risk Cut")

            elif p_pct >= TARGET_BOOK_2 and not b2:
                s_qty = qty // 2
                if s_qty > 0:
                    cur.execute("UPDATE holdings SET b2=1, qty=? WHERE symbol=?", (qty-s_qty, symbol))
                    cur.execute("UPDATE wallet SET balance=balance+?", (s_qty*last_p*0.9975,))
                    log_trade("PART_BOOK_2", symbol, s_qty, last_p, (last_p-b_p)*s_qty, f"{TARGET_BOOK_2}% Maha-Shagun | {regime}")

            elif p_pct >= TARGET_BOOK_1 and not b1:
                s_qty = qty // 4
                if s_qty > 0:
                    cur.execute("UPDATE holdings SET b1=1, qty=?, sl=? WHERE symbol=?", (qty-s_qty, b_p, symbol))
                    cur.execute("UPDATE wallet SET balance=balance+?", (s_qty*last_p*0.9975,))
                    log_trade("PART_BOOK", symbol, s_qty, last_p, 0, f"{TARGET_BOOK_1}% Shagun | {regime}")

            elif last_p <= new_sl:
                cur.execute("DELETE FROM holdings WHERE symbol=?", (symbol,))
                cur.execute("UPDATE wallet SET balance=balance+?", (qty*last_p*0.9975,))
                log_trade("EXIT", symbol, qty, last_p, (last_p-b_p)*qty, f"SL Hit @ {new_sl} | {regime}")
            else:
                cur.execute("UPDATE holdings SET peak=?, sl=? WHERE symbol=?", (new_peak, new_sl, symbol))

        elif current_holdings < MAX_SLOTS:
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
            rs = gain / loss
            rsi = 100 - (100 / (1 + rs)).iloc[-1]
            avg_vol = df['Volume'].iloc[-21:-1].mean()
            curr_vol = df['Volume'].iloc[-1]

            if (44 <= rsi <= 64 and
                last_p > df['Close'].rolling(50).mean().iloc[-1] and
                curr_vol >= (avg_vol * VOL_MULTIPLIER) and
                last_p < high_52w * FIFTY_TWO_WEEK_BUFFER):

                info = yf.Ticker(symbol, session=session).info
                sector = info.get('sector', 'Unknown')
                beta = info.get('beta', 1.0)
                s_count = cur.execute("SELECT COUNT(*) FROM holdings WHERE sector=?", (sector,)).fetchone()[0]

                if s_count < MAX_PER_SECTOR and beta <= BETA_LIMIT:
                    inv = wallet / (MAX_SLOTS - current_holdings)
                    qty = int(inv / last_p)
                    if qty > 0:
                        cur.execute("INSERT INTO holdings VALUES (?,?,?,?,0,0,?,?,?)",
                                   (symbol, last_p, last_p, qty, last_p-(atr*ATR_MULT), sector, date.today().isoformat()))
                        cur.execute("UPDATE wallet SET balance=balance-?", (qty*last_p,))
                        log_trade("BUY", symbol, qty, last_p, 0, f"V8-Vol:{curr_vol/avg_vol:.1f}x | {regime} | Slots:{MAX_SLOTS}")
        conn.commit(); conn.close()
    except Exception as e:
        print(f"Error in {symbol}: {e}")

def main_loop():
    init_db()

    now = datetime.now(IST)
    if 5 <= now.hour < 12: greet = "☀️ गुड मॉर्निंग ललित जी"
    elif 12 <= now.hour < 17: greet = "🌤️ गुड आफ्टरनून ललित जी"
    else: greet = "🌙 गुड इवनिंग ललित जी"

    send_alert(f"{greet} - संपूर्ण-कवच V19.8 एक्टिव",
               f"{greet}<br><br>V19.8 अब लाइव है। 16 कवच एक्टिव। VOL 3.0x। हर BUY/SELL पे मेल + रोज 3:35 PM हिसाब।<br><br>आज का मिशन: क्वालिटी ट्रेड।")

    audit_done = False
    summary_sent = False
    evening_greet_sent = False

    while True:
        now = datetime.now(IST)

        if now.weekday() == 6 and not audit_done:
            perform_audit(); audit_done = True
        elif now.weekday()!= 6: audit_done = False

        if now.weekday() < 5 and now.hour == 18 and now.minute == 0 and not evening_greet_sent:
            conn = sqlite3.connect(DB_NAME, timeout=20)
            wallet = conn.execute("SELECT balance FROM wallet WHERE id=1").fetchone()[0]
            h_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
            conn.close()
            send_alert("🌆 गुड इवनिंग अपडेट",
                       f"गुड इवनिंग ललित जी<br><br>मार्केट बंद हो गया।<br>💰 वॉलेट: ₹{wallet:.2f}<br>📦 होल्डिंग: {h_count} शेयर<br><br>3:35 PM का हिसाब मेल अलग से आया होगा। आराम करो।")
            evening_greet_sent = True
        elif now.hour!= 18:
            evening_greet_sent = False

        if now.weekday() < 5 and now.hour == 15 and now.minute >= 35 and not summary_sent:
            send_daily_summary()
            summary_sent = True
        elif now.hour!= 15:
            summary_sent = False

        if now.weekday() < 5 and (dt_time(9, 15) <= now.time() <= dt_time(15, 30)):
            regime = get_market_regime()
            conn = sqlite3.connect(DB_NAME, timeout=20)
            wallet = conn.execute("SELECT balance FROM wallet WHERE id=1").fetchone()[0]
            h_count = conn.execute("SELECT COUNT(*) FROM holdings").fetchone()[0]
            conn.close()
            MAX_SLOTS = get_max_slots(wallet)
            print(f"\r🔍 {regime} मोड | वॉलेट: ₹{wallet:.0f} | स्लॉट: {h_count}/{MAX_SLOTS} | {now.strftime('%I:%M %p')}", end="")
            with ThreadPoolExecutor(max_workers=3) as executor:
                for s in NIFTY_500_LIST:
                    executor.submit(process_stock, s, wallet, h_count, regime, MAX_SLOTS)
                    time.sleep(1.5)
            time.sleep(1200)
        else:
            time.sleep(300)

if __name__ == "__main__":
    main_loop()
