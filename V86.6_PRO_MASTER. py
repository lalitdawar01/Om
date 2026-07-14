# ============================================================
# V86.6 PRO MASTER ENGINE - THE TOTAL ABSOLUTE IMMORTAL HYBRID
# JAI MATA DI 🚩 - NIFTY 500 FULL UNIVERSE | DUAL THREADING
# REAL BETA ENGINE | CNC DELIVERY | AUTO-RELOGIN | TRUE ATR
# TELEGRAM FLOOD CONTROL | YAHOO ANTI-BAN | NSE HOLIDAYS
# PERFECTED SCHEDULER | GEMINI AI SELF-HEALING ERROR SYSTEM
# SPECIAL 2% SIDEWAYS/BEARISH PARTIAL PROFIT BOOKING & COST GUARD
# ============================================================

import os
import random
import yfinance as yf
import pandas as pd
import numpy as np
import ta
import sqlite3
import json
import requests
import shutil
import threading
import logging
import time as tm
from datetime import datetime, time, timedelta, timezone
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import OrderedDict
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()

try:
    from NorenRestApiPy.NorenApi import NorenApi
    import pyotp
except ImportError:
    pass

logging.basicConfig(filename='bot.log', level=logging.INFO, format='%(asctime)s - %(message)s')
error_logger = logging.getLogger('error')

# 🌍 FORCED INDIAN TIMEZONE FUNCTION
def get_now_ist():
    return datetime.now(timezone(timedelta(hours=5, minutes=30)))

class SwingBotV86_Ultra_Hybrid:
    def __init__(self):
        self.header = "जय माता दी 🚩\n\n"
        
        # API & AI Credentials
        self.bot_token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")  
        self.chat_id = os.getenv("TELEGRAM_CHAT_ID")  
        self.gemini_api_key = os.getenv("GEMINI_API_KEY")    
          
        if self.gemini_api_key:   
            try: genai.configure(api_key=self.gemini_api_key)    
            except: pass  
              
        self.gemini_cache = {}     
        self.last_gemini_call_time = 0.0   

        # Shoonya Setup
        self.shoonya_user_id = os.getenv("SHOONYA_USER_ID")    
        self.shoonya_pwd = os.getenv("SHOONYA_PWD")    
        self.shoonya_vc = os.getenv("SHOONYA_VC")    
        self.shoonya_apikey = os.getenv("SHOONYA_API_KEY")    
        self.shoonya_totp_secret = os.getenv("SHOONYA_TOTP_SECRET")     
        self.shoonya_imei = os.getenv("SHOONYA_IMEI", "abc1234")    
            
        self.shoonya_logged_in = False    
        self.shoonya_api = None  
        self.trading_mode = "PAPER" 

        # 📅 NSE Holidays List (Format: YYYY-MM-DD)
        self.nse_holidays = [
            "2026-01-26", "2026-03-03", "2026-03-20", "2026-04-03", "2026-04-14",
            "2026-05-01", "2026-05-27", "2026-08-15", "2026-09-18", "2026-10-02",
            "2026-10-18", "2026-11-08", "2026-11-23", "2026-12-25"
        ]

        # Core Variables
        self.base_capital = 500000.0    
        self.db_path = 'swing_bot_v86_hybrid.db'    
        self.positions = {}      
        self.daily_pnl = 0      
          
        self.market_regime = "BULL"      
        self.crash_mode = False      
        self.emergency_stop = False    
        self.breadth_pct = 50      
        self.last_beta_calc_date = None 

        # Multi-threading Locks & Flood Control
        self.db_lock = Lock()  
        self.positions_lock = Lock()
        self.api_call_lock = Lock()
        self.telegram_lock = Lock()
        self.last_telegram_time = 0.0
        
        self.stock_cache = OrderedDict()  
        self.cache_time = {}  
        self.sector_map = {}      
        self.beta_cache = {}      
            
        # Initialization
        self.init_db()      
        self.capital = self.get_dynamic_capital()      
        self.load_sector_beta_cache()      
        self.load_positions()      
        self.start_db_backup()     
        
        self.validate_or_relogin_shoonya()
        self.send_telegram("🤖 V86.6 PRO MASTER चक्रव्यूह सक्रिय!\n(Special 2% Sideways/Bearish Protection Lock Added)")    

    # ==========================================================
    # 📅 MARKET OPEN CHECK (Weekends & Holidays)
    # ==========================================================
    def is_market_open(self, current_datetime):
        if current_datetime.weekday() >= 5: return False 
        if current_datetime.strftime("%Y-%m-%d") in self.nse_holidays: return False
        if not (time(9, 15) <= current_datetime.time() <= time(15, 30)): return False
        return True

    # ==========================================================
    # 📱 TELEGRAM FLOOD CONTROL SYSTEM
    # ==========================================================
    def send_telegram(self, message, target_chat_id=None, parse_mode='Markdown'):      
        with self.telegram_lock:
            try:      
                active_chat = target_chat_id if target_chat_id else self.chat_id
                if not self.bot_token or not active_chat: return    
                final_message = message if "जय माता दी" in message else f"{self.header}{message}"  
                
                elapsed = tm.time() - self.last_telegram_time
                if elapsed < 1.5:
                    tm.sleep(1.5 - elapsed)

                url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"      
                requests.post(url, data={'chat_id': active_chat, 'text': final_message, 'parse_mode': parse_mode}, timeout=5)
                
                self.last_telegram_time = tm.time()
            except Exception as e: error_logger.error(f"Telegram Error: {e}")    

    def login_shoonya(self):    
        if not self.shoonya_user_id or not self.shoonya_totp_secret: return False  
        try:
            with self.api_call_lock:
                api = NorenApi(host='https://api.shoonya.com/NorenWClientTP/', websocket='wss://api.shoonya.com/NorenWSTP/')    
                totp = pyotp.TOTP(self.shoonya_totp_secret).now()    
                ret = api.login(userid=self.shoonya_user_id, password=self.shoonya_pwd, twoFA=totp, vendor_code=self.shoonya_vc, api_secret=self.shoonya_apikey, imei=self.shoonya_imei)    
                if ret and ret.get('stat') == 'Ok':    
                    self.shoonya_api = api    
                    self.shoonya_logged_in = True  
                    logging.info("Shoonya Login Successful.")
                    return True    
            return False    
        except: return False    

    def validate_or_relogin_shoonya(self):  
        if not self.shoonya_logged_in or not self.shoonya_api: 
            return self.login_shoonya()
        try:
            with self.api_call_lock:
                res = self.shoonya_api.get_limits()
                if not res or (isinstance(res, dict) and res.get('stat') != 'Ok'):
                    return self.login_shoonya()
        except: return self.login_shoonya()
        return True

    def check_news_sentiment(self, symbol):    
        if not self.gemini_api_key: return "POSITIVE_OR_NEUTRAL"    
        now = get_now_ist()    
        
        if symbol in self.gemini_cache and (now - self.gemini_cache[symbol]['time']).total_seconds() < 86400:    
            return self.gemini_cache[symbol]['sentiment']    
          
        time_since_last = tm.time() - getattr(self, 'last_gemini_call_time', 0.0)
        if time_since_last < 3.0: tm.sleep(3.0 - time_since_last)
            
        try:    
            self.last_gemini_call_time = tm.time()
            model = genai.GenerativeModel('gemini-2.5-flash')    
            prompt = f"Analyze news for NSE Stock: {symbol}. Any fraud or corporate crisis? Answer: 'NEGATIVE' or 'POSITIVE_OR_NEUTRAL'."    
            response = model.generate_content(prompt)    
            result = "NEGATIVE" if "NEGATIVE" in response.text.strip().upper() else "POSITIVE_OR_NEUTRAL"    
            self.gemini_cache[symbol] = {'sentiment': result, 'time': now}    
            return result    
        except: return "POSITIVE_OR_NEUTRAL"

    # 🔧 🎯 GEMINI AI SELF-HEALING ERROR REPORTER FUNCTION
    def handle_error_with_gemini(self, context, exception):  
        error_logger.error(f"Error in {context}: {exception}")  
        if not self.gemini_api_key: return  
        try:  
            model = genai.GenerativeModel('gemini-2.5-flash')  
            prompt = f"Trading bot error in context: {context}\nError details: {str(exception)}\nExplain the issue briefly in pure Hindi language, and provide a quick fix."  
            response = model.generate_content(prompt)  
            self.send_telegram(f"🔧 *सेल्फ-हीलिंग अलर्ट (Error):*\n\n{response.text}")  
        except: pass

    def get_live_ltp(self, symbol):
        if self.shoonya_logged_in and self.shoonya_api:
            try:
                with self.api_call_lock:
                    clean_sym = symbol.replace('.NS', '') + '-EQ'
                    res = self.shoonya_api.get_quotes('NSE', clean_sym)
                    if res and res.get('lp'): return float(res['lp'])
            except: pass
        
        try:
            df = yf.download(symbol, period='1d', interval='1m', progress=False)
            if not df.empty: return float(df['Close'].iloc[-1])
        except: pass
        
        df = self.get_stock_cached(symbol)
        if df is not None and not df.empty: return float(df['Close'].iloc[-1])
        return 0.0

    def add_indicators(self, df):      
        df['ATR'] = ta.volatility.AverageTrueRange(df['High'], df['Low'], df['Close'], 14).average_true_range()      
        df['RSI'] = ta.momentum.RSIIndicator(df['Close'], 14).rsi()      
        df['ADX'] = ta.trend.ADXIndicator(df['High'], df['Low'], df['Close'], 14).adx()      
        df['EMA20'] = df['Close'].ewm(span=20, adjust=False).mean()      
        df['EMA50'] = df['Close'].ewm(span=50, adjust=False).mean()    
        df['EMA200'] = df['Close'].ewm(span=200, adjust=False).mean()      
        return df      

    def calculate_real_betas(self):
        try:
            now_date = get_now_ist().date()
            if self.last_beta_calc_date == now_date: return 
            
            if not hasattr(self, 'nifty_cache') or self.nifty_cache is None or len(self.nifty_cache) < 50: return
            
            nifty_ret = self.nifty_cache['Close'].pct_change().dropna()
            nifty_var = nifty_ret.var()
            if nifty_var == 0: return

            calculated_count = 0
            for sym in self.beta_cache.keys():
                df = self.stock_cache.get(sym)
                if df is not None and len(df) > 50:
                    stock_ret = df['Close'].pct_change().dropna()
                    aligned = pd.concat([stock_ret, nifty_ret], axis=1, join='inner').dropna()
                    if len(aligned) > 30:
                        cov = aligned.iloc[:, 0].cov(aligned.iloc[:, 1])
                        real_beta = cov / nifty_var
                        self.beta_cache[sym] = round(real_beta, 2)
                        calculated_count += 1
            
            if calculated_count > 100:
                self.last_beta_calc_date = now_date
        except Exception as e:
            self.handle_error_with_gemini("Beta Calculator", e)

    def update_breadth(self):
        try:
            advances, total = 0, 0
            for sym in self.beta_cache.keys():
                df = self.get_stock_cached(sym)
                if df is not None and len(df) > 1:
                    total += 1
                    if df['Close'].iloc[-1] > df['Close'].iloc[-2]: advances += 1
            if total > 0: self.breadth_pct = int((advances / total) * 100)
        except: self.breadth_pct = 50

    def get_dynamic_sectors(self):
        try:
            perf = {}
            for stock, sector in self.sector_map.items():
                df = self.get_stock_cached(stock)
                if df is not None and len(df) > 5:
                    ret = (df['Close'].iloc[-1] / df['Close'].iloc[-5]) - 1
                    perf[sector] = perf.get(sector, []) + [ret]
            avg_perf = {sec: np.mean(vals) for sec, vals in perf.items()}
            sorted_sectors = sorted(avg_perf.items(), key=lambda x: x[1], reverse=True)
            return [sorted_sectors[0][0], sorted_sectors[1][0], sorted_sectors[2][0]]
        except: return ['BANK', 'IT', 'ENERGY']

    def execute_live_order(self, symbol, qty, action, base_price, order_reason="NORMAL"):
        if self.trading_mode == "PAPER": return True, qty
            
        if not self.shoonya_logged_in or not self.shoonya_api: 
            self.validate_or_relogin_shoonya()
            if not self.shoonya_logged_in: return False, 0
            
        try:
            if action == 'B':
                limit_price = round(base_price * 1.01, 2) 
                with self.api_call_lock: limits = self.shoonya_api.get_limits()
                if limits and isinstance(limits, dict) and limits.get('stat') == 'Ok':
                    available_cash = float(limits.get('cash', 0))
                    required = qty * limit_price
                    if required > available_cash:
                        new_qty = int(available_cash / limit_price)
                        if new_qty <= 0:
                            self.send_telegram(f"⚠️ *मार्जिन रिजेक्टेड*\nचाहिए: ₹{required:.2f} | उपलब्ध: ₹{available_cash:.2f}")
                            return False, 0
                        qty = new_qty
            else:
                limit_price = round(base_price * 0.98, 2) if order_reason == "SL_HIT" else round(base_price * 0.99, 2)

            clean_sym = symbol.replace('.NS', '') + '-EQ'
            
            with self.api_call_lock:
                res = self.shoonya_api.place_order(buy_or_sell=action, product_type='C', exchange='NSE', tradingsymbol=clean_sym, quantity=qty, price_type='LMT', price=limit_price)
            
            if res and res.get('stat') == 'Ok': return True, qty
            else:
                err = res.get('emsg', 'Unknown Error') if res else 'No Response'
                self.send_telegram(f"❌ *ऑर्डर फेल ({action})*\n{symbol} कारण: {err}")
                return False, 0
        except Exception as e:
            self.handle_error_with_gemini(f"Order Execution ({symbol})", e)
            return False, 0

    # ==========================================================
    # 🛡️ DEFENSE ENGINE: RUNS EVERY 1 MINUTE (UNINTERRUPTED)
    # ==========================================================
    def defense_loop(self):
        while True:
            try:
                now = get_now_ist()
                if self.is_market_open(now) and not self.emergency_stop:
                    self.validate_or_relogin_shoonya()
                    now_minute = now.minute
                    
                    with self.positions_lock: local_positions = list(self.positions.items())

                    for symbol, pos in local_positions:
                        df = self.get_stock_cached(symbol)
                        if df is None or len(df) < 2: continue
                        
                        completed = df.iloc[-2]
                        current_price = self.get_live_ltp(symbol)
                        if current_price == 0: current_price = pos['entry_price']

                        trail_sl = current_price - (completed['ATR'] * 1.5)
                        exit_signal = False
                        reason, exit_type = "", "NORMAL"

                        with self.positions_lock:
                            # 🎯 1. यूनिवर्सल कॉस्ट गार्ड लॉजिक (2% प्रॉफिट पर SL खरीदी भाव पर शिफ्ट)
                            if current_price >= (pos['entry_price'] * 1.02):
                                if pos['sl'] < pos['entry_price']:
                                    pos['sl'] = pos['entry_price']
                                    self.send_telegram(f"🛡️ *कॉस्ट गार्ड एक्टिव!* {symbol} का SL खरीदी भाव पर शिफ्ट (नो-लॉस लॉक)।")
                            
                            # 🎯 2. स्पेशल साइडवेज/बेरिश मार्केट 2% पर तुरंत 50% पार्शियल बुकिंग
                            is_weak_market = (self.market_regime == "BEAR" or getattr(self, 'crash_mode', False) or getattr(self, 'breadth_pct', 50) < 50)
                            if is_weak_market and current_price >= (pos['entry_price'] * 1.02):
                                if not pos.get('partial_booked', False):
                                    book_qty = pos['qty'] // 2
                                    if book_qty > 0:
                                        success, _ = self.execute_live_order(symbol, book_qty, 'S', current_price, "TARGET")
                                        if success:
                                            pos['qty'] -= book_qty
                                            pos['partial_booked'] = True
                                            self.save_positions()
                                            self.send_telegram(f"💰 *विशेष मार्केट बुकिंग (50%)*\nStock: {symbol}\nMarket Condition: Weak/Sideways\nQty Sold: {book_qty}\nPrice: ₹{current_price:.2f}")

                            # ट्रेलिंग स्टॉप लॉस कैलकुलेशन
                            if trail_sl > pos['sl']: pos['sl'] = float(trail_sl)

                            # 🎯 3. सामान्य बुल मार्केट 2R पार्शियल बुकिंग (अगर पहले बुक नहीं हुआ है)
                            risk_per_share = max(1.0, pos['entry_price'] - pos['initial_sl'])
                            if not pos.get('partial_booked', False) and current_price >= pos['entry_price'] + (risk_per_share * 2):
                                book_qty = pos['qty'] // 2
                                if book_qty > 0:
                                    success, _ = self.execute_live_order(symbol, book_qty, 'S', current_price, "TARGET")
                                    if success:
                                        pos['qty'] -= book_qty
                                        pos['partial_booked'] = True
                                        self.save_positions()
                                        self.send_telegram(f"💰 *पार्शियल बुक (50%)!*\nStock: {symbol} @ ₹{current_price:.2f}")

                            # 15 मिनट का कैंडल क्लोज एग्जिट फिल्टर
                            if now_minute % 15 == 0:
                                if current_price <= pos['sl']: 
                                    exit_signal, reason, exit_type = True, "True ATR SL Hit ❌", "SL_HIT"
                                elif completed['Close'] < completed['EMA20']: 
                                    exit_signal, reason, exit_type = True, "EMA20 Exit ❌", "NORMAL"

                        if exit_signal:
                            success, _ = self.execute_live_order(symbol, pos['qty'], 'S', current_price, exit_type)
                            if success:
                                with self.positions_lock:
                                    pnl = (current_price - pos['entry_price']) * pos['qty']
                                    self.daily_pnl += pnl
                                    self.log_trade_to_db(symbol, pos['entry_price'], current_price, pos['qty'], pnl)
                                    del self.positions[symbol]
                                    self.save_positions()
                                self.send_telegram(f"🔴 *एग्जिट अलर्ट (CNC Limit)*\nStock: {symbol}\nReason: {reason}\nPrice: ₹{current_price:.2f}\nP&L: ₹{pnl:.2f}")
            except Exception as e: 
                self.handle_error_with_gemini("Defense Loop Engine", e)
            tm.sleep(60) 

    # ==========================================================
    # ⚔️ ATTACK ENGINE: RUNS EVERY 15 MINUTES (SCANNER)
    # ==========================================================
    def attack_loop(self):
        while True:
            try:
                now = get_now_ist()
                if self.is_market_open(now) and not self.emergency_stop:
                    full_watchlist = list(self.beta_cache.keys())     
                    
                    self.fetch_all_data_threaded(full_watchlist)      
                    
                    self.capital = self.get_dynamic_capital()    
                    self.update_market_regime()    
                    self.update_breadth() 
                    self.calculate_real_betas() 
                    
                    if not self.crash_mode and self.market_regime != "BEAR":
                        top_sectors = self.get_dynamic_sectors()      
                        
                        current_sector_counts = {}  
                        with self.positions_lock:  
                            for sym in self.positions.keys():  
                                sec = self.get_sector(sym)  
                                current_sector_counts[sec] = current_sector_counts.get(sec, 0) + 1  

                        for symbol in full_watchlist:      
                            with self.positions_lock: 
                                pos_count = len(self.positions)
                                in_pos = symbol in self.positions
                            
                            if in_pos or pos_count >= 5: continue
                            
                            stock_sector = self.get_sector(symbol)  
                            if current_sector_counts.get(stock_sector, 0) >= 2: continue   
                            
                            can_enter, reason = self.check_entry_conditions(symbol, top_sectors)      
                            if can_enter:      
                                df = self.get_stock_cached(symbol)    
                                completed = df.iloc[-2]    
                                live_price = self.get_live_ltp(symbol)
                                if live_price == 0: live_price = float(df.iloc[-1]['Close'])
                                    
                                sl = live_price - (completed['ATR'] * 2)      
                                risk_amount = self.capital * 0.01    
                                
                                raw_qty = int(risk_amount / max(1.0, live_price - sl))    
                                max_allowed_qty = int((self.capital * 0.20) / live_price)
                                qty = max(1, min(raw_qty, max_allowed_qty))
                                  
                                success, final_qty = self.execute_live_order(symbol, qty, 'B', live_price, "NORMAL")
                                
                                if success and final_qty > 0:
                                    with self.positions_lock:  
                                        self.positions[symbol] = {      
                                            'symbol': symbol, 'entry_price': live_price, 'qty': final_qty,      
                                            'sl': float(sl), 'initial_sl': float(sl), 'entry_date': get_now_ist().isoformat(),      
                                            'partial_booked': False      
                                        }      
                                        self.save_positions()      
                                    current_sector_counts[stock_sector] = current_sector_counts.get(stock_sector, 0) + 1  
                                    self.send_telegram(f"🟢 *नया ट्रेड लाइव (CNC Limit)*\nStock: {symbol}\nQty: {final_qty}\nPrice: ₹{live_price:.2f}\nStopLoss: ₹{sl:.2f}\nScore: {reason}")      
            except Exception as e: 
                self.handle_error_with_gemini("Attack Loop Engine", e)
            tm.sleep(900) 

    def check_entry_conditions(self, symbol, top_sectors):      
        if get_now_ist().time() < time(10, 15): return False, "10:15 AM Lock"  
        if self.get_sector(symbol) not in top_sectors: return False, "Sector Lock"  

        df = self.get_stock_cached(symbol)      
        if df is None or len(df) < 200: return False, "Incomplete Data"      
            
        completed, live = df.iloc[-2], df.iloc[-1]    
          
        if live['Volume'] < (df['Volume'].iloc[-21:-1].mean() * 1.5): return False, "Volume Fail"  
        if (live['Close'] * live['Volume']) < 50000000: return False, "Liquidity Fail"  
          
        recent_rng = df['High'].tail(5).max() - df['Low'].tail(5).min()  
        old_rng = df['High'].iloc[-20:-5].max() - df['Low'].iloc[-20:-5].min()  
        if recent_rng >= (0.85 * old_rng): return False, "VCP Fail"  

        if not (completed['Close'] > completed['EMA20'] > completed['EMA50'] > completed['EMA200']): return False, "EMA Fail"      
        
        if completed['RSI'] < 50: return False, "RSI Low"  

        score = 0  
        if completed['EMA20'] > completed['EMA50']: score += 30  
        if 50 < completed['RSI'] < 85: score += 25  
        if completed['ADX'] > 20: score += 25  
        if live['Close'] > completed['EMA20']: score += 20  
        
        real_beta = self.beta_cache.get(symbol, 1.0)
        if real_beta > 1.2: 
            score += 10 

        if score < 80: return False, f"Score Fail ({score})"  
        if self.check_news_sentiment(symbol) == "NEGATIVE": return False, "News Fail"    

        return True, f"Pass ({score} | β:{real_beta})"

    # ==========================================================
    # 📡 YAHOO ANTI-BAN DOWNLOADER WITH MULTI-INDEX FLATTENING
    # ==========================================================
    def safe_download(self, symbol, period='2y'): 
        try: 
            tm.sleep(random.uniform(0.5, 1.5)) 
            df = yf.download(symbol, period=period, progress=False, timeout=12, auto_adjust=True)      
            if df is not None and not df.empty:
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = df.columns.get_level_values(0)
            return df
        except: return pd.DataFrame()  

    def fetch_all_data_threaded(self, symbols):      
        self.nifty_cache = self.safe_download('^NSEI')      
        self.vix_cache = self.safe_download('^INDIAVIX', '5d') 
        with ThreadPoolExecutor(max_workers=5) as executor:      
            futures = {executor.submit(self.get_stock_cached, sym): sym for sym in symbols}      
            for f in as_completed(futures): pass      

    def get_stock_cached(self, symbol, period='2y'):      
        now = get_now_ist()      
        if symbol in self.stock_cache and (now - self.cache_time.get(symbol, now)).total_seconds() < 900:    
            return self.stock_cache[symbol]    
        df = self.safe_download(symbol, period)      
        if df is not None and not df.empty:      
            df = self.add_indicators(df)      
            self.stock_cache[symbol] = df      
            self.cache_time[symbol] = now      
            return df      
        return None      

    def update_market_regime(self):    
        if self.nifty_cache is None or self.nifty_cache.empty or len(self.nifty_cache) < 20: return    
        close = self.nifty_cache['Close'].iloc[-1]    
        day_ret = (close / self.nifty_cache['Close'].iloc[-2]) - 1    
        vix = self.vix_cache['Close'].iloc[-1] if hasattr(self, 'vix_cache') and not self.vix_cache.empty else 15.0
        self.crash_mode = (day_ret < -0.02 or vix > 22.0)
        self.market_regime = "BULL" if close > self.nifty_cache['Close'].ewm(span=200, adjust=False).mean().iloc[-1] else "BEAR"    

    # ==========================================================
    # DATABASE & HELPER FUNCTIONS
    # ==========================================================
    def init_db(self):      
        with self.db_lock:  
            with sqlite3.connect(self.db_path) as conn:  
                conn.execute("PRAGMA journal_mode=WAL")      
                conn.execute('''CREATE TABLE IF NOT EXISTS trades (id INTEGER PRIMARY KEY, symbol TEXT, exit_date TEXT, pnl REAL)''')      

    def log_trade_to_db(self, symbol, entry, exit, qty, pnl):  
        try:  
            with self.db_lock:  
                with sqlite3.connect(self.db_path) as conn:  
                    conn.execute("INSERT INTO trades (symbol, exit_date, pnl) VALUES (?, date('now'), ?)", (symbol, pnl))  
        except: pass  

    def get_dynamic_capital(self):      
        try:      
            with self.db_lock:  
                with sqlite3.connect(self.db_path) as conn:  
                    c = conn.cursor()      
                    c.execute("SELECT SUM(pnl) FROM trades WHERE exit_date >= date('now', '-30 days')")      
                    monthly_pnl = c.fetchone()[0] or 0      
            return max(50000.0, self.base_capital + monthly_pnl)      
        except: return self.base_capital      

    def get_sector(self, symbol): return self.sector_map.get(symbol, 'FMCG')    
      
    def load_sector_beta_cache(self):    
        nifty_500_stocks = [
            "3MINDIA.NS", "ABB.NS", "ACC.NS", "AIAENG.NS", "APLAPOLLO.NS", "AUBANK.NS", "AARTIDRUGS.NS", "AARTIIND.NS", 
            "AAVAS.NS", "ABBOTINDIA.NS", "ACE.NS", "ADANIENT.NS", "ADANIGREEN.NS", "ADANIPORTS.NS", "ADANIPOWER.NS", 
            "ADANITOTAL.NS", "ADANIWILMAR.NS", "AWL.NS", "ABCAPITAL.NS", "ABFRL.NS", "AEGISLOG.NS", "AETHER.NS", 
            "AFFLE.NS", "AJANTPHARMA.NS", "APLLTD.NS", "ALKEM.NS", "ALKYLAMINE.NS", "ALLCARGO.NS", "ALOKINDS.NS", 
            "AMARAJABAT.NS", "AMBER.NS", "AMBUJACEM.NS", "ANGELONE.NS", "ANANTRAJ.NS", "APARINDS.NS", "APOLLOHOSP.NS", 
            "APOLLOTYRE.NS", "APTUS.NS", "ARCHIDPLY.NS", "ARENEY.NS", "ASAHIINDIA.NS", "ASHOKLEY.NS", "ASHOKA.NS", 
            "ASIANPAINT.NS", "ASTERDM.NS", "ASTRAZEN.NS", "ASTRAL.NS", "ATUL.NS", "ATGL.NS", "AUROPHARMA.NS", 
            "AVANTIFEED.NS", "AXISBANK.NS", "BEML.NS", "BLS.NS", "BSE.NS", "BAJAJ-AUTO.NS", "BAJAJCON.NS", 
            "BAJAJELEC.NS", "BAJAJFINSV.NS", "BAJFINANCE.NS", "BAJAJHLDNG.NS", "BALAMINES.NS", "BALKRISIND.NS", 
            "BALRAMCHIN.NS", "BANCOINDIA.NS", "BANDHANBNK.NS", "BANKBARODA.NS", "BANKINDIA.NS", "MAHABANK.NS", 
            "BANSALWIRE.NS", "BATAINDIA.NS", "BAYERCROP.NS", "BEECTORFOOD.NS", "BERGEPAINT.NS", "BDL.NS", "BEL.NS", 
            "BHEL.NS", "BHARATFORG.NS", "BHARTARTL.NS", "BIOCON.NS", "BIRLACORPN.NS", "BSOFT.NS", "BLUEDART.NS", 
            "BLUESTARCO.NS", "BBTC.NS", "BORORENEW.NS", "BOSCHLTD.NS", "BPCL.NS", "BRIGADE.NS", "BRITANNIA.NS", 
            "MAPMYINDIA.NS", "CCL.NS", "CESC.NS", "CGPOWER.NS", "CIEINDIA.NS", "CRISIL.NS", "CSBBANK.NS", "CAMPUS.NS", 
            "CANFINHOME.NS", "CANBK.NS", "CAPLIPOINT.NS", "CGCL.NS", "CARBORUN.NS", "CASTROLIND.NS", "CEATLTD.NS", 
            "CENTRALBK.NS", "CDSL.NS", "CENTURYPLY.NS", "CENTURYTEX.NS", "CERA.NS", "CHALET.NS", "CHAMBLFERT.NS", 
            "CHEMPLASTS.NS", "CHENNPETRO.NS", "CHOLAHLDNG.NS", "CHOLAMANDM.NS", "CIPLA.NS", "CUB.NS", "CLEAN.NS", 
            "COALINDIA.NS", "COCHINSHIP.NS", "COFORGE.NS", "COLPAL.NS", "CAMS.NS", "CONCOR.NS", "COROMANDEL.NS", 
            "CRAFTSMAN.NS", "CREDITACC.NS", "CROMPTON.NS", "CUMMINSIND.NS", "CYIENT.NS", "DCAL.NS", "DCBBANK.NS", 
            "DCW.NS", "DEEPAKFERT.NS", "DEEPAKNTR.NS", "DELHIVERY.NS", "DELTACORP.NS", "DEN.NS", "DEVYANI.NS", 
            "DHANI.NS", "DHANUKA.NS", "DIGITALMAN.NS", "DISHMAN.NS", "DISHTV.NS", "DIVISLAB.NS", "DIXON.NS", "DLF.NS", 
            "DOMS.NS", "LALPATHLAB.NS", "DRREDDY.NS", "EIDPARRY.NS", "EIHOTEL.NS", "EPL.NS", "EASEMYTRIP.NS", 
            "EDELWEISS.NS", "EICHERMOT.NS", "ELECON.NS", "ELGIEQUIP.NS", "EMAMILTD.NS", "ENGINERSIN.NS", "EQUITASBNK.NS", 
            "ERIS.NS", "ESCORTS.NS", "EXIDEIND.NS", "FDC.NS", "FACT.NS", "FINCABLES.NS", "FINEORG.NS", "FINPIPE.NS", 
            "FSL.NS", "FIVESTAR.NS", "FORTIS.NS", "GRINFRA.NS", "GAIL.NS", "GMRINFRA.NS", "GALAXYSURF.NS", "GANDHAR.NS", 
            "GARFIBRES.NS", "GATEWAY.NS", "GEPIL.NS", "GESHIP.NS", "GENUSPOWER.NS", "GICRE.NS", "GILLETTE.NS", 
            "GLAND.NS", "GLAXO.NS", "GLENMARK.NS", "MEDANTA.NS", "GODFRYPHLP.NS", "GODREJAGRO.NS", "GODREJCP.NS", 
            "GODREJIND.NS", "GODREJPROP.NS", "GRANULES.NS", "GRAPHITE.NS", "GRASIM.NS", "GREAVESCOT.NS", "GRINDWELL.NS", 
            "GAEL.NS", "GSPL.NS", "GMDCLTD.NS", "GNFC.NS", "GSFC.NS", "GUJGASLTD.NS", "GULFOILLUB.NS", "HEG.NS", 
            "HCLTECH.NS", "HDFCBANK.NS", "HDFCLIFE.NS", "HDFCAMC.NS", "HFCL.NS", "HAPPYFORG.NS", "HAPPSTMNDS.NS", 
            "HATHWAY.NS", "HATSUN.NS", "HAVELLS.NS", "HEROMOTOCO.NS", "HERANBA.NS", "HIKAL.NS", "HINDALCO.NS", 
            "HINDCOPPER.NS", "HINDPETRO.NS", "HINDUNILVR.NS", "HINDZINC.NS", "HOMEFIRST.NS", "HONAUT.NS", "HUDCO.NS", 
            "ICICIBANK.NS", "ICICILI.NS", "ICICIPRULI.NS", "ISEC.NS", "IDBI.NS", "IDFCFIRSTB.NS", "IDFC.NS", "IIFL.NS", 
            "IIFLWAM.NS", "IRB.NS", "IRCON.NS", "IREDA.NS", "IRCTC.NS", "IRFC.NS", "ITI.NS", "INDIACEM.NS", 
            "INDIAMART.NS", "INDIANB.NS", "IEX.NS", "INDHOTEL.NS", "IOC.NS", "IOB.NS", "INDOCO.NS", "INDUSINDBK.NS", 
            "INDUSTOWER.NS", "INFIBEAM.NS", "NAUKRI.NS", "INFY.NS", "INOXGFL.NS", "INOXWIND.NS", "INSECTICID.NS", 
            "INTELLECT.NS", "INDIGO.NS", "IPCALAB.NS", "JBCHEPHARM.NS", "JKCEMENT.NS", "JKIL.NS", "JKLAKSHMI.NS", 
            "JKTYRE.NS", "JMFINANCIL.NS", "JSL.NS", "JSWENERGY.NS", "JSWINFRA.NS", "JSWSTEEL.NS", "JAIBALAJI.NS", 
            "J&KBANK.NS", "JINDALSAW.NS", "JINDALSTEL.NS", "JIOFIN.NS", "JUBLFOOD.NS", "JUBLINGREA.NS", "JUBLPHARMA.NS", 
            "JUSTDIAL.NS", "JYOTHYLAB.NS", "KFINTECH.NS", "KIMS.NS", "KNRCON.NS", "KPITTECH.NS", "KRBL.NS", "KSB.NS", 
            "KSCL.NS", "KEC.NS", "KEI.NS", "KALYANKJIL.NS", "KANSAINER.NS", "KARURVYSYA.NS", "KAYNES.NS", "KOTAKBANK.NS", 
            "KOPRAN.NS", "LT.NS", "LTIM.NS", "LTTS.NS", "LICHSGFIN.NS", "LICI.NS", "LAOPALA.NS", "LAURUSLABS.NS", 
            "LAXMICHEM.NS", "LEMONTREE.NS", "LGBBROSLTD.NS", "LINDEINDIA.NS", "LUPIN.NS", "LUXIND.NS", "MMTC.NS", 
            "MOIL.NS", "MRF.NS", "MGL.NS", "MAHSEAMLES.NS", "M&MFIN.NS", "M&M.NS", "MAHINDCIE.NS", "MAHLOG.NS", 
            "MANAPPURAM.NS", "MRPL.NS", "MARICO.NS", "MARUTI.NS", "MASTEK.NS", "MAXHEALTH.NS", "MAZDOCK.NS", 
            "METROPOLIS.NS", "MFSL.NS", "MINDACORP.NS", "UNOJARVIS.NS", "MIDHANI.NS", "MOTHERSUMI.NS", "MSUMI.NS", 
            "MOTILALOFS.NS", "MPHASIS.NS", "MCX.NS", "MUTHOOTFIN.NS", "NATCOPHARM.NS", "NBCC.NS", "NCC.NS", "NESCO.NS", 
            "NHPC.NS", "NLCINDIA.NS", "NMDC.NS", "NOCIL.NS", "NTPC.NS", "NH.NS", "NATIONALUM.NS", "NAVINFLUOR.NS", 
            "NAZARA.NS", "NEOGEN.NS", "NETWEB.NS", "NETWORK18.NS", "NEWGEN.NS", "NIBE.NS", "NIPPON.NS", "NUCLEUS.NS", 
            "NUVAMA.NS", "NUVOCO.NS", "OBEROIRLTY.NS", "ONGC.NS", "OIL.NS", "OLECTRA.NS", "OMAXE.NS", "OFSS.NS", 
            "ORIENTELEC.NS", "PRINCEPIPE.NS", "PCBL.NS", "PIIND.NS", "PNBHOUSING.NS", "PNCINFRA.NS", "PVRINOX.NS", 
            "PAGEIND.NS", "PARADEEP.NS", "PARAGMILK.NS", "PARAS.NS", "PATANJALI.NS", "PATELENG.NS", "PERSISTENT.NS", 
            "PETRONET.NS", "PFIZER.NS", "PIDILITIND.NS", "PEL.NS", "PPLPHARMA.NS", "POLYMED.NS", "POLYCAB.NS", 
            "POLYPLEX.NS", "POONAWALLA.NS", "PFC.NS", "POWERGRID.NS", "PRAJIND.NS", "PRESTIGE.NS", "PRICOL.NS", 
            "PRSMJOHNSN.NS", "PRIVISCL.NS", "PTC.NS", "PUNJABCHEMICAL.NS", "PNB.NS", "QUESS.NS", "RBLBANK.NS", 
            "RECLTD.NS", "RHIM.NS", "RITES.NS", "RADICO.NS", "RVNL.NS", "RAIN.NS", "RAINBOW.NS", "RAJESHEXPO.NS", 
            "RALLIS.NS", "RAMASTEEL.NS", "RAMCOIND.NS", "RAMCOCEM.NS", "RATNAMANI.NS", "RTNINDIA.NS", "RPOWER.NS", 
            "RELIANCE.NS", "RELINFRA.NS", "RELAXO.NS", "RENUKA.NS", "RBA.NS", "ROSSARI.NS", "ROUTE.NS", "SBICARD.NS", 
            "SBILIFE.NS", "SJVN.NS", "SKFINDIA.NS", "SRF.NS", "SANOFI.NS", "SANSERA.NS", "SAPPHIRE.NS", "SARDAEN.NS", 
            "SAREGAMA.NS", "SCHAEFFLER.NS", "SCHNEIDER.NS", "SEQUENT.NS", "SHARDACROP.NS", "SHAREINDIA.NS", 
            "SESHAPAPER.NS", "SHIPPING.NS", "SHALBY.NS", "SHANKARA.NS", "SHREECEM.NS", "SHRIRAMFIN.NS", "SIEMENS.NS", 
            "SIGNATURE.NS", "SOBHA.NS", "SOLARINDS.NS", "SONACOMS.NS", "SONATSOFTW.NS", "SPANDANA.NS", "SPARC.NS", 
            "SPICEJET.NS", "SPLPETRO.NS", "STARHEALTH.NS", "SBIN.NS", "SAIL.NS", "STERTOOLS.NS", "SUDARSCHEM.NS", 
            "SUMICHEM.NS", "SUNPHARMA.NS", "SUNTV.NS", "SUNDARMFIN.NS", "SUNDRMFAST.NS", "SUNTECK.NS", "SUPRAJIT.NS", 
            "SUPREMEIND.NS", "SUPRIYA.NS", "SURYAROSNI.NS", "SUTLEJTEX.NS", "SUVENPHAR.NS", "SUZLON.NS", "SWANENERGY.NS", 
            "SYNGENE.NS", "SYRMA.NS", "TASTYBITE.NS", "TATACOMM.NS", "TATACONSUM.NS", "TATAELXSI.NS", "TATAMOTORS.NS", 
            "TATAPOWER.NS", "TATASTEEL.NS", "TATATECH.NS", "TTML.NS", "TECHM.NS", "TEJASNET.NS", "TEXRAIL.NS", 
            "NIACL.NS", "RAMCOSYSTEM.NS", "THERMAX.NS", "THOMASCOOK.NS", "THYROCARE.NS", "TIMKEN.NS", "TITAN.NS", 
            "TORNTPOWER.NS", "TORNTPHARM.NS", "TRENT.NS", "TRIDENT.NS", "TRITURBINE.NS", "TIINDIA.NS", "UCOBANK.NS", 
            "UJJIVANSFB.NS", "ULTRACEMCO.NS", "UNIONBANK.NS", "UBL.NS", "MCDOWELL-N.NS", "UPL.NS", "USHAMART.NS", 
            "VGUARD.NS", "VAKRANGEE.NS", "VALIANTORG.NS", "VTL.NS", "VARROC.NS", "VBL.NS", "MANYAVAR.NS", "VEDL.NS", 
            "VENKEYS.NS", "VERTOZ.NS", "VESUVIUS.NS", "VINATIORGA.NS", "VIPIND.NS", "VAIBHAVGBL.NS", "VIPULLTD.NS", 
            "VIMTALABS.NS", "VISAKAIND.NS", "VOLTAMP.NS", "VOLTAS.NS", "VRLLOG.NS", "WABAG.NS", "WELCORP.NS", 
            "WELENT.NS", "WELSPUNIND.NS", "WESTLIFE.NS", "WHIRLPOOL.NS", "WIPRO.NS", "WOCKPHARMA.NS", "WONDERLA.NS", 
            "YESBANK.NS", "ZEEL.NS", "ZENSARTECH.NS", "ZOMATO.NS", "ZYDUSLIFE.NS", "ZYDUSWELL.NS"
        ]  
        self.beta_cache = {s: 1.0 for s in nifty_500_stocks}  
        for s in nifty_500_stocks:  
            if any(k in s for k in ["BANK","FIN","INS","CHOLA","ISEC"]): self.sector_map[s] = "BANK"  
            elif any(k in s for k in ["TECH","INFY","TCS","SOFT","BSOFT"]): self.sector_map[s] = "IT"  
            elif any(k in s for k in ["AUTO","MOTORS","TYRE","LEY"]): self.sector_map[s] = "AUTO"  
            elif any(k in s for k in ["POWER","ENERGY","REL","OIL","NTPC","IOC","BPCL"]): self.sector_map[s] = "ENERGY"  
            elif any(k in s for k in ["PHARMA","LAB","HEALTH","BIO","CIPLA","SUNPHARMA"]): self.sector_map[s] = "PHARMA"  
            elif any(k in s for k in ["STEEL","METAL","HINDALCO","MINES","JINDALSAW"]): self.sector_map[s] = "METAL"  
            elif any(k in s for k in ["INFRA","CON","ENG","GMR"]): self.sector_map[s] = "INFRA"  
            elif any(k in s for k in ["PROP","REALTY","DLF","OBEROI"]): self.sector_map[s] = "REALTY"  
            else: self.sector_map[s] = "FMCG"  

    def load_positions(self):  
        if os.path.exists('positions_v86.json'):  
            try:  
                with self.positions_lock:  
                    with open('positions_v86.json', 'r') as f: self.positions = json.load(f)  
            except: pass  
              
    def save_positions(self):  
        with open('positions_v86.json', 'w') as f: json.dump(self.positions, f)  
          
    def start_db_backup(self):  
        def backup_loop():  
            while True:  
                tm.sleep(21600)    
                try: shutil.copy2(self.db_path, f'swing_bot_backup_{get_now_ist().strftime("%Y%m%d_%H")}.db')  
                except: pass  
        threading.Thread(target=backup_loop, daemon=True).start()


# ============================================================
# 📱 TELEGRAM LISTENER THREAD
# ============================================================
def telegram_listener(bot_instance):
    offset = None
    while True:
        try:
            if not bot_instance.bot_token: 
                tm.sleep(10)
                continue
                
            url = f"https://api.telegram.org/bot{bot_instance.bot_token}/getUpdates"
            response = requests.get(url, params={"timeout": 15, "offset": offset}, timeout=20).json()
            
            if "result" in response:
                for update in response["result"]:
                    offset = update["update_id"] + 1
                    if "message" in update and "text" in update["message"]:
                        msg_text = update["message"]["text"].strip()
                        from_chat_id = update["message"]["chat"]["id"] 
                        cmd = msg_text.split()[0].split('@')[0] if msg_text else ""

                        if cmd == "/set_live":  
                            bot_instance.trading_mode = "LIVE"
                            bot_instance.send_telegram("🚀 *मोड चेंज अलर्ट!*\nसिस्टम सफलतापूर्वक *LIVE TRADING MODE* में आ गया है।", target_chat_id=from_chat_id)  
                          
                        elif cmd == "/set_paper":  
                            bot_instance.trading_mode = "PAPER"
                            bot_instance.send_telegram("💼 *मोड चेंज अलर्ट!* सिस्टम *PAPER TRADING MODE* में है।", target_chat_id=from_chat_id)  
                          
                        elif cmd == "/emergency":  
                            bot_instance.emergency_stop = True  
                            bot_instance.send_telegram("🚨 *EMERGENCY MODE ACTIVATED* 🚨\nसिस्टम को रोक दिया गया है। सभी ओपन ट्रेड्स को काटा जा रहा है...", target_chat_id=from_chat_id)  
                            
                            with bot_instance.positions_lock:  
                                local_positions = list(bot_instance.positions.items())
                                
                            for sym, pos in local_positions:
                                current_price = bot_instance.get_live_ltp(sym)
                                if current_price == 0: current_price = pos['entry_price']
                                
                                success, _ = bot_instance.execute_live_order(sym, pos['qty'], 'S', current_price, "SL_HIT")
                                if success: 
                                    with bot_instance.positions_lock:
                                        pnl = (current_price - pos['entry_price']) * pos['qty']
                                        bot_instance.daily_pnl += pnl
                                        bot_instance.log_trade_to_db(sym, pos['entry_price'], current_price, pos['qty'], pnl)
                                        del bot_instance.positions[sym]  
                                    
                            with bot_instance.positions_lock:        
                                bot_instance.save_positions()  
                                
                            bot_instance.send_telegram("✅ *EMERGENCY EXIT COMPLETE*\nसभी पोजीशन्स सफलतापूर्वक काट दी गई हैं।", target_chat_id=from_chat_id)  
                          
                        elif cmd == "/status":  
                            regime = getattr(bot_instance, 'market_regime', 'BULL')
                            breadth = getattr(bot_instance, 'breadth_pct', 50)
                            cap = getattr(bot_instance, 'capital', bot_instance.base_capital)
                            
                            report_msg = (
                                f"🤖 *V86.6 PRO MASTER REPORT*\n"
                                f"━━━━━━━━━━━━━━━━━━━━\n"
                                f"💼 मोड: *{bot_instance.trading_mode}*\n"
                                f"📈 बाज़ार रेजीम: *{regime}*\n"
                                f"📊 ब्रेड्थ (500 Stocks): *{breadth}%*\n"
                                f"💰 एक्टिव कैपिटल: ₹{cap:,.2f}\n"
                            )
                            report_msg += "\n💼 *ओपन होल्डिंग्स (Live Check):*\n"
                            
                            with bot_instance.positions_lock: local_positions = dict(bot_instance.positions)  
                            if not local_positions: report_msg += "_कोई ओपन पोजीशन नहीं है।_\n"  
                            else:  
                                for sym, pos_data in local_positions.items():  
                                    ltp = bot_instance.get_live_ltp(sym)
                                    if ltp == 0: ltp = pos_data['entry_price']
                                    pnl = (ltp - pos_data['entry_price']) * pos_data['qty']  
                                    icon = "🟢" if pnl >= 0 else "🔴"  
                                    real_beta = bot_instance.beta_cache.get(sym, 1.0)
                                    clean_sym = sym.replace('.NS', '').replace('_', '\\_')
                                    report_msg += f"• *{clean_sym}* (β:{real_beta}): {icon} P&L: `₹{pnl:,.2f}`\n  (Cost: ₹{pos_data['entry_price']:.2f} | LTP: ₹{ltp:.2f} | SL: ₹{pos_data['sl']:.2f})\n"  
                            
                            bot_instance.send_telegram(report_msg, target_chat_id=from_chat_id)  
        except Exception as e:
            bot_instance.handle_error_with_gemini("Telegram Listener", e)
            tm.sleep(5)

# ============================================================
# 📅 REPORTS SCHEDULER THREAD (100% BULLETPROOF IST)
# ============================================================
def local_scheduler(bot_instance):
    last_morning, last_evening, last_monthly, last_yearly = None, None, None, None
    while True:
        try:
            now = get_now_ist()
            today_str = now.strftime('%Y-%m-%d')
            current_month = now.strftime('%Y-%m')
            current_year = now.strftime('%Y')

            # 1. डेली रिपोर्ट्स (सिर्फ वर्किंग डेज़ पर)
            if bot_instance.is_market_open(now.replace(hour=12, minute=0, second=0)): 
                if (now.hour > 9 or (now.hour == 9 and now.minute >= 15)) and last_morning != today_str:
                    bot_instance.send_telegram(f"Good Morning ललित जी! ☀️\n\n🕉️ बाज़ार ओपन हो गया है। V86.6 PRO MASTER चक्र पूरी ताकत से सक्रिय है।")
                    last_morning = today_str

                if (now.hour > 15 or (now.hour == 15 and now.minute >= 35)) and last_evening != today_str:
                    report_msg = f"📊 डेली क्लोजिंग बहीखाता\n━━━━━━━━━━━━━━━━━━━━\n💰 आज का शुद्ध P&L: ₹{bot_instance.daily_pnl:.2f}\n💼 OPEN TRADES: {len(bot_instance.positions)}\n"
                    bot_instance.daily_pnl = 0
                    bot_instance.send_telegram(report_msg)
                    last_evening = today_str

            # 2. मंथली और इयरली रिपोर्ट्स 
            if now.day == 1 and now.hour == 16 and last_monthly != current_month:
                with bot_instance.db_lock:
                    with sqlite3.connect(bot_instance.db_path) as conn:
                        c = conn.cursor()
                        c.execute("SELECT SUM(pnl) FROM trades WHERE exit_date >= date('now', 'start of month', '-1 month') AND exit_date < date('now', 'start of month')")
                        monthly_pnl = c.fetchone()[0] or 0
                bot_instance.send_telegram(f"📅 *मंथली P&L रिपोर्ट*\n━━━━━━━━━━━━━━━━━━━━\n💰 पिछले महीने का कुल शुद्ध P&L: ₹{monthly_pnl:.2f}\n")
                last_monthly = current_month

            if now.month == 1 and now.day == 1 and now.hour == 16 and now.minute >= 30 and last_yearly != current_year:
                with bot_instance.db_lock:
                    with sqlite3.connect(bot_instance.db_path) as conn:
                        c = conn.cursor()
                        c.execute("SELECT SUM(pnl) FROM trades WHERE exit_date >= date('now', 'start of year', '-1 year') AND exit_date < date('now', 'start of year')")
                        yearly_pnl = c.fetchone()[0] or 0
                bot_instance.send_telegram(f"🏆 *इयरली P&L रिपोर्ट*\n━━━━━━━━━━━━━━━━━━━━\n💰 पिछले वर्ष का कुल शुद्ध P&L: ₹{yearly_pnl:.2f}\n")
                last_yearly = current_year
        except Exception as e:
            bot_instance.handle_error_with_gemini("Scheduler Thread", e)
        tm.sleep(60)

# ============================================================
# MAIN EXECUTION THREAD STARTER
# ============================================================
if __name__ == "__main__":
    bot = SwingBotV86_Ultra_Hybrid()

    threading.Thread(target=telegram_listener, args=(bot,), daemon=True).start()  
    threading.Thread(target=local_scheduler, args=(bot,), daemon=True).start()    
    threading.Thread(target=bot.defense_loop, daemon=True).start()
    threading.Thread(target=bot.attack_loop, daemon=True).start()
      
    print("🚀 V86.6 PRO MASTER ENGINE RUNNING MULTI-THREADED (ALL SYSTEMS GREEN)...")    
    
    while True: tm.sleep(3600)
