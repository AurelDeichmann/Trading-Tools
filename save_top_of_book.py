import pandas as pd
from datetime import datetime, timedelta
import pytz
import numpy as np
import time
from py_vollib_vectorized import vectorized_implied_volatility as viv
from volatility_index import BVIX
import logging

class SaveBBO:
    
    def __init__(self, feed, db_connection):
        self.feed = feed
        self.logger = logging.getLogger("deribit")
        self.counter = 0
        self.c = db_connection["c"]
        self.conn = db_connection["conn"]
        self.engine = db_connection["engine"]
        
        self.save_interval = 60
        self.schema = "obot"
        self.table = "derbbo"
        
        self.prepare_db()
        
        self.columns = ["timestamp", "contract", "underlying", 
                        "expiration", "strike", "typ", 
                        "bid", "bid_size", "bid_iv", 
                        "ask", "ask_size", "ask_iv"]
        
        self.took_snapshot = False
        self.months = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", 
                       "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]
        
        self.shutdown= False
        self.bvix = BVIX(db_connection)
        
        
    def prepare_db(self):
        self.c.execute("CREATE SCHEMA IF NOT EXISTS {}".format(self.schema))
        self.conn.commit()
        self.c.execute("CREATE TABLE IF NOT EXISTS {}.{}("
                        "timestamp TIMESTAMPTZ, btcusd_price INTEGER, "
                        "ttmyears NUMERIC, expiration TIMESTAMPTZ, "
                        "strike INTEGER, typ TEXT, oi NUMERIC, bid NUMERIC, "
                        "bid_usd NUMERIC, bid_size NUMERIC, bid_iv NUMERIC, "
                        "ask NUMERIC, ask_usd NUMERIC, ask_size NUMERIC, "
                        "ask_iv NUMERIC)".format(self.schema, self.table))
        self.conn.commit()
        
    
    def schedule_snapshot(self):
        while True:
            try:
                now = datetime.now()
                if (now.second % self.save_interval == 0 
                    and len(self.feed.fetch_local_ob()) > 0 
                    and not self.shutdown):
                    
                    if not self.took_snapshot:
                        ts = datetime.now(pytz.UTC)
                        self.took_snapshot = True
                        self.take_snapshot(ts)
                        
                    else:
                        time.sleep(1.1)
                        
                elif self.shutdown:
                    break
                
                else:
                    time.sleep(0.2)
                    self.took_snapshot = False
            
            except Exception:
                pass
    
    def take_snapshot(self, ts):
        
        self.ob = self.feed.fetch_local_ob().copy()
        self.oi = self.feed.fetch_local_oi().copy()
        ts = ts.replace(microsecond=0)
        contracts = list(self.ob.keys())
        data = []
        
        for i in range(len(contracts)):
            
            bids = list(self.ob[contracts[i]]["bids"].keys())
            if len(bids) > 0:
                best_bid = max(bids)
                best_bid_size = self.ob[contracts[i]]["bids"][best_bid]
            else:
                best_bid = np.nan
                best_bid_size = np.nan
            
            asks = list(self.ob[contracts[i]]["asks"].keys())
            if len(asks) > 0:
                best_ask = min(asks)
                best_ask_size = self.ob[contracts[i]]["asks"][best_ask]
            else:
                best_ask = np.nan
                best_ask_size = np.nan
            
            oi = self.oi[contracts[i]]
            
            data.append([ts, contracts[i], best_bid, best_bid_size, 
                         best_ask, best_ask_size, oi])
        
        df = pd.DataFrame(data, columns=["timestamp", "contract", "bid", "bid_size", "ask", "ask_size", "oi"])
        self.options_calculations(df)
        
        
    def options_calculations(self, df):
        
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        contract_list = df["contract"].tolist()
        underlying = []
        expirations_unformatted = []
        strikes = []
        types = []
        for i in contract_list:
            underlying.append(i[:3])
            types.append(i[-1])
            first = i.find("-")
            second = i.find("-", first+1)
            third = i.find("-", second+1)
            expirations_unformatted.append(i[first+1:second])
            strikes.append(i[second+1:third])
        df["underlying"] = underlying
        df["expiration"] = expirations_unformatted
        df["strike"] = strikes
        df["typ"] = types

        df["strike"] = df["strike"].astype(int)
        
        df["year"] = "20" + df["expiration"].str.slice(-2)
        df["month_string"] = df["expiration"].str.slice(-5, -2)
        df["day"] = df["expiration"].str.slice(0, -5)
        df.drop("expiration", axis=1, inplace=True)
        
        mdf = pd.DataFrame(self.month_translator(), columns=["month_string", "month"])
        df = df.merge(mdf, on="month_string", how="left")
       
        df = df.astype({"day":str, "month":str, "year":str})
        df["expiration_date"] = df["year"] + "-" + df["month"] + "-" + df["day"]
        df["expiration"] = pd.to_datetime(df["expiration_date"], utc=True)
        df["expiration"] = df["expiration"] + timedelta(hours=8)
        df.drop(["month_string", "year", "month", "day", "expiration_date"], axis=1, inplace=True)
        df["ttmyears"] = (((df["expiration"] - df["timestamp"]).dt.total_seconds()) / (60*60*24*365)).round(6)
        
        btcusd_price = int((self.feed.fetch_btcusd_bbo("BTC-PERPETUAL", "ask") 
                            + self.feed.fetch_btcusd_bbo("BTC-PERPETUAL", "bid")) / 2)
        
        df["btcusd_price"] = btcusd_price
        df["bid_usd"] = (df["bid"] * df["btcusd_price"]).round(2)
        df["ask_usd"] = (df["ask"] * df["btcusd_price"]).round(2)
        
        df["bid_iv"] = viv(df["bid_usd"], df["btcusd_price"], df["strike"], 
                           df["ttmyears"], 0, df["typ"].str.lower(), 0, 
                           on_error="ignore", model='black_scholes_merton', 
                           return_as = 'numpy').round(4)
        
        df["ask_iv"] = viv(df["ask_usd"], df["btcusd_price"], df["strike"], 
                           df["ttmyears"], 0, df["typ"].str.lower(), 0, 
                           on_error="ignore", model='black_scholes_merton', 
                           return_as = 'numpy').round(4)
        
        df = df.astype({"btcusd_price":float, "contract":str, "ttmyears":float, 
                        "underlying":str, "strike":float, "typ":str, 
                        "bid":float, "bid_usd":float, "bid_size":float, "bid_iv":float, 
                        "ask":float, "ask_usd":float, "ask_size":float, "ask_iv":float})
        
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        df["expiration"] = pd.to_datetime(df["expiration"], utc=True)
        
        df = df.replace([np.inf, -np.inf], np.nan)
        df.drop(["contract", "underlying"], axis=1, inplace=True)
        
        try:
            df.to_sql("derbbo", con=self.engine, schema="obot", if_exists='append', index=False, chunksize=10000)
        except Exception as e:
            self.logger.info("Error writing orderbook snapshot to database: {}".format(e))

        self.bvix.create_volsurf_snapshot(df)
        self.took_snapshot = True
        
    
    def month_translator(self):
        month_numbers = [[self.months[i], i+1] for i in range(len(self.months))]
        return month_numbers 
    
