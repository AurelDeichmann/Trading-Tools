import pandas as pd
import numpy as np
from datetime import datetime
import pytz
import warnings
import logging
warnings.filterwarnings("ignore")


class BVIX:
    
    def __init__(self, db_connection):
        
        self.logger = logging.getLogger("deribit")
        self.schema = "obot"
        self.table = "bvix"
        self.c = db_connection["c"]
        self.conn = db_connection["conn"]
        self.engine = db_connection["engine"]
        
        self.days_til_maturity = [3, 7, 10, 14, 17, 21, 24, 28, 31, 35, 38, 
                                  42, 45, 49, 52, 56, 84]
        
        self.moneyness_intervals = [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.85, 0.9, 
                                    0.95, 0.96, 0.97, 0.98, 0.99, 1, 1.01, 
                                    1.02, 1.03, 1.04, 1.05, 1.1, 1.15, 1.2, 
                                    1.3, 1.4, 1.5, 1.6, 1.7]
        
        self.log_moneyness_intervals = [i-1 for i in self.moneyness_intervals]
        self.prepare_db()
        
    def prepare_db(self):
        self.c.execute("CREATE SCHEMA IF NOT EXISTS {}".format(self.schema))
        self.conn.commit()
        
        string = "CREATE TABLE IF NOT EXISTS {}.{}".format(self.schema, self.table)
        string = string + "(timestamp TIMESTAMPTZ, moneyness NUMERIC, "
        for i in self.days_til_maturity:
            string += "d" + str(i) + " NUMERIC, "
        string = string[:-2] + ")"
        self.c.execute(string)
        self.conn.commit()
        
        
    def create_volsurf_snapshot(self, df):
        try:
            df = df.astype({"btcusd_price":float, "strike":float, "ttmyears":float, 
                            "bid":float, "bid_size":float, "bid_usd":float, "bid_iv":float, 
                            "ask":float, "ask_size":float, "ask_usd":float, "ask_iv":float, 
                            "typ":str})
            
            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            df["expiration"] = pd.to_datetime(df["expiration"], utc=True)
            df["ttmdays"] = df["ttmyears"] * 365
        
            df["moneyness"] = (np.log(df["strike"] / df["btcusd_price"]))
            df["mid_iv"] = (df["bid_iv"] + df["ask_iv"]) / 2
        
            df["ask"].loc[df["ask"] >= 0.6] = 0
            df["bid"].loc[df["bid"] >= 0.6] = 0
            df = df[df["ask"] != 0.0005]
            
            df["bid"].fillna(0, inplace=True)
            df["ask"].fillna(0, inplace=True)
            
            df["bid_usd"].loc[df["bid"] == 0] = 0
            df["ask_usd"].loc[df["ask"] == 0] = 0
            df["bid_iv"].loc[df["bid"] == 0] = 0
            df["ask_iv"].loc[df["ask"] == 0] = 0
            df["bid_size"].loc[df["bid"] == 0] = 0
            df["ask_size"].loc[df["ask"] == 0] = 0
            
            df = df[(df["bid"] > 0) | (df["ask"] > 0)]
            df = df[(df["bid_iv"] > 0) | (df["ask_iv"] > 0)]
            
            itm_max = np.log(1.1)
            
            df = df[((df["moneyness"] < itm_max) & (df["typ"] == "P")) | 
                    ((df["moneyness"] > -1*itm_max) & (df["typ"] == "C"))]
            
            df["mid_iv"].loc[(df["ask_iv"] > 0) & (df["bid_iv"] == 0)] = df["ask_iv"]
            df["mid_iv"].loc[(df["bid_iv"] > 0) & (df["ask_iv"] == 0)] = df["bid_iv"]
            
            df["contract"] = df["expiration"].dt.date.astype(str) + df["strike"].astype(str) + df["typ"]
            
            df_grouped = df.groupby(["strike", "expiration"], 
                                    as_index=False).agg({"ttmdays":"first", 
                                                         "moneyness":"first", 
                                                         "mid_iv":"mean"})
            
            df_grouped.sort_values(by=["ttmdays", "moneyness"], ascending=True, inplace=True)
            df_grouped.drop(["strike", "expiration"], axis=1, inplace=True)
            
            
            unique_ttms = df_grouped["ttmdays"].unique()
            
            df_atm_intervals = pd.DataFrame()
            
            for ttm in unique_ttms:
                df2 = df_grouped[df_grouped["ttmdays"] == ttm]
                df_moneyness = pd.DataFrame(self.log_moneyness_intervals, columns=["moneyness"])
                df2 = df2.merge(df_moneyness, on="moneyness", how="outer").replace([np.inf, -np.inf], 0)
                df2["ttmdays"].fillna(method="ffill", inplace=True)
                df2["ttmdays"].fillna(method="bfill", inplace=True)
                df2 = df2.sort_values(by="moneyness", ascending=True).set_index("moneyness")
                df2 = df2.interpolate(method='slinear', limit_direction='forward', axis=0).reset_index()
                df_atm_intervals = df_atm_intervals.append(df2)
            
            
            ttm_atm_combinations = []
            
            for i in self.days_til_maturity:
                for m in self.log_moneyness_intervals:
                    ttm_atm_combinations.append([i, m])
            
            dfy = pd.DataFrame(ttm_atm_combinations, columns=["ttmdays", "moneyness"])
            
            df_atm_intervals = df_atm_intervals[df_atm_intervals["moneyness"].isin(self.log_moneyness_intervals)]
            df_atm_intervals = df_atm_intervals.append(dfy)
            
            
            unique_atms = df_atm_intervals["moneyness"].unique()
            
            df_ttm_intervals = pd.DataFrame()
            
            for m in unique_atms:
                df2 = df_atm_intervals[df_atm_intervals["moneyness"] == m]
                df2.set_index("ttmdays", inplace=True)
                df2 = df2.interpolate(method='slinear', limit_direction='forward', axis=0)
                df2.reset_index(inplace=True)
                df_ttm_intervals = df_ttm_intervals.append(df2)
                
            df_ttm_intervals = df_ttm_intervals[df_ttm_intervals["ttmdays"].isin(self.days_til_maturity)]
            df_ttm_intervals["moneyness"] = round(np.exp(df_ttm_intervals["moneyness"]), 3)
            df_ttm_intervals["ttmdays"] = df_ttm_intervals["ttmdays"].astype(int)
            
            
            df_atm_ttm = df_ttm_intervals.pivot(index="moneyness", columns="ttmdays", values="mid_iv")
            df_atm_ttm.reset_index(inplace=True)
            df_atm_ttm = df_atm_ttm.round(decimals=4)
            
            db_columns = ["moneyness"]
            for i in self.days_til_maturity:
                db_columns.append("d" + str(i))
            df_atm_ttm.columns = db_columns
            ts = datetime.now(pytz.UTC)
            ts = ts.replace(microsecond=0)
            df_atm_ttm["timestamp"] = ts
            df_atm_ttm["timestamp"] = pd.to_datetime(df_atm_ttm["timestamp"], utc=True)
        
            df_atm_ttm.to_sql("bvix", con=self.engine, schema="obot", 
                              if_exists='append', index=False, chunksize=10000)            
        except Exception as e:
            self.logger.info("Error writing volatility surface to database: {}".format(e))

    
    
