from py_vollib_vectorized import vectorized_implied_volatility as viv
from datetime import datetime
import pytz
from scipy.stats import norm
import numpy as np
import logging
from api_trading_methods import ApiMethods

class DeltaHedge:
    
    """ 
    Any options contract can be delta-hedged dynamically using the underlying.
    In this hedger, the underlying is the BTC-PERPETUAL trading pair, 
    but can be altered. 
    This module ONLY compares ALL options deltas to ONE 
    specified futures position, and hedges based on this comparison. 
    It does NOT hedge taking other futures positions into account.
    
    The hedger is activated using user input from 'custom_input_parser'.
    The hedging is not continuous, there is a max delta mismatch between an 
    options position and the hedge. This serves to reduce transaction costs and
    prevent infinite loop trading.
    When activated, deltas are being calculated and compared on each message
    the API sends out about a change in the accounts information, e.g. margin.
    This happens frequently, and the deribit-team told me it sends a message
    every time there is a change, i.e. also when a trade occurs. 
    Unfortunately, deribits naming convention requires a bit of tedious work.
    """
    
    def __init__(self, feed):
        
        self.feed = feed
        self.api_methods = ApiMethods()
        self.logger = logging.getLogger("deribit")
        self.delta_hedging_activated = False
        self.months = {"JAN":1, "FEB":2, "MAR":3, "APR":4, "MAY":5, "JUN":6, 
                       "JUL":7, "AUG":8, "SEP":9, "OCT":10, "NOV":11, "DEC":12}
        
        self.max_delta_mismatch = 0.0025 # Percentage of the underlyings value deltas may differ
        self.hedge_instrument = "BTC-PERPETUAL"
        self.op_delta = 0
        self.btchedge_delta = 0
        self.send_to_ws = None
        
    
    def check_deltas(self, send_method):
        
        if not self.delta_hedging_activated:
            pass
        
        else:
            self.send_to_ws = send_method
            current_options_delta, current_hedge_delta = self.determine_option_delta()
            
            lower_bound = self.max_delta_mismatch * self.feed.fetch_btcusd_bbo(self.hedge_instrument, "bid") * -1
            upper_bound = self.max_delta_mismatch * self.feed.fetch_btcusd_bbo(self.hedge_instrument, "bid")
            
            if abs(current_options_delta) > 0:
                if not (lower_bound < ((current_hedge_delta * -1) - current_options_delta) < upper_bound):
                    self.rehedge(current_options_delta, current_hedge_delta)
            else:
                pass
    
    
    def determine_option_delta(self):
        
        positions = self.feed.positions.copy()
        hedge_delta = 0
        if self.hedge_instrument in positions.keys():
            hedge_delta = positions[self.hedge_instrument]["size"]
        else:
            pass
        
        
        keys_to_delete = []
        
        for key in positions.keys():
            if (str(key)[-1] != "P" and str(key)[-1] != "C"):
                keys_to_delete.append(key)
            elif positions[key]["size"] == 0:
                keys_to_delete.append(key)
                
        for key in keys_to_delete:
            del positions[key]
        
        
        option_delta = 0
        
        if len(positions) > 0:
        
            for key in positions.keys():
                name = str(positions[key]["instrument_name"])
                size = positions[key]["size"]
                hedge_bid = self.feed.fetch_btcusd_bbo(self.hedge_instrument, "bid")
                hedge_ask = self.feed.fetch_btcusd_bbo(self.hedge_instrument, "ask")
                btcusd_price = int((hedge_bid + hedge_ask) / 2)
                price = positions[key]["mark_price"] * btcusd_price
                
                # tedious parsing of deribit option naming convention
                first = name.find("-")
                second = name.find("-", first+1)
                third = name.find("-", second+1)
                
                typ = name[-1]
                exp = name[first+1:second]
                strike = int(name[second+1:third])
                
                year = int(exp[-2:]) + 2000
                month = self.months[exp[-5:-2]]
                day = int(exp[:-5])
                
                expiration = datetime(year, month, day, 8, 0, 0, 0, pytz.UTC)
                ttmyears = ((expiration - datetime.now(pytz.UTC)).total_seconds()) / (60*60*24*365)
                
                iv = viv(price, btcusd_price, strike, ttmyears, 0, 
                         typ.lower(), 0, on_error="ignore", 
                         model='black_scholes_merton', 
                         return_as = 'numpy').round(4)
                iv = iv[0]
                
                delta = self.bsm_delta(btcusd_price, strike, iv, 0, 0, ttmyears, typ.lower())
                delta = delta * size * btcusd_price
                option_delta += delta
        
        self.op_delta = option_delta
        self.btchedge_delta = hedge_delta
        
        return option_delta, hedge_delta
    
    
    
    def rehedge(self, option_delta, hedge_delta):
        side = ""
        target = option_delta * -1
        diff = hedge_delta - target
        
        if diff > 0:
            side = "sell"
            price = self.feed.fetch_btcusd_bbo(self.hedge_instrument, "bid")
        else:
            side = "buy"
            price = self.feed.fetch_btcusd_bbo(self.hedge_instrument, "ask")
            
        amount = (abs(diff) // 10) * 10        
        self.logger.info("Rehedging: {} {} at market.".format(side, amount))
        
        order = self.api_methods.send_order(self.hedge_instrument, 
                                            side, amount, "market", 
                                            "delta_hedge", price)
        message = order[0]
        call_type = order[1]
        self.send_to_ws(message, call_type)          
    

    def bsm_delta(self, S,X,sigma, r, q, ttm, otype):
        delta = None
        
        b = float(r - q)
        d1 = (np.log(S/X) + (b + (sigma**2)/2)*ttm) / (sigma * np.sqrt(ttm))
        d2 = d1 - sigma * np.sqrt(ttm)
        
        if (otype == "c"):
            delta = np.exp((b-r)*ttm)*norm.cdf(d1)
        elif (otype == "p"):
            delta = -np.exp((b-r)*ttm)*norm.cdf(-d1)
        return delta