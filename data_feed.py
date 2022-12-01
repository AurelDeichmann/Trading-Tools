from datetime import datetime
import logging

class DataFeed:
    
    """
    This module processes the incoming data and stores part of it locally.
    As such, it builds and updates all options complete orderbooks, 
    keeps track of account positions and open orders and a few other things.
    """
    
    def __init__(self):
        self.logger = logging.getLogger("deribit")
        self.ob = dict() # All options contracts complete order books
        self.oi = dict() # Options OI per contract
        self.futures_bbo = dict() # All futures contracts best bid and offer
        self.account = {} # Account information e.g. balance
        self.orders = {} # Accounts open orders
        self.trades = {} # Accounts trade history, not yet implemented
        self.positions = {} # Accounts positions
        self.account_info_headers = ["available_funds", "balance", 
                                     "delta_total", "initial_margin", 
                                     "maintenance_margin", "margin_balance"]
        self.got_open_orders = False # True if accounts open orders have been received
        
    def initial_open_orders(self, data):
        for order in data:
            order["replaced"] = False
            order["original_order_type"] = "limit"
            instrument_name = order["instrument_name"]
            order_id = order["order_id"]
            
            if instrument_name not in self.orders:
                self.orders[instrument_name] = {order_id : order}
            else:
                self.orders[instrument_name][order_id] = order
        
        self.got_open_orders = True
    
    def initial_positions(self, data):
        for position in data:
            if not position["size"] == 0:
                instrument_name = position["instrument_name"]
                self.positions[instrument_name] = position
        
    
    def update_positions(self, data):
        
        """
        Instead of repeatedly calling the API for the accounts current positions, 
        this function calculates the resulting position from the initial 
        get_positions call using incoming trade confirmation messages via subscription.
        """
        
        for i in range(len(data)):
        
            instrument_name = data[i]["instrument_name"]
            side = data[i]["direction"]
            amount = data[i]["amount"]
            entry = data[i]["price"]
                        
            if instrument_name in self.positions:

                prev_amount = self.positions[instrument_name]["size"]
                prev_entry = self.positions[instrument_name]["average_price"]
                prev_side = self.positions[instrument_name]["direction"]
                
                if side == "buy":
                    self.positions[instrument_name]["size"] += amount
                elif side == "sell":
                    self.positions[instrument_name]["size"] -= amount
                else:
                    self.logger.debug("Trade message did not contain enough information.")
                
                
                if ((side == "buy" and self.positions[instrument_name]["direction"] == "buy") or 
                    (side == "sell" and self.positions[instrument_name]["direction"] == "sell")):
                                        
                    new_entry = round(prev_entry * (prev_amount / (prev_amount + amount)) + 
                                      entry * (amount / (prev_amount + amount)), 2)
                    self.positions[instrument_name]["average_price"] = new_entry
                    
                else:
                    if abs(amount) > abs(self.positions[instrument_name]["size"]):
                        self.positions[instrument_name]["average_price"] = entry
                        if prev_side == "sell":
                            self.positions[instrument_name]["direction"] = "buy"
                        elif prev_side == "buy":
                            self.positions[instrument_name]["direction"] = "sell"
                        
                    elif abs(amount) < abs(self.positions[instrument_name]["size"]):
                        self.positions[instrument_name]["average_price"] = prev_entry
                        
                    else:
                        del self.positions[instrument_name]
            
                
    
    def manage_orders(self, data):
        instrument_name = data["instrument_name"]
        order_id = data["order_id"]
        
        if data["order_type"] != "rejected":
            
            # market and market_limit orders are not open, and are thus neglected
            if data["order_type"] != "market" and data["order_type"] != "market_limit":
                
                if instrument_name not in self.orders.keys():
                    self.orders[instrument_name] = {}
                
                if order_id in self.orders[instrument_name].keys():
                    # cancelled orders are taken out
                    if data["order_state"] == "cancelled": 
                        del self.orders[instrument_name][order_id]
                    
                    # fully filled orders are taken out
                    elif (data["order_state"] == "filled" and data["filled_amount"] == data["max_show"]):
                        del self.orders[instrument_name][order_id]
                    
                    # partially filled, and generally open orders which are 
                    # already in the system are updated
                    else: 
                        self.orders[instrument_name][order_id] = data
                        
                else: # open orders which are not in the system yet
                    self.orders[instrument_name][order_id] = data
                    
        else:
            print("ORDER REJECTED! Check margin balance?")
    
    
    
    def manage_portfolio(self, data):
        for header in self.account_info_headers:
            self.account[header] = data[header]
        
        
    def manage_option_oi(self, msg):
        self.oi[msg["instrument_name"]] = msg["open_interest"]
        
        
    def build_options_ob_from_snapshots(self, snapshot):
        bids = dict()
        for bid in snapshot["bids"]:
            bids[bid[1]] = bid[2]
        asks = dict()
        for ask in snapshot["asks"]:
            asks[ask[1]] = ask[2]
        self.ob[snapshot["instrument_name"]] = {"bids":bids, "asks":asks}
        
        
    def update_options_ob(self, msg):
        contract = msg["instrument_name"]
        for side in ["bids", "asks"]:
            if len(msg[side]) > 0:
                for i in msg[side]:
                    if i[0] == "delete":
                        del self.ob[contract][side][i[1]]
                    elif i[0] == "new" or i[0] == "change":
                        self.ob[contract][side][i[1]] = i[2]
                    else:
                        pass
    
    
    def update_futures_bbo(self, message):
        instrument = message["instrument_name"]
        bid = message["bids"][0][0]
        ask = message["asks"][0][0]
        self.futures_bbo[instrument] = {"bid":bid, "ask":ask}
    
    
    # def delete_order(self):
        
    
    
    # Some callbacks for easier data retrieval from other modules
    def get_orders(self):
        return self.orders
    
    def get_account_data(self):
        return self.account
    
    def get_trades(self):
        return self.trades
    
    def get_positions(self):
        return self.positions
    
    def fetch_local_ob(self):
        return self.ob
    
    def fetch_local_oi(self):
        return self.oi
    
    def fetch_btcusd_bbo(self, instrument, side):
        return self.futures_bbo[instrument][side]
        
