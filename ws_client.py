import websocket
from datetime import datetime, timedelta
import json
import time
import secrets
import hmac
import hashlib
import threading
import pytz
import logging
import traceback
import sys

class WSClient:
    
    """ 
    This class sets up the websocket connection, subscribes to multiple data
    streams and eventually distributes the data into the next module. 
    There are a number of steps involved:
        1. Authentication (some streams are not available if not authenticated)
        2. Determination of instruments to subscribe to 
        3. Subscription to channels (private and public channels)
        4. Keeping connection alive and reconnecting if dropped
        5. Correct distribution of incoming messages to other endpoints
    """
    
    def __init__(self, feed, delta_hedger, api_key, api_secret):
        
        self.feed = feed
        self.delta_hedger = delta_hedger
        self.logger = logging.getLogger("deribit")
        self.api_key = api_key
        self.api_secret = api_secret
        self.ws_url = "wss://www.deribit.com/ws/api/v2"
        
        self.shutdown_client = False
        
        self.error_counter = 0
        self.ping_interval = 5
        self.ping_timeout = 2
        
        """ 
        The following system ensures correct interpretation of incoming 
        messages which are NOT from channels that the module is subscribed to.
        """
        # unique, ascending ID later associated with specific call types (API endpoints)
        self.api_call_id_counter = 0
        
        # Call types (API endpoints)
        self.api_call_types = ["public/get_instruments", "public/subscribe", 
                               "public/auth", "private/subscribe", 
                               "private/get_positions", "private/get_position", 
                               "private/buy", "private/sell", 
                               "private/cancel_by_label", "private/cancel_all", 
                               "private/get_open_orders_by_currency"]
        
        # For each call type, contains all associated call IDs
        self.api_call_ids = dict()
        
        self.build_api_call_ids() # Initiates the previous dictionary
        
        self.active_options_contracts = []
        self.active_futures_contracts = []
        
        
        # A number of flags associated with the state of the progess / connection
        self.connected = False        
        self.authenticated = False 
        self.got_active_contracts = False 
        self.subscribed_public = False
        self.subscribed_private = False 
        
        self.public_subscription_count = 0
        
        # updated upon (re)connection
        self.connection_initiation_time = datetime.now(pytz.UTC)
        
        
    def build_api_call_ids(self):
        for i in self.api_call_types:
            self.api_call_ids[i] = []       
        
        
    def create_ws_connection(self):
        
        self.ws = websocket.WebSocketApp(self.ws_url, 
                                         on_open=self.on_open, 
                                         on_message=self.on_message, 
                                         on_error=self.on_error, 
                                         on_close=self.on_close, 
                                         on_pong=self.on_pong)
        
        self.t1 = threading.Thread(target=lambda: self.ws.run_forever(
            skip_utf8_validation=True, 
            ping_interval=self.ping_interval, 
            ping_timeout=self.ping_timeout))
        
        self.t1.start()
        
        self.wait_for_connection()
        self.initiate_streams()
        
    
    def on_open(self, placeholder):
        self.connected = True
        self.logger.info("Connected to Websocket: {}.".format(self.connected))
        self.connection_initiation_time = datetime.now(pytz.UTC)
        
    def on_message(self, placeholder, data):
        try:
            self.message_distribution(data)
        except KeyboardInterrupt:
            self.shutdown()
        
    def on_error(self, placeholder, error):
        error_type, error_tb, tb = sys.exc_info()
        filename, lineno, func_name, line = traceback.extract_tb(tb)[-1]
        self.connected = False
        self.error_counter += 1
        self.logger.info("({}) - Error: {}. Closing Websocket connection and "
                         "reconnecting shortly.".format(self.error_counter, 
                                                        error))
        self.logger.info("Error details: {}\n{}\n{}\n{}\n".format(
            filename, lineno, func_name, line))
        self.connected = False
        self.error_counter += 1
        self.logger.info("({}) - Error: {}. Closing Websocket connection and "
                         "reconnecting shortly.".format(self.error_counter, 
                                                        error))
        self.close_ws() # closing websocket ends websocket run_forever thread
        
        # different waiting times in order to prevent spamming for reconnections
        wait_time = 0
        if self.error_counter <= 3:
            wait_time = 1
        elif self.error_counter > 3 and self.error_counter < 10:
            wait_time = 5
        else:
            wait_time = 15
        
        time.sleep(wait_time)
        
        if not self.shutdown_client:
            if not self.connected:
                self.logger.info("Reconnecting to Websocket.")
                self.create_ws_connection()
    
    
    def on_close(self, placeholder, status, message):
        self.connected = False
        self.logger.info("Websocket closed.")
        
    
    def on_pong(self, placeholder, msg):
        self.connected = True
        now = datetime.now(pytz.UTC)
        if (now - self.connection_initiation_time).total_seconds() > 60:
            self.error_counter = 0
    
    
    def shutdown(self):
        self.shutdown_client = True
        self.close_ws()
    
    def close_ws(self):
        self.reset_vars()
        self.ws.close()
        
    def reset_vars(self):
        self.connected = False
        self.authenticated = False
        self.active_options_contracts = []
        self.active_futures_contracts = []
        self.got_active_contracts = False
        self.subscribed_public = False
        self.subscribed_private = False
        self.public_subscription_count = 0
    
    
    def daily_reconnect(self):
        while True:
            if not self.shutdown_client:
                try:
                    now = datetime.now(pytz.UTC)
                    if (now.hour == 8 and now.minute == 0 and now.second == 2):
                        self.close_ws()
                        time.sleep(5)
                        self.create_ws_connection()
                    else:
                        time.sleep(0.4)        
                except Exception as e:
                    self.logger.info("Error upon reconnecting: {}".format(e))
                    pass
            else:
                break
                
            
    
    def send_to_ws(self, data, call_type):
        # method used across modules to send to websocket
        call_id = self.api_call_id_counter
        message_to_send = {"jsonrpc" : "2.0", 
                           "id" : call_id, 
                           "method" : call_type, 
                           "params" : data}
        
        json_message_to_send = json.dumps(message_to_send)
        self.ws.send(json_message_to_send)
        self.api_call_ids[call_type].append(call_id)
        self.api_call_id_counter += 1   
        
        
    def authenticate(self):
        
        clientId = self.api_key
        clientSecret = self.api_secret
        call_type = "public/auth"

        timestamp = round(datetime.now().timestamp() * 1000)
        nonce = secrets.token_hex(32)
        data = ""
        signature = hmac.new(
            bytes(clientSecret, "latin-1"),
            msg=bytes('{}\n{}\n{}'.format(timestamp, nonce, data), "latin-1"),
            digestmod=hashlib.sha256
        ).hexdigest().lower()
        
        message = {"grant_type": "client_signature", "client_id": clientId, 
                   "timestamp": timestamp, "nonce": nonce, "data": data, 
                   "signature": signature}
        
        self.send_to_ws(message, call_type)
        
        
    def get_instruments(self):
        call_type = "public/get_instruments"
        message = {"currency" : "BTC", "expired" : False}
        self.send_to_ws(message, call_type)
        
    
    def collect_active_contracts(self, data):
        for i in range(len(data["result"])):
            instrument = data["result"][i]["instrument_name"]
            
            if len(instrument) - len(instrument.replace("-", "")) == 1:
                self.active_futures_contracts.append(instrument)
                
            elif len(instrument) - len(instrument.replace("-", "")) == 3:
                if instrument[-1] == "C" or instrument[-1] == "P":
                    self.active_options_contracts.append(instrument)         
        
    
    def build_subscriptions(self, option_contracts, futures_contracts):
        
        """
        Subscribing to all channels at the same time yields a response to large
        for the websocket. Therefore, subscriptions are sent in segments.
        """
        
        mid = round(len(option_contracts) / 2)
        ob_channels_1 = ["book." + str(contract) + ".raw" 
                         for contract in option_contracts[:mid]]
        ob_channels_2 = ["book." + str(contract) + ".raw" 
                         for contract in option_contracts[mid:]]
        oi_channels_1 = ["ticker." + str(contract) + ".raw" 
                         for contract in option_contracts[:mid]]
        oi_channels_2 = ["ticker." + str(contract) + ".raw" 
                         for contract in option_contracts[mid:]]
        
        futures_bbo_channels = ["book." + str(contract) + ".none.1.100ms" 
                                for contract in futures_contracts]
        
        private_channels = ["user.orders.any.any.raw", "user.portfolio.btc", 
                          "user.trades.any.any.raw"]
        
        channels = [ob_channels_1, ob_channels_2, oi_channels_1, oi_channels_2, 
                    private_channels, futures_bbo_channels]
        
        for channel in channels:
            if channel == private_channels:
                call_type = "private/subscribe"
            else:
                call_type = "public/subscribe"
            message = {"channels": channel}
            self.send_to_ws(message, call_type)
            time.sleep(0.2)
            
            
    def get_positions(self):
        call_type = "private/get_positions"
        currency = "BTC"
        kind = ["future", "option"]
        for k in kind:
            message = {"currency":currency, "kind":k}
            self.send_to_ws(message, call_type)
    
    def get_single_position(self, instrument_name):
        call_type = "private/get_position"
        message = {"instrument_name":instrument_name}
        self.send_to_ws(message, call_type)
    
    
    def get_open_orders(self):
        call_type = "private/get_open_orders_by_currency"
        currency = "BTC"
        typ = "all"
        kind = ["future", "option"]
        for k in kind:
            message = {"currency":currency, "type":typ, "kind":k}
            self.send_to_ws(message, call_type)
    
    
    def wait_for_connection(self):
        while True:
            if self.connected:
                return True
            else:
                time.sleep(0.1)
    
    
    def wait_for_auth(self):
        while True:
            if self.authenticated:
                return True
            else:
                time.sleep(0.1)
        
        
    def wait_for_instruments(self):
        while True:
            if self.got_active_contracts:
                return True
            else:
                time.sleep(0.1)
        
        
    def wait_for_subscriptions(self):
        while True:
            if self.subscribed_public and self.subscribed_private:
                return True
            else:
                time.sleep(0.1)
    
    
    def initiate_streams(self):
        """ Procedure to set up all streams iteratively once previous step completed """
        try:
            if not self.authenticated:
                self.authenticate()
                
            self.wait_for_auth()
            
            self.get_open_orders()
            self.get_positions()
            
            if not self.got_active_contracts:
                self.get_instruments()
                
            self.wait_for_instruments()
            
            if ((not self.subscribed_public) and (not self.subscribed_private)):
                self.build_subscriptions(self.active_options_contracts, 
                                         self.active_futures_contracts)
                
            self.wait_for_subscriptions()
            
        except KeyboardInterrupt:
            self.shutdown()
        
        except Exception as e:
            self.logger.info("Exception during stream initiation: {}".format(e))
            self.on_error(0, e)
            
        
    def message_distribution(self, reply):
        
        """ 
        If response contains ID, it is generally not a message from a subscription.
        If it contains the key 'method', it is. Each reply is handled 
        corresponding to which endpoint a message was sent to, or which 
        subscribed channel the message is from.
        """
        
        reply = json.loads(reply)
        
        if "id" in reply:
            if "result" in reply:

                if reply["id"] in self.api_call_ids["public/get_instruments"]:
                    self.collect_active_contracts(reply)
                    self.got_active_contracts = True
                    
                elif reply["id"] in self.api_call_ids["public/auth"]:
                    if reply["result"]["token_type"] == "bearer":
                        self.authenticated = True
                    else:
                        self.authenticated = False
                        
                elif reply["id"] in self.api_call_ids["public/subscribe"]:
                    # 5 versions of public subscriptions are sent iteratively
                    self.public_subscription_count += 1
                    if self.public_subscription_count == 5:
                        self.subscribed_public = True
                        
                elif reply["id"] in self.api_call_ids["private/subscribe"]:
                    self.subscribed_private = True
                    
                elif reply["id"] in self.api_call_ids["private/get_positions"]:
                    self.feed.initial_positions(reply["result"])
                    
                elif reply["id"] in self.api_call_ids["private/get_position"]:
                    self.feed.initial_positions([reply["result"]])
                    
                elif reply["id"] in self.api_call_ids["private/get_open_orders_by_currency"]:
                    self.feed.initial_open_orders(reply["result"])
                    
                elif reply["id"] in self.api_call_ids["private/buy"]:
                    pass

                elif reply["id"] in self.api_call_ids["private/sell"]:
                    pass
                    
                elif reply["id"] in self.api_call_ids["private/cancel_all"]:
                    if self.feed.orders:
                        self.feed.orders = {}
                
                elif reply["id"] in self.api_call_ids["private/cancel_by_label"]:
                    pass
                    
                
                else:
                    self.logger.info("Unhandled reply (unknown id): {}".format(reply))
            else:
                self.logger.info("Unhandled reply (result not in reply): {}".format(reply))
        elif "method" not in reply:
            self.logger.info("Unhandled reply (method not in reply): {}".format(reply))
                
        
        if "method" in reply:
            
            if reply["method"] == "subscription":
                if "params" in reply:
                    if "channel" in reply["params"]:
                        if "data" in reply["params"]:
                            
                            # Orderbook updates from futures trading pairs
                            if reply["params"]["channel"][-13:] == ".none.1.100ms":
                                self.feed.update_futures_bbo(reply["params"]["data"])
                            
                            # Orderbook updates from options contracts
                            elif reply["params"]["channel"][:9] == "book.BTC-":
                                if "type" in reply["params"]["data"]:
                                    
                                    if reply["params"]["data"]["type"] == "snapshot":
                                        self.feed.build_options_ob_from_snapshots(reply["params"]["data"])
                                    
                                    elif reply["params"]["data"]["type"] == "change":
                                        self.feed.update_options_ob(reply["params"]["data"])
                            
                            # OI updates from option contracts
                            elif reply["params"]["channel"][:11] == "ticker.BTC-":
                                self.feed.manage_option_oi(reply["params"]["data"])
                            
                            # Private channels
                            elif reply["params"]["channel"] == "user.orders.any.any.raw":
                                self.feed.manage_orders(reply["params"]["data"])
                                
                            elif reply["params"]["channel"] == "user.portfolio.btc":
                                self.feed.manage_portfolio(reply["params"]["data"])
                                self.delta_hedger.check_deltas(self.send_to_ws)
                                
                                
                            elif reply["params"]["channel"] == "user.trades.any.any.raw":
                                self.feed.update_positions(reply["params"]["data"])
                                for k in range(len(reply["params"]["data"])):
                                    if reply["params"]["data"][k]["instrument_name"] not in self.feed.positions:
                                        self.get_single_position(reply["params"]["data"][k]["instrument_name"])
        
