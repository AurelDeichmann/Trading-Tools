import logging
import pandas as pd
import time

class InputParser:
    
    """
    This module allows for specific user input to trade manually via the
    deribit API. It also supports a number of commands to show open orders, 
    current positions etc. So far, it supports trading in any futures contract
    on deribit. The size multiplier is meant to reduce the number of zeros 
    necessary to type in a command. 
    Type 'help' for syntax (or check out the show_syntax method)
    """
    
    def __init__(self, client, feed, api_methods, delta_hedger):
        self.client = client
        self.feed = feed
        self.api_methods = api_methods
        self.delta_hedger = delta_hedger
        
        self.logger = logging.getLogger("deribit")
        
        self.shutdown_input = False
        self.label_counter = 0
        self.label_storage = []
        self.size_multiplier = 0
        self.size_multiplier_set = False
        self.instrument = ""
        self.instrument_set = False
        
        self.order_info = ["side", "amount", "price", "order_type", 
                           "label", "id"]
        self.position_info = ["instrument_name", "direction", "average_price", 
                              "delta", "size"]
        
        self.label_counter_updated = False
        
    
    def determine_label_counter(self):
        orders = self.feed.get_orders()
        if orders:
            for i in orders.keys():                
                for j in orders[i].keys():
                    label = orders[i][j]["label"]
                    if "manual_api_" in label:
                        label_number = label.replace("manual_api_", "")
                        if not label_number.isnumeric():
                            pass
                        else:
                            label_number = int(label_number)
                            self.label_storage.append(label_number)
        
        if self.label_storage:
            self.label_counter = max(self.label_storage) + 1

        
    def accept_user_input(self):
        
        if not self.shutdown_input:
            
            # first, an instrument is chosen and a size multiplier set
            
            if not self.instrument_set:
                
                instrument_string = "Instrument to trade: \n"
                for i in self.client.active_futures_contracts:
                    instrument_string += "{}\n".format(str(i))
                instrument_string += "\n"
                instrument = input(instrument_string)
                
                if instrument not in self.client.active_futures_contracts:
                    print("Incorrect instrument provided. Check Typos?")
                    self.instrument_set = False
                else:
                    self.instrument = instrument
                    self.instrument_set = True
                
                
            elif not self.size_multiplier_set:
                x = input("Set size multiplier: ")
                if not x.isnumeric():
                    print("Size multiplier is not numeric. Input e.g. 100.")
                    self.size_multiplier_set = False
                else:
                    self.size_multiplier = int(x)
                    self.size_multiplier_set = True
            
            elif not self.label_counter_updated:
                if not self.feed.got_open_orders:
                    time.sleep(0.2)
                else:
                    self.determine_label_counter()
                    self.label_counter_updated = True
                
            else:
                x = input("{} :~$ ".format(self.instrument))
                x = x.strip()
                
                parsed = self.custom_parse(x)
                
                if parsed:
                    for order in range(len(parsed)):
                        
                        if (parsed[order][0] and parsed[order][1]) or (parsed[order][1] == "private/cancel_all"): # THIS IS NOT THE CASE FOR CANCEL ALL CALL
                            self.client.send_to_ws(parsed[order][0], parsed[order][1])
                            
                            if len(parsed) > 1:
                                if order != len(parsed) - 1:
                                    time.sleep(0.2)
                
        else:
            self.logger.info("Shutting down user input thread.")
            
            
    def custom_parse(self, x):
        try:
            currency = "BTC"
            label = "manual_api_{}".format(str(self.label_counter))
            
            if len(x) < 2:
                raise InvalidInput
            
            if x == "shutdown" or x == "quit":
                self.shutdown_input = True
            
            if x == "help":
                self.show_syntax()
                
            elif x == "change instrument":
                self.instrument_set = False
                self.size_multiplier_set = False
                
            elif x == "activate delta hedging":
                self.delta_hedger.delta_hedging_activated = True
                self.logger.info("Checking deltas upon hedge activation.")
                self.delta_hedger.check_deltas(self.client.send_to_ws)
            
            elif x == "deactivate delta hedging":
                self.delta_hedger.delta_hedging_activated = False
                
            elif x == "delta hedging status":
                print("Delta hedging activated: {}".
                      format(self.delta_hedger.delta_hedging_activated))
            
            elif x == "funds":
                account = self.feed.get_account_data()
                current_price = self.feed.fetch_btcusd_bbo(self.instrument, "bid")
                data = []
                for i in account.keys():
                    data.append([i, account[i], 
                                 "$"+str(round(account[i]*current_price, 2))])
                
                df = pd.DataFrame(data, columns=["item", "balance_btc", "balance_usd"])
                print(df.to_string())
            
            
            elif x == "orders":
                orders = self.feed.get_orders() 
                data = []
                for i in orders.keys():
                    for j in orders[i].keys():
                        entry = [i, j, orders[i][j]["direction"], 
                                 orders[i][j]["amount"], 
                                 orders[i][j]["price"], 
                                 orders[i][j]["order_type"], 
                                 orders[i][j]["post_only"], 
                                 orders[i][j]["reduce_only"], 
                                 orders[i][j]["label"]]
                        data.append(entry)
                
                df = pd.DataFrame(data, columns=["underlying", "id", "direction", 
                                                 "amount", "price", "order_type", 
                                                 "post_only", "reduce_only", 
                                                 "label"])
                
                if len(df) > 0:
                    print(df.to_string())
                else:
                    print("No open orders at this moment.")
                    
                
            elif x == "positions":
                positions = self.feed.get_positions()
                data = []
                for i in positions.keys():
                    entry = [i, positions[i]["direction"], 
                             positions[i]["average_price"], 
                             positions[i]["size"], 
                             round(positions[i]["total_profit_loss"], 5)]
                    data.append(entry)
                    
                df = pd.DataFrame(data, columns=["underlying", "direction", 
                                                 "average_price", "size", 
                                                 "total_pnl"])
                
                if len(df) > 0:
                    print(df.to_string())
                else:
                    print("No open positions at this moment.")
                
                
            elif x == "show size multiplier":
                print(self.size_multiplier)
                
            elif x == "reset size multiplier":
                self.size_multiplier = 0
                self.size_multiplier_set = False
            
            elif x == "connection status":
                print("Connected: ", self.client.connected)
            
            elif x[0] == "c":
                if len(x) == 2:
                    if x[1] == "a":
                        return [self.api_methods.cancel_all()]
                    elif x[1] == "c":
                        if len(self.label_storage) > 0:
                            self.label_storage.sort()
                            to_cancel = self.label_storage[-1]
                            cancel_last_label = "manual_api_{}".format(to_cancel)
                            # self.logger.info("Cancelling: {}".format(cancel_last_label))
                            del self.label_storage[-1]
                            return [self.api_methods.cancel_last(cancel_last_label, currency)]
                    
                else:
                    raise InvalidInput            
            
            
            elif not x[-1] == "c":
                
                x_without_spaces = x.replace(" ", "")
                if not x_without_spaces.isalnum():
                    raise InvalidInput
                
                spaces = [0]
                for i in range((len(x) - len(x_without_spaces)) + 1):
                    space = x.find(" ", spaces[i]+1)
                    if space > 0:
                        spaces.append(space)
                del spaces[0]
                
                
                if x[0] == "a":
                    side = "buy"
                    
                elif x[0] == "d":
                    side = "sell"
                
                else:
                    raise InvalidInput
                
                
                if x[1] == "w":
                    price = self.feed.fetch_btcusd_bbo(self.instrument, "ask")
                elif x[1] == "s":
                    price = self.feed.fetch_btcusd_bbo(self.instrument, "bid")
                else:
                    price = None
                
                
                
                if len(spaces) == 0:
                    
                    if x[1].isdigit():
                        amount = x[1:]
                        if not amount.isnumeric():
                            raise InvalidInput
                        
                        elif int(amount) > 10**4:
                            raise InvalidInput
                            
                        amount = int(amount) * self.size_multiplier
                        
                        if side == "buy":
                            price = self.feed.fetch_btcusd_bbo(self.instrument, "ask") * 1.001
                        elif side == "sell":
                            price = self.feed.fetch_btcusd_bbo(self.instrument, "bid") * 0.999
                        
                        price = int(price)
                        
                        order_type = "market_limit"
                        self.label_storage.append(self.label_counter)
                        self.label_counter += 1
                        return [self.api_methods.send_order(self.instrument, 
                                                            side, amount, 
                                                            order_type, 
                                                            label, price)]
                        
                        
                    else:
                        amount = x[2:]
                        if not amount.isnumeric():
                            raise InvalidInput                        
                        elif int(amount) > 10**4:
                            raise InvalidInput
                            
                        if ((x[1] != "w") and (x[1] != "s")):
                            raise InvalidInput
                            
                        amount = int(amount) * self.size_multiplier
                        
                        if (side == "buy" and x[1] == "s") or (side == "sell" and x[1] == "w"):
                            post_only = True
                            order_type = "limit"
                        else:
                            post_only = False
                            order_type = "market_limit"
                        self.label_storage.append(self.label_counter)
                        self.label_counter += 1
                        return [self.api_methods.send_order(self.instrument, 
                                                            side, amount, 
                                                            order_type, label, 
                                                            price, 
                                                            post_only=post_only)]    
                        
                
                
                elif len(spaces) == 1:
        
                    if price:
                        amount = x[2:spaces[0]]
                    else:
                        amount = x[1:spaces[0]]
                    
                    if not amount.isnumeric():
                        raise InvalidInput
                    
                    amount = int(amount) * self.size_multiplier
                    
                    if x[-1] == "s":
                        order_type = "stop_market"
                        price = x[spaces[0] + 1 : -1]
                        
                        if not price.isnumeric():
                            raise InvalidInput
                        
                        if ((side == "buy" and price <= self.feed.fetch_btcusd_bbo(self.instrument, "ask"))
                            or (side == "sell" and price >= self.feed.fetch_btcusd_bbo(self.instrument, "bid"))):
                            raise InvalidInput
                        
                        price = int(price)
                        trigger_price = "mark_price"
                        reduce_only = True
                        self.label_storage.append(self.label_counter)                        
                        self.label_counter += 1
                        return [self.api_methods.send_order(self.instrument, 
                                                            side, amount, 
                                                            order_type, label, 
                                                            price, 
                                                            trigger_price=trigger_price, 
                                                            reduce_only=reduce_only)]
                        
                        
                    else:
                        price = x[spaces[0] + 1 :]
                        order_type = "limit"
                        
                        if not price.isnumeric():
                            raise InvalidInput
                    
                        price = int(price)
                        post_only = True
                        self.label_storage.append(self.label_counter)
                        self.label_counter += 1
                        return [self.api_methods.send_order(self.instrument, 
                                                            side, amount, 
                                                            order_type, label, 
                                                            price, 
                                                            post_only=post_only)]
                        
                    
                    
                elif len(spaces) == 3:
                    
                    if price:
                        amount = x[2:spaces[0]]
                    else:
                        amount = x[1:spaces[0]]
                    
                    if not amount.isnumeric():
                        raise InvalidInput
                    
                    amount = int(amount) * self.size_multiplier
                    
                    bound1 = x[spaces[0] + 1 : spaces[1]]
                    bound2 = x[spaces[1] + 1 : spaces[2]]
                    number_of_orders = x[spaces[2] + 1 :]
                    
                    
                    if not bound1.isnumeric():
                        raise InvalidInput
                        
                    if not bound2.isnumeric():
                        raise InvalidInput
                        
                    if not number_of_orders.isnumeric():
                        raise InvalidInput
                    
        
                    bound1 = int(bound1)
                    bound2 = int(bound2)
                    number_of_orders = int(number_of_orders)
                    if number_of_orders < 2:
                        raise InvalidInput
                    
                    distance = abs(bound1 - bound2) / number_of_orders
                    
                    if side == "buy":
                        prices = [min(bound1, bound2) + distance * i for i in range(number_of_orders)]
                    elif side == "sell":
                        prices = [max(bound1, bound2) - distance * i for i in range(number_of_orders)]
                    
                    order_type = "limit"
                    
                    laddered_orders = []
                    
                    for p in range(len(prices)):
                        price = int(prices[p])
                        label = "manual_api_{}".format(str(self.label_counter))
                        order = self.api_methods.send_order(self.instrument, 
                                                            side, amount, 
                                                            order_type, label, 
                                                            price, 
                                                            post_only=True)
                        laddered_orders.append(order)
                        self.label_storage.append(self.label_counter)
                        self.label_counter += 1
                    return laddered_orders
                    
                else:
                    raise InvalidInput
                
            else:
                print("Command not executed, last letter = 'c'.")
            
        except InvalidInput:
            print("Invalid input. Type 'help' for supported command syntax.")
    
    def show_syntax(self):
        
        print("\n-------------------------------------------------------------"
              "\nTRADING"
              "\nBegin with 'a' to buy or 'd' to sell."
              "\nOptional: second letter 's' sets price to bid, "
              "\n          'd' sets price to ask"
              "\nNext come numbers as the amount to trade."
              "\nExamples:"
              "\na200 : market_limit buy 200 (market_limit = market order "
              "\n       capped at 0.1% difference from last price)"
              "\ndw200 : post only limit sell 200 at ask"
              "\n"
              "\nOptional: follow with space and a price, overriding 's'/'w'"
              "\nOptional: follow previous set price with 's' to place "
              "\n          stop order (mark price based, reduce only, "
              "\n          prices causing immediate execution trigger error)"
              "\nExamples:"
              "\nas100 16000 : post only limit buy 100 at 16000"
              "\ndw200 18000s : market stop at 18000 for amount of 200"
              "\na100 16000 18000 10 : 10 limit buy orders size 100 each in "
              "\n                      equal spaces between 16000 and 18000"
              "\n-------------------------------------------------------------"
              "\nOTHER SUPPORTED COMMANDS"
              "\nfunds \norders \npositions"
              "\nactivate delta hedging \ndeactivate delta hedging "
              "\ndelta hedging status \nca (= cancel all) \ncc (= cancel last)"
              "\nshow size multiplier \nreset size multiplier"
              "\nchange instrument \nconnection status"
              )
    

        

class InvalidInput(Exception):
    pass