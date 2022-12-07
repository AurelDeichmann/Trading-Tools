from datetime import datetime
import pytz
import psycopg2
import threading
import time
from sqlalchemy import create_engine
import logging

from ws_client import WSClient
from save_top_of_book import SaveBBO
from data_feed import DataFeed
from hedger import DeltaHedge
from custom_input_parser import InputParser
from api_trading_methods import ApiMethods
import configparser


class Bot:
    
    """ 
    This class prepares necessary information, instantiates other modules, 
    and then sets them in motion. Additionally, it restarts the connection
    to the exchange every day at 8:00am UTC since some contracts expire at 
    that time and others are introduced. 
    """
    
    def __init__(self):
        
        self.logger = logging.getLogger("deribit")
        config = configparser.RawConfigParser()
        config.read_file(open("settings.txt"))
        
        self.api_information = dict(config.items("API"))
        self.api_key = self.api_information["api_key"]
        self.api_secret = self.api_information["api_secret"]


        """ PostgreSQL information parsing """

        self.database_information = dict(config.items("PostgreSQL"))
        database = self.database_information["database"]
        user = self.database_information["user"]
        password = self.database_information["password"]
        host = self.database_information["host"]
        port = self.database_information["port"]
        if host == "localhost":
            host_numeric = "127.0.0.1"
        else:
            host_numeric = host
        
        self.conn = psycopg2.connect(database=database, 
                                     user=user, 
                                     password=password, 
                                     host=host, 
                                     port=port)
        
        self.c = self.conn.cursor()
        self.db_connection_url = "postgresql://{}:{}@{}:{}/{}".format(database, 
                                                                      password, 
                                                                      host_numeric, 
                                                                      port,
                                                                      user)
        self.engine = create_engine(self.db_connection_url)
        
        db_connection = {"c":self.c, "conn":self.conn, "engine":self.engine}
        
        
        """ Other modules """
        
        self.api_methods = ApiMethods()
        
        self.feed = DataFeed()
        
        self.delta_hedger = DeltaHedge(self.feed)
        
        self.client = WSClient(self.feed, self.delta_hedger, 
                               self.api_key, self.api_secret)
        
        self.save_bbo = SaveBBO(self.feed, db_connection)
        
        self.input_parser = InputParser(self.client, 
                                        self.feed, 
                                        self.api_methods, 
                                        self.delta_hedger)
        
        
    def run(self):
        
        if not self.client.connected:
            self.logger.info("Starting websocket client.")
            self.client.create_ws_connection()
            
            
        """ Separate thread saves all optoins best bid and offer to DB """
        
        self.save_bbo_thread = threading.Thread(target=lambda: self.save_bbo.schedule_snapshot())
        self.save_bbo_thread.start()
        
        
        """ Separate thread to reconnect daily for new contract introduction """
        
        self.reconnection_thread = threading.Thread(target=lambda: self.client.daily_reconnect())
        self.reconnection_thread.start()
        
        
        """ Main thread will call user input function, which runs forever """
        
        while True:
            if self.client.connected and not self.input_parser.shutdown_input:
                try:
                    self.input_parser.accept_user_input()
                except KeyboardInterrupt:
                    self.shutdown_all("KeyboardInterrupt")
                    break
                
                except Exception as e:
                    error_type, error_tb, tb = sys.exc_info()
                    filename, lineno, func_name, line = traceback.extract_tb(tb)[-1]
                    self.logger.info("Exception occured (main thread): {}".format(e))
                    self.logger.info("Error details: {} {} {} {}".format(
                        filename, lineno, func_name, line))                    
            
            elif self.input_parser.shutdown_input:
                self.shutdown_all("CLI quit")
                break
            
            else:
                time.sleep(0.5)
                
    def shutdown_all(self, reason):
        self.logger.info("{} - Shutting down.".format(reason))
        self.save_bbo.shutdown = True
        self.client.shutdown()
        
        self.client.t1.join()
        self.save_bbo_thread.join()
        self.reconnection_thread.join()
