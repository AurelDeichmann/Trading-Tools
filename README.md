# Trading-Tools

This project aims to assist in manual trading and to enable automated trading in Bitcoin Futures and Options contracts on the Deribit derivative exchange. Functional versions will be uploaded with an overview of features over time. It is work in progress - lots of work remains to be done and many aspects may surely be improved. It’s modular design should make it relatively easy to integrate future modules into the current groundwork. However, as complexity grows, it may be split into separate projects over time. 

As of now, it has the following features:
- continuous connection to multiple websocket data streams from the exchange
- local replication of all options order books as well as futures top of the book quotes
- local replication and updating of open orders and current positions
- storage of all options top of the book quotes to a PostgreSQL database every one minute
- interpolation of constant maturity, constant log moneyness implied volatility surface stored in a PostgreSQL database every one minute
- automated dynamic delta hedging of any open options positions
- command line interface to accept user input to trade futures contracts manually via API

Future additions (some slightly further down the line):
- execution assistance across different option contracts to reach specific exposure
- automated dynamic theta, vega etc. hedging
- automated futures vs. perpetual swap spread trading
- automated futures market making strategies

Some of these future additions are rather ambitious and unspecific. To me, this project serves as a means to enhance my programming skills, to get some hands-on experience in, and to have continuous exposure to these topics. Let's see how far we get.

# Requirements
- PostgreSQL v9+

Python packages:
- websocket-client v1+
- psycopg2
- sqlalchemy
- py-vollib-vectorized
- Other standard libraries such as pandas, scipy, datetime, pytz, logging, threading, json etc.


# How to run

- Install PostgreSQL (+ set up a Database, schema and tables are created automatically when running the program)
- Open an account on deribit.com
- Create a pair of API keys on deribit.com
- Store API key information and PostgreSQL database information in the settings.txt file
- python3 run.py in a python virtual environment 
  1. python3 -m venv [environment_name] 
  2. source [environment_name]/bin/activate)

# Specific features
The **ws_client.py** module connects to the exchange’s API and handles incoming data as well as some outgoing data. It subscribes to several channels, which in turn result in different streams of data coming in. The following streams are connected to:
- All options contracts entire orderbook
- All options contracts ticker data, importantly including open interest data
- All futures contracts top of the book data (best bid and best offer)
- The trading accounts orders
- The trading accounts account information
- The trading accounts executed trades

The program reconnects every day at 8:00am UTC as some contracts expire at this time and others are introduced. This way, all alive contracts are captured reliably. 

The **data_feed.py** module receives all the incoming data, structures and stores it in memory, and makes it available for other modules. 

The **save_top_of_book.py** module snapshots all locally replicated options orderbooks. It then filters out best bids and offers for each contract, and drops contracts which have no bids or offers even though they are ‘alive’ contracts. It then calculates implied volatilities and stores this data to the Postgres database every full minute. 

The **volatility_index.py** module picks up where save_top_of_book.py left off. Its goal is to create implied volatility index values which are comparable over time. In order to do so, it uses the top of the book quotes from all contracts to linearly interpolate implied volatility values for constant maturities, specifically maturities that there exists no active contract for. It then does a similar interpolation for specific log moneyness values. The result is a rudimentary version of a volatility surface implied by the current options chain that can be compared over time. This again is stored to a database every full minute. Of course, the method of linearly interpolating things is imperfect at best. Future updates will use more sophisticated methods. 

The **hedger.py** module allows to delta hedge net options positions in one specific futures instrument.  Notably, it does not net all futures positions as a delta hedge and this is on purpose. In order to prevent infinite trading loops, there is an allowed mismatch between the net options delta and the futures delta. If this mismatch is exceeded, an order will be sent in the futures contract to match the options delta in opposite as closely as the minimum tick sizes allow. Currently, this mismatch is set to 0.25% of the underlying value. At a BTCUSD price of 20.000, it would therefore rehedge once the delta mismatch is larger than $50. For ATM or ITM contracts, it may be useful to increase this threshold. Eventually, it may be tied to the moneyness of a contract directly via some function. 

The **custom_input_parser.py** module allows for user input to be translated into sending orders, cancelling them or activating the delta hedging module for example. There are a number of commands supported. Commands to trade are essentially keyboard shortcuts designed for both hands for speed. An overview can be found in the module itself, or by typing 'help'.
