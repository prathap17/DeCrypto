import time
from ccxt import cex
from ccxt import bitstamp
from ccxt import livecoin

from ingestion.src.coinbase_pro import CoinbasePro
from ingestion.src.cex import Cex
from ingestion.src.bitstamp import Bitstamp
from ingestion.src.livecoin import LiveCoin

def start_coinbase_producer():
    wsClient = CoinbasePro()
    wsClient.start()

def start_cex_producer():
    products =["BTC/USD", "ETH/USD", "BCH/USD"]  
    
    for product in products:
        data = Cex( product, cex().fetch_order_book(product, 10))
        data.produce()

def start_bitstamp_producer():
    products =["BTC/USD"]  
    
    for product in products:
        data = Bitstamp( product, bitstamp().fetch_order_book(product))
        data.produce()

def start_livecoin_producer():
    products =["BTC/USD"]  
    
    for product in products:
        data = LiveCoin( product, livecoin().fetch_order_book(product, 10))
        data.produce()

