import time
from ccxt import cex
from ccxt import bitstamp

from ingestion.src.coinbase_pro import CoinbasePro
from ingestion.src.cex import Cex
from ingestion.src.bitstamp import Bitstamp
def start_coinbase_producer():
    wsClient = CoinbasePro()
    wsClient.start()

def start_cex_producer():
    products =["BTC/USD", "ETH/USD", "BCH/USD"]  
    
    for product in products:
        data = Cex( product, cex().fetch_order_book(product, 10))
        data.produce()

def start_bitstamp_producer():
    products =["BTC/USD", "ETH/USD", "BCH/USD"]  
    
    for product in products:
        data = Bitstamp( product, cex().fetch_order_book(product, 10))
        data.produce()

