import time
from ccxt import cex
from ccxt import bitstamp
from ccxt import livecoin

from ingestion.src.coinbase_pro import CoinbasePro
from ingestion.src.cex import Cex
from ingestion.src.livecoin import LiveCoin
from ingestion.src.bitstamp import BitStamp

def start_coinbase_producer():
    wsClient = CoinbasePro()
    wsClient.start()

def start_cex_producer():
    products =["BTC/USD", "ETH/USD", "BCH/USD"]  
    while True:
        for product in products:
            data = Cex( product, cex().fetch_order_book(product, 10))
            data.produce()


def start_livecoin_producer():
    products =["BTC/USD"]  
    
    for product in products:
        data = LiveCoin( product, livecoin().fetch_order_book(product, 10))
        data.produce()

def start_bitstamp_producer():
    products =["BTC/USD"]  
    
    while True:
        for product in products:
            data = BitStamp( product, bitstamp().fetch_order_book(product, 10))
            data.produce()
