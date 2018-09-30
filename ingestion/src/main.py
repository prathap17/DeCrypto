import time
from ccxt import cex

from ingestion.src.coinbase_pro import CoinbasePro
from ingestion.src.cex import Cex
def start_coinbase_producer():
    wsClient = CoinbasePro()
    wsClient.start()

def start_cex_producer():
    products =["BTC/USD", "ETH/USD", "BCH/USD"]  
    
    for product in products:
        data = Cex( product, cex().fetch_order_book(product, 10))
        data.produce()

