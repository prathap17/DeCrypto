import time
from ccxt import cex
from ccxt import bitstamp


from ingestion.src.coinbase_pro_producer import CoinbasePro

from ingestion.src.cex_producer import Cex

from ingestion.src.okcoin_bids import OkCoinBids

from ingestion.src.bitstamp_producer import BitStamp

""" 
   Created functions to start the multiple producers
   like okcoin, coinbase, bitstamp and cex 
"""

def start_okcoin_producer():
    wsClient = OkCoinBids()
    wsClient.start()


def start_coinbase_producer():
    wsClient = CoinbasePro()
    wsClient.start()


def start_cex_producer():
    products = ["BTC/USD", "ETH/USD", "BCH/USD",
                "ETH/BTC", "BCH/BTC"]
    while True:
        for product in products:
            data = Cex(product, cex().fetch_order_book(product))
            data.produce()


def start_bitstamp_producer():
    products = ["BTC/USD", "ETH/USD", "BCH/USD",
                "LTC/USD", "LTC/BTC", "ETH/BTC", "BCH/BTC"]

    while True:
        for product in products:
            data = BitStamp(product, bitstamp().fetch_order_book(product))
            data.produce()
