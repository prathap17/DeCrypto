import time
from ccxt import cex
from ccxt import bitstamp

from ingestion.src.coinbase_pro_asks import CoinbaseProAsks
from ingestion.src.coinbase_pro_bids import CoinbaseProBids

from ingestion.src.cex_asks import CexAsks
from ingestion.src.cex_bids import CexBids

from ingestion.src.okcoin_bids import OkCoinBids

from ingestion.src.bitstamp_asks import BitStampAsks
from ingestion.src.bitstamp_bids import BitStampBids


def start_okcoin_producer():
    wsClient = OkCoinBids()
    wsClient.start()


def start_coinbase_asks_producer():
    wsClient = CoinbaseProAsks()
    wsClient.start()


def start_coinbase_bids_producer():
    wsClient = CoinbaseProBids()
    wsClient.start()


def start_cex_asks_producer():
    products = ["BTC/USD", "ETH/USD", "BCH/USD"]
    while True:
        for product in products:
            data = CexAsks(product, cex().fetch_order_book(product))
            data.produce()


def start_cex_bids_producer():
    products = ["BTC/USD", "ETH/USD", "BCH/USD"]
    while True:
        data = CexBids(products, cex().fetch_order_book(products))
        data.produce()


def start_bitstamp_bids_producer():
    products = ["BTC/USD", "ETH/USD"]

    while True:
        for product in products:
            data = BitStampBids(product, bitstamp().fetch_order_book(product))
            data.produce()


def start_bitstamp_asks_producer():
    products = ["BTC/USD", "ETH/USD"]

    while True:
        for product in products:
            data = BitStampAsks(product, bitstamp().fetch_order_book(product))
            data.produce()