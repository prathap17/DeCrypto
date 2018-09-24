import time

from ingestion.src.coinbase_pro import CoinbasePro
from ingestion.src.bitfinex import BitfinexProducer

def start_coinbase_producer():
    wsClient = CoinbasePro()
    wsClient.start()

def start_bitfinex_producer():
    wsClient = BitfinexProducer()
    wsClient.start()