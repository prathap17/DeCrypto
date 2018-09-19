
import time

from ingestion.src.coinbase_pro import CoinbasePro



def start_coinbase_producer():
    wsClient = CoinbasePro()
    wsClient.start()
