from ingestion.src.main import start_coinbase_producer
from ingestion.src.main import start_cex_producer
from ingestion.src.main import start_bitstamp_producer
from ingestion.src.main import start_okcoin_producer

if __name__ == '__main__':
    start_bitstamp_producer()
    #start_coinbase_producer()
    #start_cex_producer()