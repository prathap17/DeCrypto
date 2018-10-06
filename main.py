from ingestion.src.main import start_coinbase_asks_producer
from ingestion.src.main import start_coinbase_bids_producer
from ingestion.src.main import start_cex_bids_producer
from ingestion.src.main import start_cex_asks_producer
from ingestion.src.main import start_bitstamp_bids_producer
from ingestion.src.main import start_bitstamp_asks_producer
from ingestion.src.main import start_okcoin_producer

if __name__ == '__main__':
    # start_coinbase_asks_producer()
    # start_coinbase_bids_producer()

    start_cex_bids_producer()
    # start_cex_asks_producer()

    # start_bitstamp_bids_producer()
    # start_bitstamp_asks_producer()
    # start_okcoin_producer()