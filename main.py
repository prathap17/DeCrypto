from ingestion.src.main import start_coinbase_producer
from ingestion.src.main import start_cex_producer
from ingestion.src.main import start_bitstamp_producer
from ingestion.src.main import start_livecoin_producer
from ingestion.src.main import start_bitstamp_producer




if __name__ == '__main__':
    start_coinbase_producer()
    #start_cex_producer()
    #start_bitfinex_producer()
    #while True:
        #start_bitstamp_producer()
        #start_livecoin_producer()
        #start_cex_producer()
        #start_bitstest_producer()
    