from datetime import datetime
from datetime import timedelta
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra import ReadTimeout
import pandas as pd
import datetime

"""
   FetchData connects to the cassandra database and gets the 
   best exchange based on the bids and ask at given discrete time
"""
class FetchData():
    def __init__(self):
        self.cassandra_host_name = "ec2-54-85-200-216.compute-1.amazonaws.com"
        self.cassandra_keyspace = "hft"
        self.cassandra_table = "final_trades_test"
        self.time_now = datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%H:%M:%S+0000")
        time_old = datetime.datetime.utcnow() - timedelta(
            hours=0, minutes=0, seconds=10)
        self.time_old = time_old.strftime("%Y-%m-%dT%H:%M:%S+0000")
        self.log = ''

    def start_connection(self):
        cluster = Cluster([self.cassandra_host_name])
        session = cluster.connect(self.cassandra_keyspace)
        session.row_factory = dict_factory
        return session

    def prepare_asks_query(self, session):
        query = "SELECT MAX(asks),product,market,len_asks FROM final_trades_test \
        WHERE time > self.time_old AND time < self.time_now group by product;"
        return session.prepare(query)

    def prepare_bids_query(self, session):
        query = "SELECT MAX(bids),product,market,len_bids FROM final_trades_test \
        WHERE time > self.time_old AND time < self.time_now group by product;"
        return session.prepare(query)

    def get_data_asks(self, prepared_query, session):
        result_set = session.execute_async(prepared_query)
        try:
            rows = result_set.result()
            df = pd.DataFrame(list(rows))
            if (len(df.index) == 0):
                return None
        except ReadTimeout:
            log.exception("Query timed out:")
        return df

    def get_data_bids(self, prepared_query, session):
        result_set = session.execute_async(prepared_query)
        try:
            rows = result_set.result()
            df = pd.DataFrame(list(rows))
            if (len(df.index) == 0):
                return None
        except ReadTimeout:
            log.exception("Query timed out:")
        return df