from datetime import datetime
from datetime import timedelta
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra import ReadTimeout
import pandas as pd
import datetime


class FetchData():
    def __init__(self, cond):
        self.cassandra_host_name = "ec2-54-85-200-216.compute-1.amazonaws.com"
        self.cassandra_keyspace = "hft"
        self.cassandra_table = "final_trades_test"
        self.time_now = datetime.datetime.utcnow().strftime(
            "%Y-%m-%dT%H:%M:%S+0000")
        time_old = datetime.datetime.utcnow() - timedelta(
            hours=0, minutes=0, seconds=10)
        self.time_old = time_old.strftime("%Y-%m-%dT%H:%M:%S+0000")
        cluster = Cluster([self.cassandra_host_name])
        self.session = cluster.connect(self.cassandra_keyspace)
        self.session.row_factory = dict_factory
        if (cond == 1):
            query = "SELECT MAX(asks),product,market,len_asks FROM final_trades_test group by product;"
            self.prepared_query = self.session.prepare(query)
        else:
            query = "SELECT MAX(bids),product,market,len_bids FROM final_trades_test group by product;"
            self.prepared_query = self.session.prepare(query)

    def get_data_asks(self):
        print((self.prepared_query))
        result_set = self.session.execute_async(self.prepared_query)
        try:
            rows = result_set.result()
            df = pd.DataFrame(list(rows))
            if (len(df.index) == 0):
                return None
        except ReadTimeout:
            self.log.exception("Query timed out:")
        return df

    def get_data_bids(self):
        result_set = self.session.execute_async(self.prepared_query)
        try:
            rows = result_set.result()
            df = pd.DataFrame(list(rows))
            if (len(df.index) == 0):
                return None
        except ReadTimeout:
            self.logging.info.exception("Query timed out:")
        return df