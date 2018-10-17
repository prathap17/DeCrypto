from datetime import datetime
from datetime import timedelta
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from cassandra import ReadTimeout
import pandas as pd
import datetime
import logging
log = logging.getLogger('module')

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
        self.log = logging.getLogger('module.FetchData')

    def start_connection(self):
        cluster = Cluster([self.cassandra_host_name])
        session = cluster.connect(self.cassandra_keyspace)
        session.row_factory = dict_factory
        return session

    def prepare_query(self, session):
        query = "SELECT * FROM final_trades_test \
        WHERE time > ? AND time < ? AND product = ? ALLOW FILTERING"
        return session.prepare(query)

    
    def get_data(self, prepared_query, session, product):
        result_set = session.execute_async(prepared_query, parameters =[self.time_old, self.time_now, product])
        try:
            rows = result_set.result()
            df = pd.DataFrame(list(rows))
            if (len(df.index) == 0):
                return None
        except ReadTimeout:
            self.log.exception("Query timed out:")
        return df

    