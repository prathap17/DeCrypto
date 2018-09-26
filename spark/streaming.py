from decimal import Decimal
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import redis



# These redis commands need to be defined outside of a class as it is passed in a dstream
r = redis.StrictRedis(host='', port=6379, db=0)


def set_redis_avg_spread(partition):
    for msg in partition:
        r.hset(msg[0], 'avg_spread', msg[1])


def set_redis_bid_ask(partition):
    for msg in partition:
        r.hset(msg[3], 'bid', msg[1])
        r.hset(msg[3], 'ask', msg[2])
        r.hset(msg[3], 'spread', Decimal(msg[2]) - Decimal(msg[1]))


class SparkStreamConsumer:
    def __init__(self, slide_interval=5, window_length=15):
        self.sc = SparkContext(appName='SparkStream', master='spark://.compute-1.amazonaws.com:7077')
        self.ssc = StreamingContext(self.sc, slide_interval)
        self.slide_interval = slide_interval
        self.window_length = window_length

    def start_stream(self):
        self.ssc.start()
        self.ssc.awaitTermination()

    def consume_spreads(self, spread_topics):
        self.kvs = KafkaUtils.createDirectStream(self.ssc, spread_topics,
                                                 {'metadata.broker.list': ','.join(KAFKA_NODES)})
        # messages come in [timestamp, bid, ask] format
        parsed = self.kvs.window(self.window_length, self.slide_interval).map(lambda v: json.loads(v[1])).cache()

        # set the current bid and ask prices in Redis
        parsed.foreachRDD(lambda rdd: rdd.foreachPartition(set_redis_bid_ask))

        # calculate the spread of each transaction and append a count value
        # input: [timestamp, bid, ask, asset_pair] output: (asset_pair, spread, 1)
        def calculate_spread(tx):
            asset_pair = tx[3]
            return (asset_pair, Decimal(tx[2]) - Decimal(tx[1]))

        spread_percentage_dstream = parsed.map(calculate_spread).mapValues(lambda x: (x, 1))

        print(spread_percentage_dstream)