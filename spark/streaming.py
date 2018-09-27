



from decimal import Decimal
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
#import redis
from pyspark.streaming.kafka import KafkaUtils
KAFKA_NODES = []

r = redis.StrictRedis(host='redis://localhost', port=6379, db=0)

def set_redis_data(partition):
    for msg in partition:
        print("It is in the function___________________________________________")
        print(msg)
        (r.hsetnx(msg[3], 'bid', msg[1]))
        print("redis-----------------------------------------------------------data")
        r.hsetnx(msg[3], 'ask', msg[2])


class SparkStreamConsumer:
    def __init__(self, slide_interval=5, window_length=15):
        self.sc = SparkContext(appName='SparkStream', master='spark://ec2-18-235-136-124.compute-1.amazonaws.com:7077')
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

        parsed = self.kvs.window(self.window_length, self.slide_interval).map(lambda v: json.loads(v[1]))

        print("the data is")

        parsed.foreachRDD(lambda rdd: rdd.foreachPartition(set_redis_data))
        print("data-------------------------------------is passed")
  
          
       