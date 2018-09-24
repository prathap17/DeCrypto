from decimal import Decimal
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
#import redis
KAFKA_NODES = ['ec2-54-84-42-80.compute-1.amazonaws.com:9092', 'ec2-18-211-13-85.compute-1.amazonaws.com:9092',
               'ec2-52-0-129-251.compute-1.amazonaws.com:9092', 'ec2-18-215-20-238.compute-1.amazonaws.com:9092']


def pp(partition):
    for msg in partition:
        x =0 
class SparkStreamConsumer :
    def __init__(self, slide_interval=5, window_length=15):
        self.sc = SparkContext(appName='SparkStream', master='spark://ec2-54-84-42-80.compute-1.amazonaws.com:7077')
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

        #print(parsed)

        parsed.foreachRDD(lambda rdd: rdd.foreachPartition(pp))
