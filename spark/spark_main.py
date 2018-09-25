from streaming import SparkStreamConsumer
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
KAFKA_NODES = ['ec2-54-84-42-80.compute-1.amazonaws.com:9092', 'ec2-18-211-13-85.compute-1.amazonaws.com:9092',
               'ec2-52-0-129-251.compute-1.amazonaws.com:9092', 'ec2-18-215-20-238.compute-1.amazonaws.com:9092']




if __name__ == '__main__':
    # Kraken asset pairs for BTC, ETH, and LTC to USD prices
    

    #consumer = SparkStreamConsumer()
    #consumer.consume_spreads(['Coinbase'])
    #consumer.start_stream()
    sc = SparkContext(appName='SparkStream', master='spark://ec2-54-84-42-80.compute-1.amazonaws.com:7077')
    ssc = StreamingContext(sc)
    kvs = KafkaUtils.createDirectStream(ssc, ['Coinbase'],
                                                 {'metadata.broker.list': ','.join(KAFKA_NODES)})
        # messages come in [timestamp, bid, ask] format
    parsed = kvs.map(lambda v: json.loads(v[1]))
        

        
    parsed.pprint()

    

