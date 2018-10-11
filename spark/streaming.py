from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark.sql import SparkSession, SQLContext
import redis
from pyspark import SparkConf, SparkContext
from pyspark import SparkContext
SparkContext.setSystemProperty("spark.cassandra.connection.host",
                               'ec2-54-85-200-216.compute-1.amazonaws.com')


def processPartition(partition, table, keyspace, sc):
    if partition.isEmpty():
        return
    else:
        spark = SparkSession(sc)

        def f(accum, x):
            if ('asks' in list(accum.keys()) and 'asks' in list(x.keys())):
                if ((float(accum['bids']) - float(accum['asks'])) <
                        (float(x['bids']) - float(x['asks']))):
                    return accum
                else:
                    return x
            else:
                if ('asks' in list(accum.keys())):
                    return accum
                elif ('asks' in list(x.keys())):
                    return x

        partition.map(lambda x: ((x['time'] + x['product']), x)).filter(
            lambda x: x is not None).filter(lambda x: x != "").reduceByKey(f)

        hasattr(partition, "toDF")
        df = partition.map(lambda t: Row(id=t[0], **t[1])).toDF()
        df.show()

        df.write.format("org.apache.spark.sql.cassandra").mode(
            "append").options(
                table=table, keyspace=keyspace).save()


class SparkConsumer:
    def __init__(self):
        self.sc = SparkContext(
            appName='Stream',
            master='spark://ec2-18-235-136-124.compute-1.amazonaws.com:7077')
        self.sc.setLogLevel("WARN")
        self.ssc = StreamingContext(self.sc, 1)

    def start_stream(self):
        self.ssc.start()
        self.ssc.awaitTermination()

    def consume(self, spread_topics):
        kafka_data = KafkaUtils.createDirectStream(
            self.ssc, spread_topics,
            {'metadata.broker.list': ','.join(KAFKA_NODES)})

        parsed_test = kafka_data.map(lambda v: (json.loads(v[1])))

        parsed_test.foreachRDD(
            lambda x: processPartition(x, 'final_trades', 'hft', self.sc))