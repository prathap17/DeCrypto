from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark.sql import SparkSession, SQLContext
import redis
from pyspark import SparkConf, SparkContext
from pyspark import SparkContext
SparkContext.setSystemProperty("spark.cassandra.connection.host",'ec2-54-85-200-216.compute-1.amazonaws.com')




def processPartition(partition, table, keyspace):
    if partition.isEmpty():
        print('data is empty---------------------------------------------------')
        return
    else:
        print("this is the partition ---------------------------",partition)
        spark = SparkSession(sc)
        hasattr(partition, "toDF")
        df = partition.toDF(schema =['timestamp','best_bid','best_ask','product','exchange'])
        df.show()
        df.write.format("org.apache.spark.sql.cassandra").mode("append").options(table=table, keyspace=keyspace).save()
    
        
class SparkStreamConsumer:
    def __init__(self):
        self.sc = SparkContext(appName='Stream', master='spark://ec2-18-235-136-124.compute-1.amazonaws.com:7077')
        self.sc.setLogLevel("WARN")
        self.ssc = StreamingContext(self.sc, 60)

    def start_stream(self):
        self.ssc.start()
        self.ssc.awaitTermination()

    def consume(self, spread_topics):
        kafka_data = KafkaUtils.createDirectStream(self.ssc, spread_topics,
                                                 {'metadata.broker.list': ','.join(KAFKA_NODES)})

        parsed = kafka_data.map(lambda v: json.loads(v[1]))
        

        print("the data is")

        parsed.count().pprint()

        parsed.foreachRDD(lambda x : processPartition(x,'trading','hft'))

        
       
  
          
       