from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import json
from pyspark.sql import SparkSession, SQLContext
import redis

redis_server = 'ec2-18-235-136-124.compute-1.amazonaws.com'
r = redis.StrictRedis(host=redis_server, port=6379, db=0)

def processPartition(partition, table, keyspace):
    print('data is in the partition-------------------------------------')
    redis_db = redis.StrictRedis(host='localhost', port=6379, db=0)
    #for x in partition:
        #json_data = json.loads(x)
    #schema = []
    spark = SparkSession(sc)
    hasattr(partition, "toDF")
    #df = sqlContext.createDataFrame(partition, schema)
    #print(x,'--------------------------------------------------------------')
    df = partition.toDF(schema =['timestamp','best_bid','best_ask','product','exchange'],,sampleRatio=0.2).write.format("org.apache.spark.sql.cassandra").mode("append").options(table=real_trip_table, keyspace=keyspace).save()
    df.show()
    #df.write.format("org.apache.spark.sql.redis").mode("append").options(table=real_trip_table, keyspace=keyspace).save()
    #redis_db.set('year', df)


def process(rdd):
    if rdd.isEmpty():
        print('data is empty---------------------------------------------------')
        return
    print('data is not empty-------------------------------------')
    print(type(rdd))
    print(rdd.take(5))
    print(rdd.getNumPartitions())
    #rdd.filter(lambda x: x is not '').
    rdd.foreachPartition(processPartition)

def f(x): print('ts in the function--------------------------',x)



sc = SparkContext(appName="Test")
sc.setLogLevel("WARN")

ssc = StreamingContext(sc, 5)

KAFKA_NODES = ['ec2-18-235-136-124.compute-1.amazonaws.com:9092', 'ec2-18-215-66-27.compute-1.amazonaws.com:9092',
               'ec2-18-214-218-100.compute-1.amazonaws.com:9092', 'ec2-52-205-137-129.compute-1.amazonaws.com:9092']


kvs = KafkaUtils.createDirectStream(ssc, ['Coinbase', 'Cex'],
                                                 {"metadata.broker.list": ','.join(KAFKA_NODES)})

parsed = kvs.map(lambda v: json.loads(v[1]))
print("----------------------------------------------Before Parsed")
parsed.count().pprint()
#parsed.forsaveAsTextFiles("spark/here.txt")
                                                                                                                                                                                          1,1           Top
parsed.foreachRDD(lambda x : processPartition(x,'trading','hft'))
ssc.start()
ssc.awaitTermination()