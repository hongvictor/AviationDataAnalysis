"""
 Print top 10 airports 
 Usage: airportcount.py <zk> <topic>
 
 To run this on your local machine, you need to setup Kafka and 
 create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart
 and then run the example
 $ bin/spark-submit --jars \
   external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
   examples/src/main/python/streaming/airportcount.py \
   localhost:2181 TopicG1Q1`

"""
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def print_top_airports(rdd):
    top_airports = rdd.take(10)
    print("*************************************")
    print("Top 10 airports (%d total):" % rdd.count())
    print("-------------------------------------")
    for tuple in top_airports:
        print("\t%s \t%d" % (tuple[1], tuple[0]))
    print("*************************************")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: airportcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="KafkaAirportCount")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("hdfs://ip-172-31-61-76:9000/checkpoint/G1Q1")

    def updateFunc(new_values, last_sum):
        return sum(new_values) + (last_sum or 0)

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {"TopicG1Q1": 12})
    lines = kvs.map(lambda x: x[1])

    counts = lines.flatMap(lambda x:[x.split("\t")[2], x.split("\t")[3]]) \
        .map(lambda x: (x, 1)) \
        .updateStateByKey(updateFunc) \
        .map(lambda x:(x[1],x[0])) \
        .transform(lambda x: x.sortByKey(ascending=False))
    
    counts.foreachRDD(print_top_airports)
    ssc.start()
    ssc.awaitTermination()

