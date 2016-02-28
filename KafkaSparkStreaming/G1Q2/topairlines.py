"""
 Print top 10 airlines 
 Usage: topairlines.py <zk> <topic>
 
 To run this on your local machine, you need to setup Kafka and 
 create a producer first, see
 http://kafka.apache.org/documentation.html#quickstart
 and then run the example
$ bin/spark-submit --jars \
  external/kafka-assembly/target/scala-*/spark-streaming-kafka-assembly-*.jar \
  examples/src/main/python/streaming/airportcount.py \
  localhost:2181 testTopic`
"""
from __future__ import print_function
from __future__ import division

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

def print_top_airlines(rdd):
    top_airlines = rdd.take(10)
    print("*************************************")
    print("Top 10 airlines (%d total):" % rdd.count())
    print("-------------------------------------")
    for tuple in top_airlines:
        print("\t%s \t%f" % (tuple[1], tuple[0]))
    print("*************************************")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: topairlines.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="KafkaTopAirlines")
    ssc = StreamingContext(sc, 60)
    ssc.checkpoint("hdfs://ip-172-31-61-76:9000/checkpoint/G1Q2")

  
    def updateFunc(new_values_pair, last_sum_pair = None):

        if last_sum_pair is None:
            last_sum_pair = (0,0)

        try:
           x = new_values_pair[0][0] + last_sum_pair[0]
           y = new_values_pair[0][1] + last_sum_pair[1]
           z = (x,y)
        except IndexError:
           z = last_sum_pair
        return z

    '''
    def updateFunc(new_values_pair, last_sum_pair = None):

        if last_sum_pair is None:
            last_sum_pair = [(0,0)]

        try:
           x = new_values_pair[0][0] + last_sum_pair[0][0]
           y = new_values_pair[0][1] + last_sum_pair[0][1]
           z = [(x,y)]
        except IndexError:
           z = last_sum_pair
        return z
    '''
    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {"TopicG1Q2": 12})
    lines = kvs.map(lambda x: x[1])

    counts = lines.map(lambda line: (line.split("\t")[0], line.split("\t")[5])) \
        .combineByKey(lambda value: (int(float(value)), 1), 
                      lambda x, value: (x[0] + int(float(value)), x[1] + 1),
                      lambda x, y: (x[0] + y[0], x[1] + y[1]))  \
        .updateStateByKey(updateFunc)  \
        .mapValues(lambda sum_count:  1 - sum_count[0] / sum_count[1]) \
        .map(lambda x:(x[1],x[0])) \
        .transform(lambda x: x.sortByKey(ascending=False))
    
    counts.foreachRDD(print_top_airlines)
#    counts.pprint()
    ssc.start()
    ssc.awaitTermination()
