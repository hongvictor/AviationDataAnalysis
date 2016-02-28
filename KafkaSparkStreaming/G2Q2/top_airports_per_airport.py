"""
 For each airport X, rank the top-10 airports in decreasing order of 
 on-time departure performance from X.

 This program prints top 10 airports per airport 
 Usage: top_airports_per_airport.py <zk> <topic>
"""
from __future__ import print_function
from __future__ import division

import sys
import heapq

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import pyspark_cassandra
from pyspark_cassandra import streaming

def takeOrderedByKey(self, num, sortValue = None, reverse=False):
 
        def init(a):
            return [a]
 
        def combine(agg, a):
            agg.append(a)
            return getTopN(agg)
 
        def merge(a, b):
            agg = a + b
            return getTopN(agg)
 
        def getTopN(agg):
            if reverse == True:
                return heapq.nlargest(num, agg, sortValue)
            else:
                return heapq.nsmallest(num, agg, sortValue)              
 
        return self.combineByKey(init, combine, merge)
 
 
from pyspark.rdd import RDD
RDD.takeOrderedByKey = takeOrderedByKey

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: top_airports_per_airport.py <zk> <topic>", file=sys.stderr)
        exit(-1)

    sc = SparkContext(appName="Top airpots per Airport")
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint("hdfs://ip-172-31-61-76:9000/checkpoint/G2Q2")

  
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

    zkQuorum, topic = sys.argv[1:]
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1})
    lines = kvs.map(lambda x: x[1])

    counts = lines.map(lambda line:((line.split("\t")[2], 
                                     line.split("\t")[3]),  
                                 int(line.split("\t")[4]))) \
        .combineByKey(lambda value: (value, 1), 
                      lambda x, value: (x[0] + value, x[1] + 1),
                      lambda x, y: (x[0] + y[0], x[1] + y[1]))  \
        .updateStateByKey(updateFunc)  \
        .mapValues(lambda sum_count:  1 - sum_count[0] / sum_count[1])  \
        .map(lambda x: (x[0][0], (x[0][1], x[1])))  \
        .transform(lambda x: x.takeOrderedByKey(10, sortValue=lambda v: v[1], reverse=True))  \
        .mapValues(lambda x: dict(x))  \
        .map(lambda x: {"origin":x[0], "airports":x[1]})   \
        .saveToCassandra("capstone", "top_ten_airports")

#    counts.pprint();
    ssc.start()
    ssc.awaitTermination()
