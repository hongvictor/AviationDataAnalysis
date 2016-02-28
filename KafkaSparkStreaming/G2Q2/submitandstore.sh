#!/bin/bash
/usr/local/spark/bin/spark-submit  \
--master spark://ip-172-31-61-76:7077  \
--executor-memory 12G \
--total-executor-cores 36 \
--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0,TargetHolding/pyspark-cassandra:0.2.7  \
--conf spark.cassandra.connection.host=172.31.61.75 \
top_airports_per_airport.py ip-172-31-61-76:2181 TopicG2Q2
