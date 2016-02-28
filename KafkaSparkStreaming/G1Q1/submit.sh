#!/bin/bash
/usr/local/spark/bin/spark-submit  \
--master spark://ip-172-31-61-76:7077  \
--executor-memory 6G \
--total-executor-cores 9 \
--packages org.apache.spark:spark-streaming-kafka_2.10:1.6.0  \
topairports.py ip-172-31-61-76:2181 TopicG1Q1
