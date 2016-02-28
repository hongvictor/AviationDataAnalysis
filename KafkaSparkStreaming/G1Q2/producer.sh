#!/bin/bash

INPUT=/dataset/ontimeperf/part*
TOPIC=TopicG1Q2

for F in $(hdfs dfs -ls $INPUT | awk '{print $8}')
do
   echo "Processing $F"
   hdfs dfs -cat $F | ~/kafka/bin/kafka-console-producer.sh --broker-list ip-172-31-61-76:9092,ip-172-31-61-76:9093,ip-172-31-61-76:9094,ip-172-31-61-76:9095 --topic $TOPIC

done
