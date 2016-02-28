#!/bin/bash
hadoop fs -rm -r /output_T1/G2Q3
hadoop fs -mkdir /output_T1/G2Q3
pig -Dpig.additional.jars=/usr/share/cassandra/*.jar:/usr/share/cassandra/lib/*.jar -param PIG_IN_DIR=/dataset_T1/G2 -f topairlines_per_xy.pig
rm -r output
mkdir output
hadoop fs -copyToLocal /output_T1/G2Q3/* output/*
