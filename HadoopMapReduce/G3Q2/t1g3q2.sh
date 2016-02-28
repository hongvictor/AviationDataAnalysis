#!/bin/bash
pig -Dpig.additional.jars=/usr/share/cassandra/*.jar:/usr/share/cassandra/lib/*.jar -param PIG_IN_DIR=/tmp/G3Q2  -x local -f bestroute.pig
