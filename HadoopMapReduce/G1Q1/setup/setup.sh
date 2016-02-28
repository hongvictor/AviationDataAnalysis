#!/bin/bash
# misc dir
hadoop fs -mkdir -p /T1/misc
echo " \t,;.?!-:@[](){}_*/" > delimiters.txt
hadoop fs -copyFromLocal delimiters.txt /T1/misc

