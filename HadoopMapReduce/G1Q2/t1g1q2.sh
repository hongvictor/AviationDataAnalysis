#!/bin/bash
hadoop fs -rm /output_T1/G1Q2/*
hadoop jar TopOntimeArrival.jar TopOntimeArrival -D N=10 /dataset_T1/G1Q2 /output_T1/G1Q2 
hadoop fs -copyToLocal /output_T1/G1Q2/* output/*
