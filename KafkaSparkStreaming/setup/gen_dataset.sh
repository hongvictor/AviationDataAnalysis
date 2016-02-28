#!/bin/bash
DBG=0

question=ontimeperf

# dataset source dir
src_dir=/data/aviation/airline_ontime/*

# HDFS data dir 
# this folder needs to be available on HDFS
dataset_dir=/dataset/${question}


hdfs dfs -test -d $dataset_dir
if [ $? == 0 ]; then
   echo "HDFS folder $dataset_dir exists, script continue ..."
   hadoop fs -rm $dataset_dir/*
else
   echo "HDFS folder $dataset_dir does not exist, create the folder"
   hadoop fs -mkdir $dataset_dir
fi

# HDFS tmp dir
dataset_tmp=/dataset/tmp_${question}

hdfs dfs -test -d $dataset_tmp
if [ $? == 0 ]; then
   echo "HDFS folder $dataset_tmp exists, script continue ..."
   hadoop fs -rm $dataset_tmp/*.*
else
   echo "HDFS folder $dataset_tmp does not exist, creating the folder..."
   hadoop fs -mkdir $dataset_tmp
fi

# HDFS PIG data tmp output
pig_dir=${dataset_tmp}/pigdata
hdfs dfs -rm -r $pig_dir

CURR_DIR=${PWD}
tmp_dir=/tmp/${question}
rm -r $tmp_dir
mkdir $tmp_dir
cd $tmp_dir

n=0
for year_dir in $src_dir
do
   year=$(basename "$year_dir")

   i=0
   for file in $year_dir/*.zip
   do
      echo "Processing $file"
      let n=$n+1
      let i=$i+1

      filename=$(basename "$file")
      base="${filename%.*}"       

      # if it is good zip file then process
      unzip -l $file > /dev/null
      if  [ $? -eq 0 ]; then
         unzip -c $file ${base}.csv | sed '1,3d' | gzip > ${base}.gz
      else
         echo "File skipped: Incorrect file format ${file}" 
      fi

      # for sanity check
      if [ $DBG -eq 1 ] && [ $i -gt 1 ]; then
         echo "Sanity check: Reaching $i, exiting loop..."
         break
      fi
    
   done

   # for sanity check
   if [ $DBG -eq 1 ] && [ $n -gt 4 ]; then
      echo "Sanity check: Reaching $n, terminating..."
      break
   fi

done

echo 'Number of files cleaned:' $n

hadoop fs -copyFromLocal *.gz ${dataset_tmp}/.
pig -x mapreduce -param PIG_IN_DIR=${dataset_tmp} -param PIG_OUT_DIR=${pig_dir} -f ${CURR_DIR}/ontimeperf.pig 

hadoop fs -test -f ${pig_dir}/_SUCCESS 
if [ $? -eq 0 ]; then
   echo 'Files processed successfully.' $n
   hadoop fs -mv ${pig_dir}/part* ${dataset_dir} 
   echo 'Files generated in HDFS' ${dataset_dir} 
else 
   echo 'Failed!'
fi

cd $CURR_DIR
