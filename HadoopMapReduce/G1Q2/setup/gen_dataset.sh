#!/bin/bash
DBG=0

question=G1Q2

# dataset source dir
src_dir=/data/aviation/airline_ontime/*

# test dir
#src_dir=/home/ubuntu/avia_info/dataset1/*.zip

# HDFS output dir 
# this folder needs to be available on HDFS
dataset_dir=/dataset_T1/${question}

hdfs dfs -test -d $dataset_dir
if [ $? == 0 ]; then
   echo "HDFS folder $dataset_dir exists, script continue ..."
   hdfs dfs -rm $dataset_dir/*
else
   echo "HDFS folder $dataset_dir does not exist, create the folder"
   hdfs dfs -mkdir $dataset_dir
fi

# HDFS tmp dir
dataset_tmp=/dataset_T1/tmp_${question}

hdfs dfs -test -d $dataset_tmp
if [ $? == 0 ]; then
   echo "HDFS folder $dataset_tmp exists, script continue ..."
   hadoop fs -rm $dataset_tmp/*.*
else
   echo "HDFS folder $dataset_tmp does not exist, creating the folder..."
   hadoop fs -mkdir $dataset_tmp
fi

# HDFS PIG data tmp output
#pig_dir=${dataset_tmp}/pigdata

CURR_DIR=${PWD}
tmp_dir=/tmp/${question}
pig_dir=${tmp_dir}/pigdata
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

      unzip -l $file > /dev/null
      if [ $? == 0 ]; then
         unzip -c $file On_Time_On_Time_Performance_${year}_*.csv | sed '1,3d' | gzip > ${base}.gz

      else
         echo "File skipped: Incorrect format $files"
      fi

      # for sanity check
      if [ $DBG -eq 1 ] && [ $i -gt 1 ]; then
         echo "Sanity check: Reaching $i, exiting loop..."
         break
      fi
    
   done
 
   # for sanity check
   if [ $DBG -eq 1 ] && [ $n -gt 2 ]; then
      echo "Sanity check: Reaching $n, terminating..."
      break
   fi
	
done

echo 'Number of files cleaned:' $n

# hadoop fs -copyFromLocal *.gz ${dataset_tmp}/.
pig -x local -param PIG_IN_DIR=${tmp_dir} -param PIG_OUT_DIR=${pig_dir} -f ${CURR_DIR}/arr_delay.pig 

if [ -f ${pig_dir}/_SUCCESS ]; then
   echo 'Files processed successfully.' $n
   hdfs dfs -copyFromLocal ${pig_dir}/part* ${dataset_dir}
   echo 'Files generated in HDFS' ${dataset_dir}
else
   echo 'Failed!'
fi

cd $CURR_DIR

