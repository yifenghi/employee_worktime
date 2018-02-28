#!/bin/bash

set -ux

work_dir=$(readlink -f $(dirname $0))/..
source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class="com.doodod.staffmanagement.statistic.MergeLauncher"
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml
if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date time_tag"
  exit 1
fi

hour_tag=`date -d "$date" "+%H%M"`
dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
dir_name_old=`date -d "$date 15 minute ago" "+%Y%m%d/%H/%M"`

input_part="$statistic/hour/$dir_name/part*"
input_empty="$statistic/hour/empty"
hexist $input_part
if [ $? != 0 ]
then
  input_part=$input_empty
fi
input_total="$statistic/day/$dir_name_old/part*"
hexist $input_total
if [ $? != 0 ]
then input_total=$input_empty
fi

if [ $hour_tag == $hour_empty_tag ]
then
  input_total=$input_empty
fi
#exit 

output="$statistic/day/$dir_name"
hrmr $output 
lib_jars="/var/lib/hadoop-hdfs/lifeng/employee_statistic/libjar/employee_statistic-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
main_jar="/var/lib/hadoop-hdfs/lifeng/employee_statistic/target/employee_statistic-0.0.1-SNAPSHOT.jar"
HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-libjars $lib_jars \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D merge.input.part="$input_part" \
-D merge.input.total="$input_total" \

exit 0;

