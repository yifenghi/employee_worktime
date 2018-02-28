#!/bin/bash

set -ux

work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.staffmanagement.statistic.EmployeeLauncher1
job_name=$(basename ${0%.sh})

if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date"
  exit 1
fi

date_5min_ago=`date -d "$date 5 minute ago" "+%Y%m%d/%H/%M"`
date_10min_ago=`date -d "$date 10 minute ago" "+%Y%m%d/%H/%M"`
#date_15min_ago=`date -d "$date 15 minute ago" "+%Y%m%d/%H/%M"`
dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
input="/group/doodod/mall/statistic/min/$date_5min_ago/part-r-00000,/group/doodod/mall/statistic/min/$date_10min_ago/part-r-00000,/group/doodod/mall/statistic/min/$dir_name/part-r-00000"
output="$statistic/hour/$dir_name"
hrmr $output
lib_jars="/var/lib/hadoop-hdfs/lifeng/employee_statistic/Statistic-1.0-SNAPSHOT-jar-with-dependencies.jar"
#lib_jars="/var/lib/hadoop-hdfs/lifeng/employee_statistic/libjar/employee_statistic-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
main_jar="/var/lib/hadoop-hdfs/lifeng/employee_statistic/target/employee_statistic-0.0.1-SNAPSHOT.jar"
HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-libjars $lib_jars \
-conf $hadoop_xml \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
exit 0;
