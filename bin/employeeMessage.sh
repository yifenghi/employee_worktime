#!/bin/bash

set -ux

work_dir=$(readlink -f $(dirname $0))/..

source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class=com.doodod.staffmanagement.statistic.EmployeeLauncher
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml

if [ $# -eq 1 ]
then 
  date=$1
else
  echo "Usgae: $0 date"
  exit 1
fi

date_1hour_ago=`date -d "$date 2 hour ago" "+%Y-%m-%d %H:%M:%S"`
date_today=`date -d "$date_1hour_ago" "+%Y%m%d"`
dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
mac_list="$work_dir/data/mac_list"
output="$statistic/hour/$dir_name"
hrmr $output
#lib_jars="/var/lib/hadoop-hdfs/lifeng/employee_statistic/Statistic-1.0-SNAPSHOT-jar-with-dependencies.jar"
#lib_jars="/var/lib/hadoop-hdfs/lifeng/employee_statistic/libjar/employee_statistic-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
#main_jar="/var/lib/hadoop-hdfs/lifeng/employee_statistic/target/employee_statistic-0.0.1-SNAPSHOT.jar"
HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-conf $hadoop_xml \
-conf $job_conf \
-files $mac_list \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D company.businesstime.start="$date_1hour_ago" \
-D company.businesstime.now="$date" \
-D conf.mac.list="`basename $mac_list`" 
exit 0; 















