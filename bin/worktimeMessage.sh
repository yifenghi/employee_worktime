#!/bin/bash

set -ux

work_dir=$(readlink -f $(dirname $0))/..
source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

class="com.doodod.staffmanagement.statistic.WorkTimeLauncher"
job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml
if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date time_tag"
  exit 1
fi
mac_list="$work_dir/data/mac_list"
filter_list="$work_dir/data/filter_list"
dir_name=`date -d "$date" "+%Y%m%d/%H/%M"`
input="$statistic/day/$dir_name/part*"
today=`date -d "$date 15 minute ago" "+%Y-%m-%d"`
tomorrow=`date -d "$today next-day" "+%Y-%m-%d"`
output="$statistic/result/$dir_name"
hrmr $output
lib_jars="/var/lib/hadoop-hdfs/lifeng/employee_statistic/target/employee_statistic-0.0.1-SNAPSHOT-jar-with-dependencies.jar"
main_jar="/var/lib/hadoop-hdfs/lifeng/employee_statistic/target/employee_statistic-0.0.1-SNAPSHOT.jar"
HADOOP_CLASSPATH=$hadoop_classpath \
hadoop jar \
$main_jar $class \
-libjars $lib_jars \
-conf $hadoop_xml \
-conf $job_conf \
-D mapreduce.job.name="$job_name" \
-D mapreduce.output.fileoutputformat.outputdir="$output" \
-D mapreduce.input.fileinputformat.inputdir="$input" \
-D employee.system.today="$today" \
-D employee.system.tomorrow="$tomorrow" \
exit 0; 
