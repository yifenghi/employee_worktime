#!/bin/bash

set -ux

work_dir=$(readlink -f $(dirname $0))/..
source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc

job_name=$(basename ${0%.sh})
job_conf=$work_dir/conf/$job_name.xml
if [ $# -eq 1 ]
then
  date=$1
else
  echo "Usgae: $0 date time_tag"
  exit 1
fi


today=`date -d "$date 40 minute ago" "+%Y-%m-%d"`
main_jar="/var/lib/hadoop-hdfs/lifeng/employee_statistic/libjar/Incorrect.jar"
java -jar $main_jar $today $job_conf

exit 0
