#!/bin/bash

set -ux
if [ $# -eq 1 ]
then 
  date=$1
else
  echo "Usgae: $0 date"
  exit 1
fi


smtp="smtp.ym.163.com"
from="system@palmaplus.com"
to="jie.gao@palmaplus.com"
copyto="lanxin_houtai@126.com"
username="system@palmaplus.com"
password="system\$Palmap+"
path="/var/lib/hadoop-hdfs/lifeng/employee_statistic/data/mail_status"
curr_date=`date -d "$date" "+%Y-%m-%d %H:%M:%S"`

log_date=`date -d "$date" +%Y%m%d`
log_path=/var/lib/hadoop-hdfs/lifeng/employee_statistic/log/employee_day.$log_date.log

#grep -50 '15/01/13 23:54:16' $log_path | tee -a /var/lib/hadoop-hdfs/lifeng/employee_statistic/log/loginfo

find_time=`date -d "$date" "+%y/%m/%d %H:%M"`

content=$(grep -50 "$find_time" $log_path)
#record_num=`grep -50 "$find_time" $log_path | grep 'Reduce output records'|awk '{print $3}'`

  
java -jar /var/lib/hadoop-hdfs/lifeng/employee_statistic/libjar/WarningMail.jar \
"$smtp" \
"$from" \
"$to" \
"$copyto" \
"$content" \
"$username" \
"$password" \
"$path" \
"$curr_date" 

exit 0



