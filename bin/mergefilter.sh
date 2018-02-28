#!/bin/sh

set -ux

work_dir=$(readlink -f $(dirname $0))/..
source $work_dir/conf/hadoop.conf
source $work_dir/bin/hadoop.rc
hdfs dfs -text /group/doodod/employee/statistic/result/filter/p* > $work_dir/data/temp

cat $work_dir/data/temp $work_dir/data/filter_list | sort -u > $work_dir/data/filter_list1
rm $work_dir/data/temp
rm $work_dir/data/filter_list
mv $work_dir/data/filter_list1 $work_dir/data/filter_list
