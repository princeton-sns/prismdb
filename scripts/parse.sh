#!/bin/sh

log=$1
num_partitions=$2

echo $'tput (kops/s)
optane usage (GB)
ratio of updates 
ratio of inserts
# of migrations 
# of migration keys 
migration time (ms)
optane write IO (GB)
optane read IO (GB)
qlc write IO (GB)
qlc read IO (GB)
avg total CPU util (%)\n'

# tput
./report.sh $log/output.log $num_partitions $num_partitions | grep "total throughput" | awk '{print $4}'

# optane usage
./report.sh $log/output.log $num_partitions $num_partitions | grep "total optane usage" | awk '{print $5}'

# update ratio
./report.sh $log/output.log $num_partitions $num_partitions | grep "average ratio of updates" | awk '{print $5}'

# insert ratio
./report.sh $log/output.log $num_partitions $num_partitions | grep "average ratio of inserts" | awk '{print $5}'

# num of migrations
./report.sh $log/output.log $num_partitions $num_partitions | grep "total # of migrations" | awk '{print $6}'

# num of migration keys 
./report.sh $log/output.log $num_partitions $num_partitions | grep "average # of migrated" | awk '{print $6}'

# migration time
./report.sh $log/output.log $num_partitions $num_partitions | grep "average time of migration" | awk '{print $6}'

# optane write io 
./iostat_parser_intel.sh nvme4n1 nvme2n1 $log | grep "Write" | head -1 | awk '{print $4}'

# optane read io 
./iostat_parser_intel.sh nvme4n1 nvme2n1 $log | grep "Read" | head -1 | awk '{print $4}'

# qlc write io 
./iostat_parser_intel.sh nvme4n1 nvme2n1 $log | grep "Write" | tail -1 | awk '{print $4}'

# qlc read io 
./iostat_parser_intel.sh nvme4n1 nvme2n1 $log | grep "Read" | tail -1 | awk '{print $4}'

# avg cpu util
./iostat_parser_intel.sh nvme4n1 nvme2n1 $log | grep "Avg CPU" | awk '{print $4}'
