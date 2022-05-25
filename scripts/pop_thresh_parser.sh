#!/bin/sh

log=$1
num_partitions=$2


# tput
./report.sh $log/output.log $num_partitions $num_partitions | tail -2 | head -1 | awk -F' ' '{print $4}'
# num migs, keys/mig, mig time
./report.sh $log/output.log $num_partitions $num_partitions | tail -22 | head -3 | awk -F' ' '{print $6}'

# optane write (GB)
./iostat_parser_intel.sh nvme4n1 nvme5n1 $log | tail -5 | head -1 | awk -F' ' '{print $4}'
# optane read (GB)
./iostat_parser_intel.sh nvme4n1 nvme5n1 $log | tail -4 | head -1 | awk -F' ' '{print $4}'
# qlc write (GB)
./iostat_parser_intel.sh nvme4n1 nvme5n1 $log | tail -2 | head -1 | awk -F' ' '{print $4}'
# qlc read (GB)
./iostat_parser_intel.sh nvme4n1 nvme5n1 $log | tail -1 | awk -F' ' '{print $4}'

# optane usage
./report.sh $log/output.log $num_partitions $num_partitions | tail -1 | awk -F' ' '{print $5}'
