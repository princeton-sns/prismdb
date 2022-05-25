#!/bin/sh

cpu_log=$1
sudo rm -f $cpu_log

while true
do
        top -b -n1 | grep 'db_bench' | head -n1 >> $cpu_log
        sleep 1
done
