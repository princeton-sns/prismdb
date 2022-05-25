#!/bin/sh

optane=$1
qlc=$2
log=$3

# Always assume that the last workload in the log is the interested workload 

# Find the running phase time
n=$(grep -a "DBBENCH_START" $log/output.log | wc -l)
runphase=0
runphase_end=0
if [ "$n" -ge "2" ]; then
  start_loading=$(grep -a "DBBENCH_START" $log/output.log | head -1 | awk '{print $2}')
  start_workload=$(grep -a "DBBENCH_START" $log/output.log | tail -1 | awk '{print $2}')
  end_workload=$(grep -a "DBBENCH_END" $log/output.log | tail -1 | awk '{print $2}')
  echo $start_loading, $start_workload, $end_workload
  runphase=$(echo "($start_workload-$start_loading)/1000000"| bc)
  # subtract 150 seconds for the cooldown period
  #runphase_end=$(echo "($end_workload-$start_loading)/1000000 -150"| bc)
  runphase_end=$(echo "($end_workload-$start_loading)/1000000"| bc)
fi

echo "Workload start time (secs) =" $runphase
echo "Workload end time (secs) =" $runphase_end

echo "OPTANE:"
cat $log/io.out | grep $optane | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR<n) {sum+=$5; count+=1}; if (NR<n && $5>max) {max=$5}} END {print "Write (MB) total=", sum/1000, "; avg (MB/s)=", (sum/1000)/count, "; max (MB/s)=", max/1000}'
cat $log/io.out | grep $optane | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR<n) {sum+=$4; count+=1}; if (NR<n && $4>max) {max=$4}} END {print "Read (MB) total=", sum/1000, "; avg (MB/s)=", (sum/1000)/count, "; max (MB/s)=", max/1000}'
echo "QLC:"
cat $log/io.out | grep $qlc | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR<n) {sum+=$5; count+=1}; if (NR<n && $5>max) {max=$5}} END {print "Write (MB) total=", sum/1000, "; avg (MB/s)=", (sum/1000)/count, "; max (MB/s)=", max/1000}'
cat $log/io.out | grep $qlc | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR<n) {sum+=$4; count+=1}; if (NR<n && $4>max) {max=$4}} END {print "Read (MB) total=", sum/1000, "; avg (MB/s)=", (sum/1000)/count, "; max (MB/s)=", max/1000}'

echo "CPU Utilization"
cat $log/cpu.out | grep db_bench | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR<n) {cpu+=$9; count+=1}} END {print "Avg CPU(%)=", cpu/count}'
cat $log/cpu.out | grep db_bench | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR<n && $9 > max) {max = $9}} END {print "Max CPU(%)=", max}'

echo "CPU Utilization (iostat)"
cat $log/io.out | grep -A 1 -a "avg-cpu" | awk '{if ((NR%3) == 2) {print}}' | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR<n) {sum += 10*($1+$2+$3); sum2 += 10*($1+$2+$3+$4); count+=1}} END {print "Avg CPU(%)=", sum/count, "Avg CPU(%) with iowait=", sum2/count}'
