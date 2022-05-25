#!/bin/sh

optane=$1
qlc=$2
log=$3

# Always assume that the last workload in the log is the interested workload 

# Find the running phase time
n=$(grep "DBBENCH_START" $log/output.log | wc -l)
runphase=0
runphase_end=0
if [ "$n" -ge "2" ]; then
  start_loading=$(grep "DBBENCH_START" $log/output.log | head -1 | awk '{print $2}')
  start_workload=$(grep "DBBENCH_START" $log/output.log | tail -1 | awk '{print $2}')
  end_workload=$(grep "DBBENCH_END" $log/output.log | tail -1 | awk '{print $2}')
  echo $start_loading, $start_workload, $end_workload
  runphase=$(echo "($start_workload-$start_loading)/1000000"| bc)
  runphase_end=$(echo "($end_workload-$start_loading)/1000000"| bc)
fi

echo "Workload start time (secs) =" $runphase
echo "Workload end time (secs) =" $runphase_end

echo "OPTANE:"
cat $log/io.out | grep $optane | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR>n && NR<nend) {sum+=$9; count+=1}; if (NR>n && $9>max && NR<nend) {max=$9}} END {print "Write (MB) total=", sum/1000, "; avg (MB/s)=", (sum/1000)/count, "; max (MB/s)=", max/1000}'
cat $log/io.out | grep $optane | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR>n && NR<nend) {sum+=$3; count+=1}; if (NR>n && $3>max && NR<nend) {max=$3}} END {print "Read (MB) total=", sum/1000, "; avg (MB/s)=", (sum/1000)/count, "; max (MB/s)=", max/1000}'
echo "QLC:"
cat $log/io.out | grep $qlc | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR>n && NR<nend) {sum+=$9; count+=1}; if (NR>n && $9>max && NR<nend) {max=$9}} END {print "Write (MB) total=", sum/1000, "; avg (MB/s)=", (sum/1000)/count, "; max (MB/s)=", max/1000}'
cat $log/io.out | grep $qlc | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR>n && NR<nend) {sum+=$3; count+=1}; if (NR>n && $3>max && NR<nend) {max=$3}} END {print "Read (MB) total=", sum/1000, "; avg (MB/s)=", (sum/1000)/count, "; max (MB/s)=", max/1000}'

echo "CPU Utilization"
cat $log/cpu.out | grep db_bench | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR>n && NR<nend) {cpu+=$9; count+=1}} END {print "Avg CPU(%)=", cpu/count}'
cat $log/cpu.out | grep db_bench | awk -v n=$runphase -v nend=$runphase_end -F' ' '{if (NR>n && $9 > max && NR<nend) {max = $9}} END {print "Max CPU(%)=", max}'

