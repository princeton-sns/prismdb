#!/bin/bash

# Usage: ./report.sh filename partition_num thread_num

# filename
F=$1
# number of partitions
P=$2
# number of ycsb threads (should be same as number of partitions)
T=$2
echo "file name: $F, # of partitions: $P, # of ycsb threads: $T"

for ((i=0;i<$P;i++))
do 
	grep curr $F | grep "partition $i" | grep -v "migration" | tail -1 | awk '{ printf("partition '$i' max memory usage %.3f GB\n", $9/1024./1024./1024.) }'
done


for ((i=0;i<$T;i++))
do 
	grep "ops/sec" $F | grep "thread $i" | tail -1 | awk '{ split($9, t1, ")"); split(t1[1], t2, ","); printf("thread '$i' throughput kops/s: %.1f\n", t2[2]/1000) }' 
done

grep "Num of Migrations:" $F | tail -$P
grep "Average num keys for Migration:" $F | tail -$P
grep "Average time for BackgroundCall:" $F | tail -$P
grep "Num of Puts:" $F | tail -$P
grep "Num of Inserts" $F | tail -$P 
grep "Average time for Put():" $F | tail -$P
grep "Num of Gets:" $F | tail -$P
grep "Num of optane:" $F | tail -$P
grep "Average time for Get() from optane:" $F | tail -$P
grep "Average time for Get() from qlc:" $F | tail -$P

echo "----------------------------------------"

grep "Num of Migrations:" $F | tail -$P | awk '{sum += $4} END{printf("total # of migrations : %d\n", sum)}'
grep "Num of Migrations:\|Average num keys for Migration:" $F | tail -$(($P*2)) | awk '{if ($1=="Num") { m=$4; totalm+=$4} else if ($1 == "Average") { k=$6} if (NR%2==0 && m!=0) { totalk+=(m*k); m=0; k=0 }} END { if (totalm > 0) {printf("average # of migrated keys: %.f\n", totalk/totalm) } else {print "average # of migrated keys: 0"}}'
grep "Num of Migrations:\|Average time for BackgroundCall:" $F | tail -$(($P*2)) | awk '{if ($1=="Num") { m=$4; totalm+=$4;} else if ($1 == "Average") { b=$5;} if (NR%2==0 && m!=0) { totalb+=(m*b); m=0; b=0 }} END { if (totalm > 0) {printf("average time of migration (ms): %.f\n", totalb/totalm/1000000) } else {print "average time of migration (ms): 0"}}'
grep "Num of Puts:" $F | tail -$P | awk '{sum += $4} END{printf("total # of puts : %d\n", sum)}'
grep "Num of Puts:\|Average time for Put():" $F | tail -$(($P*2)) | awk '{if ($1=="Num") { p+=$4; totalp+=$4 } else if ($1 == "Average") { t=$5 } if (NR%2==0 && p!=0) { totalt+=(p*t); p=0; t=0 }} END { printf("average time of put (us): %.3f\n", totalt/totalp/1000.) }'
grep "Num of Puts:\|Num of Inserts" $F | tail -$(($P*2)) | awk '{if ($3=="Puts:") { totalp+=$4 } else if ($3=="Inserts") { totalu+=$8 }} END { printf("average ratio of inserts: %.2f\naverage ratio of updates: %.2f\n", 1-totalu/totalp, totalu/totalp) }'
grep "Num of Inserts" $F | tail -$P | awk '{split($4, t1, ";"); sum += t1[1]} END{printf("total # of inserts : %d\n", sum)}'

grep "Num of Inserts\|Average time for insert_item_sync:" $F | tail -$(($P*2)) | awk '{if ($1=="Num") { split($4, t1, ";"); p+=t1[1]; totalp+=t1[1] } else if ($1 == "Average") { t=$5 } if (NR%2==0 && p!=0) { totalt+=(p*t); p=0; t=0 }} END { if (totalp > 0) {printf("average time of add_item_sync (us): %.3f\n", totalt/totalp/1000.)} else {print "average time of add_item_sync (us): 0"} }'
grep "Num of Inserts" $F | tail -$P | awk '{ sum += $8 } END{printf("total # of updates : %d\n", sum)}'
grep "Num of Inserts\|Average time for update_item_sync:" $F | tail -$(($P*2)) | awk '{if ($1=="Num") { p+=$8; totalp+=$8 } else if ($1 == "Average") { t=$5 } if (NR%2==0 && p!=0) { totalt+=(p*t); p=0; t=0 }} END { if (totalp > 0) {printf("average time of update_item_sync (us): %.3f\n", totalt/totalp/1000.) } else {print "average time of update_item_sync (us): 0" }}'

grep "Num of Gets:" $F | tail -$P | awk '{sum += $4} END{printf("total # of gets : %d\n", sum)}'
grep "Num of optane:" $F | tail -$P | awk '{split($4, t1, ";"); sum += t1[1]} END{printf("total # of gets from optane: %d\n", sum)}'
grep "Num of optane:" $F | tail -$P | awk '{ sum += $8 } END{printf("total # of gets from qlc: %d\n", sum)}'
grep "Num of Gets:\|Num of optane" $F | tail -$(($P*2)) | awk '{if ($3=="Gets:") { totalp+=$4 } else if ($3=="optane:") { totalu+=$8 }} END { if (totalp > 0 ) { printf("average ratio of optane reads: %.2f\naverage ratio of qlc reads: %.2f\n", 1-totalu/totalp, totalu/totalp) } else {print "average ratio of optane reads: 0\naverage ratio of qlc reads: 0"}}'
grep "Num of optane:\|Average time for read_item_key_val:" $F | tail -$(($P*2)) | awk '{if ($1=="Num") { split($4, t1, ";"); p+=t1[1]; totalp+=t1[1] } else if ($1 == "Average") { t=$5 } if (NR%2==0 && p!=0) { totalt+=(p*t); p=0; t=0 }} END { if (totalp > 0) { printf("average time of reading from optane (us): %.3f\n", totalt/totalp/1000.) } else {print "average time of reading from optane (us): 0"}}'
grep "Num of optane:\|Average time for reading from qlc:" $F | tail -$(($P*2)) | awk '{if ($1=="Num") { p+=$8; totalp+=$8 } else if ($1 == "Average") { t=$7 } if (NR%2==0 && p!=0) { totalt+=(p*t); p=0; t=0 }} END { if (totalp > 0) {printf("average time of reading from qlc (us): %.3f\n", totalt/totalp/1000.) } else {print "average time of reading from qlc (us): 0"}}'
grep -nr "NOT-FOUND IN TABLE CACHE" $F | wc -l | awk '{printf("total # of not-found table cache : %d\n", $1)}'
grep -nr "ERROR" $F | wc -l | awk '{printf("total # of errors (for migration): %d\n", $1)}'

totalT=0
for ((i=0;i<$T;i++))
do 
	var=$(grep "ops/sec" $F | grep "thread $i" | tail -1 | awk '{ split($9, t1, ")"); split(t1[1], t2, ","); print t2[2]}')
	totalT=$(echo "$totalT+$var" | bc)
done
awk -v totalT="$totalT" 'BEGIN{printf("total throughput (kops/s): %.1f\n", totalT/1000)}'

totalUsage=0
for ((i=0;i<$P;i++))
do 
	usage=$(grep curr $F |grep "partition $i" | grep -v "migration" | tail -1 | awk '{ print $9/1024./1024./1024. }')
	totalUsage=$(echo "$totalUsage+$usage" | bc)
done
awk -v totalUsage="$totalUsage" 'BEGIN { printf("total optane usage (GB): %.2f\n", totalUsage)}'
