#!/bin/sh

# CPU CONFIG
max_cpus=32
requested_cpus=$1

#echo "on" | sudo tee /sys/devices/system/cpu/smt/control
# enable requested, (CPU-0 gives permission denied, starting from 1)
#for CPUID in $( seq 1 $((requested_cpus-1)) )
#do
#    cd /sys/devices/system/cpu/cpu$CPUID/
#    echo 1 | sudo tee online
#done

# disable others
#for CPUID in $( seq $requested_cpus $((max_cpus-1)) )
#do
#    cd /sys/devices/system/cpu/cpu$CPUID/
#    echo 0 | sudo tee online
#done

# MEMORY CONFIG
mem_size_in_bytes=$2
sudo cgdelete memory:mlsm
sudo cgdelete cpu:clsm
#sudo cgcreate -t araina:ashwini -a araina:ashwini -g memory:mlsm -g cpu:clsm
sudo cgcreate -g memory:mlsm -g cpu:clsm
echo $mem_size_in_bytes | sudo tee /sys/fs/cgroup/memory/mlsm/memory.limit_in_bytes

# nsdi experiment setting - 1ms period
#min_cpu_quota=1000
#cpu_period=1000

# better experiment setting - 100ms period
min_cpu_quota=100000
cpu_period=100000
cpu_quota=$(($requested_cpus * $cpu_period))
echo $cpu_period | sudo tee /sys/fs/cgroup/cpu/clsm/cpu.cfs_period_us
echo $cpu_quota | sudo tee /sys/fs/cgroup/cpu/clsm/cpu.cfs_quota_us # 1000 is minimum

# invalidate the OS page cache
#sudo sync; echo 3 | sudo tee /proc/sys/vm/drop_caches

# Kill any previously running benchmark processes
sudo pkill -f db_bench
sudo pkill -f "iostat -x -p nvme2n1 nvme1n1 1"
sudo pkill -f "cpu.sh"

#sudo ipmctl show -memoryresources
#sudo ipmctl create -goal PersistentMemoryType=AppDirect
