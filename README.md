# Efficient Compactions Between Storage Tiers with PrismDB (ASPLOS 2023)

This repository contains source code and instructions to reproduce key results in *Efficient Compactions between Storage Tiers with PrismDB* paper from **ASPLOS 2023**.

  * Contacts: Ashwini Raina (araina@cs.princeton.edu) and Jianan Lu (jiananl@princeton.edu)

# Getting the Source

```bash
git clone --recurse-submodules https://github.com/princeton-sns/prismdb.git
```

# Dependencies

  * sudo apt update
  * sudo apt install python3-pip
  * pip3 install -U PyYAML (needs YAML version 5.1 or later)
  * sudo apt-get install cgroup-tools
  * sudo apt-get install cmake
  * sudo apt-get install libsnappy-dev
  * sudo apt install libtbb-dev
  * sudo apt-get install google-perftools (note: if libtcmalloc is not installed in /usr/lib then run this command (with the correct libtcmalloc version):
  `sudo ln -s /usr/lib/x86_64-linux-gnu/libtcmalloc.so.<insert version here> /usr/lib/libtcmalloc.so)`

 
# Experiment Configuration

Setting paths for optane and flash disk
  * Create a folder `prism` on the optane ssd.
  * Create a folder `prism` on the flash ssd.
  
Configure the experiment parameters in `scripts/config_test_example.yml` file
  * **mem_alloc**: cgroup dram allocation
  * **cpu_alloc**: cgroup cpu allocation
  * **ssd**: "het" for optane+flash setting, "optane" for optane only and "qlc" for flash only
  * **optane_ratio**: optane to flash ratio 
  * **pop_cache_size**: clock cache size in terms of keys
  * **pop_cache_thresh**: clock cache optane pinning threshold
  * **num_partitions**: number of partitions
  * **db_size**: database size in terms of keys
  * **key_size**: key size in bytes
  * **value_size**: value size in bytes
  * **num_clients**: number of database clients (NOTE: it should be same as number of partitions)
  * **workload**: ycsb workload "ycsba", "ycsbb", "ycsbc", ycsbd", "ycsbe", "ycsbf"
  * **distribution_param**: (reads distibution,  writes distribution). 0 stands for uniform and -1 stands for both read and write coming from the same distribution. distribution_param [(0.99 -1)] means both reads and writes follow Zipfian 0.99
  * **read_ratio**: reads to writes ratio
  * **workload_operations**: number of operations
  * **warmup_ratio**: warmup ratio 

# Building and Running

Compile and run the experiment
  * On terminal, go to the scripts folder and run `sudo python3 run.py <experiment config file> <log folder name>`
  * e.g. `sudo python3 run.py config_test_example.yml ycsb_log`
  * Logs for this experiment will get stored in `scripts/logs/ycsb_log` folder
  * Results are stored in `scripts/logs/ycsb_log/output.log`. Throughput and latency results can be extracted from the output.log

Throughput:
  * From the scripts folder run `./report.sh <output log path> <num_partitions>` 
  * e.g. `./report.sh scripts/logs/ycsb_log/<experiment_folder>/output.log 8` will print total throughput (kops/s) 

Latency:
  * grep for "LATENCY HISTOGRAM" in `output.log`. It reports two instances - one for the loading phase and one for the running phase.


# Parameter Tuning for Promotions 

Read heavy workloads can benefit from PrismDB **promotions**. Promotions need some parameter tuning in file `db/db_impl.h`.
  * **read_dominated_threshold** - for what read/write ratio should promotions be enabled? (default value is 0.95).

Promotions need some time to take effect. Evaluation of read-heavy workloads (like ycsb B and C) should be done with a longer workload (> 200 million operations).

# Limitations

  * Delete(key) api is buggy 


# Paper
  *  https://dl.acm.org/doi/10.1145/3582016.3582052

