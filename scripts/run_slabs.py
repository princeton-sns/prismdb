import matplotlib as mpl
mpl.use('Agg')

import sys
import numpy as np
# import matplotlib.pyplot as plt
import os
import subprocess
import yaml

# debug flags
dbg_skip_src_compilation = False
#prismdb_src_path = '/home/user/hetsys/prismdb/p2'
#rocksdb_src_path = '/home/user/hetsys/ra-lsm'
#default_log_path = '/home/user/hetsys/tlc_ssd/p2_logs'
#optane_prism_path = '/home/user/hetsys/optane_ssd/prism'
#qlc_prism_path = '/home/user/hetsys/qlc_ssd/prism'
#optane_db_path = '/home/user/hetsys/optane_ssd/db'
#optane_wal_dir_path = '/home/user/hetsys/optane_ssd/wal_dir'

prismdb_src_path = '/home/ashwini/hetsys/prismdb/p2'
rocksdb_src_path = '/home/ashwini/hetsys/ra-lsm'
default_log_path = '/home/ashwini/hetsys/tlc_ssd/p2_logs'
optane_prism_path = '/home/ashwini/hetsys/optane_ssd/prism'
qlc_prism_path = '/home/ashwini/hetsys/qlc_ssd/prism'
optane_db_path = '/home/ashwini/hetsys/optane_ssd/db'
optane_wal_dir_path = '/home/ashwini/hetsys/optane_ssd/wal_dir'
master_cfg = {}

def read_exp_master_config(yaml_file):
    with open(yaml_file) as f:
        global master_cfg
        master_cfg = yaml.load(f, Loader=yaml.FullLoader)
        print (master_cfg)

# generates a string from exp config for log folder naming
def create_config_str(cfg):
    code = cfg["code"][0]
    ssd = cfg["ssd"][0:3]+str(cfg["optane_ratio"])
    pc_size = "pc"+str(cfg["pop_cache_size"]/1e6)+"M"
    pc_cache_thresh = "pct"+str(cfg["pop_cache_thresh"])
    bc_size = "bc"+str(int(cfg["blockcache_size"]/(2<<19)))+"M"
    num_partitions = "p"+str(cfg["num_partitions"])
    db_size = "db"+str(int(cfg["db_size"]/1e6))+"M"
    key_size = "k"+str(cfg["key_size"])
    value_size = "v"+str(cfg["key_size"])
    num_clients = "c"+str(cfg["num_clients"])
    loop = cfg["loop"].capitalize()[0]+"L"
    workload = "w"+str(cfg["workload"][-1])
    read_ratio = "rr"+str(cfg["read_ratio"])
    #deprecated
    #dist = cfg["distribution"][0]
    read_dist_param = cfg["distribution_param"].split(" ")[0].split("(")[1]
    write_dist_param = cfg["distribution_param"].split(" ")[1].split(")")[0]
    dp_r = "rZ"+read_dist_param
    dp_w = "wZ"+write_dist_param
    warmup_ratio = str(cfg["warmup_ratio"])
    read_logging = str(cfg["read_logging"])
    migration_logging = str(cfg["migration_logging"])
    migration_policy = str(cfg["migration_policy"])
    migration_rand_range_num = str(cfg["migration_rand_range_num"])
    migration_metric = str(cfg["migration_metric"])
    operations = "opr"+str(int(cfg["workload_operations"]/1e6))+"M"

    exp_str = code+"_"+ssd+"_"+num_partitions+"_"+bc_size+"_"+dp_r+"_"+dp_w+"_"+pc_size+"_"+pc_cache_thresh+"_"+db_size+"_"+key_size+"_"+value_size+"_"+operations+"_"+num_clients+"_"+read_ratio+"_"+workload+"_"+migration_policy+"_"+migration_rand_range_num+"_"+migration_metric
    return exp_str

def write_config_file(cfg, filename):
    with open(filename, 'w') as file:
        file.write("[mem_alloc]\n"+str(cfg["mem_alloc"])+"\n\n")
        file.write("[cpu_alloc]\n"+str(cfg["cpu_alloc"])+"\n\n")
        file.write("[code]\n"+cfg["code"]+"\n\n")
        file.write("[ssd]\n"+cfg["ssd"]+"\n\n")
        file.write("[optane_ratio]\n"+str(cfg["optane_ratio"])+"\n\n")
        file.write("[pop_cache_size]\n"+str(cfg["pop_cache_size"])+"\n\n")
        file.write("[pop_cache_thresh]\n"+str(cfg["pop_cache_thresh"])+"\n\n")
        file.write("[blockcache_size]\n"+str(cfg["blockcache_size"])+"\n\n")
        file.write("[num_partitions]\n"+str(cfg["num_partitions"])+"\n\n")
        file.write("[db_size]\n"+str(cfg["db_size"])+"\n\n")
        file.write("[key_size]\n"+str(cfg["key_size"])+"\n\n")
        file.write("[value_size]\n"+str(cfg["value_size"])+"\n\n")
        file.write("[num_clients]\n"+str(cfg["num_clients"])+"\n\n")
        file.write("[loop]\n"+str(cfg["loop"])+"\n\n")
        file.write("[workload]\n"+str(cfg["workload"])+"\n\n")
        file.write("[read_ratio]\n"+str(cfg["read_ratio"])+"\n\n")
        file.write("[distribution]\n"+cfg["distribution"]+"\n\n")
        file.write("[distribution_param]\n"+str(cfg["distribution_param"])+"\n\n")
        file.write("[workload_operations]\n"+str(cfg["workload_operations"])+"\n")
        file.write("[warmup_ratio]\n"+str(cfg["warmup_ratio"])+"\n\n")
        file.write("[read_logging]\n"+str(cfg["read_logging"])+"\n\n")
        file.write("[migration_logging]\n"+str(cfg["migration_logging"])+"\n\n")
        file.write("[migration_policy]\n"+str(cfg["migration_policy"])+"\n\n")
        file.write("[migration_rand_range_num]\n"+str(cfg["migration_rand_range_num"])+"\n\n")
        file.write("[migration_metric]\n"+str(cfg["migration_metric"])+"\n\n")


def compile_src_changes(exp_cfg):
    global rocksdb_src_path
    global prismdb_src_path
    if exp_cfg["code"] == "prismdb":
        subprocess.call(["sed -i 's/.*float optaneThreshold.*/  float optaneThreshold = "+str(exp_cfg["optane_ratio"])+";/g w /dev/stdout' "+prismdb_src_path+"/db/db_impl.h"], shell=True)
        subprocess.call(["sed -i 's/.*uint32_t popCacheSize.*/  uint32_t popCacheSize = "+str(exp_cfg["pop_cache_size"])+";/g w /dev/stdout' "+prismdb_src_path+"/db/db_impl.h"], shell=True)
        subprocess.call(["sed -i 's/.*float popThreshold.*/  float popThreshold = "+str(exp_cfg["pop_cache_thresh"])+";/g w /dev/stdout' "+prismdb_src_path+"/db/db_impl.h"], shell=True)
        subprocess.call(["sed -i 's/.*uint64_t numKeys.*/  uint64_t numKeys = "+str(exp_cfg["db_size"])+";/g w /dev/stdout' "+prismdb_src_path+"/db/db_impl.h"], shell=True)
        subprocess.call(["sed -i 's/.*uint32_t maxKeySizeBytes.*/  uint32_t maxKeySizeBytes = "+str(exp_cfg["key_size"])+";/g w /dev/stdout' "+prismdb_src_path+"/db/db_impl.h"], shell=True)
        subprocess.call(["sed -i 's/.*uint64_t numPartitions.*/  uint64_t numPartitions = "+str(exp_cfg["num_partitions"])+";/g w /dev/stdout' "+prismdb_src_path+"/db/db_impl.h"], shell=True)
        kv_size = int(exp_cfg["key_size"])+int(exp_cfg["value_size"])
        align_to = 64
        #https://stackoverflow.com/questions/29925524/how-do-i-round-to-the-next-32-bit-alignment
        subprocess.call(["sed -i 's/.*uint32_t maxKVSizeBytes.*/  uint32_t maxKVSizeBytes = "+str(kv_size+align_to-(kv_size%align_to))+";/g w /dev/stdout' "+prismdb_src_path+"/db/db_impl.h"], shell=True)
        subprocess.call(["sed -i 's/.*static int FLAGS_threadpool_num.*/static int FLAGS_threadpool_num = "+str(exp_cfg["num_partitions"])+";/g w /dev/stdout' "+prismdb_src_path+"/benchmarks/db_bench.cc"], shell=True)

        if dbg_skip_src_compilation == False:
            subprocess.call(["( cd "+prismdb_src_path+"/scripts ; ./make.sh "+prismdb_src_path+" "+optane_prism_path+" "+qlc_prism_path+" )"], shell=True)
            print("---------Applied Source Changes----------", exp_cfg["code"], exp_cfg["ssd"], exp_cfg["blockcache_size"])
        return
        
    # For rocksdb variants
    if exp_cfg["code"] == "rocksdb":
        subprocess.call(["sed -i 's/.*bool mutant.*/bool mutant = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        # subprocess.call(["sed -i 's/.*static bool mutant.*/static bool mutant = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/version_set.cc"], shell=True)
        subprocess.call(["sed -i 's/.*bool clock_cache.*/bool clock_cache = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        subprocess.call(["sed -i 's/.*bool oracle_cache.*/bool oracle_cache = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        subprocess.call(["sed -i 's/.*CompactionPri compaction_pri = .*/  CompactionPri compaction_pri = kMinOverlappingRatio;/g w /dev/stdout' "+rocksdb_src_path+"/include/rocksdb/advanced_options.h"], shell=True)
        subprocess.call(["sed -i 's/.*bool run_optimization.*/  bool run_optimization = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/compaction/compaction_job.cc"], shell=True)
        subprocess.call(["sed -i 's/.*size_t num_read_cache_keys =.*/  size_t num_read_cache_keys = 0;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)

    elif exp_cfg["code"] == "old_prismdb":
        subprocess.call(["sed -i 's/.*bool mutant.*/bool mutant = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        # subprocess.call(["sed -i 's/.*static bool mutant.*/static bool mutant = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/version_set.cc"], shell=True)
        subprocess.call(["sed -i 's/.*bool clock_cache.*/bool clock_cache = true;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        subprocess.call(["sed -i 's/.*bool oracle_cache.*/bool oracle_cache = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        subprocess.call(["sed -i 's/.*CompactionPri compaction_pri = .*/  CompactionPri compaction_pri = kLeastPopular;/g w /dev/stdout' "+rocksdb_src_path+"/include/rocksdb/advanced_options.h"], shell=True)
        subprocess.call(["sed -i 's/.*bool run_optimization.*/  bool run_optimization = true;/g w /dev/stdout' "+rocksdb_src_path+"/db/compaction/compaction_job.cc"], shell=True)
        subprocess.call(["sed -i 's/.*size_t num_read_cache_keys =.*/  size_t num_read_cache_keys = "+str(exp_cfg["pop_cache_size"])+";/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        subprocess.call(["sed -i 's/.*int threshold_percent.*/  int threshold_percent = "+str(exp_cfg["pop_cache_thresh"])+";/g w /dev/stdout' "+rocksdb_src_path+"/db/compaction/compaction_job.cc"], shell=True)

    elif exp_cfg["code"] == "mutant":
        subprocess.call(["sed -i 's/.*bool mutant.*/bool mutant = true;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        subprocess.call(["sed -i 's/.*bool clock_cache.*/bool clock_cache = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        subprocess.call(["sed -i 's/.*bool oracle_cache.*/bool oracle_cache = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)
        subprocess.call(["sed -i 's/.*CompactionPri compaction_pri = .*/  CompactionPri compaction_pri = kMinOverlappingRatio;/g w /dev/stdout' "+rocksdb_src_path+"/include/rocksdb/advanced_options.h"], shell=True)
        subprocess.call(["sed -i 's/.*bool run_optimization.*/  bool run_optimization = false;/g w /dev/stdout' "+rocksdb_src_path+"/db/compaction/compaction_job.cc"], shell=True)
        subprocess.call(["sed -i 's/.*size_t num_read_cache_keys =.*/  size_t num_read_cache_keys = 0;/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl.cc"], shell=True)

    subprocess.call(["sed -i 's/.*std::string setup =.*/  std::string setup = \""+exp_cfg["ssd"]+"\";/g w /dev/stdout' "+rocksdb_src_path+"/db/db_impl/db_impl_open.cc"], shell=True)

    if exp_cfg["blockcache_size"] == 0:
        subprocess.call(["sed -i 's/.*no_block_cache=.*/  no_block_cache=true/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*cache_index_and_filter_blocks=.*/  cache_index_and_filter_blocks=false/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*cache_index_and_filter_blocks_with_high_priority=.*/  cache_index_and_filter_blocks_with_high_priority=false/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*pin_l0_filter_and_index_blocks_in_cache=.*/  pin_l0_filter_and_index_blocks_in_cache=false/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*pin_top_level_index_and_filter=.*/  pin_top_level_index_and_filter=false/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
    else:
        subprocess.call(["sed -i 's/.*no_block_cache=.*/  no_block_cache=false/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*cache_index_and_filter_blocks=.*/  cache_index_and_filter_blocks=true/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*cache_index_and_filter_blocks_with_high_priority=.*/  cache_index_and_filter_blocks_with_high_priority=true/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*pin_l0_filter_and_index_blocks_in_cache=.*/  pin_l0_filter_and_index_blocks_in_cache=true/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*pin_top_level_index_and_filter=.*/  pin_top_level_index_and_filter=true/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
        # subprocess.call(["sed -i 's/.*static size_t block_cache_size =.*/static size_t block_cache_size = "+str(bc_size)+";/g w /dev/stdout' "+rocksdb_src_path+"table/block_based/block_based_table_factory.cc"], shell=True)

    if dbg_skip_src_compilation == False:
        subprocess.call(["( cd "+rocksdb_src_path+" ; ./make_java.sh )"], shell=True)

    print ("---------Applied Source Changes----------", exp_cfg["code"], exp_cfg["ssd"], exp_cfg["blockcache_size"])

def run_single_exp(exp_cfg, log_folder):

    global rocksdb_src_path
    global prismdb_src_path
    # setup the hardware
    subprocess.call(["( cd "+prismdb_src_path+"/scripts/ ; sudo ./hw_setup.sh "+str(exp_cfg["cpu_alloc"])+" "+str(exp_cfg["mem_alloc"])+")"], shell=True)

    if exp_cfg["ssd"] == "het":
        ssd = "optane_ssd"
    else:
        ssd = exp_cfg["ssd"]+"_ssd"

    # if exp_cfg["io_type"] == "direct":
    #     subprocess.call(["sed -i 's/.*use_direct_reads=.*/  use_direct_reads=true/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
    # else:
    #     subprocess.call(["sed -i 's/.*use_direct_reads=.*/  use_direct_reads=false/g w /dev/stdout' ~/lsm/ycsb/rocksdb/rocksdb_option_file_lite.ini"], shell=True);
    
    #subprocess.call(["sed -i 's/.*wal_dir=.*/  wal_dir=\/home\/user\/hetsys\/"+ssd+"\/wal_dir_ash/g w /dev/stdout' \/home\/user\/hetsys\/ra-lsm\/rocksdb_option_file_8gb.ini"], shell=True);
    
    #subprocess.call(["sed -i 's/.*requestdistribution=.*/requestdistribution="+exp_cfg["distribution"]+"/g w /dev/stdout' ~/lsm/ycsb/workloads/workloada"], shell=True);
    # if exp_cfg["distribution"] == "zipfian":
    #     subprocess.call(["sed -i 's/.*public static final double ZIPFIAN_CONSTANT =.*/  public static final double ZIPFIAN_CONSTANT = "+str(exp_cfg["distribution_param"])+";/g w /dev/stdout' ~/lsm/ycsb/core/src/main/java/site/ycsb/generator/ZipfianGenerator.java"], shell=True);
    # subprocess.call(["sed -i 's/.*recordcount=.*/recordcount="+str(exp_cfg["db_size"])+"/g w /dev/stdout' ~/lsm/ycsb/workloads/workloada"], shell=True);

    print ("-------Starting Main Experiment---------", log_folder.split("/")[-2])

    # auto exp takes these args - num_threads, warmup ops, warmup reads, warmup updates, main ops, main reads, main updates
    #arg_str = ssd+" "+str(exp_cfg["num_clients"])+" "+str(exp_cfg["warmup_operations"])+" "+str(exp_cfg["warmup_read_percent"])+" "+str("{:.2f}".format(1-exp_cfg["warmup_read_percent"]))+" "+str(exp_cfg["workload_operations"])+" "+str(exp_cfg["workload_read_percent"])+" "+str("{:.2f}".format(1-exp_cfg["workload_read_percent"]))+" "+str(exp_cfg["workload"])

    # free the page cache
    #subprocess.call(["( sync; echo 1 | sudo tee /proc/sys/vm/drop_caches )"], shell=True)

    # call the experiment script
    # subprocess.call(["( cd ~/lsm/ycsb/ ; cgexec -g memory:mlsm ./auto_exp_skipload.sh "+arg_str+" > "+log_folder+"output.log 2>&1 )"], shell=True);
    #subprocess.call(["( cd ~/lsm/ycsb/ ; cgexec -g memory:mlsm ./auto_exp_skipload.sh "+arg_str+" > "+log_folder+"output.log 2>&1 )"], shell=True)

    ycsb_dict = {'ycsba':'ycsbwklda', 'ycsbb':'ycsbwkldb', 'ycsbc':'ycsbwkldc', 'ycsbd':'ycsbwkldd','ycsbe':'ycsbwklde','ycsbf':'ycsbwkldf', 'twitter':'twitter', 'ycsb_slabs':'ycsb_slabs'}
    loop = 0
    num_threadpool_threads = exp_cfg["num_partitions"]
    if exp_cfg["loop"] == "open":
        loop = 1
    else:
        num_threadpool_threads = 0
    #arg_str = "--benchmarks="+ycsb_dict[exp_cfg["workload"]]
    #arg_str = "--benchmarks=ycsbfilldb,"+ycsb_dict[exp_cfg["workload"]]
    arg_str = "--benchmarks=ycsbfilldb_slabs,"+ycsb_dict[exp_cfg["workload"]]
    #arg_str = "--benchmarks=ycsbfilldb,ycsbwarmup,"+ycsb_dict[exp_cfg["workload"]]
    arg_str += " --num="+str(exp_cfg["db_size"])
    arg_str += " --reads="+str(exp_cfg["workload_operations"])
    arg_str += " --open_loop="+str(loop)
    arg_str += " --threads="+str(exp_cfg["num_clients"])
    #arg_str += " --threadpool_num="+str(num_threadpool_threads)
    arg_str += " --load_threads="+str(exp_cfg["num_partitions"])  # for speeding up load process
    arg_str += " --key_size="+str(exp_cfg["key_size"])
    arg_str += " --value_size="+str(exp_cfg["value_size"])
    arg_str += " --pop_file="+exp_cfg["pop_file"]
    arg_str += " --cache_size="+str(exp_cfg["blockcache_size"])

    # ycsb-d uses latest dist for reads and zipfian for writes
    if exp_cfg["workload"] == "ycsbd":
        arg_str += " --read_ratio=0.95"
        arg_str += " --YCSB_separate_write=1"
    else:
        arg_str += " --read_ratio="+str(exp_cfg["read_ratio"])
    
    arg_str += " --warmup_ratio="+str(exp_cfg["warmup_ratio"])
    arg_str += " --read_logging="+str(exp_cfg["read_logging"])
    arg_str += " --migration_logging="+str(exp_cfg["migration_logging"])
    arg_str += " --migration_policy="+str(exp_cfg["migration_policy"])
    arg_str += " --migration_rand_range_num="+str(exp_cfg["migration_rand_range_num"])
    arg_str += " --migration_metric="+str(exp_cfg["migration_metric"])
    #arg_str += " --bloom_bits="+str(10) # hardcoded bloom filter to 10bits
    read_dist_param = exp_cfg["distribution_param"].split(" ")[0].split("(")[1]
    write_dist_param = exp_cfg["distribution_param"].split(" ")[1].split(")")[0]

    if float(read_dist_param) == 0:
        arg_str += " --YCSB_uniform_distribution=1"
    else:
        arg_str += " --YCSB_zipfian_alpha="+read_dist_param
    if float(write_dist_param) == 0:
        arg_str += " --YCSB_uniform_distribution_write=1"
        if float(read_dist_param) != 0:
            arg_str += " --YCSB_separate_write=1"
    else:
        if float(write_dist_param) == -1:
            arg_str += " --YCSB_zipfian_alpha_write="+read_dist_param
        else:
            arg_str += " --YCSB_separate_write=1"
            arg_str += " --YCSB_zipfian_alpha_write="+write_dist_param
    # deprecated
    #if exp_cfg["distribution"] == "uniform":
    #  arg_str += " --YCSB_uniform_distribution=1"
    arg_str += " >& "+log_folder+"output.log"
    print ("COMMAND: sudo cgexec -g memory:mlsm -g cpu:clsm ./db_bench", arg_str)
    io = subprocess.Popen("exec ./io.sh "+log_folder+"io.out", stdout=subprocess.PIPE, shell=True)
    cpu = subprocess.Popen("exec ./cpu.sh "+log_folder+"cpu.out", stdout=subprocess.PIPE, shell=True)
    subprocess.call(["( cd "+prismdb_src_path+"/build; sudo cgexec -g memory:mlsm -g cpu:clsm ./db_bench "+arg_str+")"], shell=True)
    io.kill()
    cpu.kill()
  
    # kill background iostat if not killed successfully
    subprocess.call(["( var=$(ps ax | grep iostat | head -1 | awk '{print $1}'); sudo kill $var )"], shell=True)
    

    # move log files
    subprocess.call(["cp "+qlc_prism_path+"/test-0/dbbench/LOG "+log_folder], shell=True);

    print ("-------Completed Main Experiment---------", log_folder.split("/")[-2])

def get_logging_info(master_cfg, exp_cfg):
    # assume logging variables are all lists with no more than one entry
    logging_labels = ["read_logging", "migration_logging"]
    for i in logging_labels:
      if (i in master_cfg) and (len(master_cfg[i]) > 0):
        exp_cfg[i] = master_cfg[i][0]

def get_migration_info(master_cfg, exp_cfg):
    # assume migration variables are all lists with no more than one entry
    migration_labels = ["migration_policy", "migration_rand_range_num", "migration_rand_range_size"]
    for i in migration_labels:
      if (i in master_cfg) and (len(master_cfg[i]) > 0):
        exp_cfg[i] = master_cfg[i][0]

def nested_call(master_cfg, exp_cfg, default_log_path, exp_name):
    for read_ratio in master_cfg["read_ratio"]:
        exp_cfg["read_ratio"] = read_ratio
        for read_logging in master_cfg["read_logging"]:
            exp_cfg["read_logging"] = read_logging
            for mig_logging in master_cfg["migration_logging"]:
                exp_cfg["migration_logging"] = mig_logging
                for mig_policy in master_cfg["migration_policy"]:
                    exp_cfg["migration_policy"] = mig_policy
                    if mig_policy == 1:
                        exp_cfg["migration_rand_range_num"] = 1
                        for mig_metric in master_cfg["migration_metric"]:
                            exp_cfg["migration_metric"] = mig_metric

                            cfg_str = create_config_str(exp_cfg)
                            log_folder = os.path.join(default_log_path+'/'+exp_name+'/'+cfg_str+'/')
                            os.makedirs(os.path.dirname(log_folder), exist_ok=True)
                            write_config_file(exp_cfg, log_folder+"config")
                            run_single_exp(exp_cfg, log_folder)
                            print ("Logs: ", log_folder)

                    else:
                        for rand_range_num in master_cfg["migration_rand_range_num"]:
                            exp_cfg["migration_rand_range_num"] = rand_range_num
                            for mig_metric in master_cfg["migration_metric"]:
                                exp_cfg["migration_metric"] = mig_metric

                                cfg_str = create_config_str(exp_cfg)
                                log_folder = os.path.join(default_log_path+'/'+exp_name+'/'+cfg_str+'/')
                                os.makedirs(os.path.dirname(log_folder), exist_ok=True)
                                write_config_file(exp_cfg, log_folder+"config")
                                run_single_exp(exp_cfg, log_folder)
                                print ("Logs: ", log_folder)

def run_exp(exp_name):
    exp_cfg = {}
    for mem_alloc in master_cfg["mem_alloc"]:
        exp_cfg["mem_alloc"] = mem_alloc
        for cpu_alloc in master_cfg["cpu_alloc"]:
            exp_cfg["cpu_alloc"] = cpu_alloc
            for code in master_cfg["code"]:
                exp_cfg["code"] = code
                for ssd in master_cfg["ssd"]:
                    exp_cfg["ssd"] = ssd
                    for optane_ratio in master_cfg["optane_ratio"]:
                        exp_cfg["optane_ratio"] = optane_ratio
                        for pop_cache_size in master_cfg["pop_cache_size"]:
                            exp_cfg["pop_cache_size"] = pop_cache_size
                            for pop_cache_thresh in master_cfg["pop_cache_thresh"]:
                                exp_cfg["pop_cache_thresh"] = pop_cache_thresh
                                for blockcache_size in master_cfg["blockcache_size"]:
                                    exp_cfg["blockcache_size"] = blockcache_size
                                    for num_partitions in master_cfg["num_partitions"]:
                                        exp_cfg["num_partitions"] = num_partitions
                                        for num_clients in master_cfg["num_clients"]:
                                            #exp_cfg["num_clients"] = num_clients
                                            exp_cfg["num_clients"] = num_partitions
                                            for db_size in master_cfg["db_size"]:
                                                exp_cfg["db_size"] = db_size
                                                for key_size in master_cfg["key_size"]:
                                                    exp_cfg["key_size"] = key_size
                                                    for value_size in master_cfg["value_size"]:
                                                        exp_cfg["value_size"] = value_size
                                                        compile_src_changes(exp_cfg)
                                                        for loop in master_cfg["loop"]:
                                                            exp_cfg["loop"] = loop
                                                            for workload in master_cfg["workload"]:
                                                                exp_cfg["workload"] = workload
                                                                # distribution is deprecated
                                                                for distribution in master_cfg["distribution"]:
                                                                    exp_cfg["distribution"] = distribution
                                                                    for distribution_param in master_cfg["distribution_param"]:
                                                                        exp_cfg["distribution_param"] = distribution_param
                                                                        for workload_operations in master_cfg["workload_operations"]:
                                                                            exp_cfg["workload_operations"] = workload_operations
                                                                            for warmup_ratio in master_cfg["warmup_ratio"]:
                                                                                exp_cfg["warmup_ratio"] = warmup_ratio
                                                                                for pop_file in master_cfg["pop_file"]:
                                                                                    exp_cfg["pop_file"] = pop_file
                                                                                    nested_call(master_cfg, exp_cfg, default_log_path, exp_name)
                                                                                    #get_logging_info(master_cfg, exp_cfg)
                                                                                    #get_migration_info(master_cfg, exp_cfg)
                                                                                    #cfg_str = create_config_str(exp_cfg)
                                                                                    #log_folder = os.path.join(default_log_path+'/'+exp_name+'/'+cfg_str+'/')
                                                                                    #os.makedirs(os.path.dirname(log_folder), exist_ok=True)
                                                                                    #write_config_file(exp_cfg, log_folder+"config")
                                                                                    ##load_db(db_type)
                                                                                    #run_single_exp(exp_cfg, log_folder)
                                                                                    #print ("Logs: ", log_folder)



# MAIN
# example: python3 run.py config_test.yml p2_exp
assert(len(sys.argv) == 3)
read_exp_master_config(sys.argv[1])
exp_name = sys.argv[2]

run_exp(exp_name)
