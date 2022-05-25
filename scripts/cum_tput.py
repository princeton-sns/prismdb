import matplotlib as mpl
mpl.use('Agg')

import sys

import matplotlib.pyplot as plt
import matplotlib.ticker as tick

include_warmup = False

def plot_cum_tput(log):
    fd = open(log, 'r')
    data = fd.readlines()
    tid_tput = {}
    prev_time = -1
    i = 0
    plot_x = []
    plot_y = []
    start = False
    for line in data:
        if "DBBENCH_START" in line:
          if include_warmup:
            if "ycsbwarmup" in line:
              start = True
          else:
            if "ycsbwkld" in line:
              start = True
        if "twitter" in line:
          start = True
        #if "DBBENCH_START ycsbwkld" in line:
        #    start = True
        if start == False:
            continue
        if "ops/second in" in line:
            s = line.split()
            # print (s)
            offset = 0
            if "microsecond" in line:
                offset = 1
            tid = s[3+offset].split(":")[0]
            tput_str = s[7+offset].split(",")[0].split("(")[1]
            time = int(float(s[10+offset].split(",")[1].split(")")[0]))
            #if (time < 250 or time > 330):
            #    continue
            #if tput_str == "-nan":
            #    continue
            tput = float(tput_str)
            if prev_time == -1:
                prev_time = time
            else:
                if time != prev_time:
                    cum_tput = 0
                    num_tids = 0
                    for (k,v) in tid_tput.items():
                        #print (k, v)
                        cum_tput += float(float(sum(v)/len(v))/1000)
                        num_tids += 1
                    print ("Cumulative tput =", round(cum_tput, 3), "num tids =", num_tids, "time =", time)
                    tid_tput.clear()
                    plot_x.append(i)
                    i += 1
                    plot_y.append(cum_tput)
                    prev_time = time

            if tid not in tid_tput:
                tid_tput[tid] = [tput]
            else:
                tid_tput[tid].append(tput)

    cum_tput = 0
    if len(tid_tput) != 0:
        for (k,v) in tid_tput.items():
            # print (k, v)
            cum_tput += int(int(sum(v)/len(v))/1000)
        plot_x.append(i)
        i += 1
        plot_y.append(cum_tput)
        print ("Cumulative tput =", cum_tput)

    fd.close()

    plt.plot(plot_x, plot_y)
    plt.xlabel("time (sec)")
    plt.ylabel("tput (Kops/sec)")
    plt.savefig("cumulative_tput.png")
    plt.close()

log = sys.argv[1]

plot_cum_tput(log)
