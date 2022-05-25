import matplotlib as mpl
mpl.use('Agg')

import sys

import matplotlib.pyplot as plt
import matplotlib.ticker as tick

def plot_optane_vs_qlc_reads(log):
    fd = open(log, 'r')
    data = fd.readlines()
    plot_x = []
    plot_y = []
    i = 0
    for line in data:
        if "optane reads" not in line:
            continue
        s = line.split()
        opt = int(s[3])
        qlc = int(s[6])
        # each sample is over 1M operations
        plot_x.append(i)
        plot_y.append(opt)
        i = i+1
    fd.close()

    plt.scatter(plot_x, plot_y, marker='.')
    plt.xlabel("num read operations (M)")
    plt.ylabel("optane reads (%)")
    plt.ylim(ymin=0)
    plt.savefig("optane_vs_qlc_reads.png")
    plt.grid()
    plt.close()

log = sys.argv[1]

plot_optane_vs_qlc_reads(log)


