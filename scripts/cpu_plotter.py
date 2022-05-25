import matplotlib.pyplot as plt
import numpy
import os
import sys


def plotter(log):
    f = open(log, 'r')
    data = f.readlines()

    user = []
    nice = []
    system = []
    iowait = []
    steal = []
    idle = []
    parse = False

    for d in data:
      if "avg-cpu" in d:
        parse = True
        continue
      if parse == True:
        ds = d.split()
        print (ds)
        user.append(float(ds[0]))
        nice.append(float(ds[1]))
        system.append(float(ds[2]))
        iowait.append(float(ds[3]))
        steal.append(float(ds[4]))
        idle.append(float(ds[5]))
        parse = False

    plt.plot(idle, label='idle')
    plt.plot(system, label='system')
    plt.plot(user, label='user')
    # plt.plot(nice, label='nice')
    plt.plot(iowait, label='iowait')
    # plt.plot(steal, label='steal')
    
    plt.legend()
    plt.xlabel("time (secs)")
    plt.ylabel("%")

    plt.show()

# command line args
assert(len(sys.argv) == 2)
log = sys.argv[1]
plotter(log)