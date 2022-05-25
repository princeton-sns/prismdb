import sys

log = sys.argv[1]
print (log)
f = open(log, 'r')
data = f.readlines()

#batch_size=1000000
batch_size=10000
optane_gets=0
qlc_gets=0
total_gets=0

for d in data:
    if "Optane" in d:
        optane_gets += 1
    elif "QLC" in d:
        qlc_gets += 1
    if (optane_gets + qlc_gets == batch_size):
        total_gets += batch_size
        print("Total", total_gets, "optane-qlc read (%) =", int(float(optane_gets)*100/batch_size), int(float(qlc_gets)*100/batch_size))
        optane_gets = 0
        qlc_gets = 0
