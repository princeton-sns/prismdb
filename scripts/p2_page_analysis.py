from collections import Counter

f = open ('os_pagecache_analysis.log', 'r')
data = f.readlines()

pagesize = 4096
count = 0

for d in data:
    a = d.split()
    size = int(a[2])
    if (size != 200):
        continue
    if (count < 10):
        count += 1
        print ("skipping count=", count)
        continue
    i = 0
    biddict = {}
    for slot in a:
        if (i<3):
            i += 1
            continue
        bid = int(int(slot)/(int(int(pagesize)/size)))
        #print ("INFO slot pagesize size bid a", slot, pagesize, size, bid, pagesize/size)
        if bid not in biddict:
            biddict[bid] = 1
        else:
            biddict[bid] += 1

    freq = list(biddict.values())
    #counted = Counter(freq)
    #ordered = [value for value, count in counted.most_common()]
    new_vals = Counter(freq).most_common()
    new_vals = new_vals[::-1] #this sorts the list in ascending order
    print ("Num free slots in a block : freq")
    for a, b in new_vals:
        print (a," : ", b)

    #print ("Unique values and freq: ", ordered)
    #freq_set = set(freq)
    #unique_freq = (list(freq_set))
    #print ("Num unique values ", len(unique_freq))
    #for x in unique_freq:
    #    print (x)
    #freq.sort(reverse=False)
    #print (freq)
    #abd

