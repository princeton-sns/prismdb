import sys
import random

def scramble(infile):
    with open(infile, 'r') as f:
        data = f.readlines()
        prev_freq = -1
        keys = []
        #outfile = ""
        #if (len(infile.split("/")[-1].split(".")) == 2):
        #    extn = infile.split("/")[-1].split(".")[1]
        #    outfile = infile.split("/")[-1].split(".")[0]+"_scrambled."+extn
        #else:
        #    outfile = infile+"_scrambled"

        out = open(infile+"_scrambled", 'w')
        for d in data:
            if (d == "\n"):
                continue
            freq = int(d.split()[0]) 
            key = int(d.split()[1])
            if freq != prev_freq:
                random.shuffle(keys)
                for k in keys:
                    out.write(str(prev_freq)+" "+str(k)+"\n")
                keys = []
                prev_freq = freq
                keys.append(key)
            else:
                keys.append(key)
        if (len(keys) != 0):
            random.shuffle(keys)
            for k in keys:
                out.write(str(prev_freq)+" "+str(k)+"\n")
        out.close()

def parse_and_scramble(infile):
    print ("Not implemented")

# command line args
# option = 1 (scramble parsed log file) or 2 (parse log file and then scramble it). Option 2 not implemented
assert(len(sys.argv) == 3)
option = sys.argv[1]
infile = sys.argv[2]

if option == "1":
    scramble(infile)
elif option == "2":
    parse_and_scramble(infile)
else:
    print ("ERROR option", option)
