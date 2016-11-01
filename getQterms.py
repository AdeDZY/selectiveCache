#!/opt/python27/bin/python
import argparse
import math

parser = argparse.ArgumentParser()
parser.add_argument("queryfile")
parser.add_argument("stop_file")
parser.add_argument("--rankbiased", '-r', action="store_true")

args = parser.parse_args()

termset = {}

stoplist = set()
for line in open(args.stop_file):
    stoplist.add(line.strip())

for line in open(args.queryfile):

    if args.rankbiased:
        items = line.strip().split('\t')
        r = int(items[-1])
        terms = items[0].split()
    else:
        terms = line.strip().split()

    for t in terms:
        if t.isalpha() and t not in stoplist:
            if args.rankbiased:
                #termset[t] = termset.get(t, 0) + 1.0/math.log(r + 1)
                termset[t] = termset.get(t, 0) + 1.0/math.log(int(r/3)*3 + 2)
            else:
                termset[t] = termset.get(t, 0) + 1.0

t2 = sorted([(v, k) for k, v in termset.items()], reverse=True)

for f, t in t2:
    print t + " " + str(f)
