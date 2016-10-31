#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("cached_queries_dir")
    args = parser.parse_args()

    n_machines = 16 
    # read cached
    cached = {}
    for m in range(1, n_machines + 1):
        cached_queries_file = open(join(args.cached_queries_dir, str(m)))
        tmp = set()
        for line in cached_queries_file:
            items = line.strip().split(' ')
            term, tid = items[0:2]
            shard = int(items[-1])
            tid = int(tid)
            if tid not in cached:
                cached[tid] = []
            cached[tid].append(shard)
            tmp.add(tid)
        print len(tmp)

    print len(cached)
    nshard = 0
    #for tid, vals in cached.items():
    #    print tid, len(vals),
    #    nshard += len(vals)
    #    for v in vals:
    #        print v,
    #    print ""

    #print nshard/float(len(cached))
