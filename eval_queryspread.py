#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join
import math

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("shard_qtfdf_dir")
    parser.add_argument("--n_shards", "-n", type=int, default=123)
    args = parser.parse_args()

    term2shards = {}
    for shard in range(1, args.n_shards + 1):
        for line in open(args.shard_qtfdf_dir + "/{0}.qtfdf".format(shard)):
            term, tid, qtfdf, qtf, df = line.strip().split()
            tid = int(tid)
            if tid not in term2shards:
                term2shards[tid] = [0 for i in range(args.n_shards)]
            term2shards[tid][shard - 1] = float(qtf)
    tmp = sorted([(len([v for v in vals if v > 5]), tid, vals) for tid, vals in term2shards.items()], reverse=True)

    for l, tid, vals in tmp:
        #print tid, l, 
        #vals = sorted(vals, reverse=True) 
        #for v in vals:
        #    print v,
        #print ""
        if l >= 60: 
            print tid,

