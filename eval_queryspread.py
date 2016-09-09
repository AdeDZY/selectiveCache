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
    for shard in range(args.n_shards):
        for line in open(args.shard_qtfdf_dir + "/{0}.qtfdf".format(shard)):
            term, tid, qtfdf, qtf, df = line.strip().split()
            tid = int(tid)
            term2shards[tid] = term2shards.get(tid, 0) + 1

    for tid, nshard in term2shards:
        print tid, nshard

