#!/opt/python27/bin/python
import argparse
import numpy as np


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("shard_qtfdf_dir")
    parser.add_argument("n_machines", type=int)
    parser.add_argument("n_shards", type=int)
    args = parser.parse_args()

    i = 0
    shards = [i + 1 for i in range(args.n_shards)]
    shard_loads = [0 for i in range(args.n_shards)]
    for shard in shards:
        for line in open(args.shard_qtfdf_dir + "/{0}.qtfdf".format(shard)):
            term, tid, qtfdf, qtf, df = line.strip().split()
            shard_loads[shard - 1] += int(df) * int(qtf)

    tmp = sorted([(load, shard) for shard, load in enumerate(shard_loads)], reverse=True)
    distr = [[] for m in range(args.n_machines)]
    distr_m = [0 for m in range(args.n_machines)]
    for load, shard in tmp:
        curr_min = np.argmin(np.array(distr_m))
        distr[curr_min].append(shard)
        distr_m[curr_min] += load

    for d in distr:
        for i in d:
            print i + 1,
        print ''



