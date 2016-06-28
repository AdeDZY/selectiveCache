#!/opt/python27/bin/python
import argparse
import random
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("n_shards", type=int)
    parser.add_argument("n_machines", type=int)
    parser.add_argument("shard_distribution_file", type=argparse.FileType('w'))
    args = parser.parse_args()

    l = [i for i in range(1, args.n_shards + 1)] 
    #random.shuffle(l)

    t = int((float(args.n_shards) / args.n_machines))
    for i in range(args.n_machines):
        s = i * t
        e = s + t
        if e > args.n_shards:
            e = args.n_shards
        elif e + t > args.n_shards:
            e = args.n_shards
        sub_l = l[s: e]
        args.shard_distribution_file.write(' '.join([str(s) for s in sub_l]))
        args.shard_distribution_file.write('\n')



