#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join
import math
import numpy as np

def softmax(w, t = 1.0):
    e = np.exp(np.array(w) / t)
    dist = e / np.sum(e)
    return dist

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("shard_qtfdf_dir")
    parser.add_argument("shard_feature_dir")
    parser.add_argument("global_qtfdf_file", type=argparse.FileType('r'))
    parser.add_argument("memory_size", type=float, help="in GB, per machine")
    parser.add_argument("shard_distribution_file", type=argparse.FileType('r'))
    parser.add_argument("output_dir")
    args = parser.parse_args()

    upper_bound = args.memory_size * 1024 * 1024 * 1024 / 8

    global_qtfdf = []
    for line in args.global_qtfdf_file:
        term, tid, qtfdf, qtf, df = line.split(' ')
        global_qtfdf.append((int(tid), line))


    # read machine df and machine query log
    shard_df = {}
    shard_qtf = {}
    for shard in range(1, 124):
        shard_df[shard] = {}
        p = join(args.shard_feature_dir, str(shard) + '.feat')
        with open(p) as f:
            for line2 in f:
                tid, df, ctf, cent = line2.split(' ')
                tid = int(tid)
                df = int(df)
                ctf = int(ctf)
                if tid == -1:
                    continue
                shard_df[shard][tid] = df

        shard_qtf[shard] = {}
        for line4 in open(args.shard_qtfdf_dir + "/{0}.qtfdf".format(shard)):
            term, tid, qtfdf, qtf, df = line4.strip().split()
            tid = int(tid)
            df = int(df)
            if df <= 0:
                continue
            shard_qtf[shard][tid] = shard_qtf[shard].get(tid, 0) + int(qtf)

    # for each machine
    i = 0 
    for line in args.shard_distribution_file:
        i += 1 
        shards = [int(t) for t in line.split()] # shards on this machine
        fout = open(join(args.output_dir, str(i)), 'w')  # cached file for this machine

        global_total = 0
        spear = 0
        printed = False
        for tid, line3 in global_qtfdf:
            qtfs = [(shard_qtf[shard].get(tid, 0), shard) for shard in range(1, 124)]
            qtfs = sorted(qtfs, reverse=True)
            vals = [val for val, shard in qtfs]
            eqtfs = softmax(vals, float(vals[0]))
            for k in range(123):
                if eqtfs < 0.0075:
                    break
            cutoff = k
            j = 0
            for qtf, shard in qtfs[0:k]:
                j += 1 
                if shard not in shards:
                    continue
                if shard_df[shard].get(tid, 0) > 0:
                    if global_total + shard_df[shard][tid] <= upper_bound:
                        fout.write(line3.strip() + ' ' + str(shard) + '\n')
                        global_total += shard_df[shard][tid]
                    else:
                        spear += shard_df[shard][tid] 
                        if not printed and global_total + spear >= upper_bound:
                            print spear, line3.strip() 
                            printed = True
