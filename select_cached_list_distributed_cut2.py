#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join
import math

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

    i = 0
    # for each machine
    for line in args.shard_distribution_file:
        i += 1
        shards = [int(t) for t in line.split()] # shards on this machine
        fout = open(join(args.output_dir, str(i)), 'w') # cached file for this machine

        # select global terms
        machine_df = {}
        shard_df = {}
        for shard in shards:
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
                    machine_df[tid] = machine_df.get(tid, 0) + df
                    shard_df[shard][tid] = df

        # local qtfdf
        tmp = []
        shard_qtf = {}
        for shard in shards:
            shard_qtf[shard] = {}
            for line4 in open(args.shard_qtfdf_dir + "/{0}.qtfdf".format(shard)):
                term, tid, qtfdf, qtf, df = line4.strip().split()
                tid = int(tid)
                df = int(df)
                if df <= 0:
                    continue
                shard_qtf[shard][tid] = shard_qtf[shard].get(tid, 0) + int(qtf)

        global_total = 0
        for tid, line3 in global_qtfdf:
            if tid not in machine_df:
                continue
            for shard in shards:
                if shard_df[shard].get(tid, 0) > 0 and not (shard_qtf[shard].get(tid, 0) <= 0):
                    if global_total + shard_df[shard][tid] < upper_bound:
                        fout.write(line3.strip() + ' ' + str(shard) + '\n')
                        global_total += shard_df[shard][tid]

       # print total,
       # for shard, t in shards_total.items():
       #     print "{0}:{1}".format(shard, t/1000), #/float(total)),
       # print ""
