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
    parser.add_argument("memory_ratio", type=float, help="cache ratio for global caching")
    parser.add_argument("output_dir")
    args = parser.parse_args()

    upper_bound = args.memory_size * 1024 * 1024 * 1024 / 8

    global_qtfdf = []
    for line in args.term_qtfdf_file:
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

        global_total = 0
        global_cached = set()
        for tid, line3 in global_qtfdf:
            if tid not in machine_df:
                continue
            if global_total + machine_df[tid] < upper_bound * args.memroy_ratio:
                for shard in shards:
                    if shard_df[shard].get(tid, 0) > 0:
                        fout.write(line.strip() + ' ' + str(machine_df.get(tid, 0)) + ' ' + str(shard) + '\n')
                global_total += machine_df[tid]
                global_cached.add(tid)
            else:
                break
                # continue

        # sort all qtfdf
        tmp = []
        for shard in shards:
            for line in open(args.shard_qtfdf_dir + "/{0}.qtfdf".format(shard)):
                term, tid, qtfdf, qtf, df = line.strip().split()
                tid = int(tid)
                qtfdf = float(qtfdf)
                df = int(df)
                if df <= 0: continue
                tmp.append((qtfdf, term, tid, qtf, df, shard))
        tmp = sorted(tmp, reverse=True)

        total = 0
        shards_total = {}
        for qtfdf, term, tid, qtf, df, shard in tmp:
            if tid in global_cached:
                continue
            if total + df < upper_bound - global_total:
                shards_total[shard] = shards_total.get(shard, 0) + df
                fout.write(term + ' ' + str(tid) + ' ' + str(qtfdf) + ' ' + str(qtf) + ' ' + str(df) + ' ' + str(shard))
                fout.write('\n')
                total += df
            else: 
                continue

       # print total,
       # for shard, t in shards_total.items():
       #     print "{0}:{1}".format(shard, t/1000), #/float(total)),
       # print ""