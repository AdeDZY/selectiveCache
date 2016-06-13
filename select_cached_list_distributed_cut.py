#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("term_qtfdf_file", type=argparse.FileType('r'))
    parser.add_argument("memory_size", type=float, help="in GB, per machine")
    parser.add_argument("shard_feature_dir")
    parser.add_argument("shard_distribution_file", type=argparse.FileType('r'))
    parser.add_argument("output_dir")
    parser.add_argument("cutoff", type=int)
    args = parser.parse_args()

    upper_bound = args.memory_size * 1024 * 1024 * 1024 / 8

    l = []

    for line in args.term_qtfdf_file:
        line = line.strip()
        term, tid, qtfdf, qtf, df = line.split(' ')
        l.append((int(tid), line))

    machine2shards = []
    shards_df = {}
    for line in args.shard_distribution_file:
        shards = [int(t) for t in line.split()]
        machine2shards.append(shards)
        for shard in shards:
            shard_df = {}
            p = join(args.shard_feature_dir, str(shard) + '.feat')
            with open(p) as f:
                for line in f:
                    tid, df, ctf, cent = line.split(' ')
                    tid = int(tid)
                    df = int(df)
                    ctf = int(ctf)
                    if tid == -1:
                        continue
                    shard_df[tid] = df
            shards_df[shard] = shard_df

    shards_df_cut = {}
    for tid, line in l:
        # get this term's df in each shard
        tmp = [(shard_df.get(tid, 0), shard) for shard, shard_df in shards_df.items()]
        tmp = sorted(tmp, reverse=True)
        for i in range(args.cutoff):
            df, shard = tmp[i]
            if df == 0:
                break
            if shard not in shards_df_cut:
                shards_df_cut[shard] = {}
            shards_df_cut[shard][tid] = df

    # get machine df
    m = 0
    for shards in machine2shards:
        m += 1
        fout = open(join(args.output_dir, str(m)), 'w')
        machine_df = {}
        shard_with_term = {}
        for shard in shards:
            if shard not in shards_df_cut:
                continue
            for tid, df in shards_df_cut[shard].items():
                machine_df[tid] = machine_df.get(tid, 0) + df
                if tid not in shard_with_term:
                    shard_with_term[tid] = []
                shard_with_term[tid].append(shard)

        total = 0
        for tid, line in l:
            if tid not in machine_df:
                continue
            if total + machine_df[tid] < upper_bound:
                fout.write(line)
                for shard in shard_with_term:
                    fout.write(str(shard) + ' ') # shards with this term
                fout.write('\n')
                total += machine_df[tid]
            else:
                break
