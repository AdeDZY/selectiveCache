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
    args = parser.parse_args()

    upper_bound = args.memory_size * 1024 * 1024 * 1024 / 8

    l = []
    for line in args.term_qtfdf_file:
        term, tid, qtfdf, qtf, df = line.split(' ')
        l.append((int(tid), line, int(qtf)))

    i = 0
    for line in args.shard_distribution_file:
        # for each machine
        i += 1
        shards = [int(t) for t in line.split()]
        fout = open(join(args.output_dir, str(i)), 'w')
        machine_df = {}
        for shard in shards:
            p = join(args.shard_feature_dir, str(shard) + '.feat')
            with open(p) as f:
                for line in f:
                    tid, df, ctf, cent = line.split(' ')
                    tid = int(tid)
                    df = int(df)
                    ctf = int(ctf)
                    if tid == -1:
                        continue
                    machine_df[tid] = machine_df.get(tid, 0) + df

        # re-sort terms based on qtf/machine_df
        tmp = []
        for tid, line, qtf in l:
            if tid not in machine_df:
                continue
            tmp.append((float(qtf)/machine_df[tid], tid, line))
        tmp = sorted(tmp, reverse=True)

        # select terms
        total = 0
        for v, tid, line in tmp:
            if tid not in machine_df:
                continue
            if total + machine_df[tid] < upper_bound:
                fout.write(line)
                total += machine_df[tid]
            else:
                break
