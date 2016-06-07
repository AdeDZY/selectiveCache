#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("term_qtfdf_file", type=argparse.FileType('r'))
    parser.add_argument("memory_size", type=float, help="in GB, per machine")
    parser.add_argument("shard_feature_dir")
    parser.add_argument("output_dir")
    args = parser.parse_args()

    upper_bound = args.memory_size * 1024 * 1024 * 1024 / 8

    l = []
    for line in args.term_qtfdf_file:
        term, tid, qtfdf, qtf, df = line.split(' ')
        l.append((tid, line.strip()))

    i = 0
    while True:
        i += 1
        p = join(args.shard_feature_dir, str(i) + '.feat')

        if not isfile(p):
            break

        with open(p) as f, open(join(args.output_dir), str(i)) as fout:
            shard_df = {}
            for line in f:
                tid, df, ctf, cent = line.split(' ')
                tid = int(tid)
                df = int(df)
                ctf = int(ctf)
                if tid == -1:
                    continue
                shard_df[tid] = df

            total = 0
            for tid, line in l:
                if tid not in shard_df:
                    continue
                if total + shard_df[tid] < upper_bound:
                    print line
                    total += df
                else:
                    break