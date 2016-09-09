#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join
import math

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("shard_qtfdf_dir")
    parser.add_argument("org_qtfdf_file", type=argparse.FileType('r'))
    parser.add_argument("n_shards", type=int)
    parser.add_argument("output_file", type=argparse.FileType('w'))
    args = parser.parse_args()

    org_qtfdf = []
    for line in args.org_qtfdf_file:
        term, tid, qtfdf, qtf, df = line.split(' ')
        org_qtfdf.append((int(tid), term, qtfdf, qtf, df))

    # recompute df
    cut_df = {}
    for shard in range(1, args.n_shards + 1):
        met = set()
        # select global terms
        p = join(args.shard_qtfdf_dir, str(shard) + '.qtfdf')
        with open(p) as f:
            for line in f:  # local query log
                term, tid, qtfdf, qtf, df = line.strip().split()
                tid = int(tid)
                df = int(df)
                qtf = int(qtf)
                if df <= 0 or tid in met:
                    continue
                if qtf <= 0 and df > 500:
                    continue
                cut_df[tid] = cut_df.get(tid, 0) + df
                met.add(tid)

    new_qtfdf = []
    for tid, term, tmp1, qtf, tmp2 in org_qtfdf:
        if tid not in cut_df:
            continue
        df = cut_df[tid]
        qtf = int(qtf)
        qtfdf = qtf/float(tmp2)
        new_qtfdf.append((qtfdf, "{0} {1} {2} {3} {4}\n".format(term, tid, qtfdf, qtf, df)))

    new_qtfdf = sorted(new_qtfdf, reverse=True)
    for qtfdf, line in new_qtfdf:
        args.output_file.write(line)



    # print total,
    # for shard, t in shards_total.items():
    #     print "{0}:{1}".format(shard, t/1000), #/float(total)),
    # print ""
