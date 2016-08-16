#!/opt/python27/bin/python
import argparse
import math

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("int_term_file", type=argparse.FileType('r'))
    parser.add_argument("shard_df_file", type=argparse.FileType('r'))
    args = parser.parse_args()

    # read shard df
    shard_df = {}
    for line in args.shard_df_file:
        tid, df = [int(t) for t in line.strip().split()]
        shard_df[tid] = df

    # get dtfdf
    all_qtf = {}
    all_df = {}
    all_term = {}
    for line in args.int_term_file:
        term, qtf, tid, df = line.split(' ')
        qtf = int(qtf)
        tid = int(tid)
        df = shard_df.get(tid, 0)
        if df < 1:
            continue
        all_df[tid] = df
        all_qtf[tid] = all_qtf.get(tid, 0) + qtf
        if tid not in all_term:
            all_term[tid] = term  

    all_qtfdf = {}
    for tid in all_qtf:
        qtf = all_qtf[tid]
        df = all_df[tid]
        all_qtfdf[tid] = (float(qtf))/(df)
    res = sorted([(v, k) for k, v in all_qtfdf.items()], reverse=True)
    for qtfdf, tid in res:
        if tid not in shard_df: continue
        print all_term[tid], tid, qtfdf, all_qtf[tid], shard_df[tid]
