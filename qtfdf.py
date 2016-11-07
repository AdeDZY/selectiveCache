#!/opt/python27/bin/python
import argparse
import math

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("int_term_file", type=argparse.FileType('r'))
    args = parser.parse_args()

    all_df = {}
    all_qtf = {}
    all_term = {}
    for line in args.int_term_file:
        term, qtf, tid, df = line.split(' ')
        qtf = float(qtf)
        tid = int(tid)
        df = int(df)
        if df < 1: continue
        all_df[tid] = df
        all_qtf[tid] = all_qtf.get(tid, 0) + qtf
        if tid not in all_term:
            all_term[tid] = term  

    all_qtfdf = {}
    for tid in all_qtf:
        qtf = all_qtf[tid]
        df = all_df[tid]
        all_qtfdf[tid] = float(qtf)/df
    res = sorted([(v, k) for k, v in all_qtfdf.items()], reverse=True)
    for qtfdf, tid in res:
        print all_term[tid], tid, qtfdf, all_qtf[tid], all_df[tid]
