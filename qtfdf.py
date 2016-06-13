#!/opt/python27/bin/python
import argparse
import math

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("int_term_file", type=argparse.FileType('r'))
    args = parser.parse_args()

    res = []
    for line in args.int_term_file:
        term, qtf, tid, df = line.split(' ')
        qtf = int(qtf)
        tid = int(tid)
        df = int(df)
        if df < 1 :
            continue
        #qtfdf = float(qtf)/math.log(df)
        qtfdf = float(qtf)/df
        res.append((qtfdf, tid, term, qtf, df))

    res = sorted(res, reverse=True)
    for qtfdf, tid, term, qtf, df in res:
        print term, tid, qtfdf, qtf, df
