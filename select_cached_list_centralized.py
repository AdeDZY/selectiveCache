#!/opt/python27/bin/python
import argparse
import sys

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("term_qtfdf_file", type=argparse.FileType('r'))
    parser.add_argument("memory_size", type=float, help="in GB")
    args = parser.parse_args()

    total = 0
    upper_bound = args.memory_size * 1024 * 1024 * 1024 / 8
    selected = set()
    for line in args.term_qtfdf_file:
        term, tid, qtfdf, qtf, df = line.split(' ')
        df = int(df)
        if total + df < upper_bound:
            if df > 0 and tid not in selected:
                selected.add(tid)
                print line.strip()
                total += df
        else:
            break
