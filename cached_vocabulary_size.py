#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("cached_queries_dir")
    args = parser.parse_args()

    n_machines = 16 
    # read cached
    cached = set() 
    for m in range(1, n_machines + 1):
        cached_queries_file = open(join(args.cached_queries_dir, str(m)))
        tmp = set()
        for line in cached_queries_file:
            items = line.split(' ')
            term, tid = items[0:2]
            tid = int(tid)
            cached.add(tid)
            tmp.add(tid)
        print len(tmp)

    print len(cached)

