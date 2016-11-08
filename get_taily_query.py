#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("shardlist_file", type=argparse.FileType('r'))
    parser.add_argument("query_file", type=argparse.FileType('r'))
    args = parser.parse_args()

    # read query txt
    queries = []
    for line in args.query_file:
        queries.append(line)

    # read shardlist
    for line in args.shardlist_file:
        items = [int(t) for t in line.strip().split()]
        qid = items[0]
        query = queries[qid - 1]
        print query



