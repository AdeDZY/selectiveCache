#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("shardlist_file", type=argparse.FileType('r'))
    parser.add_argument("query_file", type=argparse.FileType('r'))
    parser.add_argument("shard_distribution_file", type=argparse.FileType('r'))
    parser.add_argument("output_dir")
    parser.add_argument("cutoff", type=int)
    args = parser.parse_args()

    # read in shard dist
    shard2machine = {}
    i = 0
    for line in args.shard_distribution_file:
        shards = [int(t) for t in line.split()]
        for shard in shards:
            shard2machine[shard] = i
        i += 1
    n_machines = i

    # read query txt
    queries = []
    for line in args.query_file:
        queries.append(line)

    # output_files
    fouts = []
    for i in range(1, n_machines + 1):
        fouts.append(open(join(args.output_dir, str(i)), 'w'))

    # read shardlist
    for line in args.shardlist_file:
        items = [int(t) for t in line.strip().split()]
        qid = items[0]
        query = queries[qid - 1]
        ms = []
        r = 0
        for shard in items[1: min(1 + args.cutoff, len(items))]:
            r += 1
            m = shard2machine[shard]
            ms.append((m, r))
        for m, r in ms:
            fouts[m].write(query.strip() + "\t" + str(r) + '\n')

    for f in fouts:
        f.close()
