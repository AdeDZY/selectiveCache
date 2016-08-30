#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("cached_queries_dir")
    parser.add_argument("intQterm_file", type=argparse.FileType('r'))
    parser.add_argument("test_queries_file", type=argparse.FileType('r'))
    parser.add_argument("query_shardlist_file", type=argparse.FileType('r'))
    parser.add_argument("shard_distribution_file", type=argparse.FileType('r'))
    parser.add_argument("stoplist_file", type=argparse.FileType('r'))
    parser.add_argument("--shardlim", "-l", type=int, default=123)
    args = parser.parse_args()

    # read stopwords
    stoplist = set()
    for line in args.stoplist_file:
        stoplist.add(line.strip())

    # read shard load balancing
    shard2machine = {}
    m = 0
    n_shards = 0
    for line in args.shard_distribution_file:
        shards = [int(t) for t in line.split()]
        for s in shards:
            shard2machine[s] = m
            n_shards += 1
        m += 1
    n_machines = m

    # read vocab
    vocab = {}
    for line in args.intQterm_file:
        term, tid, df = line.split(' ')
        tid = int(tid)
        vocab[term] = tid

    # read cached
    cached = [set() for i in range(n_shards)]
    for m in range(1, n_machines + 1):
        cached_queries_file = open(join(args.cached_queries_dir, str(m)))
        tmp = set()
        for line in cached_queries_file:
            items = line.split(' ')
            tid = int(items[1])
            shards = items[5:]
            for s in shards:
                cached[int(s) - 1].add(tid)

    # read shardlist
    shardlist = {}
    machinelist = {}
    for line in args.query_shardlist_file:
        items = [int(t) for t in line.strip().split(' ')]
        q = items[0]
        shardlist[q - 1] = items[1:min(args.shardlim + 1,len(items))]
        machinelist[q - 1] = set()
        for s in items[1:min(args.shardlim + 1,len(items))]:
            m = shard2machine[s]
            machinelist[q - 1].add(m)

    # queries
    n_all_cached = 0
    n_hit = 0
    n_search = 0
    n_queries = 0
    qid = 0
    for line in args.test_queries_file:
        qterms = line.split(' ')
        all_cached = True
        has_term = False
        for term in qterms:
            term = term.strip().lower()
            if not term.isalpha() or term in stoplist:
                continue
            has_term = True
            tid = vocab.get(term, -1)
            for s in shardlist[qid]:
                n_search += 1
                if tid not in cached[s - 1]:
                    # print term, s,
                    all_cached = False
                else:
                    n_hit += 1
        if all_cached and has_term:
            n_all_cached += 1
        if has_term and not all_cached:
            pass
            # print line
        if has_term:
            n_queries += 1
        qid += 1

    print n_queries, n_all_cached, n_search, n_hit, float(n_hit)/n_search


