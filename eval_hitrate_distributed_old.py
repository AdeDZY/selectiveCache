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
    parser.add_argument("--shardlim", "-l", type=int, default=1003)
    args = parser.parse_args()

    # read stopwords
    stoplist = set()
    for line in args.stoplist_file:
        stoplist.add(line.strip())

    # read shard load balancing
    shard2machine = {}
    m = 0
    for line in args.shard_distribution_file:
        shards = [int(t) for t in line.split()]
        for s in shards:
            shard2machine[s] = m
        m += 1
    n_machines = m

    # read vocab
    vocab = {}
    for line in args.intQterm_file:
        term, tid, df = line.split(' ')
        tid = int(tid)
        vocab[term] = tid

    # read cached
    cached = []
    for m in range(1, n_machines + 1):
        cached_queries_file = open(join(args.cached_queries_dir, str(m)))
        tmp = set()
        for line in cached_queries_file:
            items = line.split(' ')
            term, tid = items[0:2]
            tid = int(tid)
            tmp.add(tid)
        cached.append(tmp)

    # read shardlist
    shardlist = {}
    machinelist = {}
    for line in args.query_shardlist_file:
        items = [int(t) for t in line.strip().split(' ')]
        q = items[0]
        lim = min(len(items) - 1, args.shardlim)
        shardlist[q - 1] = items[1:lim + 1]
        machinelist[q - 1] = {} 
        for s in items[1:lim + 1]:
            m = shard2machine[s]
            machinelist[q - 1][m] = machinelist[q - 1].get(m, 0) + 1

    # queries
    n_all_cached = 0
    n_hit = 0
    n_search = 0
    n_queries = 0
    miss_single = 0
    miss_multi = 0
    qid = 0
    for line in args.test_queries_file:
        if qid not in shardlist:
            qid += 1
            continue
        qterms = line.split(' ')
        all_cached = True
        has_term = False
        for term in qterms:
            term = term.strip().lower()
            if not term.isalpha() or term in stoplist:
                continue
            has_term = True
            tid = vocab.get(term, -1)
            for m, t in machinelist[qid].items():
                n_search += t
                if tid not in cached[m]:
                    all_cached = False
                    #print term, m, 
                else:
                    n_hit += t
        if all_cached and has_term:
            n_all_cached += 1
        if not all_cached and has_term:
            if len(qterms) == 1:
                miss_single += 1
            else:
                miss_multi += 1
            pass
            #print line 
        if has_term:
            n_queries += 1
        qid += 1

    print n_queries, n_all_cached, n_search, n_hit, float(n_hit)/n_search, miss_single, miss_multi


