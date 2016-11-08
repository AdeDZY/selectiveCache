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
    parser.add_argument("shard_df_dir")
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
    print n_shards
    n_machines = m

    # read vocab
    vocab = {}
    for line in args.intQterm_file:
        term, qtf, tid, df = line.split(' ')
        tid = int(tid)
        vocab[term] = tid

    # read shard vocab
    shard_vocab = [set() for i in range(n_shards)]
    for i in range(n_shards):
        with open(args.shard_df_dir + '/{0}'.format(i + 1)) as f:
            for line in f:
                tid, freq = line.split()
                tid = int(tid)
                shard_vocab[i].add(tid)


    #missed_terms = {}

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
        if args.shardlim == 130:
            items = [q] + range(1, 131)
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
    missing_shard_single = 0
    missing_shard_multi = 0
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
        shard_has_all = [0 for s in range(n_shards)]
        for term in qterms:
            term = term.strip().lower()
            if not term.isalpha() or term in stoplist:
                continue
            has_term = True
            tid = vocab.get(term, -1)
            for s in shardlist[qid]:
                n_search += 1
                if tid in shard_vocab[s - 1] and tid not in cached[s - 1]:
                    #print term, s,
                    all_cached = False
                    shard_has_all[s - 1] = 1 
                    #missed_terms[term] = missed_terms.get(term, 0) + 1
                else:
                    n_hit += 1
        if all_cached and has_term:
            n_all_cached += 1
        if has_term and not all_cached:
            if len(qterms) == 1:
                miss_single += 1
                missing_shard_single += sum(shard_has_all)
            else:
                miss_multi += 1
                missing_shard_multi += sum(shard_has_all)
            #pass
            #print line
        if has_term:
            n_queries += 1
        qid += 1

    print n_queries, n_all_cached, n_search, n_hit, float(n_hit)/n_search, miss_single, miss_multi, missing_shard_single, missing_shard_multi
    #tmp = sorted([(n, term) for term, n in missed_terms.items()], reverse=True)
    #for term, n in tmp:
    #    print term, n


