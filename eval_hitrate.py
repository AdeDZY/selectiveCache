#!/opt/python27/bin/python
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("cached_queries_file", type=argparse.FileType('r'))
    parser.add_argument("intQterm_file", type=argparse.FileType('r'))
    parser.add_argument("test_queries_file", type=argparse.FileType('r'))
    parser.add_argument("stoplist_file", type=argparse.FileType('r'))
    args = parser.parse_args()

    # read stopwords
    stoplist = set()
    for line in args.stoplist_file:
        stoplist.add(line.strip())

    # read vocab
    vocab = {}
    for line in args.intQterm_file:
        term, tid, df = line.split(' ')
        tid = int(tid)
        vocab[term] = tid

    # read cached
    cached = set()
    for line in args.cached_queries_file:
        term, tid, a, b, c = line.split(' ')
        tid = int(tid)
        cached.add(tid)

    # queries
    n_all_cached = 0
    n_cached_terms = 0
    n_terms = 0
    n_queries = 0
    for line in args.test_queries_file:
        qterms = line.split(' ')
        all_cached = True
        has_term = False
        for term in qterms:
            term = term.strip().lower()
            if not term.isalpha() or term in stoplist:
                continue
            has_term = True
            n_terms += 1
            tid = vocab.get(term, -1)
            if tid not in cached:
                all_cached = False
            else:
                n_cached_terms += 1
        if all_cached and has_term:
            n_all_cached += 1
        if has_term:
            n_queries += 1

    print n_queries, n_all_cached, n_terms, n_cached_terms, float(n_cached_terms)/n_terms


