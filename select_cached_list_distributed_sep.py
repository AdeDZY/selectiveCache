#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("shard_qtfdf_dir")
    parser.add_argument("memory_size", type=float, help="in GB, per machine")
    parser.add_argument("shard_distribution_file", type=argparse.FileType('r'))
    parser.add_argument("output_dir")
    args = parser.parse_args()

    upper_bound = args.memory_size * 1024 * 1024 * 1024 / 8

    i = 0
    for line in args.shard_distribution_file:
        i += 1
        shards = [int(t) for t in line.split()]
        fout = open(join(args.output_dir, str(i)), 'w')

        # sort all qtfdf
        tmp = []
        for shard in shards:
            for line in open(args.shard_qtfdf_dir + "/{0}.qtfdf".format(shard)):
                term, tid, qtfdf, qtf, df = line.strip().split()
                tid = int(tid)
                qtfdf = float(qtfdf)
                df = int(df)
                if df <= 0: continue
                tmp.append((qtfdf, term, tid, qtf, df, shard))
        tmp = sorted(tmp, reverse=True)

        total = 0
        shards_total = {}
        for qtfdf, term, tid, qtf, df, shard in tmp:
            if total + df < upper_bound:
                shards_total[shard] = shards_total.get(shard, 0) + df
                fout.write(term + ' ' + str(tid) + ' ' + str(qtfdf) + ' ' + str(qtf) + ' ' + str(df) + ' ' + str(shard))
                fout.write('\n')
                total += df
            else:
                continue

        print total,
        for shard, t in shards_total.items():
            print "{0}:{1}".format(shard, t),
        print ""
