#!/opt/python27/bin/python
import argparse
from os import listdir
from os.path import isfile, join

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("shard_feature_dir")
    parser.add_argument("shard_distribution_file", type=argparse.FileType('r'))
    parser.add_argument("output_dir")
    args = parser.parse_args()

    machine2shards = []
    shards_df = {}
    n_machine = 0
    for line in args.shard_distribution_file:
        n_machine += 1
        shards = [int(t) for t in line.split()]
        machine2shards.append(shards)
        for shard in shards:
            shard_df = {}
            p = join(args.shard_feature_dir, str(shard) + '.feat')
            with open(p) as f:
                for line in f:
                    tid, df, ctf, cent = line.split(' ')
                    tid = int(tid)
                    df = int(df)
                    ctf = int(ctf)
                    if tid == -1:
                        continue
                    shard_df[tid] = df
            shards_df[shard] = shard_df

    # get machine df
    m = 0
    for shards in machine2shards:
        m += 1
        fout = open(join(args.output_dir, str(m)), 'w')
        machine_df = {}
        for shard in shards:
            if shard not in shards_df:
                continue
            for tid, df in shards_df[shard].items():
                machine_df[tid] = machine_df.get(tid, 0) + df

        fout = open(join(args.output_dir, str(m)), 'w')
        for tid, df in machine_df.items():
            fout.write("{0} {1}\n".format(tid, df))

        fout.close()
