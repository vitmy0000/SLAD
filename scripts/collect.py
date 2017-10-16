import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='collect partial OTU tables')
parser.add_argument('-i', '--inputs', nargs='+', help='partial OTU table', required=True)
parser.add_argument('-c', '--centroids', nargs='+', help='centroid files', required=True)
parser.add_argument('-o', '--output_dir', help='output dir', required=True)
args = parser.parse_args()
assert(len(args.inputs) == len(args.centroids))

centroid_cnt = 0
centroid_2_otu = {}
with open(args.output_dir + '/centroids.fa', 'w') as fo:
    for c in args.centroids:
        with open(c) as f:
            read_buffer = ''
            otu_str = ''
            for line in f:
                if line.startswith('>'):
                    if len(otu_str) != 0:
                        fo.write('>{}\n{}\n'.format(otu_str, read_buffer))
                        read_buffer = ''
                    otu_str = 'OTU_' + str(centroid_cnt)
                    centroid_2_otu[line.strip()[1:]] = otu_str
                    centroid_cnt += 1
                else:
                    read_buffer += line.strip()
            fo.write('>{}\n{}\n'.format(otu_str, read_buffer))

df_list = [pd.read_csv(x, sep='\t', index_col=0) for x in args.inputs]
collected_table = pd.concat(df_list, axis=1).fillna(0)
# print(collected_table)
assert(len(collected_table.columns) <= centroid_cnt)
collected_table = collected_table.rename(columns=centroid_2_otu)
collected_table = collected_table.reindex_axis(
    sorted(collected_table.columns, key=lambda x: int(x[len('OTU_'):])), axis=1)
collected_table = collected_table.transpose()
collected_table.to_csv(args.output_dir + '/raw_OTU_table.txt', sep='\t')
