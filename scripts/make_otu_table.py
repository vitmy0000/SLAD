import argparse
import pandas as pd

parser = argparse.ArgumentParser(description='build OTU partial table')
parser.add_argument('-u', '--user_fp', help='usearch file', required=True)
parser.add_argument('-o', '--output_fp', help='output partial csv table', required=True)
args = parser.parse_args()

centroid_2_cnts = {}
with open(args.user_fp) as f:
    for line in f:
        seq_id, centroid = line.strip().split('\t')[:2]
        sample_id, seq_index = seq_id.split('_')
        centroid_2_cnts.setdefault(centroid, {})
        cnts = centroid_2_cnts[centroid]
        cnts[sample_id] = cnts.get(sample_id, 0) + 1

table_df_dict = {}
for centroid, cnts in centroid_2_cnts.items():
    table_df_dict[centroid] = pd.Series(cnts)
partial_otu_df = pd.DataFrame(table_df_dict).fillna(0)
# print(partial_otu_df)
partial_otu_df.to_csv(args.output_fp, sep='\t')