import argparse
import pandas as pd
import re

parser = argparse.ArgumentParser(description='build OTU partial table')
parser.add_argument('-u', '--user_fp', help='usearch file', required=True)
parser.add_argument('-o', '--output_fp', help='output partial csv table', required=True)
args = parser.parse_args()

centroid_2_cnts = {}
with open(args.user_fp) as f:
    for line in f:
        seq_id, centroid = line.strip().split('\t')[:2]
        m = re.match(r'(.+)_(\d+);size=(\d+);', seq_id)
        if not m:
        	raise Exception('not match: ' + seq_id)
        sample_id = m.group(1)
        seq_index = int(m.group(2))
        abundance = int(m.group(3))
        centroid_2_cnts.setdefault(centroid, {})
        cnts = centroid_2_cnts[centroid]
        cnts[sample_id] = cnts.get(sample_id, 0) + abundance

table_df_dict = {}
for centroid, cnts in centroid_2_cnts.items():
    table_df_dict[centroid] = pd.Series(cnts)
partial_otu_df = pd.DataFrame(table_df_dict).fillna(0)
# print(partial_otu_df)
partial_otu_df.to_csv(args.output_fp, sep='\t')