import argparse
import random
from itertools import islice

random.seed(0)
parser = argparse.ArgumentParser(description='Sample sub dataset')
parser.add_argument('-f', '--fasta_fp', help='fasta file', required=True)
parser.add_argument('-u', '--hit_fp', help='usearch hit file', required=True)
parser.add_argument('-o', '--output_dir', help='output directory', required=True)
parser.add_argument('-c', '--count_fp', help='number of sub-clusters', required=True)
args = parser.parse_args()

all_headers = []
header_2_read = {}
with open(args.fasta_fp) as f:
    while True:
        next_n = list(islice(f, 2))
        if not next_n:
            break
        header = next_n[0].strip()[1:]
        read = next_n[1].strip()
        header_2_read[header] = read
        all_headers.append(header)

header_2_assignment = {}
assignment_set = set()
with open(args.hit_fp) as f:
    for line in f:
        header, cluster = line.strip().split('\t')[:2]
        assignment_set.add(cluster)
        header_2_assignment[header] = cluster

assignment_2_headers = {}
for header in all_headers:
    assignment = header_2_assignment[header] if header in header_2_assignment else random.choice(list(assignment_set))
    if assignment in assignment_2_headers:
        assignment_2_headers[assignment].append(header)
    else:
        assignment_2_headers[assignment] = [header]

with open(args.count_fp, 'w') as fo:
    fo.write('{}\n'.format(len(assignment_2_headers)))

for k, v in assignment_2_headers.items():
    with open(args.output_dir + '/' + k, 'w') as fo:
        for header in v:
            fo.write('>{}\n{}\n'.format(
                header, header_2_read[header]))
