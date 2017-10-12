import argparse
import random
import re
from itertools import islice

random.seed(0)
parser = argparse.ArgumentParser(description='partition based on slad result')
parser.add_argument('-f', '--fasta_fp', help='fasta file', required=True)
parser.add_argument('-u', '--hit_fp', help='usearch hit file', required=True)
parser.add_argument('-o', '--output_dir', help='output directory', required=True)
parser.add_argument('-c', '--count_fp', help='number of sub-clusters', required=True)
args = parser.parse_args()

header_2_read = {}
with open(args.fasta_fp) as f:
    while True:
        next_n = list(islice(f, 2))
        if not next_n:
            break
        header = next_n[0].strip()[1:]
        read = next_n[1].strip()
        header_2_read[header] = read

header_2_assignment = {}
assignment_set = set()
with open(args.hit_fp) as f:
    for line in f:
        header, cluster_assignment = line.strip().split('\t')[:2]
        assignment_set.add(cluster_assignment)
        header_2_assignment[header] = cluster_assignment

assignment_2_headers = {}
for header in header_2_read.keys():
    assignment = header_2_assignment[header] if header in header_2_assignment else random.choice(list(assignment_set))
    assignment_2_headers.setdefault(assignment, []).append(header)

with open(args.count_fp, 'w') as fo:
    fo.write('{}\n'.format(len(assignment_2_headers)))

for assignment, headers in assignment_2_headers.items():
    with open(args.output_dir + '/' + assignment, 'w') as fo:
        for header in headers:
            fo.write('>{}\n{}\n'.format(
                header, header_2_read[header]))

