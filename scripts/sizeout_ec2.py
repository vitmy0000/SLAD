import argparse
import re

parser = argparse.ArgumentParser(description='convert into uparse sizeout format')
parser.add_argument('-f', '--fasta_fp', help='fasta file', required=True)
parser.add_argument('-o', '--output_fp', help='output fasta file in sizeout format', required=True)
args = parser.parse_args()

if __name__ == "__main__":
    with open(args.fasta_fp) as f, open(args.output_fp, 'w') as fo:
        for line in f:
            m = re.match('>Set[(](.+)[)]', line.strip())
            if m:
                seq_ids = m.group(1).split(',')
                fo.write('>{};size={};\n'.format(seq_ids[0], len(seq_ids)))
            else:
                fo.write(line)
