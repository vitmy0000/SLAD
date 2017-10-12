import argparse
import os
from string import Template
from glob import glob

parser = argparse.ArgumentParser(description='Step1: split into clusters')
parser.add_argument('-i', '--input_dir', help='input dir(slad output dir)', required=True)
parser.add_argument('-v', '--vsearch_dir', help='vsearch dir', required=True)
parser.add_argument('-s', '--slad_dir', help='slad dir', required=True)
args = parser.parse_args()

template = Template('''\
#!/bin/sh
#SBATCH --partition=general-compute --qos=general-compute
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=3000
#SBATCH --constraint=CPU-E5645
#SBATCH --job-name=post_1_${x}
#SBATCH --output=post_1_${x}.out

set -o errexit
ulimit -s unlimited

mkdir -p non_blank
mkdir -p hit
mkdir -p clusters_${x}
sed 's/ //g' "${input_dir}/derep/${x}" > "./non_blank/${x}"
${vsearch_dir}/bin/vsearch -usearch_global "./non_blank/${x}" -db "${input_dir}/landmarks/part-00000" -id 0.6 -blast6out "./hit/${x}" -strand plus
python ${slad_dir}/scripts/partition_ec2.py -f "./non_blank/${x}" -u "./hit/${x}" -o "clusters_${x}" -c "clusters_${x}/sub_count.txt"

echo 'All done!'
''')

def build_job(input_dir, vsearch_dir, slad_dir, x):
    script = template.substitute(
        input_dir=input_dir, vsearch_dir=vsearch_dir, slad_dir=slad_dir, x=x)
    with open('./post_1_{}.sh'.format(x), 'w') as fo:
        fo.write(script)

if __name__ == "__main__":
    for x in glob('{}/derep/part-*'.format(args.input_dir)):
        build_job(args.input_dir, args.vsearch_dir, args.slad_dir, os.path.basename(x))

