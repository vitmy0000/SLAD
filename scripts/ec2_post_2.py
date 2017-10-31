import argparse
import os
from string import Template
from glob import glob

parser = argparse.ArgumentParser(description='Step2: parallel vparse')
parser.add_argument('-v', '--vsearch_dir', help='vsearch dir', required=True)
parser.add_argument('-s', '--slad_dir', help='slad dir', required=True)
parser.add_argument('-g', '--gold_fa', help='gold.fa file path', required=True)
parser.add_argument('-l', '--otu_level', help='otu_level', required=True)
args = parser.parse_args()

template = Template('''\
#!/bin/sh
#SBATCH --partition=general-compute --qos=general-compute
#SBATCH --time=06:00:00
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --mem=3000
#SBATCH --constraint=CPU-E5645
#SBATCH --job-name=post_2_${x}
#SBATCH --output=post_2_${x}.out

set -o errexit
ulimit -s unlimited
module load python/anaconda2-4.2.0

python ${slad_dir}/scripts/sizeout_ec2.py -f "clusters/${x}" -o "clusters/${x}_sizeout.fa"
${vsearch_dir}/bin/vsearch -sortbysize "clusters/${x}_sizeout.fa" -output "clusters/${x}_sorted.fa" -minsize 2
${vsearch_dir}/bin/vsearch -cluster_smallmem "clusters/${x}_sorted.fa" -usersort -id "${otu_level}" -centroids "clusters/${x}_centroids.fa" -userout "clusters/${x}_user.txt" -userfields "query+target+id"
${vsearch_dir}/bin/vsearch -uchime_ref "clusters/${x}_centroids.fa" -db ${gold_fa} -strand plus -nonchimeras "clusters/${x}_otus.fa"
${vsearch_dir}/bin/vsearch -usearch_global "clusters/${x}_sizeout.fa" -db "clusters/${x}_otus.fa" -strand plus -id ${otu_level} -blast6out "clusters/${x}_hit.txt"
python ${slad_dir}/scripts/make_otu_table_ec2.py -u "clusters/${x}_hit.txt" -o "clusters/${x}_table.txt"

echo 'All done!'
''')

def build_job(x, vsearch_dir, slad_dir, gold_fa, otu_level):
    script = template.substitute(
        x=x, vsearch_dir=vsearch_dir, slad_dir=slad_dir,
        gold_fa=gold_fa, otu_level=otu_level)
    with open('./post_2_{}.sh'.format(x), 'w') as fo:
        fo.write(script)

if __name__ == "__main__":
    for x in glob("./clusters/*"):
        build_job(os.path.basename(x), args.vsearch_dir, args.slad_dir,
                  args.gold_fa, args.otu_level)

