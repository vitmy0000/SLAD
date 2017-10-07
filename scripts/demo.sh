#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset
# set -o xtrace

# Set magic variables for current file & dir
__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
__file="${__dir}/$(basename "${BASH_SOURCE[0]}")"
__base="$(basename ${__file} .sh)"

# Arguments
if [ "$#" -ne 1 ]; then
    echo "Please provide <MASTER_URL>"
    exit
fi
MASTER_URL="${1}"

# Top-level partition phase
rm -rf res
mkdir -p res
time \
../Spark/bin/spark-submit \
--master "${MASTER_URL}" \
--conf spark.default.parallelism=16 \
--conf spark.executor.memory=8G \
--class "com.weiz.slad.Program" \
--jars \
../SLAD/lib/scallop_2.11-2.1.2.jar \
../SLAD/target/scala-2.11/slad_2.11-0.1.0.jar \
--input-file-path "./seqs.fa" \
--output-dir "./res/" \
--word-size 8 \
--abundance 2 \
--radius 0.17 \
--min-size 500 \
--num-leave-cluster 0 \
--num-power-iteration 10 \
--random-seed 0 \
2> /dev/null 
../SLAD/scripts/u9_mac -usearch_global "./seqs.fa" -db "./res/landmarks.fa" -id 0.6 -blast6out "./res/hit.txt" -strand plus -threads 4
mkdir res/clusters/
python ../SLAD/scripts/partition.py -f ./seqs.fa -u ./res/hit.txt -o ./res/clusters -c ./res/sub_count.txt
rm -r res/derep
rm -r res/partition

# Sub-clustering phase
for x in $(ls ./res/clusters/); do
    { \
    ../SLAD/scripts/u9_mac --sortbylength ./res/clusters/${x} --fastaout ./res/clusters/${x}_sorted.fa; \
    ../SLAD/scripts/u9_mac -cluster_smallmem ./res/clusters/${x}_sorted.fa -id 0.97 -centroids ./res/clusters/${x}_centroids.fa -userout ./res/clusters/${x}_user.txt -userfields query+target+id; \
    rm ./res/clusters/${x}_sorted.fa; \
    } &
done
for job in `jobs -p`; do
    wait $job
done

echo 'All done!'
