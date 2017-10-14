#!/usr/bin/env bash
set -o errexit
set -o pipefail
# set -o nounset
set -o xtrace

# SET VARIABLES
INPUT_FASTA="./seqs.fa"
OUTPUT_DIR="./res"
MASTER_URL="spark://coe-biomac3002.med.buffalo.edu:7077"
SPARK_DIR="../Spark"
SLAD_DIR="../SLAD"
VSEARCH_DIR="../vsearch"
MEMORY=8G
NUM_CORE=4
OTU_LEVEL=0.97
NUM_LEAVE_CLUSTER=8
## For termination condition, we recommend to set 
## NUM_LEAVE_CLUSTER to 2 X NUM_CORE
## and leave other two as default.
## Please check the paper for more detailed discussion.
RADIUS=0.15
MIN_SIZE=100
WORD_SIZE=8
ABUNDANCE=2
NUM_POWER_ITERATION=10
RANDOM_SEED=0

# Set magic variables for current file & dir
__dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
__file="${__dir}/$(basename "${BASH_SOURCE[0]}")"
__base="$(basename ${__file} .sh)"

# Top-level partition phase
rm -rf ${OUTPUT_DIR}
mkdir -p ${OUTPUT_DIR}
time \
${SPARK_DIR}/bin/spark-submit \
--master "${MASTER_URL}" \
--conf spark.default.parallelism=$((4*${NUM_CORE})) \
--conf spark.executor.memory=${MEMORY} \
--class "com.weiz.slad.Program" \
--jars \
${SLAD_DIR}/lib/scallop_2.11-2.1.2.jar \
${SLAD_DIR}/target/scala-2.11/slad_2.11-0.1.0.jar \
--input-file-path "${INPUT_FASTA}" \
--output-dir "${OUTPUT_DIR}" \
--word-size ${WORD_SIZE} \
--abundance ${ABUNDANCE} \
--radius ${RADIUS} \
--min-size ${MIN_SIZE} \
--num-leave-cluster ${NUM_LEAVE_CLUSTER} \
--num-power-iteration ${NUM_POWER_ITERATION} \
--random-seed ${RANDOM_SEED} \
2> debug.txt
mkdir ${OUTPUT_DIR}/non_blank
mkdir ${OUTPUT_DIR}/hit
for x in $(ls ${OUTPUT_DIR}/derep/part-*); do
    sed 's/ //g' "${x}" > "${OUTPUT_DIR}/non_blank/$(basename ${x})"
    ${VSEARCH_DIR}/bin/vsearch -usearch_global "${OUTPUT_DIR}/non_blank/$(basename ${x})" -db "${OUTPUT_DIR}/landmarks.fa" -id 0.6 -blast6out "${OUTPUT_DIR}/hit/$(basename ${x})" -strand plus -threads ${NUM_CORE}
done
cat ${OUTPUT_DIR}/hit/* > "${OUTPUT_DIR}/hit.txt"
rm -r ${OUTPUT_DIR}/derep
rm -r ${OUTPUT_DIR}/partition
rm -r ${OUTPUT_DIR}/hit
rm -r ${OUTPUT_DIR}/non_blank
mkdir ${OUTPUT_DIR}/clusters
python ${SLAD_DIR}/scripts/partition.py -f "${INPUT_FASTA}" -u "${OUTPUT_DIR}/hit.txt" -o "${OUTPUT_DIR}/clusters" -c "${OUTPUT_DIR}/sub_count.txt"

# Sub-clustering phase
for x in $(ls ${OUTPUT_DIR}/clusters); do
    { \
    ../vsearch/bin/vsearch -sortbylength ${OUTPUT_DIR}/clusters/${x} -output ${OUTPUT_DIR}/clusters/${x}_sorted.fa; \
    ../vsearch/bin/vsearch -cluster_smallmem ${OUTPUT_DIR}/clusters/${x}_sorted.fa -id ${OTU_LEVEL} -centroids ${OUTPUT_DIR}/clusters/${x}_centroids.fa -userout ${OUTPUT_DIR}/clusters/${x}_user.txt -userfields query+target+id; \
    rm ${OUTPUT_DIR}/clusters/${x}_sorted.fa; \
    } &
done
for job in `jobs -p`; do
    wait $job
done

echo 'All done!'
