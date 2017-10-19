# SLAD

SLAD (Separation via Landmark-based Active Divisive clustering) is a generic computational framework that can be used to parallelize various _de novo_ operational taxonomic unit (OTU) picking methods.

## Requirements

* Scala 2.11
* Java >= 1.6
* Sbt >= 0.13
* Apache Spark >= 2.0

## Demo

A demo of SLAD coupled with [vsearch](https://github.com/torognes/vsearch) on a single multi-core Mac machine.
`vsearch` is a versatile open-source tool for metagenomics.
SLAD also uses [scallop](https://github.com/scallop/scallop) for command-line arguments parsing.

#### Setup

Create a directory for demo and all the following commands are assumed to be run under this directory.
```bash
mkdir SLAD_DEMO && cd SLAD_DEMO
```

Install `Java`, `Scala`, `Sbt` using [`Homebrew`](https://brew.sh/).

```bash
brew cask install java
brew install scala@2.11
brew install sbt
```

Download [Apache Spark](https://spark.apache.org/downloads.html) and decompress to the demo directory.
```bash
tar -xzvf spark-2.X.X-bin-hadoop2.X.tgz && mv spark-2.X.X-bin-hadoop2.X Spark
```

Download [vsearch](https://github.com/torognes/vsearch) and compile it.
GNU autotools (version 2.63 or later) and the gcc compiler is required to build vsearch.
After compilation, the `vsearch` executable file will be placed under `vsearch/bin`.
```bash
git clone https://github.com/torognes/vsearch.git
cd vsearch
./autogen.sh
./configure
make
```

Download SLAD source code under the demo directory.
```bash
git clone https://github.com/vitmy0000/SLAD.git
```

Download the [demo sequence file](https://buffalo.box.com/s/jm18zyifyeqbb3773w2tsp6bcze3dugt) and create a directory named `Demo` for storing it.

To this end, the demo directory should have the following layout.
```bash
tree -L 2 -C SLAD_DEMO/
```

![tree](misc/tree.png)

#### Launch cluster

Start the master node `./Spark/sbin/start-master.sh`, open `http://localhost:8080/` in web browser and get the `MASTER_URL` as highlighted in the following screenshot.
![master](misc/master.png)
Start a slave node `./Spark/sbin/start-slave.sh <MASTER_URL>`.
![slave](misc/slave.png)
To stop the master and slave nodes.
```bash
./Spark/sbin/stop-slave.sh
./Spark/sbin/stop-master.sh
```

#### Compile
```bash
cd SLAD
sbt package
```

#### Run

Check the help information
```bash
../Spark/bin/spark-submit \
--master spark://coe-biomac3002.med.buffalo.edu:7077 \
--conf spark.default.parallelism=16 \
--conf spark.executor.memory=8G \
--class "com.weiz.slad.Program" \
--jars \
../SLAD/lib/scallop_2.11-2.1.2.jar \
../SLAD/target/scala-2.11/slad_2.11-0.1.0.jar \
--help
```

```
        ____  _        _    ____
       / ___|| |      / \  |  _ \
       \___ \| |     / _ \ | | | |
        ___) | |___ / ___ \| |_| |
       |____/|_____/_/   \_\____/

Version 0.1.0 (C) 2016 Wei Zheng
Usage: slad -i INPUT_FILE_PAHT -o OUTPUT_DIR [OPTION]...

SLAD is an Spark based parallel framework for denovo OTU picking.
Options:

      --abundance  <arg>             Sequences with abundance greater than or
                                     equal to this threshold will be treated as
                                     non-trivial sequences.
                                     Required to be posotive integer.
                                     Default: 2
  -i, --input-file-path  <arg>       (REQUIRED)Input fasta file.
      --min-size  <arg>              Clusters with size smaller than this
                                     threshold will not be further partitioned.
                                     Required to be positive integer >= 100
                                     Default: 100
      --num-leave-cluster  <arg>     Number of desired leave clusters
                                     If set to 0, this option has no effect.
                                     Required to be posotive integer or 0.
                                     Default: 0
      --num-power-iteration  <arg>   Number of PIC iterations.
                                     Required to be posotive integer.
                                     Default: 10
  -o, --output-dir  <arg>            (REQUIRED)Output directory.
                                     Directory will be created automatically.
                                     Make sure the directory does not exisit.
                                     Default: "./slad_results"
      --radius  <arg>                Clusters with radius smaller than this
                                     threshold will not be further partitioned.
                                     Required to be a double between 0 and 1.
                                     Default: 0.15
      --random-seed  <arg>           Random seed.
                                     This option will effect landmark selection.
                                     Default: 0
      --word-size  <arg>             Kmer word size.
                                     Required to be positive integer and < 16.
                                     Default: 8
      --help                         Show help message
      --version                      Show version of this program

For all other tricks, consult the documentation!
```

Below is a sample script for demo and it is available under `SLAD/scripts`.
`SET VARIABLES` section should be modified accordingly.
```bash
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
mkdir -p ${OUTPUT_DIR}/non_blank
mkdir -p ${OUTPUT_DIR}/hit
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
    ${VSEARCH_DIR}/bin/vsearch -sortbylength ${OUTPUT_DIR}/clusters/${x} -output ${OUTPUT_DIR}/clusters/${x}_sorted.fa; \
    ${VSEARCH_DIR}/bin/vsearch -cluster_smallmem ${OUTPUT_DIR}/clusters/${x}_sorted.fa -id ${OTU_LEVEL} -centroids ${OUTPUT_DIR}/clusters/${x}_centroids.fa -userout ${OUTPUT_DIR}/clusters/${x}_user.txt -userfields query+target+id; \
    python ${SLAD_DIR}/scripts/make_otu_table.py -u ${OUTPUT_DIR}/clusters/${x}_user.txt -o ${OUTPUT_DIR}/clusters/${x}_table.txt; \
    } &
done
for job in `jobs -p`; do
    wait $job
done

# Build OTU table from clustering results
mkdir -p ${OUTPUT_DIR}/table
python ${SLAD_DIR}/scripts/collect.py -i ${OUTPUT_DIR}/clusters/cluster_*_table.txt -c ${OUTPUT_DIR}/clusters/cluster_*_centroids.fa -o ${OUTPUT_DIR}/table

echo 'All done!'
```

To run the demo script
```bash
cd SLAD_DEMO/Demo
cp ../SLAD/scripts/demo.sh .
bash demo.sh
```

To clean up
```bash
rm -r Spark/work/*
```

### EC2

This section gives a quick demo of running SLAD coupled with [vsearch](https://github.com/torognes/vsearch) in a distrubuted evironment. Please note that we run the top-level partition phase on [AWS EMR](https://aws.amazon.com/emr/) and the sub-clustering phase can run on a cluster with simple job scheduling system, e.g. [slurm](https://slurm.schedmd.com/).

#### Launch AWS EC2 cluster

1. Login to AWS EMR service, create a new cluster.
![emr_create](misc/emr_create.png)

2. Configure the cluster.

Specify the `Cluster name` as you like, choose `Spark` for `Applications` and select your `EC2 key pair`(Please follow the help link to creat a new one, if you are new to AWS EC2). For `Instance type` and `Number of instances`, please check the AWS EC2 instance [types](https://aws.amazon.com/ec2/instance-types/) and [prices](https://aws.amazon.com/ec2/pricing/on-demand/) to make sure the memory and CPU cores fit the data and use the money wisely! 😄
![emr_config](misc/emr_config.png)

3. Connect to the cluster.

First wait a minite for all the instances to boot up and then connect using SSH. The `.pem` file is the `EC2 key pair`. We also recommend to use `Tmux` to prevent connection loss.
![emr_wait](misc/emr_wait.png)
![emr_ssh](misc/emr_ssh.png)

#### Transferring data

`AWS S3` can be used for storing input and output data. You can optionally use `GUI` by dragging and dropping the file into browser or the command line. After the job is finished, output files are also saved into `S3`. Please download the results and clean up `S3` to avoid unnecessary storage charges.

create a bucket
![s3](misc/s3.png)

Setup a python virtual environment for data transferring using `AWS Command Line Interface`.
`aws configure` will ask for `AWS Access Key ID` and `AWS Secret Access Key`. Please generate it using [AWS IAM](https://aws.amazon.com/iam/) and save it. (Make sure your `AWS Access Key ID` and `AWS Secret Access Key` do not contain `/`)
```bash
conda create -m -p s3-env python=2.7
source activate s3-env
pip install awscli
aws configure
```

Upload input data
```bash
aws s3 cp IBD_sample_filtered.fna s3://weiz-ibd/
```

Download output data
```bash
aws s3 sync s3://weiz-ibd/slad_out slad_out
```

#### Top-level partition

After logging into EC2 cluster, run the SLAD job.
```bash
# install sbt and git
sudo yum -y install sbt
sudo yum -y install git

# download source code and compile
git clone https://github.com/vitmy0000/SLAD.git
cd SLAD
bash get_all_branches.sh
git checkout ec2
sbt package
cd ..

# run the job script
cp SLAD/scripts/ec2.sh .
bash ec2.sh
```

Below is an example of _ec2.sh_ and it is available under `SLAD/scripts`. Please change the parameters accordingly and provide your own `AWS Access Key ID` and `AWS Secret Access Key` information.
```bash
{ time \
spark-submit \
--master yarn \
--deploy-mode client \
--class "com.weiz.slad.Program" \
--executor-cores 4 \
--conf "spark.default.parallelism=64" \
--conf "spark.dynamicAllocation.enabled=false" \
--jars \
SLAD/lib/aws-java-sdk-core-1.10.62.jar,\
SLAD/lib/aws-java-sdk-s3-1.10.62.jar,\
SLAD/lib/scallop_2.11-2.1.2.jar \
SLAD/target/scala-2.11/slad_2.11-0.1.0.jar \
--input-file-path "weiz-ibd/IBD_sample_filtered.fna" \
--output-dir "weiz-ibd/slad_out" \
--aws-access-id "XXXXXXXXXXXXXXXXXXXX" \
--aws-access-key "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX" \
--word-size 8 \
--abundance 2 \
--radius 0.20 \
--min-size 1000 \
--num-leave-cluster 64 \
--num-power-iteration 10 \
--random-seed 0 \
2> debug.txt ; \
} 2> time.txt
```

#### Parallel sub-clustering

After the job is finished on EC2 cluster, download the results.
```bash
aws s3 sync s3://weiz-ibd/slad_out slad_out
```

Download SLAD code which includes scripts for `vsearch` sub-clustering.
```bash
git clone https://github.com/vitmy0000/SLAD.git
cd SLAD
bash get_all_branches.sh
git checkout ec2
cd ..
```

Download and compile `vsearch`.
```bash
git clone https://github.com/torognes/vsearch.git
cd vsearch
./autogen.sh
./configure
make
cd ..
```

Assume there are following items in current directory:
- slad_out
- SLAD
- vsearch

Run post-processing
```bash
mkdir post
cd post

# STEP 1
python ../SLAD/scripts/ec2_post_1.py -i ../slad_out -v ../vsearch -s ../SLAD
for x in $(ls post_1_*.sh); do sbatch ${x}; done

# Wait for all step 1 jobs to finish
mkdir clusters
for x in $(sed -n 's/^>//p' ../slad_out/landmarks/part-00000 | sort | uniq); do cat clusters_part-*/${x} > clusters/${x}; done

# STEP 2
wget http://drive5.com/uchime/gold.fa
python ../SLAD/scripts/ec2_post_2.py -v ../vsearch -s ../SLAD -g gold.fa -l 0.97
for x in $(ls post_2_*.sh); do sbatch ${x}; done

# Wait for all step 2 jobs to finish
mkdir table
python ../SLAD/scripts/collect.py -i clusters/cluster_*_table.txt -c clusters/cluster_*_centroids.fa -o table
```

The raw OTU table and representative sequences are placed under `post/table` directory.
The chimera check step is included in the post-processing and we recommend to [remove spurious OTUs](http://qiime.org/scripts/filter_otus_from_otu_table.html) before diving into down-stream data analysis.

## Tips

* The top-level partition results can be used for sub-clustering phase with different OTU-level parameters applied.
* Input sequence header should not contain blank space.
