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
