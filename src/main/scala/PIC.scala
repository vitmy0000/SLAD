package com.weiz.slad

import org.apache.spark.SparkContext
import org.apache.spark.mllib.clustering.PowerIterationClustering

object PIC {
  def powerIterationClustering(
      sc: SparkContext,
      landmarks: List[SeqAsKmerCnt],
      radius: Double,
      numCluster: Int,
      numIteration: Int)
    : Option[Map[Int, Int]] = {
    // Map(landmarkIndex -> childLeftRightBinary 0/1)
    val indexedLandmarks = sc.parallelize(landmarks).zipWithIndex
    val pairwiseSimilarity = indexedLandmarks.cartesian(indexedLandmarks)
        .coalesce(sc.defaultParallelism)
        .flatMap { case ((seq0, i0), (seq1, i1)) =>
      if (i0 < i1) {
        Some((i0.toLong, i1.toLong, 1 - SeqUtil.nwDist(seq0.getRead, seq1.getRead)))
        //Some((i0.toLong, i1.toLong, 1 - SeqUtil.kmerDist(seq0, seq1)))
      } else {
        None
      }
    }.cache()
    
    val sumDist = new Array[Double](landmarks.size)
    pairwiseSimilarity.collect().map {
      case (i, j, sim) => {
        sumDist(i.toInt) += 1 - sim
        sumDist(j.toInt) += 1 - sim
      }
    }
    val estimatedRadius = sumDist.min / (landmarks.size - 1)
    println(f"\tEstimated radius: $estimatedRadius%.4f")
    if (estimatedRadius <= radius) {
      None
    } else {
      val model = new PowerIterationClustering()
        .setK(numCluster)
        .setMaxIterations(numIteration)
        .run(pairwiseSimilarity)
      Some(model.assignments.map(x => (x.id.toInt, x.cluster)).collect.toMap)
    }
  }
}
