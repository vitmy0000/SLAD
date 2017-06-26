package com.weiz.slad

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import scala.math
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import scala.util.control.Breaks._

class SLAD (
    private val sc: SparkContext,
    private val random: Random,
    private val numPowerIteration: Int,
    private val numLeaveCluster: Int,
    private val radius: Double,
    private val minSize: Int) {

  def run(seqs: RDD[SeqAsKmerCnt])
      :(RDD[Int], Map[Int, List[String]]) = {
    // init
    println("BisectingSLAD...")
    var currentNumLeaveCluster = 1
    // cluster index starts from 1 for easy tree recovery
    var clusterIndcies: RDD[Int] = seqs.map(x => 1).cache()
    val totalSeqNum = clusterIndcies.count()
    var currentSize = totalSeqNum
    val divisibleClusterSizeInfo: mutable.Map[Int, Long] =
        mutable.Map(1 -> totalSeqNum)
    val leaveClusterLandmarkInfo: mutable.Map[Int, List[String]] = mutable.Map()

    breakable { while (divisibleClusterSizeInfo.size != 0 &&
        (numLeaveCluster == 0 || currentNumLeaveCluster < numLeaveCluster) ) {
      // choose largest cluster for partitioning
      val (partitioningClusterIndex, partitioningSize) 
          = divisibleClusterSizeInfo.maxBy(_._2)
      if (partitioningSize <= minSize) {
        break
      }
      divisibleClusterSizeInfo -= partitioningClusterIndex
      println(s"Partitioning Cluster: $partitioningClusterIndex($partitioningSize)")
      val landmarkQuota: Int =
          math.ceil(math.log(partitioningSize) / math.log(2)).toInt
      val mask: RDD[Boolean] =
          clusterIndcies.map { _ == partitioningClusterIndex }.cache()
      val (landmarks, distRecords): (List[SeqAsKmerCnt], List[RDD[Double]]) =
          selectLandmarks(seqs, clusterIndcies, mask, landmarkQuota)

      println("\tPower iteration clustering...")
      val picResult = PIC.powerIterationClustering(
        sc, landmarks, radius, 2, numPowerIteration)

      if (picResult.isEmpty) {
        leaveClusterLandmarkInfo +=
            (partitioningClusterIndex -> landmarks.map(_.getRead))
        println("\tRadius condition satisfied!")
      } else {
        val landmarkAssignmentSummary: Map[Int, List[Int]] =
            picResult.get.groupBy(_._2).map {
          case (k, v) => (partitioningClusterIndex * 2 + k, v.keys.toList)
        }
        println("\tPIC summary: " + landmarkAssignmentSummary)
        val (newClusterIndices, leaveClusterSizeInfo, leftLandmarks, rightLandmarks) =
            assign(clusterIndcies, mask, picResult.get, landmarks, distRecords)
        clusterIndcies.unpersist()
        clusterIndcies = newClusterIndices
        val leftChildIndex = partitioningClusterIndex * 2 + 0
        val rightChildIndex = partitioningClusterIndex * 2 + 1
        divisibleClusterSizeInfo +=
            (leftChildIndex -> leaveClusterSizeInfo(leftChildIndex))
        divisibleClusterSizeInfo +=
            (rightChildIndex -> leaveClusterSizeInfo(rightChildIndex))
        leaveClusterLandmarkInfo -= partitioningClusterIndex
        leaveClusterLandmarkInfo += (leftChildIndex -> leftLandmarks)
        leaveClusterLandmarkInfo += (rightChildIndex -> rightLandmarks)
        currentNumLeaveCluster += 1
        print(s"\tSplit to: $leftChildIndex(${leaveClusterSizeInfo(leftChildIndex)})")
        println(s" $rightChildIndex(${leaveClusterSizeInfo(rightChildIndex)})")
        println(leaveClusterSizeInfo)
      } 

      // clean
      mask.unpersist()
      distRecords.map(_.unpersist())
    } } // while breakable

    // return
    (clusterIndcies, leaveClusterLandmarkInfo.toMap)
  }

  def assign(
      clusterIndcies: RDD[Int], mask: RDD[Boolean],
      landmarkAssignment: Map[Int, Int], landmarks: List[SeqAsKmerCnt],
      distRecords: List[RDD[Double]])
      : (RDD[Int], Map[Int, Long], List[String], List[String]) = {
    assert(landmarks.size == distRecords.size)
    println("\tAverage assigning...")
    var leftSumDist: RDD[Double] = clusterIndcies.map(x => 0.0)
    var rightSumDist: RDD[Double] = clusterIndcies.map(x => 0.0)
    val leftLandmarks: ListBuffer[String] = ListBuffer()
    val rightLandmarks: ListBuffer[String] = ListBuffer()

    for (iter <- 0 until distRecords.size) {
      if (landmarkAssignment(iter) == 0) {//left
        leftLandmarks += landmarks(iter).getRead
        leftSumDist = leftSumDist.zip(distRecords(iter)).map {
          case (sum, delta) =>
          sum + delta
        }
      } else {//right
        rightLandmarks += landmarks(iter).getRead
        rightSumDist = rightSumDist.zip(distRecords(iter)).map {
          case (sum, delta) =>
          sum + delta
        }
      }
    } // for

    val leftCnt = leftLandmarks.size
    val rightCnt = rightLandmarks.size
    val newClusterIndices =
        mask.zip(clusterIndcies.zip(leftSumDist.zip(rightSumDist))).map {
      case (flag, (parentClusterIndex, (leftSum, rightSum))) =>
      if (flag) {
        if (leftSum / leftCnt <= rightSum / rightCnt) {
          parentClusterIndex * 2 + 0
        } else {
          parentClusterIndex * 2 + 1
        }
      } else {
        parentClusterIndex
      }
    }.cache().localCheckpoint()
    val leaveClusterSizeInfo = newClusterIndices.map {
      case clusterIndex => (clusterIndex, 1L)
    }.reduceByKey(_ + _).collect.toMap

    (newClusterIndices, leaveClusterSizeInfo,
      leftLandmarks.toList, rightLandmarks.toList)
  }

  def selectLandmarks(
      seqs: RDD[SeqAsKmerCnt], clusterIndcies: RDD[Int],
      mask: RDD[Boolean], quota: Int)
      : (List[SeqAsKmerCnt], List[RDD[Double]]) = {
    println("\tSelecting landmarks...")
    val landmarks: ListBuffer[SeqAsKmerCnt] = ListBuffer()
    val distRecords: ListBuffer[RDD[Double]] = ListBuffer()

    // select init landmark
    var lastLandmark: SeqAsKmerCnt = seqs.zip(mask).filter {
      case (seq, flag) => flag
    }.takeSample(withReplacement=false, num=1, seed=random.nextLong)(0)._1
    landmarks += lastLandmark
    println(s"\t1/$quota")

    var distsToLandmarkSet: RDD[Double] = clusterIndcies.map(x => 1.0)
    for (iter <- 2 to quota) {
      println(s"\t$iter/$quota")
      val distsToLandmark: RDD[Double] = seqs.zip(mask).map {
        case (seq, flag) =>
        if (flag) {
          SeqUtil.kmerDist(seq, lastLandmark)
          //SeqUtil.nwDist(seq.getRead, lastLandmark.getRead)
        } else {
          0.0
        }
      }.cache()
      distsToLandmark.count()
      distRecords += distsToLandmark
      distsToLandmarkSet = distsToLandmarkSet.zip(distsToLandmark).map {
        case (minDist, newDist) => math.min(minDist, newDist)
      }

      // skip if this is the final landmark
      if (iter < quota) {
        val medians = mask.zip(seqs.zip(distsToLandmarkSet)).mapPartitions {
          iterator: Iterator[(Boolean, (SeqAsKmerCnt, Double))] => {
            val subList = iterator.toList.filter {
              case (flag, (seq, dist)) => flag
            }
            if (subList.size == 0) {
              List().iterator
            } else {
              List(subList.sortWith((a, b) => a._2._2 < b._2._2)
                  .apply(subList.size / 2)).iterator
            }
          }
        }.collect()
        //medians foreach println
        val momDist = medians.sortWith((a, b) => a._2._2 < b._2._2)
            .apply(medians.size / 2)._2._2
        lastLandmark = mask.zip(seqs.zip(distsToLandmarkSet)).filter {
          case (flag, (seq, dist)) => flag && dist >= momDist
        }.takeSample(withReplacement=false, num=1, seed=random.nextLong)(0)._2._1
        landmarks += lastLandmark
      } // if
    } // for

    //landmarks.map(_.getRead) foreach println
    return (landmarks.toList, distRecords.toList)
  }
}
