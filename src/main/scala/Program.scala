package com.weiz.slad

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
// import org.apache.log4j.Logger
// import org.apache.log4j.Level

import org.rogach.scallop._
import scala.collection.mutable
import scala.util.Random
import java.io._

class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
  version("Version 0.1.0 (C) 2016 Wei Zheng")
  banner("""Usage: slad -i INPUT_FILE_PAHT -o OUTPUT_DIR [OPTION]...
           |
           |SLAD is an Spark based parallel framework for denovo OTU picking.
           |Options:
           """.stripMargin)
  footer("\nFor all other tricks, consult the documentation!")

  val inputFilePath = opt[String](required = true,
    descr = """(REQUIRED)Input fasta file.""")
  val outputDir = opt[String](required = true,
    descr = """(REQUIRED)Output directory.                |
              |Directory will be created automatically.   |
              |Make sure the directory does not exisit.   |
              |Default: "./slad_results" """
              .stripMargin.replace("|\n", " "))
  val wordSize = opt[Int](default = Some(8),
    noshort = true, validate = (x => x > 0 && x < 16),
    descr = """Kmer word size.                            |
              |Required to be positive integer and < 16.  |
              |Default: 8 """
              .stripMargin.replace("|\n", " "))
  val abundance = opt[Long](default = Some(2L),
    noshort = true, validate = (0<),
    descr = """Sequences with abundance greater than or   |
              |equal to this threshold will be treated as |
              |non-trivial sequences.                     |
              |Required to be posotive integer.           |
              |Default: 2 """
              .stripMargin.replace("|\n", " "))
  val radius = opt[Double](default = Some(0.15),
    noshort = true, validate = (x => x > 0 && x < 1),
    descr = """Clusters with radius smaller than this   |
              |threshold will not be further partitioned. |
              |Required to be a double between 0 and 1.   |
              |Default: 0.15 """
              .stripMargin.replace("|\n", " "))
  val minSize = opt[Int](default = Some(100),
    noshort = true, validate = (0<=),
    descr = """Clusters with size smaller than this       |
              |threshold will not be further partitioned. |
              |Required to be positive integer >= 100    |
              |Default: 100 """
              .stripMargin.replace("|\n", " "))
  val numLeaveCluster = opt[Int](default = Some(0),
    noshort = true, validate = (0<=),
    descr = """Number of desired leave clusters           |
              |If set to 0, this option has no effect.    |
              |Required to be posotive integer or 0.      |
              |Default: 0 """
              .stripMargin.replace("|\n", " "))
  val numPowerIteration = opt[Int](default = Some(10),
    noshort = true, validate = (0<),
    descr = """Number of PIC iterations.                  |
              |Required to be posotive integer.           |
              |Default: 10 """
              .stripMargin.replace("|\n", " "))
  val randomSeed = opt[Long](default = Some(0L),
    noshort = true,
    descr = """Random seed.                               |
              |This option will effect landmark selection.|
              |Default: 0 """
              .stripMargin.replace("|\n", " "))
  verify()
}

object Program {
  def main(args: Array[String]) {
    println("""
            |        ____  _        _    ____
            |       / ___|| |      / \  |  _ \
            |       \___ \| |     / _ \ | | | |
            |        ___) | |___ / ___ \| |_| |
            |       |____/|_____/_/   \_\____/
            |""".stripMargin)

    // init
    val sladConf = new Conf(args)
    val sladInputFilePath = sladConf.inputFilePath()
    val sladOutputDir = sladConf.outputDir()
    val sladWordSize = sladConf.wordSize()
    val sladAbundance = sladConf.abundance()
    val sladRadius = sladConf.radius()
    val sladMinSize = sladConf.minSize()
    val sladNumLeaveCluster = sladConf.numLeaveCluster()
    val sladNumPowerIteration = sladConf.numPowerIteration()
    val sladRandomSeed = sladConf.randomSeed()
    val sparkConf = new SparkConf()
    sparkConf.setAppName("SLAD Application")
    val sc = new SparkContext(sparkConf)
    // Logger.getRootLogger.setLevel(Level.DEBUG)

    // LOAD INPUT
    println("Loading input...")
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", ">")
    val inputStr = sc.textFile(sladInputFilePath, sc.defaultParallelism)
        .filter(_.size > 0) // need to remove first empty string
    println("Partition number: " + inputStr.getNumPartitions)
    println("Input sequence number: %d".format(inputStr.count()))

    // DEREPLICATION
    println("Dereplicating...")
    val inputStrDerep: RDD[(String, mutable.HashSet[String])] =
      inputStr.map(_.replaceFirst("\\n", "\n\u0000")
      .replace("\n", "").split("\u0000") match {
        case Array(header, read) => (read.toUpperCase, header)
      }).aggregateByKey(mutable.HashSet.empty[String])(
        //addToHeaderSet, mergeHeaderSets
        (s: mutable.HashSet[String], v: String) => s += v,
        (s1: mutable.HashSet[String], s2: mutable.HashSet[String]) => s1 ++= s2)
      .persist(StorageLevel.MEMORY_ONLY_SER)//.cache()
    println("Unique sequence number: %d".format(inputStrDerep.count()))
    inputStrDerep.map {
      case (read, headers) => s">$headers\n$read"
    }.saveAsTextFile(sladOutputDir + "/derep")

    // AUNDANCE FILTERING and KMER CONVERSION
    val seqs: RDD[SeqAsKmerCnt] = inputStrDerep.filter { case (read, headers) =>
      headers.size >= sladAbundance
    }.coalesce(sc.defaultParallelism).map { case (read, headers) =>
      new SeqAsKmerCnt(read, sladWordSize)
    }.persist(StorageLevel.MEMORY_ONLY_SER)//.cache()
    inputStrDerep.unpersist()
    println("Abundant sequence number: %d".format(seqs.count()))

    val random: Random = new Random(sladRandomSeed)
    val slad = new SLAD(
        sc, random, sladNumPowerIteration,
        sladNumLeaveCluster, sladRadius, sladMinSize)
    val (clusterIndcies, leaveClusterLandmarkInfo)
        : (RDD[Int], Map[Int, List[String]]) = slad.run(seqs)

    clusterIndcies.zip(seqs).map { case (clusterIndex, seq) =>
      s">cluster_$clusterIndex\n${seq.getRead}"
    }.saveAsTextFile(sladOutputDir + "/partition")



    val file = new File(sladOutputDir + "/landmarks.fa")
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(
      leaveClusterLandmarkInfo.map { case (clusterIndex, landmarks) =>
        landmarks.map { landmark => s">cluster_$clusterIndex\n$landmark\n" }
      }.flatten.mkString
    )
    bw.close()

    // end
    sc.stop()
    println("### Done! ###")
  }
}
