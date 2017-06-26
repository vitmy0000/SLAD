package com.weiz.slad

import com.amazonaws.auth.AWSCredentials
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3Client
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.HashPartitioner
// import org.apache.log4j.Logger
// import org.apache.log4j.Level

import org.rogach.scallop._
import scala.collection.mutable
import scala.collection.JavaConversions._
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

  val inputDir = opt[String](required = true,
    descr = """(REQUIRED)Input directory.""")
  val outputDir = opt[String](required = true,
    descr = """(REQUIRED)Output directory.                |
              |Directory will be created automatically.   |
              |Make sure the directory does not exisit.   |
              |Default: "./slad_results" """
              .stripMargin.replace("|\n", " "))
  val awsAccessId = opt[String](required = true,
    descr = """(REQUIRED)AWS_ACCESS_KEY_ID"""
              .stripMargin.replace("|\n", " "))
  val awsAccessKey = opt[String](required = true,
    descr = """(REQUIRED)AWS_SECRET_ACCESS_KEY"""
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
  def getListOfFilePaths(
      s3dir: String,
      awsAccessId: String,
      awsAccessKey: String): List[String] = {
    val credentials: AWSCredentials = new BasicAWSCredentials(
        awsAccessId, awsAccessKey)
    val s3client: AmazonS3= new AmazonS3Client(credentials)
    val pattern = "s3://(.+?)/(.+)/?".r
    val pattern(bucket, folder) = s3dir
    val fileList = s3client.listObjects(bucket, folder)
      .getObjectSummaries.toList
      .filter(x => x.getSize > 0)
      .map(x => s3dir + "/" + (new File(x.getKey)).getName)
    fileList
  }

  // assume single line read
  def loadFile(fp: String, sc: SparkContext): RDD[(String, String)] = {
    println("Loading file: %s".format(fp))
    sc.textFile(fp)
      .filter(_.size > 0)
      .map(_.split("\n") match {
        case Array(header, read) => (read, header)
      })
  }

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
    val sladInputDir = sladConf.inputDir()
    val sladOutputDir = sladConf.outputDir()
    val sladAwsAccessId = sladConf.awsAccessId()
    val sladAwsAccessKey = sladConf.awsAccessKey()
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

    val inputFiles: List[String] = getListOfFilePaths(
        sladInputDir, sladAwsAccessId, sladAwsAccessKey)
    println("Input files:")
    inputFiles.foreach(println)

    // DEREPLICATION
    inputFiles.map { case inputFilePath =>
      loadFile(inputFilePath, sc).aggregateByKey(mutable.HashSet.empty[String])(
        //addToHeaderSet, mergeHeaderSets
        (s: mutable.HashSet[String], v: String) => s += v,
        (s1: mutable.HashSet[String], s2: mutable.HashSet[String]) => s1 ++= s2)
      .map {
        case (read, headers) => s">$headers\n$read"
      }.saveAsTextFile(sladOutputDir + "/derep/" +
          (new File(inputFilePath)).getName() + "/")
    }

    // RELOAD DEREPLICATED FILES
    // ABUNDANCE FILTER
    val nonTrivialReads: RDD[(String, Int)] = inputFiles.map {
        case inputFilePath =>
      getListOfFilePaths(
        sladOutputDir + "/derep/" + (new File(inputFilePath)).getName() + "/",
        sladAwsAccessId, sladAwsAccessKey)
      .filter(_.contains("part"))
      .map(fp => loadFile(fp, sc))
      .reduce(_ union _)
      .map { case (read, header) =>
        (read, header.split(", ").size)
      }.filter { case (header, cnt) =>
        cnt >= sladAbundance
      }
    }.reduce(_ union _)
    .coalesce(sc.defaultParallelism)
    .cache
    .localCheckpoint
    println("Abundant sequence number: %d".format(nonTrivialReads.count()))

    // PARSE
    println("Parsing...")
    val seqs: RDD[SeqAsKmerCnt] = nonTrivialReads.map {
        case (read, abundance) =>
      new SeqAsKmerCnt(read, sladWordSize)
    }.cache.localCheckpoint
    seqs.count()
    nonTrivialReads.unpersist()

    val random: Random = new Random(sladRandomSeed)
    val slad = new SLAD(
        sc, random, sladNumPowerIteration,
        sladNumLeaveCluster, sladRadius, sladMinSize)
    val (clusterIndcies, leaveClusterLandmarkInfo)
        : (RDD[Int], Map[Int, List[String]]) = slad.run(seqs)

    clusterIndcies.zip(seqs).map { case (clusterIndex, seq) =>
      s">cluster_$clusterIndex\n${seq.getRead}"
    }.saveAsTextFile(sladOutputDir + "/partition")



    val file = new File("landmarks.fa")
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
