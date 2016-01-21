package com.cloudera.spark.mllib.kmeans

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

/**
  * Created by abruneau on 20/01/2016.
  */
object ScalaKMeans {
  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KMeans <run local (l or c)> <input_file> <k> <max_iterations> [<runs>]")
      System.exit(1)
    }

    val runLocally = args(0).toLowerCase.equals("l")
    val inputFile: String = args(1)
    val k: Int = args(2).toInt
    val iterations: Int = args(3).toInt
    var runs: Int = 1

    if (args.length >= 5) {
      runs = args(4).toInt
    }

    val sc: SparkContext = if (runLocally) {
      val sparkConfig = new SparkConf()
      sparkConfig.set("spark.broadcast.compress", "false")
      sparkConfig.set("spark.shuffle.compress", "false")
      sparkConfig.set("spark.shuffle.spill.compress", "false")
      new SparkContext("local[" + 1 + "]", "test", sparkConfig)
    } else {
      val sparkConfig = new SparkConf().setAppName("KMeans")
      new SparkContext(sparkConfig)
    }


    val data = sc.textFile(inputFile)
    val parsedData = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

    val clusters = KMeans.train(parsedData, k, iterations, runs)

    // print cluster centers
    System.out.println("Cluster centers:")
    for (center <- clusters.clusterCenters) {
      System.out.println(" " + center)
    }

    // compute cost
    val cost = clusters.computeCost(parsedData)
    System.out.println("Cost: " + cost)

    sc.stop

  }
}
