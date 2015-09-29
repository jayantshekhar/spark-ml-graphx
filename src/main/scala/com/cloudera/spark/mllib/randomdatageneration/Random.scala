package com.cloudera.spark.mllib.randomdatageneration

import com.cloudera.spark.graphx.dataset.DatasetSimpleGraph
import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.mllib.random.RandomRDDs
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by jayant on 9/29/15.
 */
object Random {

  def main(args: Array[String]) {

    random()

  }

  def random(): Unit = {
    println("======================================")
    println("|             Random                 |")
    println("======================================")

    val sparkConf: SparkConf = new SparkConf().setAppName("Random")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    val normalRDD = RandomRDDs.normalRDD(sc, 1000, 10)
    val temp = normalRDD.collect();
    println(temp.mkString(" "))

    sc.stop()

  }

}