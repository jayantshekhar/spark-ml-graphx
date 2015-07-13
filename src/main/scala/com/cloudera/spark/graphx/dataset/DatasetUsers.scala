package com.cloudera.spark.graphx.dataset

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{GraphLoader, Graph}
import org.apache.spark.rdd.RDD

/**
 * Created by jayant on 7/13/15.
 */
object DatasetUsers {

  def users(sc : SparkContext): RDD[(Long, String)] = {

    val users = sc.textFile("datagraphx/users").map { line =>
      val fields = line.split(",")
      (fields(0).toLong, fields(1))
    }

    users
  }
}
