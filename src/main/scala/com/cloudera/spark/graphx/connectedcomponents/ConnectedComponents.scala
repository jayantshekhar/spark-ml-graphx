package com.cloudera.spark.graphx.connectedcomponents

import com.cloudera.spark.graphx.dataset.{DatasetUsers, DatasetFollowers, DatasetSimpleGraph}
import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by jayant on 7/12/15.
 */
object ConnectedComponents {

  def main(args: Array[String]) {

    connectedComponents()

  }

  def connectedComponents(): Unit = {
    println("======================================")
    println("|             Connected Components   |")
    println("======================================")

    val sparkConf: SparkConf = new SparkConf().setAppName("Test")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    // Load the edges as a graph
    val graph = DatasetFollowers.graph(sc)

    // Find the connected components
    val cc = graph.connectedComponents().vertices

    // Join the connected components with the usernames
    val users = DatasetUsers.users(sc)

    val ccByUsername = users.join(cc).map {
      case (id, (username, cc)) => (username, cc)
    }

    // Print the result
    println(ccByUsername.collect().mkString("\n"))

    sc.stop()
  }
}
