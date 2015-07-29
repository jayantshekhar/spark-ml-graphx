package com.cloudera.spark.graphx.pagerank

import com.cloudera.spark.graphx.dataset.{DatasetUsers, DatasetFollowers, DatasetSimpleGraph}
import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by jayant on 7/12/15.
 */
object PageRank {

  def main(args: Array[String]) {

    pageRank()

  }

  def pageRank(): Unit = {
    println("======================================")
    println("|             Page Rank              |")
    println("======================================")

    val sparkConf: SparkConf = new SparkConf().setAppName("PageRank")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    // Load the edges as a graph
    val graph = DatasetFollowers.graph(sc)

    // Run PageRank
    val ranks = graph.pageRank(0.0001).vertices

    // Join the ranks with the usernames
    val users = DatasetUsers.users(sc)

    val ranksByUsername = users.join(ranks).map {
      case (id, (username, rank)) => (username, rank)
    }

    // Print the result
    println(ranksByUsername.collect().mkString("\n"))

    sc.stop()
  }
}
