package com.cloudera.spark.graphx.dataset

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{GraphLoader, Edge, Graph}

/**
 * Created by jayant on 7/12/15.
 */

object DatasetFollowers {

  def graph(sc : SparkContext): Graph[(String, Int), Int] = {

    val graph = GraphLoader.edgeListFile(sc, "graphx/data/followers.txt")

    graph

  }
}