package com.cloudera.spark.graphx.wikipedia

import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.graphx.{VertexId, Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WikipediaPageRank {

  def main(args: Array[String]): Unit = {

    // spark context
    val sparkConf: SparkConf = new SparkConf().setAppName("Wikipedia Page Rank")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    // RDD's
    val articles: RDD[String] = sc.textFile("datagraphx/wikipedia/vertices.txt")
    val links: RDD[String] = sc.textFile("datagraphx/wikipedia/edges.txt")

    println(articles.first())

    // vertices
    val vertices = articles.map { line =>
      val fields = line.split('\t')
      (fields(0).toLong, fields(1))
    }

    // edges
    val edges = links.map { line =>
      val fields = line.split('\t')
      Edge(fields(0).toLong, fields(1).toLong, 0)
    }

    // graph
    val graph = Graph(vertices, edges, "").cache()

    println(graph.vertices.count())
    println(graph.triplets.count)
    graph.triplets.take(5).foreach(println(_))

    // pagerank
    val prGraph = graph.pageRank(0.001).cache()

    prGraph.vertices.take(5).foreach(println(_))

    // title/page rank
    val titleAndPrGraph = graph.outerJoinVertices(prGraph.vertices) {
      (v, title, rank) => (rank.getOrElse(0.0), title)
    }

    // top 10 title/page rank
    titleAndPrGraph.vertices.top(10) {
      Ordering.by((entry: (VertexId, (Double, String))) => entry._2._1)
    }.foreach(t => println(t._2._2 + ": " + t._2._1))
  }

}
