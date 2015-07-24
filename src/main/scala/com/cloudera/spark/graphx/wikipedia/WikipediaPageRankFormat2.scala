package com.cloudera.spark.graphx.wikipedia;

import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object WikipediaPageRankFormat2 {

  def main(args: Array[String]): Unit = {

    // spark context
    val sparkConf: SparkConf = new SparkConf().setAppName("Wikipedia Page Rank")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    //Raw files being read : http://haselgrove.id.au/wikipedia.htm
    val rawLinks:RDD[String] = sc.textFile("datagraphx/wikipediaV2/links-simple-sorted-top100.txt");
    val rawVertex:RDD[String] = sc.textFile("datagraphx/wikipediaV2/titles-sorted-top500.txt");

    println(rawLinks.first())

    //Create the edge RDD
    val edges = rawLinks.flatMap(line => {
      val Array(key, values) = line.split(":",2)
      for(value <- values.trim.split("""\s+"""))
        yield (Edge(key.toLong, value.trim.toLong, 0))
    })
    edges.collect

    val numberOfEdges = edges.count()
    println("Number of edges:"+ numberOfEdges)

    //Create the vertex RDD
    val vertices = rawVertex.zipWithIndex().map({ row =>
      val nodeName = row._1
      val nodeIndex = row._2
      (nodeIndex.toLong,nodeName)
    });

    val numberOfVertices = vertices.count()
    println("Number of vertices:"+ numberOfVertices)

    //Create the graph object
    val graph = Graph(vertices, edges, "").cache()

    //Run page rank
    val wikiGraph = graph.pageRank(0.01).cache()

    wikiGraph.vertices.take(5).foreach(println(_))

  }


}