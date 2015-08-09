package com.cloudera.spark.graphx.triangle

import com.cloudera.spark.graphx.dataset.DatasetSimpleGraph
import com.cloudera.spark.mllib.SparkConfUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by jayant on 7/12/15.
 */

object TriangleCount {

  def main(args: Array[String]) {

    triangleCount()

  }

  def triangleCount(): Unit = {
    println("======================================")
    println("|             Triangle Count         |")
    println("======================================")

    val sparkConf: SparkConf = new SparkConf().setAppName("Test")
    SparkConfUtil.setConf(sparkConf)
    val sc: SparkContext = new SparkContext(sparkConf)

    val graph = DatasetSimpleGraph.graph(sc)

    val outdegrees = graph.outDegrees;
    val aaa = outdegrees.collect();
    println(aaa.mkString(" "))

    val indegrees = graph.inDegrees;
    val bbb = indegrees.collect();
    println(bbb.mkString(" "))

    // number of triangles passing through each vertex
    val triangles = graph.triangleCount()

    val numv = triangles.numVertices
    println("Number of Vertices " + numv)

    println("Triangles: " + triangles.vertices.map {
      case (vid, data) => data.toLong
    }.reduce(_ + _) / 3)

    sc.stop()
  }
}