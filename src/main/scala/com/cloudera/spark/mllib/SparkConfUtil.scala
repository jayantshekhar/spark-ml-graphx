package com.cloudera.spark.mllib

import org.apache.spark.SparkConf

/**
 * Created by jayant on 6/22/15.
 */
object SparkConfUtil {

  val isLocal = false;

  def setConf(conf: SparkConf): Unit = {

    if (isLocal) {
     conf.setMaster("local")
     conf.set("spark.broadcast.compress", "false")
     conf.set("spark.shuffle.compress", "false")
    }
  }
}
