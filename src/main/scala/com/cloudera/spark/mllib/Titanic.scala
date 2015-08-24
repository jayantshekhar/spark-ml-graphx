package com.cloudera.spark.mllib

/**
 * Created by jayantshekhar on 8/24/15.
 */

import com.cloudera.spark.dataset.DatasetMovieLens
import com.cloudera.spark.dataset.DatasetTitanic
import com.cloudera.spark.dataset.DatasetTitanic
import com.cloudera.spark.mllib.SparkConfUtil
import com.cloudera.spark.randomforest.JavaRandomForest
import com.cloudera.spark.randomforest.JavaRandomForest
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.ml.recommendation.ALSModel
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

/**
 * Created by jayantshekhar on 6/27/15.
 */
object Titanic {

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: Titanic <input_file>")
      System.exit(1)
    }

    val inputFile: String = args(0)
    val sparkConf: SparkConf = new SparkConf().setAppName("JavaTitanic")
    SparkConfUtil.setConf(sparkConf)

    val sc: JavaSparkContext = new JavaSparkContext(sparkConf)
    val sqlContext: SQLContext = new SQLContext(sc)
    val results: DataFrame = DatasetTitanic.createDF(sqlContext, inputFile)

    results.printSchema

    val data: JavaRDD[LabeledPoint] = DatasetTitanic.createLabeledPointsRDD(sc, sqlContext, inputFile)
    val splits: Array[JavaRDD[LabeledPoint]] = data.randomSplit(Array[Double](0.7, 0.3))
    val trainingData: JavaRDD[LabeledPoint] = splits(0)
    val testData: JavaRDD[LabeledPoint] = splits(1)

    System.out.println("\nRunning example of classification using RandomForest\n")
    JavaRandomForest.testClassification(trainingData, testData)

    System.out.println("\nRunning example of regression using RandomForest\n")
    JavaRandomForest.testRegression(trainingData, testData)

    sc.stop
  }
}

