package com.cloudera.spark.housing;

import com.cloudera.spark.dataset.DatasetHousing;
import com.cloudera.spark.dataset.DatasetIris;
import com.cloudera.spark.mllib.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by jayant on 6/25/15.
 */

public class JavaHousing {


    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println(
                    "Usage: JavaHousing <input_file>");
            System.exit(1);
        }
        String inputFile = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("JavaHousing");
        SparkConfUtil.setConf(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // create RDD
        JavaRDD<LabeledPoint> points = DatasetHousing.createRDD(sc, inputFile);

        // print the first 5 records
        List<LabeledPoint> list = points.take(5);
        for (LabeledPoint v : list) {
            System.out.println(v.toString());
        }

        sc.stop();
    }

}
