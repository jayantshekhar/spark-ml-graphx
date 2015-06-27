package com.cloudera.spark.titanic;

import com.cloudera.spark.dataset.DatasetMovieLens;
import com.cloudera.spark.dataset.DatasetTitanic;
import com.cloudera.spark.mllib.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jayantshekhar on 6/27/15.
 */
public class JavaTitanic {

    public static void main(String[] args) {

        // usage
        if (args.length < 1) {
            System.err.println(
                    "Usage: JavaTitanic <input_file>");
            System.exit(1);
        }

        // input parameters
        String inputFile = args[0];


        // spark context
        SparkConf sparkConf = new SparkConf().setAppName("JavaTitanic");
        SparkConfUtil.setConf(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // create data frame
        DataFrame results = DatasetTitanic.createDF(sqlContext, inputFile);

        results.printSchema();

        JavaRDD<LabeledPoint> rdd = DatasetTitanic.createLabeledPointsRDD(sc, sqlContext, inputFile);

        long count = rdd.count();

        sc.stop();

    }

}
