package com.cloudera.spark.movie;

import com.cloudera.spark.dataset.DatasetMovieLens;
import com.cloudera.spark.mllib.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;

/**
 * Created by jayant on 6/24/15.
 */
public final class JavaDFMovieLensALS {

    public static void main(String[] args) {

        // usage
        if (args.length < 3) {
            System.err.println(
                    "Usage: JavaDFMovieLensALS <input_file> <rank> <num_iterations> [<lambda>]");
            System.exit(1);
        }

        // input parameters
        String inputFile = args[0];
        int rank = Integer.parseInt(args[1]);
        int iterations = Integer.parseInt(args[2]);
        double lambda = 1;

        if (args.length >= 4) {
            lambda = Double.parseDouble(args[3]);
        }

        // spark context
        SparkConf sparkConf = new SparkConf().setAppName("JavaMovieLensALS");
        SparkConfUtil.setConf(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(sc);

        // create data frame
        DataFrame results = DatasetMovieLens.createDF(sqlContext, inputFile);

        // split the dataset
        DataFrame training = results.sample(true, .8);
        DataFrame test = results.sample(true, .2);

        org.apache.spark.ml.recommendation.ALS als = new org.apache.spark.ml.recommendation.ALS();
        als.setUserCol("user").setItemCol("movie").setRank(rank).setMaxIter(iterations);
        ALSModel model =  als.fit(training);

        DataFrame pred = model.transform(test);
        pred.show();

        sc.stop();

    }

}
