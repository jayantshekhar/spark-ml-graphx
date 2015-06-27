package com.cloudera.spark.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;

/**
 * Created by jayantshekhar on 6/27/15.
 */
public class DatasetMovieLens {


    public static JavaRDD<Rating> createRDD(JavaSparkContext sc, String inputFile) {

        // create RDD
        JavaRDD<String> lines = sc.textFile(inputFile);

        JavaRDD<Rating> ratings = lines.map(new Function<String, Rating>() {
            @Override
            public Rating call(String s) throws Exception {
                String[] arr = s.split("::");
                return new Rating(Integer.parseInt(arr[0]), Integer.parseInt(arr[1]), Double.parseDouble(arr[2]));
            }
        });

        return ratings;
    }


    public static DataFrame createDF(SQLContext sqlContext, String inputFile) {
        // options
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("header", "false");
        options.put("path", inputFile);
        options.put("delimiter", ",");

        // create dataframe from input file
        DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
        df.printSchema();

        // name the columns
        DataFrame newdf = df.toDF("user", "movie", "rating");
        newdf.printSchema();

        // register as a temporary table
        newdf.registerTempTable("ratings");

        // convert to proper types
        DataFrame results = sqlContext.sql("SELECT cast(user as int) user, cast(movie as int) movie, cast(rating as int) rating FROM ratings");
        results.printSchema();
        results.show();

        return results;
    }

}
