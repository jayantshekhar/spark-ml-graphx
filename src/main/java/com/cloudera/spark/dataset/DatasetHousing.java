package com.cloudera.spark.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.regex.Pattern;

/**
 * Created by jayantshekhar on 6/27/15.
 */
public class DatasetHousing {

    public static JavaRDD<LabeledPoint> createRDD(JavaSparkContext sc, String inputFile) {

        JavaRDD<String> data = sc.textFile(inputFile);

        data = data.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                if (s.contains("price"))
                    return false;

                return true;
            }
        });

        JavaRDD<LabeledPoint> parsedData = data.map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");

                        // label : price
                        Double price = Double.parseDouble(parts[1]);

                        double[] v = new double[4];
                        v[0] = Double.parseDouble(parts[2]); // lotsize
                        v[1] = Double.parseDouble(parts[3]); // bedrooms
                        v[2] = Double.parseDouble(parts[4]); // bathrooms
                        v[3] = Double.parseDouble(parts[5]); // stories

                        return new LabeledPoint(price, Vectors.dense(v));
                    }
                }
        );

        return parsedData;
    }

}
