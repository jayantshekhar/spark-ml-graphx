package com.cloudera.spark.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.regex.Pattern;

/**
 * Created by jayantshekhar on 6/27/15.
 */
public class DatasetIris {


    private static class ParsePoint implements Function<String, Vector> {
        private static final Pattern COMMA = Pattern.compile(",");

        @Override
        public Vector call(String line) {
            String[] tok = COMMA.split(line);
            double[] point = new double[tok.length-1];
            for (int i = 0; i < tok.length-1; ++i) {
                point[i] = Double.parseDouble(tok[i]);
            }
            return Vectors.dense(point);
        }
    }


    public static JavaRDD<Vector> createRDD(JavaSparkContext sc, String inputFile) {
        JavaRDD<String> lines = sc.textFile(inputFile);

        JavaRDD<Vector> points = lines.map(new ParsePoint());

        return points;
    }

}
