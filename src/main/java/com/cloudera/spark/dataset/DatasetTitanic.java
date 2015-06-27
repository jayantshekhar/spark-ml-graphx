package com.cloudera.spark.dataset;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;

/**
 * Created by jayantshekhar on 6/27/15.
 */
public class DatasetTitanic {

    public static DataFrame createDF(SQLContext sqlContext, String inputFile) {
        // options
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("header", "true");
        options.put("path", inputFile);
        options.put("delimiter", ",");

        // create dataframe from input file
        DataFrame df = sqlContext.load("com.databricks.spark.csv", options);
        df.printSchema();

        return df;
    }

    // create an RDD of Vectors from a DataFrame
    public static JavaRDD<LabeledPoint> createLabeledPointsRDD(JavaSparkContext ctx, SQLContext sqlContext, String inputFile) {

        DataFrame df = createDF(sqlContext, inputFile);

        // convert dataframe to an RDD of Vectors
        JavaRDD<LabeledPoint> rdd = df.toJavaRDD().map(new Function<Row, LabeledPoint>() {

            @Override
            public LabeledPoint call(Row row) {

                int survived = toInt(row.getString(1));

                double[] arr = new double[2];
                arr[0] = toDouble(row.getString(5));
                arr[1] = toDouble(row.getString(6));

                Vector vector = Vectors.dense(arr);

                LabeledPoint labeledPoint = new LabeledPoint(survived, vector);

                return labeledPoint;
            }
        });

        return rdd;
    }

    public static int toInt(String str) {
        return Integer.parseInt(str);
    }


    public static double toDouble(String str) {
        if (str.length() == 0)
            return 0.0;

        return Double.parseDouble(str);
    }
}
