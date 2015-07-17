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

/*
Name                                     Data Type    Measurement                       Description

Elevation                               quantitative    meters                       Elevation in meters
Aspect                                  quantitative    azimuth                      Aspect in degrees azimuth
Slope                                   quantitative    degrees                      Slope in degrees
Horizontal_Distance_To_Hydrology        quantitative    meters                       Horz Dist to nearest surface water features
Vertical_Distance_To_Hydrology          quantitative    meters                       Vert Dist to nearest surface water features
Horizontal_Distance_To_Roadways         quantitative    meters                       Horz Dist to nearest roadway
Hillshade_9am                           quantitative    0 to 255 index               Hillshade index at 9am, summer solstice
Hillshade_Noon                          quantitative    0 to 255 index               Hillshade index at noon, summer soltice
Hillshade_3pm                           quantitative    0 to 255 index               Hillshade index at 3pm, summer solstice
Horizontal_Distance_To_Fire_Points      quantitative    meters                       Horz Dist to nearest wildfire ignition points
Wilderness_Area (4 binary columns)      qualitative     0 (absence) or 1 (presence)  Wilderness area designation
Soil_Type (40 binary columns)           qualitative     0 (absence) or 1 (presence)  Soil Type designation
Cover_Type (7 types)                    integer         1 to 7                       Forest Cover Type designation
 */

public class DatasetCovtype {

    public static JavaRDD<LabeledPoint> createRDD(JavaSparkContext sc, String inputFile) {

        JavaRDD<String> data = sc.textFile(inputFile);

        data = data.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
                return true;
            }
        });

        JavaRDD<LabeledPoint> parsedData = data.map(
                new Function<String, LabeledPoint>() {
                    public LabeledPoint call(String line) {
                        String[] parts = line.split(",");

                        // label : price (last column)
                        Double covtype = Double.parseDouble(parts[parts.length-1]);

                        // all except the label
                        double[] v = new double[parts.length-1];

                        for (int i=0; i<v.length; i++) {
                            v[i] = Double.parseDouble(parts[i]);
                        }

                        return new LabeledPoint(covtype, Vectors.dense(v));
                    }
                }
        );

        return parsedData;
    }

}
