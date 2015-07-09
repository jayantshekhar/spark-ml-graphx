/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.spark.housing;

import com.cloudera.spark.dataset.DatasetHousing;
import com.cloudera.spark.mllib.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.regression.*;
import scala.Tuple2;

import java.util.List;

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
        JavaRDD<LabeledPoint> parsedData = DatasetHousing.createRDD(sc, inputFile);
        parsedData.cache();

        // print the first 5 records
        List<LabeledPoint> list = parsedData.take(5);
        for (LabeledPoint v : list) {
            System.out.println(v.toString());
        }

        // linear regression with SGD
        linearRegressionWithSGD(sc, parsedData);

        // ridge regression with SGD
        ridgeRegressionWithSGD(sc, parsedData);

        // lasso with SGD
        lassoWithSGD(sc, parsedData);


        sc.stop();
    }

    private static void linearRegressionWithSGD(JavaSparkContext sc, JavaRDD<LabeledPoint> parsedData) {

        // Building the model
        int numIterations = 20;
        final LinearRegressionModel model =
                LinearRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);

        System.out.println(model.toString());

        evaluateModel(sc, parsedData, model);
    }

    private static void evaluateModel(JavaSparkContext sc, JavaRDD<LabeledPoint> parsedData, final GeneralizedLinearModel model) {

        // Evaluate model on training examples and compute training error
        JavaRDD<Tuple2<Double, Double>> valuesAndPreds = parsedData.map(
                new Function<LabeledPoint, Tuple2<Double, Double>>() {
                    public Tuple2<Double, Double> call(LabeledPoint point) {
                        double prediction = model.predict(point.features());
                        return new Tuple2<Double, Double>(prediction, point.label());
                    }
                }
        );
        double MSE = new JavaDoubleRDD(valuesAndPreds.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        return Math.pow(pair._1() - pair._2(), 2.0);
                    }
                }
        ).rdd()).mean();
        System.out.println("training Mean Squared Error = " + MSE);

        // Save and load model
        //model.save(sc.sc(), "myModelPath");
        //LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(), "myModelPath");

    }


    private static void ridgeRegressionWithSGD(JavaSparkContext sc, JavaRDD<LabeledPoint> parsedData) {

        // Building the model
        int numIterations = 20;
        final RidgeRegressionModel model =
                RidgeRegressionWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);

        System.out.println(model.toString());

        evaluateModel(sc, parsedData, model);

        /***

        // Save and load model
        model.save(sc.sc(), "myModelPath");
        LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(), "myModelPath");

         ***/

    }

    private static void lassoWithSGD(JavaSparkContext sc, JavaRDD<LabeledPoint> parsedData) {

        // Building the model
        int numIterations = 20;
        final LassoModel model =
                LassoWithSGD.train(JavaRDD.toRDD(parsedData), numIterations);

        System.out.println(model.toString());

        evaluateModel(sc, parsedData, model);

        /***

        // Save and load model
        model.save(sc.sc(), "myModelPath");
        LinearRegressionModel sameModel = LinearRegressionModel.load(sc.sc(), "myModelPath");
        ***/

    }
}
