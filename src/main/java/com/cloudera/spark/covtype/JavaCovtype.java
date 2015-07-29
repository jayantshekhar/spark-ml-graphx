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

package com.cloudera.spark.covtype;

import com.cloudera.spark.dataset.DatasetCovtype;
import com.cloudera.spark.dataset.DatasetHousing;
import com.cloudera.spark.mllib.SparkConfUtil;
import com.cloudera.spark.randomforest.JavaRandomForest;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.regression.*;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

/**
 * Created by jayant on 6/25/15.
 */

public class JavaCovtype {


    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println(
                    "Usage: JavaCovtype <input_file>");
            System.exit(1);
        }
        String inputFile = args[0];

        SparkConf sparkConf = new SparkConf().setAppName("JavaCovtype");
        SparkConfUtil.setConf(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // create RDD
        JavaRDD<LabeledPoint> data = DatasetCovtype.createRDD(sc, inputFile);
        data.cache();

        // print the first 5 records
        List<LabeledPoint> list = data.take(5);
        for (LabeledPoint v : list) {
            System.out.println(v.toString());
        }

        // Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        // classification using RandomForest
        System.out.println("\nRunningclassification using RandomForest\n");
        classify(trainingData, testData);

        sc.stop();
    }

    /**
     * Note: This example illustrates binary classification.
     * For information on multiclass classification, please refer to the JavaDecisionTree.java
     * example.
     */
    public static void classify(JavaRDD<LabeledPoint> trainingData,
                                          JavaRDD<LabeledPoint> testData) {

        trainingData.cache();
        testData.cache();

        // Train a RandomForest model.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 7;

        // storing arity of categorical features. E.g., an entry (n -> k) indicates that feature n is categorical
        // with k categories indexed from 0: {0, 1, ..., k-1}
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        categoricalFeaturesInfo.put(10, 4);
        categoricalFeaturesInfo.put(11, 40);

        Integer numTrees = 10; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "entropy"; // gini/entropy
        Integer maxDepth = 20;
        Integer maxBins = 300;
        Integer seed = 12345;


        long featuresize = trainingData.take(1).get(0).features().size();

        final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);

        // Evaluate model on test instances
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });

        // print model
        System.out.println(model.toString());

        // compute test error
        Double testErr =
                1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return !pl._1().equals(pl._2());
                    }
                }).count() / testData.count();
        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification forest model:\n" + model.toDebugString());
    }

}
