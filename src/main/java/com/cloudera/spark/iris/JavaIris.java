package com.cloudera.spark.iris;

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
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by jayant on 6/25/15.
 */

// iris dataset : http://archive.ics.uci.edu/ml/machine-learning-databases/iris/

public class JavaIris {


    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println(
                    "Usage: JavaIris <input_file> <k> <max_iterations> [<runs>]");
            System.exit(1);
        }
        String inputFile = args[0];
        int k = Integer.parseInt(args[1]);
        int iterations = Integer.parseInt(args[2]);
        int runs = 1;

        if (args.length >= 4) {
            runs = Integer.parseInt(args[3]);
        }

        SparkConf sparkConf = new SparkConf().setAppName("JavaIris");
        SparkConfUtil.setConf(sparkConf);
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        // create RDD
        JavaRDD<Vector> points = DatasetIris.createRDD(sc, inputFile);

        // print the first 5 records
        List<Vector> list = points.take(5);
        for (Vector v : list) {
            System.out.println(v.toString());
        }

        // print summary statistics
        summaryStatistics(points);

        // correlation
        correlation(points);

        // cluster
        cluster(points, k, iterations, runs);

        sc.stop();
    }

    public static void summaryStatistics(JavaRDD<Vector> points) {

        // Compute column summary statistics.
        MultivariateStatisticalSummary summary = Statistics.colStats(points.rdd());
        System.out.println("Mean : " + summary.mean()); // a dense vector containing the mean value for each column
        System.out.println("Variance : "+summary.variance()); // column-wise variance
        System.out.println("Non-zeros : "+summary.numNonzeros()); // number of nonzeros in each column
    }

    public static void correlation(JavaRDD<Vector> points) {
        Matrix matrix = Statistics.corr(points.rdd());

        String str = matrix.toString();
        System.out.println(str);
    }
    
    public static void cluster(JavaRDD<Vector> points, int k, int iterations, int runs) {

        // train
        KMeansModel model = KMeans.train(points.rdd(), k, iterations, runs, KMeans.K_MEANS_PARALLEL());

        // print cluster centers
        System.out.println("Cluster centers:");
        for (Vector center : model.clusterCenters()) {
            System.out.println(" " + center);
        }

        // compute cost
        double cost = model.computeCost(points.rdd());
        System.out.println("Cost: " + cost);

    }

}
