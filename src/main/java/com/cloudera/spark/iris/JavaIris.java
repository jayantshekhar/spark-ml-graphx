package com.cloudera.spark.iris;

import com.cloudera.spark.mllib.SparkConfUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;

import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by jayant on 6/25/15.
 */

// iris dataset : http://archive.ics.uci.edu/ml/machine-learning-databases/iris/

public class JavaIris {


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
        JavaRDD<Vector> points = createRDD(sc, inputFile);

        // print the first 5 records
        List<Vector> list = points.take(5);
        for (Vector v : list) {
            System.out.println(v.toString());
        }

        // cluster
        cluster(points, k, iterations, runs);

        sc.stop();
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
