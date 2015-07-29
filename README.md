Run KMeans
----------

spark-submit --class com.cloudera.spark.kmeans.JavaKMeans  --master yarn target/spark-mllib-1.0.jar data/kmeans 2 5

Run Movie Lens
--------------

spark-submit --class com.cloudera.spark.movie.JavaMovieLensALS  --master yarn target/spark-mllib-1.0.jar data/movielens_small 5 5

Run DataFrames version of Movie Lens
------------------------------------

spark-submit --class com.cloudera.spark.movie.JavaDFMovieLensALS  --master yarn target/spark-mllib-1.0-jar-with-dependencies.jar data/movielens/ratings 5 5

Run Iris
--------

spark-submit --class com.cloudera.spark.iris.JavaIris  --master yarn target/spark-mllib-1.0-jar-with-dependencies.jar data/iris 3 5

Run Covtype
-----------

spark-submit --class com.cloudera.spark.covtype.JavaCovtype  --master yarn target/spark-mllib-1.0-jar-with-dependencies.jar data/covtype


Run FPGrowth
------------

spark-submit --class com.cloudera.spark.fpg.JavaFPGrowth  --master yarn target/spark-mllib-1.0.jar data/fpg


Run Wikipedia PageRank
----------------------

spark-submit --class com.cloudera.spark.graphx.wikipedia.WikipediaPageRankFormat2  --master yarn target/spark-mllib-1.0.jar


Run Triangle Count
------------------

spark-submit --class com.cloudera.spark.graphx.triangle.TriangleCount  --master yarn target/spark-mllib-1.0.jar


Run PageRank
------------

spark-submit --class com.cloudera.spark.graphx.pagerank.PageRank  --master yarn target/spark-mllib-1.0.jar





