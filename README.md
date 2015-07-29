Run KMeans
----------

spark-submit --class com.cloudera.spark.kmeans.JavaKMeans  --master yarn target/spark-mllib-1.0.jar data/kmeans 5 5

Run Movie Lens
--------------

spark-submit --class com.cloudera.spark.movie.JavaMovieLensALS  --master yarn target/spark-mllib-1.0.jar data/movielens_small 5 5

Run DataFrames version of Movie Lens
------------------------------------

spark-submit --class com.cloudera.spark.movie.JavaDFMovieLensALS  --master yarn target/spark-mllib-1.0-jar-with-dependencies.jar data/movielens/ratings 5 5


Run FPGrowth
------------

spark-submit --class com.cloudera.spark.fpg.JavaFPGrowth  --master yarn target/spark-mllib-1.0.jar data/fpg