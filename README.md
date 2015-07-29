# spark-ml-graphx

LOAD DATA INTO HDFS
-------------------

	hadoop fs -put data
	hadoop fs -put datagraphx

Run KMeans
----------

spark-submit --class com.cloudera.spark.kmeans.JavaKMeans  --master yarn target/spark-recipes-1.0.jar data/kmeans 5 5

