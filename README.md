# Pyspark Custom Repartitioner

When Spark is loading data to object storage systems like HDFS, S3 etc, it can result in large number of small files. This is mainly because Spark is a parallel processing system and data loading is done through multiple tasks where each task can load into multiple partitions.

# I have a solution for this...

Just use this custom repartitioner :-) 

This is an utility to handle small files issue in Spark. This utility help to repartition dataframe based on salted key. Salted key provides better distribition of records so that target hdfs path will have files of same size.

# Thank You
