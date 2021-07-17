#Author : Gaurav Patil
#Date: 2021-07-01 

#Description: Utility to handle small files issue in Spark. This utility help to repartition dataframe based on salted key. Salted key provides better distribition of records so that target hdfs path will have files of same size.

#Please give credit by copying git link if you wish to put this code on your blog.

from pyspark.sql import functions as F 

'''
@parameters:
df = pyspark dataframe 
max_records_in_each_file = number of rows per file : example: 200000
distribution_keys = comma separated partition columns : example: "load_date,city"
'''

def custom_repartitioner(df, max_records_in_each_file, distribution_keys):
    print("Started custom partitioner code...")
    
    dist_cols = [key.strip() for key in distribution_keys.split(",")]
    print("distribution cols: "+str(dist_cols))
    
    # find total count of records per partition 
    agg_df = df.select(*dist_cols)\
                .withColumn('_partColAgg', F.concat(*dist_cols))\
                .drop(*dist_cols)\
                .groupBy('_partColAgg')\
                .agg(F.count(F.lit(1)).alias("records_count"))
    
    # total no. of files = total number of records per partition / max records in each file 
    agg_df = agg_df.withColumn('_num', F.ceil(F.col('records_count').cast('double')/F.lit(max_records_in_each_file)))\
                    .select('_num', '_partColAgg')
        
    agg_df.cache()

    # find max number of total files. This value we will use in repartition function at the end.
    number_of_files = max(int(partition._num) for partition in agg_df.collect())
    print('max num of files: '+str(number_of_files))

    # Join main df with agg_df on concated column 
    df = df.withColumn('_partColMain', F.concat(*dist_cols))
    df = df.join(F.broadcast(agg_df), F.col('_partColMain')==F.col('_partColAgg'), 'inner')\
                .drop('_partColAgg')
    
    # we will use mod operator to generate salted key for each partition 
    df = df.withColumn('_unique_id', F.monotonically_increasing_id())\
            .withColumn('_salted_key', F.col('_unique_id') % F.col('_num'))
    
    df = df.drop('_partColMain', '_num', '_unique_id')
    
    # repartition dataframe based on salted key
    df = df.repartition(number_of_files, '_salted_key')\
            .drop('_salted_key')
    
    #df.printSchema()
    
    print("Custom partitioner code finished...")
    
    return df 
