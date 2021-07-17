from pyspark.sql.functions import * 
from pyspark.sql import SparkSession 
from custom_repartitioner import custom_repartitioner
from datetime import datetime
import sys 

if __name__=='__main__':
    print("Starting data load...")

    spark = SparkSession.builder.master('yarn').appName("test").enableHiveSupport().getOrCreate()
    
    df = spark.sql("select * from test_db.test_table where load_date='2021-05-12'")
    
    agg_df = df.groupBy('emp_id')\
                .agg(max('salary').alias("new_salary"))
                
    df = df.join(agg_df, df.emp_id==agg_df.emp_id, 'inner')\
            .drop(agg_df.emp_id)
    
    print("Num partitions at this point: ", df.rdd.getNumPartitions())
    
    #df.printSchema()
    
    print("Current time: "+str(datetime.now()))
    
    # call custom_repartitioner function by passing arguments such as: dataframe, max number of records per file, partition columns
    
    df = custom_repartitioner(df, 5000000, 'load_date,dept_id')
        
    print("Current time: "+str(datetime.now()))
    
    #df.printSchema()
    
    df.write.mode("overwrite").partitionBy('load_date', 'dept_id').saveAsTable("test_db.test_table_target", overwrite=True)
    
    print("Current time: "+str(datetime.now()))
    
    print("Data load is completed...")
