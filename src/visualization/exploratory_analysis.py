'''
Hands-On Smart Cities
PySpark for Big Data and Machine Learning
'''

# paths to spark and python3
import os
from time import time

os.environ['PYSPARK_SUBMIT_ARGS'] = '--executor-memory 1G pyspark-shell'
os.environ["SPARK_HOME"] = "/home/pacha/spark"
os.environ["PYSPARK_PYTHON"]="/usr/bin/python3"

# execute PYSPARK
exec(open('/home/pacha/spark/python/pyspark/shell.py').read())

# inititate session
spark = SparkSession.builder.appName('smart_cities_machine_learning').getOrCreate()

# import data
df = spark.read.csv('datasets/raw/sample_service_requests_ny.csv' ,
                    header=True, 
                    inferSchema=False)

#===================================#
# EXPLORATION
#===================================#

import pyspark.sql.functions as SF

# visualize data schema
df.printSchema()

###--------------- groupby and aggregate

def groupAndSave(sparkdf, column):
    # count and sort
    gdf = sparkdf.select(column).groupby(column).count().alias('count').sort('count', ascending=False)
    # calculate proportion
    gdf = gdf.crossJoin( 
            gdf.select(SF.sum('count').alias("sum_count"))
        ).withColumn("proportion", SF.col("count") / SF.col("sum_count"))
            # remove column
    gdf = gdf.drop('sum_count')
    # transform in pandas df
    gdf = gdf.toPandas()    
    # save data
    gdf.to_csv('src/visualization/tables/'+column+'_count.csv')
    # return pandas
    return gdf

groupAndSave(df, 'Location Type')
#===================================#
# DATA PRE-PROCESSING
#===================================#

##### DATE

# view columns
df.select('Created Date', 'Closed Date').show(5, truncate=False)

### transform in timestamp and date

# import functions
from pyspark.sql.functions import to_timestamp, year, month, dayofweek

# create column timestamp
df = df.withColumn('created_date', to_timestamp('Created Date', 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn('closed_date', to_timestamp('Closed Date', 'MM/dd/yyyy hh:mm:ss a'))

# view created columns
df.select('created_date','created_date2', 'closed_date').show(4)

# extract year, month and day from created_date
df = df.withColumn('created_year', year('created_date'))
df = df.withColumn('created_month', month('created_date'))
df = df.withColumn('created_day', dayofweek('created_date'))

df.select('created_year', 'created_month', 'created_day').show()

# calculate time of duration
df = df.withColumn('time_duration',(( df['closed_date']) -( df['created_date']) ))

unionDF.write.parquet("/tmp/databricks-df-example.parquet")
parquetDF = spark.read.parquet("/tmp/databricks-df-example.parquet")

