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
spark = SparkSession.builder.appName('features_builder').getOrCreate()

sc = spark.sparkContext

# using SQLContext to read parquet file
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

# to read parquet file
df = sqlContext.read.parquet('datasets/raw/service_requests_ny.parquet.gzip')

# print schema and head
df.printSchema()
df.select('Latitude').show()

#===================================#
# data pre-processing
#==================================

