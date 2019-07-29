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
spark = SparkSession.builder.appName('random_forest_regression').getOrCreate()

# import data
df = spark.read.csv('t/311_Service_Requests_from_2010_to_Present.csv' ,header=True, inferSchema=False)

df.printSchema()

# take sample
t0 = time()
sdf = spark.createDataFrame(df.rdd.takeSample(False, 50000, seed=0))
tt = time() - t0

# export sample
pdf = sdf.toPandas()
pdf.to_csv('service_requests_ny.csv')

print('Task 2 performed in {} seconds'.format(tt))
