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
df = spark.read.csv('/home/pacha/Documents/DataScience/Bases de Dados/NY Open Data/311_Service_Requests_from_2010_to_Present.csv' ,
                    header=True, 
                    inferSchema=False)


#===================================#
# EXPLORATION
#===================================#

# visualize data schema
df.printSchema()

# *open notepad and paste
df.show(4)                      
df.show(4, truncate=False)

#


#===================================#
# DATA PRE-PROCESSING
#===================================#

##### DATE

# view column
df.select('Created Date', 'Closed Date').show(5, truncate=False)

### transform in timestamp and date

# import functions
from pyspark.sql.functions import to_timestamp, year, month, dayofweek

# create timestamp
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


# take sample
t0 = time()
sdf = spark.createDataFrame(df.rdd.takeSample(False, 50000, seed=0))
tt = time() - t0

print('Task 2 performed in {} seconds'.format(tt)) # measure time to take sample

# export sample
pdf = sdf.toPandas()
pdf.to_csv('service_requests_ny.csv')

