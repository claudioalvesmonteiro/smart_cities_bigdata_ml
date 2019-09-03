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
                    inferSchema=True)

# visualize count
def groupAndSelect(sparkdf, column, rep=1):
    ''' count and sort
        calculate proportion
        remove column
    '''
    gdf = sparkdf.select(column).groupby(column).count().alias('count').sort('count', ascending=False)
    gdf = gdf.crossJoin( 
            gdf.select(SF.sum('count').alias("sum_count"))
        ).withColumn("proportion", (SF.col("count") / SF.col("sum_count"))*100 )
    gdf = gdf.drop('sum_count')
    return gdf

# function to generate categories above X% of representation
def catRepresent(df, column, new_column, rep=1):
    ''' group by column and evaluate categories > 1% of representation
        drop unwanted columns and join with dataframe
        create column where representation > 1%    
    '''
    t1 = groupAndSelect(df, column)
    t1 = t1.withColumn('SIG', SF.when(SF.col("proportion")>rep, SF.lit("1")).otherwise(SF.lit('0')))
    t1 = t1.drop('count','proportion')
    df = df.join(t1, [column], 'left')
    df = df.withColumn(new_column, SF.when(SF.col("SIG")==1, SF.col(column)).otherwise(SF.lit('OUTROS')))
    df = df.drop('SIG')
    return df

#=================================#
# BUILD FEATURES
#================================#

#-------- DATE

# import functions
import pyspark.sql.functions as SF
from pyspark.ml.feature import StringIndexer

# create column timestamp
df = df.withColumn('created_date', SF.to_timestamp('Created Date', 'MM/dd/yyyy hh:mm:ss a'))
df = df.withColumn('closed_date', SF.to_timestamp('Closed Date', 'MM/dd/yyyy hh:mm:ss a'))

# extract year, month and day from created_date
df = df.withColumn('created_year', SF.year('created_date'))
df = df.withColumn('created_month', SF.month('created_date'))
df = df.withColumn('created_day', SF.dayofweek('created_date'))

# calculate time of duration
timeDiff = (SF.unix_timestamp('Closed Date', "MM/dd/yyyy hh:mm:ss a") -SF. unix_timestamp('Created Date', "MM/dd/yyyy hh:mm:ss a"))
df = df.withColumn("Duration", timeDiff)

#----- AGENCY
df = df.withColumn('agency_HPD', SF.when(df.Agency == 'HPD', 1).otherwise(0))
df = df.withColumn('agency_NYPD', SF.when(df.Agency == 'NYPD', 1).otherwise(0))
df = df.withColumn('agency_DOT', SF.when(df.Agency == 'DOT', 1).otherwise(0))
df = df.withColumn('agency_DSNY', SF.when(df.Agency == 'DSNY', 1).otherwise(0))

#----- COMPLAINT TYPE

# apply
df = catRepresent(df, 'Complaint Type', 'complaint_type')

# transform in index
indexer = StringIndexer(inputCol='complaint_type', outputCol='complaint_type_index')
df = indexer.fit(df).transform(df)

#--- DESCRIPTOR 

# apply
df = catRepresent(df, 'Descriptor', 'descriptor')

# transform in index
indexer = StringIndexer(inputCol='descriptor', outputCol='descriptor_index')
df = indexer.fit(df).transform(df)

#--- LOCATION TYPE 

# apply
df = catRepresent(df, 'Location Type', 'location_type', 5)

# transform in index
indexer = StringIndexer(inputCol='location_type', outputCol='location_type_index')
df = indexer.fit(df).transform(df)

#--- INDICIDENT ZIP

# apply
df = catRepresent(df, 'Incident Zip', 'incident_zip')

# transform in index
indexer = StringIndexer(inputCol='incident_zip', outputCol='incident_zip_index')
df = indexer.fit(df).transform(df)

#--- CROSS STREET
# cross street not null
df = df.withColumn('cross_street_not_null', SF.when(df['Cross Street 1'].isNotNull(), 1).otherwise(0))

#--- INTERSECTION STREET
# intersection street not null
df = df.withColumn('intersection_street_not_null', SF.when(df['Intersection Street 1'].isNotNull(), 1).otherwise(0))

df.show(10)

#--- ADRESS TYPE

# apply
df = catRepresent(df, 'Address Type', 'adress_type', 5)

# transform in index
indexer = StringIndexer(inputCol=['adress_type'], outputCol=['adress_type_index'])
df = indexer.fit(df).transform(df)

#===============================#
# BUILD AND SAVE PIPELINE
#==============================#

groupAndSelect(df, 'City').show(100)

