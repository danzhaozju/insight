import pyspark
from pyspark.sql import SparkSession

def create_spark_session(app_name):
	# Create a new spark session with the APP_NAME.
	spark = SparkSession.builder.appName(app_name).getOrCreate()
	return spark

def create_df_from_csv_paths(spark, paths, schema = None):
	# Create a dataframe from csv paths. 
	if schema:
		df = spark.read.format("csv").option("header", "true").\
        	schema(schema).\
        	load(paths.split(','))
    else:
    	df = spark.read.format("csv").option("header", "true").\
        	option('inferschema','true').\
        	load(paths.split(','))
    return df