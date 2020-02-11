import pyspark
from pyspark.sql import SparkSession

# Create a new spark session with the APP_NAME.
def create_spark_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

# Create a dataframe from csv paths.
def create_df_from_csv_paths(spark, paths, schema = None):
    if schema:
		df = spark.read.format("csv").option("header", "true").\
        	schema(schema).\
        	load(paths.split(','))
    else:
    	df = spark.read.format("csv").option("header", "true").\
        	option('inferschema','true').\
        	load(paths.split(','))
    return df

# Generate path monthly with output_format between time1 and time2
def generate_paths(head, tail, time1, time2, output_format):
    months = pd.date_range(time1,time2, 
              freq='MS').strftime(output_format).tolist()
    paths = [front + month + tail for month in months]
    return paths