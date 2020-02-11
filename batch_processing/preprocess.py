import findspark
import pyspark
from pyspark.sql.functions import *
from util import create_spark_session, create_df_from_csv_paths
from schema import BIKE_SCHEMA, YELLOW_TAXI_SCHEMA_201308_201412, YELLOW_TAXI_SCHEMA_201501_201606, YELLOW_TAXI_SCHEMA_201607_201906, GREEN_TAXI_SCHEMA_201308_201412, GREEN_TAXI_SCHEMA_201501_201606, GREEN_TAXI_SCHEMA_201607_201812, GREEN_TAXI_SCHEMA_201901_201906

findspark.init("/usr/local/spark")

def preprocess_bike(spark):
	"""
	The funcion reads in all the .csv files of New York City citibike as dataframes with 
	the same schema, select the needed fields and save it back to s3 in Parquet format.
	"""
	bike_paths = 's3a://citi-bike-trip-data/*-citibike-tripdata.csv'
	trips = create_df_from_csv_paths(spark, bike_paths, BIKE_SCHEMA)
	preprocessed_trips = trips.select(['duration','start_time','end_time','start_latitude','start_longitude','end_latitude','end_longitude'])
	target_path = 's3a://citi-bike-trip-data/parquet/preprocessed-citi-bike-trips'
	preprocessed_trips.write.parquet(target_path)

def preprocess_yellow_taxi(spark):
	head = 's3a://ny-taxi-trip-data/yellow_taxi/yellow_taxi/yellow_tripdata_'
	tail = '.csv'
	yellow_taxi_paths = generate_paths(head,tail,'2013-08-01','2014-12-01','%Y-%m')
	trips = create_df_from_csv_paths(spark, yellow_taxi_paths, YELLOW_TAXI_SCHEMA_201308_201412)
	trips.show(1)

if __name__ == '__main__':
	spark = create_spark_session('preprocess_trips_data')
	#preprocess_bike(spark)
	preprocess_yellow_taxi(spark)