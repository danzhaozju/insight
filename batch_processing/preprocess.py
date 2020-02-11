import findspark
import pyspark
from pyspark.sql.functions import *
from util import create_spark_session, create_df_from_csv_paths, generate_paths
from schema import BIKE_SCHEMA, YELLOW_TAXI_SCHEMA_201308_201412, YELLOW_TAXI_SCHEMA_201501_201606, YELLOW_TAXI_SCHEMA_201607_201906, GREEN_TAXI_SCHEMA_201308_201412, GREEN_TAXI_SCHEMA_201501_201606, GREEN_TAXI_SCHEMA_201607_201812, GREEN_TAXI_SCHEMA_201901_201906

def preprocess_bike(spark):
	"""
	The funcion reads in all the .csv files of New York City citibike as dataframes with 
	the same schema, select the needed fields and save it back to s3 in Parquet format.
	"""
	head = 's3a://citi-bike-trip-data/'
	tail = '-citibike-tripdata.csv'

	bike_paths = generate_paths(head, tail,'2013-08-01','2019-06-01','%Y%m')
	trips = create_df_from_csv_paths(spark, bike_paths, BIKE_SCHEMA)
	preprocessed_trips = trips.select(['duration','start_time','end_time','start_latitude',
		'start_longitude','end_latitude','end_longitude'])
	target_path = 's3a://citi-bike-trip-data/parquet/preprocessed-citi-bike-trips-201308_201906'
	preprocessed_trips.write.mode("overwrite").parquet(target_path)

def preprocess_yellow_taxi(spark):
	"""
	The funcion reads in all the .csv files of New York City yellow taxi trip records as dataframes with 
	the same schema, select the needed fields and save it back to s3 in Parquet format.
	"""
	head = 's3a://ny-taxi-trip-data/yellow_taxi/yellow_tripdata_'
	tail = '.csv'

	yellow_taxi_paths = generate_paths(head, tail,'2013-08-01','2014-12-01','%Y-%m')
	trips = create_df_from_csv_paths(spark, yellow_taxi_paths, YELLOW_TAXI_SCHEMA_201308_201412)
	preprocessed_trips = trips.select(['start_time','end_time','start_longitude','start_latitude',
		'end_longitude','end_latitude','passenger_count','distance','total_amount'])
	target_path = 's3a://ny-taxi-trip-data/yellow_taxi/parquet/preprocessed-yellow-taxi-201308_201412'
	preprocessed_trips.write.mode("overwrite").parquet(target_path)

	yellow_taxi_paths = generate_paths(head, tail,'2015-01-01','2016-06-01','%Y-%m')
	trips = create_df_from_csv_paths(spark, yellow_taxi_paths, YELLOW_TAXI_SCHEMA_201501_201606)
	preprocessed_trips = trips.select(['start_time','end_time','start_longitude','start_latitude',
		'end_longitude','end_latitude','passenger_count','distance','total_amount'])
	target_path = 's3a://ny-taxi-trip-data/yellow_taxi/parquet/preprocessed-yellow-taxi-201501_201606'
	preprocessed_trips.write.mode("overwrite").parquet(target_path)

	yellow_taxi_paths = generate_paths(head, tail,'2016-07-01','2019-06-01','%Y-%m')
	trips = create_df_from_csv_paths(spark, yellow_taxi_paths, YELLOW_TAXI_SCHEMA_201607_201906)
	preprocessed_trips = trips.select(['start_time','end_time','start_locationID','end_locationID',
		'passenger_count','distance','total_amount'])
	preprocessed_trips.createOrReplaceTempView("taxi")
	taxi_lat_lon = spark.sql("SELECT start_time,end_time,P.longitude AS start_longitude,P.latitude AS start_latitude,D.longitude AS end_longitude,D.latitude AS end_latitude,passenger_count,distance,total_amount\
		FROM taxi,loc_id AS P,loc_id AS D\
		WHERE start_locationID = P.location_i\
		AND end_locationID = D.location_i")
	target_path = 's3a://ny-taxi-trip-data/yellow_taxi/parquet/preprocessed-yellow-taxi-201607_201906'
	taxi_lat_lon.write.mode("overwrite").parquet(target_path)

def preprocess_green_taxi(spark):
	"""
	The funcion reads in all the .csv files of New York City green taxi trip records as dataframes with 
	the same schema, select the needed fields and save it back to s3 in Parquet format.
	"""

	head = 's3a://ny-taxi-trip-data/green_taxi/green_tripdata_'
	tail = '.csv'

	green_taxi_paths = generate_paths(head, tail,'2013-08-01','2014-12-01','%Y-%m')
	trips = create_df_from_csv_paths(spark, green_taxi_paths, GREEN_TAXI_SCHEMA_201308_201412)
	preprocessed_trips = trips.select(['start_time','end_time','start_longitude','start_latitude',
		'end_longitude','end_latitude','passenger_count','distance','total_amount'])
	target_path = 's3a://ny-taxi-trip-data/green_taxi/parquet/preprocessed-green-taxi-201308_201412'
	preprocessed_trips.write.mode("overwrite").parquet(target_path)

	green_taxi_paths = generate_paths(head, tail,'2015-01-01','2016-06-01','%Y-%m')
	trips = create_df_from_csv_paths(spark, green_taxi_paths, GREEN_TAXI_SCHEMA_201501_201606)
	preprocessed_trips = trips.select(['start_time','end_time','start_longitude','start_latitude',
		'end_longitude','end_latitude','passenger_count','distance','total_amount'])
	target_path = 's3a://ny-taxi-trip-data/green_taxi/parquet/preprocessed-green-taxi-201501_201606'
	preprocessed_trips.write.mode("overwrite").parquet(target_path)

	green_taxi_paths = generate_paths(head, tail,'2016-07-01','2018-12-01','%Y-%m')
	trips = create_df_from_csv_paths(spark, green_taxi_paths, GREEN_TAXI_SCHEMA_201607_201812)
	preprocessed_trips = trips.select(['start_time','end_time','start_locationID','end_locationID',
		'passenger_count','distance','total_amount'])
	preprocessed_trips.createOrReplaceTempView("taxi")
	taxi_lat_lon = spark.sql("SELECT start_time,end_time,P.longitude AS start_longitude,P.latitude AS start_latitude,D.longitude AS end_longitude,D.latitude AS end_latitude,passenger_count,distance,total_amount\
		FROM taxi,loc_id AS P,loc_id AS D\
		WHERE start_locationID = P.location_i\
		AND end_locationID = D.location_i")
	target_path = 's3a://ny-taxi-trip-data/green_taxi/parquet/preprocessed-green-taxi-201607_201812'
	taxi_lat_lon.write.mode("overwrite").parquet(target_path)

	green_taxi_paths = generate_paths(head, tail,'2019-01-01','2019-06-01','%Y-%m')
	trips = create_df_from_csv_paths(spark, green_taxi_paths, GREEN_TAXI_SCHEMA_201901_201906)
	preprocessed_trips = trips.select(['start_time','end_time','start_locationID','end_locationID',
		'passenger_count','distance','total_amount'])
	preprocessed_trips.createOrReplaceTempView("taxi")
	taxi_lat_lon = spark.sql("SELECT start_time,end_time,P.longitude AS start_longitude,P.latitude AS start_latitude,D.longitude AS end_longitude,D.latitude AS end_latitude,passenger_count,distance,total_amount\
		FROM taxi,loc_id AS P,loc_id AS D\
		WHERE start_locationID = P.location_i\
		AND end_locationID = D.location_i")
	target_path = 's3a://ny-taxi-trip-data/green_taxi/parquet/preprocessed-green-taxi-201901_201906'
	taxi_lat_lon.write.mode("overwrite").parquet(target_path)


if __name__ == '__main__':
	findspark.init("/usr/local/spark")
	spark = create_spark_session('preprocess_taxi_bike_trips_data')

	taxi_locID_path = 's3a://ny-taxi-trip-data/taxi_locID_lon_lat.csv'
	taxi_locIDs = create_df_from_csv_paths(spark, taxi_locID_path)
	taxi_locIDs.createOrReplaceTempView("loc_id")

	preprocess_bike(spark)
	preprocess_yellow_taxi(spark)
	preprocess_green_taxi(spark)





