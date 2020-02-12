import findspark
import pyspark
from pyspark.sql.functions import *
from util import *

def process_bike(spark):
	path = 's3a://citi-bike-trip-data/parquet/preprocessed-citi-bike-trips-201308_201906'
	bike_trips = spark.read.parquet(path)
	bike_trips = split_start_time(bike_trips)
	bike_trips.show()

def process_yellow_taxi(spark):
	path = 's3a://ny-taxi-trip-data/yellow_taxi/parquet/*'
	taxi_trips = spark.read.parquet(path)
	taxi_trips = split_start_time(taxi_trips)
	taxi_trips.show()

def process_green_taxi(spark):
	path = 's3a://ny-taxi-trip-data/green_taxi/parquet/*'
	taxi_trips = spark.read.parquet(path)
	taxi_trips = split_start_time(taxi_trips)
	taxi_trips = add_duration(taxi_trips)
	taxi_trips.show()


if __name__ == '__main__':
	findspark.init("/usr/local/spark")
	spark = create_spark_session('join_taxi_bike')

	# process_bike(spark)
	# process_yellow_taxi(spark)
	process_green_taxi(spark)