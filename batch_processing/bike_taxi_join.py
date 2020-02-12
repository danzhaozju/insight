import findspark
import pyspark
import geohash2
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
	trips = spark.read.parquet(path)
	trips = split_start_time(trips)
	trips = add_duration(trips)
	trips = trips.withColumn("start_geohash", geo_encoding(col('start_latitude'), col('start_longitude')))
	trips.show()
	# trips.createTempView(trips)
	# trips = spark.sql("SELECT *\
	# 	FROM trips\
	# 	")


	taxi_trips.show()


if __name__ == '__main__':
	findspark.init("/usr/local/spark")
	spark = create_spark_session('join_taxi_bike')

	spark.udf.register("geo_encoding", lambda lat,lon: geohash2.encode(lat,lon,6))
	spark.udf.register("geo_lat", lambda geo_string: geohash2.decode(geo_string)[0])
	spark.udf.register("geo_lon", lambda geo_string: geohash2.decode(geo_string)[1])

	subway_station_path = 's3a://ny-taxi-trip-data/NY_subway_station_loc.csv'
	stations = create_df_from_csv_paths(spark, subway_station_path)
	stations.createOrReplaceTempView("stations")

	# process_bike(spark)
	# process_yellow_taxi(spark)
	process_green_taxi(spark)









