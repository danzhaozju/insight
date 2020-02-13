import findspark
import pyspark
import geohash2
from pyspark.sql.functions import *
from util import *

def process_bike(spark):
	path = 's3a://citi-bike-trip-data/parquet/*'
	trips = spark.read.parquet(path)
	trips.show(1)
	# trips = split_start_time(trips)
	# trips = add_geohash(trips)

	# trips.createOrReplaceTempView('trips')
	# trips_p = spark.sql("SELECT start_geohash, end_geohash, year, month,\
	# 		COUNT(*) AS count, AVG(duration) AS avg_duration\
	# 	FROM trips\
	# 	GROUP BY start_geohash, end_geohash, year, month\
	# 	ORDER BY count DESC")
	# print("trips_p:")
	# trips_p.show(1)
	# print(trips_p.count())

	# trips_p.createOrReplaceTempView("trips_p")
	# bike_from_station = spark.sql("SELECT S.station_name AS start_station, S.latitude, S.longitude, T.*\
	# 	FROM trips_p AS T, stations AS S\
	# 	WHERE T.start_geohash = S.geohash")
	# bike_from_station.show()
	# print(bike_from_station.count())

	# return bike_from_station

def process_yellow_taxi(spark):
	path = 's3a://ny-taxi-trip-data/yellow_taxi/parquet/*'
	yellow_taxi_from_station = process_taxi(spark, path)
	return yellow_taxi_from_station

def process_green_taxi(spark):
	path = 's3a://ny-taxi-trip-data/green_taxi/parquet/*'
	green_taxi_from_station = process_taxi(spark, path)
	return green_taxi_from_station
	# precision=6 3237174records

def process_taxi(spark, path):
	trips = spark.read.parquet(path)
	trips = split_start_time(trips)
	trips = add_duration(trips)
	trips = add_geohash(trips)

	trips.createOrReplaceTempView('trips')
	trips_p = spark.sql("SELECT start_geohash, end_geohash, year, month, COUNT(*) AS count,\
			AVG(passenger_count) AS avg_passengers,AVG(distance) AS avg_distance,\
			AVG(total_amount) AS avg_cost, AVG(duration) AS avg_duration\
		FROM trips\
		GROUP BY start_geohash, end_geohash, year, month\
		ORDER BY count DESC")
	trips_p.createOrReplaceTempView("trips_p")
	trips_from_station = spark.sql("SELECT S.station_name AS start_station, S.latitude, S.longitude, T.*\
		FROM trips_p AS T, stations AS S\
		WHERE T.start_geohash = S.geohash")
	trips_from_station.show()
	print(trips_from_station.count())

	return trips_from_station


def add_geohash(df):
    df = df.withColumn("start_geohash", block_geo_encoding(col('start_latitude'), col('start_longitude')))\
                .withColumn("end_geohash",block_geo_encoding(col('end_latitude'), col('end_longitude')))
    return df

if __name__ == '__main__':
	findspark.init("/usr/local/spark")
	spark = create_spark_session('join_taxi_bike')

	station_precision = 6
	station_geo_encoding = udf(lambda lat, lon: geohash2.encode(lat,lon,station_precision))
	block_precision = 6
	block_geo_encoding = udf(lambda lat, lon: geohash2.encode(lat,lon,block_precision))

	# spark.udf.register("geo_lat", lambda geo_string: geohash2.decode(geo_string)[0])
	# spark.udf.register("geo_lon", lambda geo_string: geohash2.decode(geo_string)[1])

	subway_station_path = 's3a://ny-taxi-trip-data/NY_subway_station_loc.csv'
	stations = create_df_from_csv_paths(spark, subway_station_path)
	stations = stations.withColumn("geohash", station_geo_encoding(col('latitude'), col('longitude')))
	stations.createOrReplaceTempView("stations")

	bike = process_bike(spark)
	# yellow = process_yellow_taxi(spark)
	# green = process_green_taxi(spark)









