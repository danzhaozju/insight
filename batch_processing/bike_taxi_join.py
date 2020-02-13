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
	trips = add_geohash(trips, precision)

	trips.createOrReplaceTempView('trips')
	trips_p = spark.sql("SELECT start_geohash, end_geohash, year, month, COUNT(*) AS green_count, \
		AVG(passenger_count) AS green_avg_passengers,AVG(distance) AS green_avg_distance, \
		AVG(total_amount) AS green_avg_cost, AVG(duration) AS green_avg_duration\
		FROM trips\
		GROUP BY start_geohash, end_geohash, year, month\
		ORDER BY green_count DESC")
	trips_p.show()
	print(trips_p.count())

	trips_p.createOrReplaceTempView("trips_p")

	# trips_from_station = spark.sql("SELECT S.station_name, T.*\
	# 	FROM trips AS T, stations AS S\
	# 	WHERE T.start_geohash = S.geohash")
	# trips_from_station.show(2)
	# print(trips_from_station.count())

def add_geohash(df, precision=6):
    df = df.withColumn("start_geohash", geo_encoding(col('start_latitude'), col('start_longitude')))\
                .withColumn("end_geohash",geo_encoding(col('end_latitude'),col('end_longitude')))
    return df


if __name__ == '__main__':
	findspark.init("/usr/local/spark")
	spark = create_spark_session('join_taxi_bike')

	precision = 6
	geo_encoding = udf(lambda lat,lon: geohash2.encode(lat,lon,precision))
	# spark.udf.register("geo_encoding", )
	# spark.udf.register("geo_lat", lambda geo_string: geohash2.decode(geo_string)[0])
	# spark.udf.register("geo_lon", lambda geo_string: geohash2.decode(geo_string)[1])

	subway_station_path = 's3a://ny-taxi-trip-data/NY_subway_station_loc.csv'
	stations = create_df_from_csv_paths(spark, subway_station_path)
	stations = stations.withColumn("geohash", geo_encoding(col('latitude'), col('longitude')))
	stations.createOrReplaceTempView("stations")
	stations.show(2)
	print(stations.count())

	# process_bike(spark)
	# process_yellow_taxi(spark)
	process_green_taxi(spark)









