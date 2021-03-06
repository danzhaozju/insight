import findspark
import pyspark
import geohash2
import psycopg2
from pyspark.sql.functions import *
from util import *
from config import host, port, dbname, user, password

def process_bike(spark):
	path = 's3a://citi-bike-trip-data/parquet/*'
	trips = spark.read.parquet(path)
	trips = split_start_time(trips)
	trips = add_geohash(trips)

	trips.createOrReplaceTempView('trips')
	trips_p = spark.sql("SELECT start_geohash, end_geohash, year, month,\
			COUNT(*) AS count, AVG(duration)/60 AS avg_duration\
		FROM trips\
		WHERE start_latitude IS NOT NULL\
		GROUP BY start_geohash, end_geohash, year, month")
	# trips_p.show()
	# print(trips_p.count())
	trips_p.createOrReplaceTempView("trips_p")
	bike_from_station = spark.sql("SELECT S.station_name AS start_station, S.latitude, S.longitude, T.*\
		FROM trips_p AS T, stations AS S\
		WHERE T.start_geohash = S.geohash\
		ORDER BY year, month, start_station, count DESC")
	# bike_from_station.show()
	# print(bike_from_station.count())
	return bike_from_station

def process_yellow_taxi(spark):
	path = 's3a://ny-taxi-trip-data/yellow_taxi/parquet/*'
	yellow_taxi_from_station = process_taxi(spark, path)
	return yellow_taxi_from_station

def process_green_taxi(spark):
	path = 's3a://ny-taxi-trip-data/green_taxi/parquet/*'
	green_taxi_from_station = process_taxi(spark, path)
	return green_taxi_from_station

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
		WHERE start_latitude IS NOT NULL\
		GROUP BY start_geohash, end_geohash, year, month")
	trips_p.createOrReplaceTempView("trips_p")
	trips_from_station = spark.sql("SELECT S.station_name AS start_station, S.latitude, S.longitude, T.*\
		FROM trips_p AS T, stations AS S\
		WHERE T.start_geohash = S.geohash\
		ORDER BY year, month, start_station, count DESC")
	# trips_from_station.show()
	# print(trips_from_station.count())
	return trips_from_station


def add_geohash(df):
    df = df.withColumn("start_geohash", block_geo_encoding(col('start_latitude'), col('start_longitude')))\
                .withColumn("end_geohash",block_geo_encoding(col('end_latitude'), col('end_longitude')))
    return df

def add_start_end_points(df_name):
	cur.execute("ALTER TABLE " + df_name + " ADD COLUMN start_point geometry(POINT,4326);")
	cur.execute("UPDATE " + df_name + " SET start_point = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326);")
	cur.execute("ALTER TABLE " + df_name + " ADD COLUMN end_point geometry(POINT,4326);")
	cur.execute("UPDATE " + df_name + " SET end_point = ST_SetSRID(ST_PointFromGeoHash(end_geohash), 4326);")
	cur.execute("ALTER TABLE " + df_name + " ADD COLUMN end_longitude float;")
	cur.execute("UPDATE " + df_name + " SET end_longitude = ST_X(end_point);")
	cur.execute("ALTER TABLE " + df_name + " ADD COLUMN end_latitude float;")
	cur.execute("UPDATE " + df_name + " SET end_latitude = ST_Y(end_point);")
	if df_name == "bike":
		cur.execute("ALTER TABLE " + df_name + " ADD COLUMN avg_distance float;")
		cur.execute("UPDATE " + df_name + " SET avg_distance = ST_Distance(start_point::geography, end_point::geography)/1609.344;")
	conn.commit()

if __name__ == '__main__':
	findspark.init("/usr/local/spark")
	spark = create_spark_session('join_taxi_bike')

	station_precision = 6
	station_geo_encoding = udf(lambda lat, lon: geohash2.encode(lat,lon,station_precision))
	block_precision = 7
	block_geo_encoding = udf(lambda lat, lon: geohash2.encode(lat,lon,block_precision))

	# geo_decoding_lat = udf(lambda geohash_string: geohash2.decode(geohash_string)[0])
	# geo_decoding_lon = udf(lambda geohash_string: geohash2.decode(geohash_string)[1])

	subway_station_path = 's3a://ny-taxi-trip-data/NY_subway_station_loc.csv'
	stations = create_df_from_csv_paths(spark, subway_station_path)
	stations = stations.withColumn("geohash", station_geo_encoding(col('latitude'), col('longitude')))
	stations.createOrReplaceTempView("stations")

	bike = process_bike(spark)
	yellow = process_yellow_taxi(spark)
	green = process_green_taxi(spark)

	mode = "overwrite"
	url = "jdbc:postgresql://10.0.0.11:5432/insight"
	properties = {"user":"dan","password":"zhaodan","driver":"org.postgresql.Driver"}
	bike.write.jdbc(url=url, table = "bike7", mode=mode, properties=properties)
	yellow.write.jdbc(url=url, table = "yellow7", mode=mode, properties=properties)
	green.write.jdbc(url=url, table = "green7", mode=mode, properties=properties)

	# Connect to PostgreSQL database
	conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
	cur = conn.cursor()

	add_start_end_points("bike7");
	add_start_end_points("yellow7");
	add_start_end_points("green7");







