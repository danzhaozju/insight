import findspark
import pyspark
from pyspark.sql.functions import *
from util import create_spark_session

if __name__ == '__main__':
	findspark.init("/usr/local/spark")
	spark = create_spark_session('join_taxi_bike')

	path = 's3a://citi-bike-trip-data/parquet/preprocessed-citi-bike-trips-201308_201906'
	bike_trips = spark.read.parquet(path)
	bike_trips.show()

	bike_trips = split_start_time(bike_trips)
	bike_trips.show()

