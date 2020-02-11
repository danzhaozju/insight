from pyspark.sql.types import *

"""
Define the schema for .csv files of New York City citibike and taxi data
"""
BIKE_SCHEMA = StructType([
	StructField('duration', IntegerType(), True),
	StructField('start_time', TimestampType(), True),
	StructField('end_time', TimestampType(), True),
	StructField('start_stationID', IntegerType(), True),
	StructField('start_station_name', StringType(), True),
	StructField('start_latitude', DoubleType(), True),
	StructField('start_longitude', DoubleType(), True),
	StructField('end_stationID', IntegerType(), True),
	StructField('end_station_name', StringType(), True),
	StructField('end_latitude', DoubleType(), True),
	StructField('end_longitude', DoubleType(), True),
	StructField('bikeID', IntegerType(), True),
	StructField('user_type', StringType(), True),
	StructField('birth_year', StringType(), True),
	StructField('gender', IntegerType(), True)
])

YELLOW_TAXI_SCHEMA_201308_201412 = StructType([
	StructField('vendorID', StringType(), True),
	StructField('start_time', TimestampType(), True),
	StructField('end_time', TimestampType(), True),
	StructField('passenger_count', IntegerType(), True),
	StructField('distance', DoubleType(), True),
	StructField('start_longitude', DoubleType(), True),
	StructField('start_latitude', DoubleType(), True),
	StructField('ratecodeID', IntegerType(), True),
	StructField('store_and_fwd_flag', StringType(), True),
	StructField('end_longitude', DoubleType(), True),
	StructField('end_latitude', DoubleType(), True),
	StructField('payment_type', StringType(), True),
	StructField('fare_amount', DoubleType(), True),
	StructField('extra', DoubleType(), True),
	StructField('mta_tax', DoubleType(), True),
	StructField('tip_amount', DoubleType(), True),
	StructField('tolls_amount', DoubleType(), True),
	StructField('total_amount', DoubleType(), True)
])

YELLOW_TAXI_SCHEMA_201501_201606 = StructType([
	StructField('vendorID', StringType(), True),
	StructField('start_time', TimestampType(), True),
	StructField('end_time', TimestampType(), True),
	StructField('passenger_count', IntegerType(), True),
	StructField('distance', DoubleType(), True),
	StructField('start_longitude', DoubleType(), True),
	StructField('start_latitude', DoubleType(), True),
	StructField('ratecodeID', IntegerType(), True),
	StructField('store_and_fwd_flag', StringType(), True),
	StructField('end_longitude', DoubleType(), True),
	StructField('end_latitude', DoubleType(), True),
	StructField('payment_type', StringType(), True),
	StructField('fare_amount', DoubleType(), True),
	StructField('extra', DoubleType(), True),
	StructField('mta_tax', DoubleType(), True),
	StructField('tip_amount', DoubleType(), True),
	StructField('tolls_amount', DoubleType(), True),
	StructField('improvement_surcharge', DoubleType(), True),
	StructField('total_amount', DoubleType(), True)
])

YELLOW_TAXI_SCHEMA_201607_201906 = StructType([
	StructField('vendorID', StringType(), True),
	StructField('start_time', TimestampType(), True),
	StructField('end_time', TimestampType(), True),
	StructField('passenger_count', IntegerType(), True),
	StructField('distance', DoubleType(), True),
	StructField('ratecodeID', IntegerType(), True),
	StructField('store_and_fwd_flag', StringType(), True),
	StructField('start_locationID', IntegerType(), True),
	StructField('end_locationID', IntegerType(), True),
	StructField('payment_type', StringType(), True),
	StructField('fare_amount', DoubleType(), True),
	StructField('extra', DoubleType(), True),
	StructField('mta_tax', DoubleType(), True),
	StructField('tip_amount', DoubleType(), True),
	StructField('tolls_amount', DoubleType(), True),
	StructField('improvement_surcharge', DoubleType(), True),
	StructField('total_amount', DoubleType(), True)
])

GREEN_TAXI_SCHEMA_201308_201412 = StructType([
	StructField('vendorID', StringType(), True),
	StructField('start_time', TimestampType(), True),
	StructField('end_time', TimestampType(), True),
	StructField('store_and_fwd_flag', StringType(), True),
	StructField('ratecodeID', IntegerType(), True),
	StructField('start_longitude', DoubleType(), True),
	StructField('start_latitude', DoubleType(), True),
	StructField('end_longitude', DoubleType(), True),
	StructField('end_latitude', DoubleType(), True),
	StructField('passenger_count', IntegerType(), True),
	StructField('distance', DoubleType(), True),
	StructField('fare_amount', DoubleType(), True),
	StructField('extra', DoubleType(), True),
	StructField('mta_tax', DoubleType(), True),
	StructField('tip_amount', DoubleType(), True),
	StructField('tolls_amount', DoubleType(), True),
	StructField('ehail_fee', DoubleType(), True),
	StructField('total_amount', DoubleType(), True),
	StructField('payment_type', StringType(), True),
	StructField('trip_type', IntegerType(), True)
])

GREEN_TAXI_SCHEMA_201501_201606 = StructType([
	StructField('vendorID', StringType(), True),
	StructField('start_time', TimestampType(), True),
	StructField('end_time', TimestampType(), True),
	StructField('store_and_fwd_flag', StringType(), True),
	StructField('ratecodeID', IntegerType(), True),
	StructField('start_longitude', DoubleType(), True),
	StructField('start_latitude', DoubleType(), True),
	StructField('end_longitude', DoubleType(), True),
	StructField('end_latitude', DoubleType(), True),
	StructField('passenger_count', IntegerType(), True),
	StructField('distance', DoubleType(), True),
	StructField('fare_amount', DoubleType(), True),
	StructField('extra', DoubleType(), True),
	StructField('mta_tax', DoubleType(), True),
	StructField('tip_amount', DoubleType(), True),
	StructField('tolls_amount', DoubleType(), True),
	StructField('ehail_fee', DoubleType(), True),
	StructField('improvement_surcharge', DoubleType(), True),
	StructField('total_amount', DoubleType(), True),
	StructField('payment_type', StringType(), True),
	StructField('trip_type', IntegerType(), True)
])

GREEN_TAXI_SCHEMA_201607_201812 = StructType([
	StructField('vendorID', StringType(), True),
	StructField('start_time', TimestampType(), True),
	StructField('end_time', TimestampType(), True),
	StructField('store_and_fwd_flag', StringType(), True),
	StructField('ratecodeID', IntegerType(), True),
	StructField('start_locationID', IntegerType(), True),
	StructField('end_locationID', IntegerType(), True),
	StructField('passenger_count', IntegerType(), True),
	StructField('distance', DoubleType(), True),
	StructField('fare_amount', DoubleType(), True),
	StructField('extra', DoubleType(), True),
	StructField('mta_tax', DoubleType(), True),
	StructField('tip_amount', DoubleType(), True),
	StructField('tolls_amount', DoubleType(), True),
	StructField('ehail_fee', DoubleType(), True),
	StructField('improvement_surcharge', DoubleType(), True),
	StructField('total_amount', DoubleType(), True),
	StructField('payment_type', StringType(), True),
	StructField('trip_type', IntegerType(), True)
])

GREEN_TAXI_SCHEMA_201901_201906 = StructType([
	StructField('vendorID', StringType(), True),
	StructField('start_time', TimestampType(), True),
	StructField('end_time', TimestampType(), True),
	StructField('store_and_fwd_flag', StringType(), True),
	StructField('ratecodeID', IntegerType(), True),
	StructField('start_locationID', IntegerType(), True),
	StructField('end_locationID', IntegerType(), True),
	StructField('passenger_count', IntegerType(), True),
	StructField('distance', DoubleType(), True),
	StructField('fare_amount', DoubleType(), True),
	StructField('extra', DoubleType(), True),
	StructField('mta_tax', DoubleType(), True),
	StructField('tip_amount', DoubleType(), True),
	StructField('tolls_amount', DoubleType(), True),
	StructField('ehail_fee', DoubleType(), True),
	StructField('improvement_surcharge', DoubleType(), True),
	StructField('total_amount', DoubleType(), True),
	StructField('payment_type', StringType(), True),
	StructField('trip_type', IntegerType(), True),
	StructField('congestion_surcharge', DoubleType(), True)
])











