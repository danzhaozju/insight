import findspark
import pyspark
from pyspark.sql.functions import *
from util import create_spark_session

if __name__ == '__main__':
	findspark.init("/usr/local/spark")
	spark = create_spark_session('join_taxi_bike')

	# df = spark.read.parquet()