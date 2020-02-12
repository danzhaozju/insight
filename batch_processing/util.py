import pyspark
import pandas as pd
import geohash2
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def create_spark_session(app_name):
    """
    Create a new spark session with the APP_NAME.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def generate_paths(head, tail, time1, time2, output_format):
    """
    Generate path monthly with output_format between time1 and time2
    """
    months = pd.date_range(time1,time2, 
              freq='MS').strftime(output_format).tolist()
    paths = [head + month + tail for month in months]
    return paths

def create_df_from_csv_paths(spark, paths, schema = None):
    """
    Create a dataframe from csv paths.
    """
    if schema:
        df = spark.read.format("csv")\
            .options(header='true') \
            .options(delimiter=',') \
            .options(quote='"') \
            .options(escape='"') \
            .schema(schema)\
            .load(paths)
    else:
        df = spark.read.format("csv").option("header", "true").\
            option('inferschema','true').\
            load(paths)
    return df

def split_start_time(df):
    df = df.withColumn("year", year("start_time"))\
            .withColumn("month",month("start_time"))\
            .withColumn("day",dayofmonth("start_time"))\
            .withColumn("hour",hour("start_time"))
    return df

def add_duration(df):
    df = df.withColumn("duration", (col('end_time').cast(DoubleType())-col('start_time').cast(DoubleType()))/60)
    return df




