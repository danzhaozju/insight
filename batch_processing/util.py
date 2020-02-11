import pyspark
import pandas as pd
from pyspark.sql import SparkSession


def create_spark_session(app_name):
    """
    Create a new spark session with the APP_NAME.
    """
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

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

def create_df_from_parquet_paths(spark, paths):
    """
    Create a dataframe from parquet format files
    """
    df = spark.read.parquet(paths)
    return df

def generate_paths(head, tail, time1, time2, output_format):
    """
    Generate path monthly with output_format between time1 and time2
    """
    months = pd.date_range(time1,time2, 
              freq='MS').strftime(output_format).tolist()
    paths = [head + month + tail for month in months]
    return paths