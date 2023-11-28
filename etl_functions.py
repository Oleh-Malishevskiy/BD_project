from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import Window
from pyspark.sql import functions as f
import pyspark.sql.types as t



def create_spark_session(app_name="NYC_Taxi_ETL"):
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_taxi_data(spark, data_path):
    return spark.read.option("header", "true").csv(data_path, inferSchema=True)

def describe_data(taxi_data):
    taxi_data.show()
    taxi_data.describe().show()
    taxi_data.printSchema()
    column_count = len(taxi_data.columns)
    print("Column count:", column_count)
    row_count = taxi_data.count()
    print("Row count:", row_count)
    return

def extract_transform_load(taxi_data):
   
    selected_columns = ["trip_distance"]
    transformed_data = taxi_data.select(*selected_columns)

    return transformed_data

def write_result_to_csv(result_df, csv_path):
    result_df.write.csv(csv_path, header=True, mode="overwrite")

def preprocess_data(taxi_data):
    taxi_data = taxi_data.withColumn("trip_distance", calculate_trip_distance(
        f.col("pickup_longitude"), f.col("pickup_latitude"),
        f.col("dropoff_longitude"), f.col("dropoff_latitude")
    ))
    return taxi_data

def calculate_trip_distance(pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude):
    return f.sqrt((dropoff_longitude - pickup_longitude)**2 + (dropoff_latitude - pickup_latitude)**2)

