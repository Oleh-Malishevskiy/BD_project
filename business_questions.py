from pyspark.sql.functions import col
from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import desc


def calculate_top_drivers(taxi_data, n=5):
    window_spec = Window.partitionBy("hack_license").orderBy(f.desc("trip_distance"))
    taxi_data_with_total_distance = taxi_data.withColumn("total_trip_distance", f.sum("trip_distance").over(window_spec))
    top_n_drivers = (
        taxi_data_with_total_distance
        .withColumn("row_num", f.row_number().over(window_spec))
        .filter(f.col("row_num") <= n)
        .select("hack_license", "total_trip_distance", "trip_distance")
    )
    return top_n_drivers
    

def calculate_trip_distance_distribution(taxi_data):
    distance_distribution = taxi_data.groupBy("trip_distance").count()
    return distance_distribution


def find_longest_streaks(taxi_data, n=5, distance_threshold=10):
    window_spec = Window.partitionBy("hack_license").orderBy("pickup_datetime")
    exceeding_threshold = (f.col("trip_distance") > distance_threshold).cast("int")
    consecutive_streak = f.sum(exceeding_threshold).over(window_spec)
    filtered_data = taxi_data.filter(exceeding_threshold == 1)
    top_n_longest_streaks = (
        filtered_data
        .withColumn("consecutive_streak", consecutive_streak)
        .groupBy("hack_license", "consecutive_streak")
        .agg(f.sum("trip_distance").alias("total_trip_distance"))
        .orderBy(f.desc("total_trip_distance"))
        .limit(n)
    )
    return top_n_longest_streaks

def analyze_passenger_influence(data_path):
  
    passenger_analysis = data_path.groupBy("passenger_count").agg(
        f.avg("trip_time_in_secs").alias("avg_trip_duration"),
        f.avg("trip_distance").alias("avg_trip_distance")
    )
    return passenger_analysis

def analyze_trip_distribution(taxi_data):
    taxi_data = taxi_data.withColumn("pickup_datetime", f.to_timestamp("pickup_datetime"))
    taxi_data = taxi_data.withColumn("hour_of_day", f.hour("pickup_datetime"))
    time_intervals = {
        "Morning": [0, 6],
        "Afternoon": [6, 12],
        "Evening": [12, 18],
        "Night": [18, 24]
    }
    taxi_data = taxi_data.withColumn("time_interval", f.when(
        (f.col("hour_of_day") >= time_intervals["Morning"][0]) & (f.col("hour_of_day") < time_intervals["Morning"][1]), "Morning"
    ).when(
        (f.col("hour_of_day") >= time_intervals["Afternoon"][0]) & (f.col("hour_of_day") < time_intervals["Afternoon"][1]), "Afternoon"
    ).when(
        (f.col("hour_of_day") >= time_intervals["Evening"][0]) & (f.col("hour_of_day") < time_intervals["Evening"][1]), "Evening"
    ).otherwise("Night"))

    trip_distribution = taxi_data.groupBy("time_interval").count()

    return trip_distribution

def tips_analyze(fare_data):
    fare_data = fare_data.select([f.col(column).alias(column.replace(' ', '')) for column in fare_data.columns])
    filtered_data = fare_data.filter((f.col("payment_type").isin("CSH")) & (f.col("tip_amount").isNotNull()))
    driver_tip_total = (
        filtered_data
        .groupBy("medallion")
        .agg(
            f.sum("tip_amount").alias("total_tip_amount"),
            f.count("tip_amount").alias("tip_count")
        )
    )
    top_drivers = (
        driver_tip_total
        .orderBy(f.desc("total_tip_amount"))
        .limit(10)  
    )
    return top_drivers