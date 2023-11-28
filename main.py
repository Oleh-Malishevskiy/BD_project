from etl_functions import create_spark_session, describe_data, load_taxi_data, extract_transform_load, preprocess_data,write_result_to_csv
from business_questions import analyze_passenger_influence, tips_analyze, analyze_trip_distribution, calculate_trip_distance_distribution,calculate_top_drivers, find_longest_streaks
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as f
def main():
 
    spark = create_spark_session()

    data_path = "trip_data_1.csv"
    data_fare_path = "trip_fare_1.csv"
    taxi_data = load_taxi_data(spark, data_path)
    taxi_fare = load_taxi_data(spark,data_fare_path)
    
    transformed_data = extract_transform_load(taxi_data)

    distance_distribution = calculate_trip_distance_distribution(transformed_data)

    describe_data(taxi_data)
    describe_data(taxi_fare)

    print("Trip Distance Distribution:")
    taxi_data.select("trip_distance").show(10)
    distance_distribution.show()
    write_result_to_csv(distance_distribution,"distance_distribution.scv")

   
    

    print("Top Drivers:")
    taxi_data.select("hack_license","trip_distance").show(10)
    taxi_data = preprocess_data(taxi_data)
    top_drivers = calculate_top_drivers(taxi_data, n=5)
    top_drivers.show()
    write_result_to_csv(top_drivers,"top_n_drivers.scv")


    taxi_data_time = taxi_data.withColumn("pickup_datetime", taxi_data["pickup_datetime"].cast(TimestampType()))
    taxi_data_time = taxi_data.withColumn("dropoff_datetime", taxi_data["dropoff_datetime"].cast(TimestampType()))
    
    taxi_data_time = taxi_data_time.dropna()

    print("Top drivers with longest streaks:")
    taxi_data.select("hack_license","pickup_datetime","trip_distance").show(10)
    top_n_longest_streaks = find_longest_streaks(taxi_data_time, n=5, distance_threshold=10)
    top_n_longest_streaks.show()

    write_result_to_csv(top_n_longest_streaks,"top_n_longest_streaks.scv")
    
    print("Passenger count influence:")
    taxi_data.select("passenger_count","trip_time_in_secs","trip_distance").show(10)
    passager_count = analyze_passenger_influence(taxi_data)
    passager_count.show()
    write_result_to_csv(top_drivers,"passager_count.scv")

    print("Part of the day Influence:")
    taxi_data.select("pickup_datetime").show(5)
    part_of_the_day_count = analyze_trip_distribution(taxi_data)
    part_of_the_day_count.show()

    print("Top drivers with the most tips:")
    taxi_fare = taxi_fare.select([f.col(column).alias(column.replace(' ', '')) for column in taxi_fare.columns])
    taxi_fare.select("payment_type","medallion","tip_amount").show(10)
    driver_top_tips = tips_analyze(taxi_fare)
    driver_top_tips.show()
    
    spark.stop()

if __name__ == "__main__":
    main()