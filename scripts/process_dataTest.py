from utilsTest import *
from pyspark.sql import functions as F
import argparse, shutil, os, logging
from datetime import datetime, timedelta
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DoubleType

def main():

    parser = argparse.ArgumentParser(description="Process booking data.")
    parser.add_argument('--src_file_location', required=True, help='Path to booking data')
    parser.add_argument('--airport_country_mapping', required=True, help='Path to airport-country mapping data')
    parser.add_argument('--start_date', required=True, help='Start date for filtering bookings (YYYY-MM-DD)')
    parser.add_argument('--end_date', required=True, help='End date for filtering bookings (YYYY-MM-DD)')
    parser.add_argument('--processed_output_file', required=True, help='Output file for processed results')
    parser.add_argument('--aggregated_output_file', required=True, help='Output file for aggregated results')

    args = parser.parse_args()

    spark = create_spark_session()

    airports_df = read_airports_data(spark, args.airport_country_mapping)

    # Parse and re partition by departure date
    bookings_df = read_bookings_data(spark, args.src_file_location)
    bookings_df = join_with_airports(bookings_df, airports_df)
    # only dutch airports
    dutchies = airports_df.filter(airports_df.Country == 'Netherlands').select(['IATA'])
    dutch_list = [dutch['IATA'] for dutch in dutchies.collect()]

    bookings_df = bookings_df.filter(
        (bookings_df.originAirport.isin(dutch_list)) &
        (bookings_df.marketingAirline == "KL") &
        (bookings_df.bookingStatus == "CONFIRMED")
    )

    bookings_df.printSchema()
    bookings_df.show()

    write_partitioned_data(bookings_df, "processed_data")
    print(f" ---- Data successfully written!!!  -----")

    # Process by day, in the param's time range
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")

    current_date = start_date
    while current_date <= end_date:
        # Format the current date as 'year=YYYY/month=MM/day=DD'
        partitioned_path = f"processed_data/year={current_date.year}/month={current_date.month}/day={current_date.day}"
        print(f'created partition path: {partitioned_path}')

        if os.path.exists(partitioned_path):
            print(f'partition path: {partitioned_path} do exists')
            current_day_data = read_partitioned_data(spark, partitioned_path)

            if current_day_data:

                # add weekday, season and passenger's counts
                current_day_data = add_weekday_and_season(current_day_data)
                current_day_data = add_passenger_counts(current_day_data)

                current_day_data.printSchema()
                current_day_data.show(5)

                # Aggregation
                aggregated_data = current_day_data.groupBy("destination_country", "weekday", "season").agg(
                    F.sum("num_passengers").alias("num_passengers"),
                    F.avg("avg_age").alias("avg_age"),
                    F.sum("num_adults").alias("num_adults"),
                    F.sum("num_children").alias("num_children")
                ).orderBy(F.desc("num_passengers"))

                aggregated_data \
                    .write \
                    .mode("append") \
                    .parquet(f"{args.aggregated_output_file}/temp")

                print(f"Aggregated data for {current_date} written to {args.aggregated_output_file}")
                current_date += timedelta(days=1)
        else:
            logging.error(f"Partition path {partitioned_path} does not exist.")
            current_date += timedelta(days=1)

    print("completed aggregations")

    aggSchema = StructType([
        StructField("destination_country", StringType(), True),
        StructField("weekday", IntegerType(), True),
        StructField("season", StringType(), False),
        StructField("num_passengers", LongType(), True),
        StructField("avg_age", DoubleType(), True),
        StructField("num_adults", LongType(), True),
        StructField("num_children", LongType(), True)
    ])

    all_aggregated_data = spark.read.schema(aggSchema).parquet(f"{args.aggregated_output_file}/temp/*")

    all_aggregated_data.printSchema()
    all_aggregated_data.show(11)

    all_aggregated_data.coalesce(1) \
            .write \
            .mode("overwrite") \
            .parquet(f"{args.aggregated_output_file}/final_aggregated_data.parquet")

    # Read the Aggregated Data
    aggregated_df = spark.read.schema(aggSchema).parquet(f"{args.aggregated_output_file}/final_aggregated_data.parquet")
    aggregated_df.createOrReplaceTempView("aggregated_bookings")
    query = """
        SELECT destination_country, 
        season, 
        weekday, 
        SUM(num_passengers) AS total_passengers, 
        SUM(num_adults) AS Total_Adults, 
        SUM(num_children) AS Total_Children,
        AVG(avg_age) AS Age_Average
        FROM aggregated_bookings
        GROUP BY destination_country, season, weekday
        ORDER BY total_passengers DESC
    """
    query_result = spark.sql(query)
    query_result.show(truncate=False)

    #shutil.rmtree(f"{args.aggregated_output_file}/temp")

if __name__ == "__main__":
    main()
