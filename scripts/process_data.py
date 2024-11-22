import argparse
from pyspark.sql.functions import col, to_date
from pyspark.sql import functions as F
from utils import (
    initialize_spark_session,
    load_json_data,
    load_airport_data,
    handle_invalid_json,
    flatten_nested_structures,
    filter_kl_flights,
    deduplicate_passengers,
    adjust_timezone_and_day,
    aggregate_bookings,
    save_to_csv
)

def main(args):
    # Initialize Spark session
    spark = initialize_spark_session()

    # Load booking data
    booking_df = load_json_data(spark, args.src_file_location)
    print("Booking data schema:")
    booking_df.printSchema()  # Debugging: Inspect schema

    # Handle invalid JSON entries
    booking_df = handle_invalid_json(booking_df)

    # Flatten nested structures
    booking_df = flatten_nested_structures(booking_df)

    # Load airport data
    airport_df = load_airport_data(spark, args.airport_country_mapping)
    airport_df.printSchema()  # This will show all the columns in the DataFrame
    airport_df.show(5)
    # Filter KL flights departing from the Netherlands
    booking_df = filter_kl_flights(booking_df, airport_df, "Netherlands")

    # Filter data by date range
    booking_df = booking_df.withColumn("departureDate", to_date(col("departureDate")))
    booking_df = booking_df.filter(
        (booking_df["departureDate"] >= args.start_date) & (booking_df["departureDate"] <= args.end_date)
    )
    print(f"Rows after date filtering: {booking_df.count()}")

    # Deduplicate passengers per flight leg
    booking_df = deduplicate_passengers(booking_df)

    # Adjust timezone and extract day of the week
    booking_df = adjust_timezone_and_day(booking_df, airport_df)

    # Save intermediate processed data
    save_to_csv(booking_df, args.processed_output_file)

    # Aggregate bookings
    aggregated_df = aggregate_bookings(booking_df)

    # Save aggregated results
    save_to_csv(aggregated_df, args.aggregated_output_file)

    print("Processing completed successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Process booking data.")
    parser.add_argument("--src_file_location", required=True, help="Path to source JSON booking file.")
    parser.add_argument("--airport_country_mapping", required=True, help="Path to airport-country mapping file.")
    parser.add_argument("--start_date", required=True, help="Start date for filtering (YYYY-MM-DD).")
    parser.add_argument("--end_date", required=True, help="End date for filtering (YYYY-MM-DD).")
    parser.add_argument("--processed_output_file", required=True, help="Path to save processed output CSV.")
    parser.add_argument("--aggregated_output_file", required=True, help="Path to save aggregated output CSV.")
    args = parser.parse_args()

    main(args)
