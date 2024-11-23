from utils import create_spark_session, read_bookings_data, read_airports_data, filter_confirmed_bookings, join_with_airports, add_weekday_and_season
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
import argparse

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

    # Read the booking data and airport data
    bookings_df = read_bookings_data(spark, args.src_file_location)
    airports_df = read_airports_data(spark, args.airport_country_mapping)

    # Filter for confirmed bookings only
    bookings_df.filter(bookings_df['bookingStatus'] == 'CONFIRMED')

    # Filter for KLM flights departing from the Netherlands
    netherlands_airports = ['AMS', 'RTM', 'EIN']
    bookings_df = bookings_df.filter(
        (bookings_df.originAirport.isin(netherlands_airports)) &
        (bookings_df.marketingAirline == "KL")
    )

    # Filter bookings based on the specified date range
    bookings_df = bookings_df.filter(
        (F.to_date(bookings_df.departureDate, "yyyy-MM-dd") >= F.to_date(F.lit(args.start_date), "yyyy-MM-dd")) &
        (F.to_date(bookings_df.departureDate, "yyyy-MM-dd") <= F.to_date(F.lit(args.end_date), "yyyy-MM-dd"))
    )

    # Join the bookings data with airport country mapping
    bookings_df = join_with_airports(bookings_df, airports_df)

    print('bookings_df after filtering Netherlands s airports and KL and date range')
    bookings_df.printSchema()
    bookings_df.show(5)

    # Add weekday and season information
    bookings_df = add_weekday_and_season(bookings_df)

    # Aggregation to get the number of passengers per country, per day of week, per season
    result = bookings_df.groupBy("destination_country", "weekday", "season").agg(
        F.countDistinct("uci").alias("num_passengers")
    ).orderBy(F.desc("num_passengers"))

    # Use coalesce(1) to combine the output into a single CSV file
    result.coalesce(1).write.mode("overwrite").csv(args.aggregated_output_file, header=True)

    print(f"Aggregation complete, results saved to: {args.aggregated_output_file}")
    result.show()


if __name__ == "__main__":
    main()
