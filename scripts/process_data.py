from utils import *
from pyspark.sql import functions as F
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

    # Read data
    bookings_df = read_bookings_data(spark, args.src_file_location)
    airports_df = read_airports_data(spark, args.airport_country_mapping)

    # Filter for KLM flights departing from the Netherlands, confirmed bookings only
    dutchies = airports_df.filter(airports_df.Country == 'Netherlands').select(['IATA'])
    dutch_list = [row['IATA'] for row in dutchies.collect()]

    bookings_df = bookings_df.filter(
        (bookings_df.originAirport.isin(dutch_list)) &
        (bookings_df.marketingAirline == "KL") &
        (bookings_df.bookingStatus == "CONFIRMED")
    )

    # Filter on specified input date
    bookings_df = bookings_df.filter(
        (F.to_date(bookings_df.departureDate, "yyyy-MM-dd") >= F.to_date(F.lit(args.start_date), "yyyy-MM-dd")) &
        (F.to_date(bookings_df.departureDate, "yyyy-MM-dd") <= F.to_date(F.lit(args.end_date), "yyyy-MM-dd"))
    )

    # Join the bookings data with airports dimensions
    bookings_df = join_with_airports(bookings_df, airports_df)
    # add weekdays, seasons
    bookings_df = add_weekday_and_season(bookings_df)
    # counts adult, children and total
    bookings_df = add_passenger_counts(bookings_df)

    result = bookings_df.groupBy("destination_country", "weekday", "season").agg(
        F.sum("num_passengers").alias("total_passengers_count"),
        F.avg("avg_age"),  # average age
        F.sum("num_adults").alias("num_adults"),  # adults
        F.sum("num_children").alias("num_children")  # children
    ).orderBy(F.desc("total_passengers_count"))

    result.coalesce(1).write.mode("overwrite").csv(args.aggregated_output_file, header=True)

    print(f"Aggregation complete, results saved to: {args.aggregated_output_file}")
    result.show()

if __name__ == "__main__":
    main()
