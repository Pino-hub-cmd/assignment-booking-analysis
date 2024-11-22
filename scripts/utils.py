import logging
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def initialize_spark_session(app_name="BookingDataProcessing"):
    """Initialize a Spark session."""
    logger.info("Initializing Spark session...")
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logger.info("Spark session initialized successfully.")
        return spark
    except Exception as e:
        logger.error(f"Failed to initialize Spark session: {str(e)}")
        raise

def load_json_data(spark, file_path):
    """Load JSON data from the given file path."""
    logger.info(f"Loading data from {file_path}...")
    try:
        df = spark.read.json(file_path)
        logger.info(f"Data loaded successfully with {df.count()} rows.")
        df.printSchema()  # Print schema for debugging
        return df
    except Exception as e:
        logger.error(f"Failed to load JSON data from {file_path}: {str(e)}")
        raise

def load_airport_data(spark, airport_country_mapping):
    """Load airport country mapping data and assign column names."""
    logger.info(f"Loading airport data from {airport_country_mapping}...")

    # Define the column names explicitly
    columns = [
        "AirportID", "Name", "City", "Country", "IATA", "ICAO", "Latitude", "Longitude",
        "Altitude", "Timezone", "DST", "TzDatabaseTimezone", "Type", "Source"
    ]

    # Load the data without headers
    airport_df = spark.read.csv(airport_country_mapping, header=False, inferSchema=True)

    # Assign the column names to the dataframe
    airport_df = airport_df.toDF(*columns)

    # Print columns to verify
    logger.info(f"Column names in airport data: {airport_df.columns}")

    # Ensure the 'Country' column is stripped of any leading/trailing spaces and is in correct case
    airport_df = airport_df.withColumn("Country", F.trim(F.col("Country")))

    logger.info(f"Airport data loaded successfully with {airport_df.count()} rows.")
    return airport_df
def handle_invalid_json(df):
    """Handle invalid or corrupted JSON entries."""
    logger.info("Handling invalid JSON entries...")
    try:
        # Drop rows with invalid JSON or missing important data
        df_cleaned = df.filter(df.event.isNotNull())
        logger.info(f"Handled invalid JSON entries, resulting in {df_cleaned.count()} rows.")
        return df_cleaned
    except Exception as e:
        logger.error(f"Failed to handle invalid JSON: {str(e)}")
        raise

def flatten_nested_structures(df):
    """Flatten nested structures to make the data more accessible."""
    logger.info("Flattening nested structures...")
    try:
        # Exploding arrays and structuring the data for easier access
        df_exploded = df.withColumn("passenger", F.explode("event.DataElement.travelrecord.passengersList"))
        logger.info(f"Flattened data with {df_exploded.count()} rows.")
        return df_exploded
    except Exception as e:
        logger.error(f"Failed to flatten nested structures: {str(e)}")
        raise

def filter_kl_flights(booking_df, airport_df, country):
    try:
        # Exploding the 'productsList' to handle individual elements
        flattened_booking_df = booking_df.withColumn(
            "product", F.explode(F.col("event.DataElement.travelrecord.productsList"))
        )

        # Accessing the 'departureDate' from 'flight' after explosion
        flattened_booking_df = flattened_booking_df.withColumn(
            "departureDate",
            F.to_date(F.col("product.flight.departureDate"), "yyyy-MM-dd")
        )

        # Filter KL flights (assuming this is part of your logic)
        # Example filtering condition (you can modify this as needed)
        kl_flights_df = flattened_booking_df.filter(
            (F.col("product.aircraftType") == "KL") &
            (F.col("product.bookingClass") == "Y") &
            (F.col("product.type") == "Flight")
        )

        # If you want to join with airport data, assuming you're filtering by country
        kl_flights_df = kl_flights_df.join(
            airport_df,
            kl_flights_df["product.flight.originAirport"] == airport_df["IATA"],
            how="left"
        ).filter(airport_df["Country"] == country)

        return kl_flights_df

    except Exception as e:
        print(f"ERROR:utils:Failed to filter KL flights: {str(e)}")
        raise e

def deduplicate_passengers(booking_df):
    try:
        # Exploding 'productsList' and 'passengersList' for further processing
        flattened_booking_df = booking_df.withColumn(
            "product", F.explode(F.col("event.DataElement.travelrecord.productsList"))
        )

        flattened_booking_df = flattened_booking_df.withColumn(
            "passenger", F.explode(F.col("event.DataElement.travelrecord.passengersList"))
        )

        # Adjust the path to correctly reference 'journeyInTattoo' and other columns
        flattened_booking_df = flattened_booking_df.withColumn(
            "journeyInTattoo", F.col("product.journeyInTattoo")  # Adjusted path
        )

        # Create 'passenger_id' from 'tattoo' or another unique identifier
        flattened_booking_df = flattened_booking_df.withColumn(
            "passenger_id", F.col("passenger.tattoo")  # Assuming 'tattoo' is the passenger ID
        )

        # Deduplicate based on 'passenger_id', 'journeyInTattoo', and correct 'originAirport' reference
        df_dedup = flattened_booking_df.dropDuplicates(
            ["passenger_id", "journeyInTattoo", "product.flight.originAirport", "departureDate"]
        )

        return df_dedup

    except Exception as e:
        print(f"ERROR:utils:Failed to deduplicate passengers: {str(e)}")
        raise e

def adjust_timezone_and_day(df, airport_df):
    """Adjust the departure datetime by airport timezone."""
    logger.info("Adjusting departure times to local time...")
    try:
        df = df.join(airport_df, df.originAirport == airport_df.IATA, "left")
        df = df.withColumn(
            "local_departure_datetime",
            F.from_utc_timestamp(F.col("departureDate"), F.col("timezone"))
        )
        logger.info("Timezone adjustment completed.")
        return df
    except Exception as e:
        logger.error(f"Failed to adjust timezone: {str(e)}")
        raise

def aggregate_bookings(df):
    """Perform aggregation on the booking data."""
    logger.info("Aggregating bookings...")
    try:
        aggregated_df = df.groupBy("originAirport", "departureDate").agg(
            F.sum("nbPassengers").alias("total_passengers"),
            F.countDistinct("passenger_id").alias("unique_passenger_count")
        )
        logger.info("Aggregation completed.")
        return aggregated_df
    except Exception as e:
        logger.error(f"Failed to aggregate bookings: {str(e)}")
        raise

def save_to_csv(df, output_path):
    """Save the DataFrame to a CSV file."""
    logger.info(f"Saving results to {output_path}...")
    try:
        df.coalesce(1).write.option("header", "true").mode("overwrite").csv(output_path)
        logger.info(f"Results saved to {output_path}.")
    except Exception as e:
        logger.error(f"Failed to save results to CSV: {str(e)}")
        raise
