from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
import logging


def create_spark_session():
    """
    Creates spark session
    """
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("KLM Booking Analysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark


def read_bookings_data(spark, bookings_path):
    """
    Reads and flat booking data
    """
    # Schema passengersList
    passengers_schema = ArrayType(StructType([
        StructField("uci", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("passengerType", StringType(), True)
    ]))

    # Schema productsList
    products_schema = ArrayType(StructType([
        StructField("bookingStatus", StringType(), True),
        StructField("flight", StructType([
            StructField("originAirport", StringType(), True),
            StructField("destinationAirport", StringType(), True),
            StructField("departureDate", StringType(), True),
            StructField("arrivalDate", StringType(), True),
            StructField("operatingAirline", StringType(), True),
            StructField("marketingAirline", StringType(), True)
        ]))
    ]))

    # Define a schema for the entire JSON
    schema = StructType([
        StructField("event", StructType([
            StructField("DataElement", StructType([
                StructField("travelrecord", StructType([
                    StructField("envelopNumber", StringType(), True),
                    StructField("creationDate", StringType(), True),
                    StructField("nbPassengers", IntegerType(), True),
                    StructField("isMarketingBlockspace", StringType(), True),
                    StructField("isTechnicalLastUpdater", StringType(), True),
                    StructField("passengersList", StringType(), True),  # As string initially
                    StructField("productsList", StringType(), True)  # As string initially
                ]))
            ]))
        ]))
    ])

    try:
        # Read JSON with defined schema and handle malformed records gracefully
        df = spark.read.option("mode", "DROPMALFORMED") \
            .schema(schema) \
            .json(bookings_path)

        # Check the schema and first few rows to debug
        #df.printSchema()
        #df.show(5, truncate=False)

        # Check the content of productsList and passengersList before parsing
        df.select("event.DataElement.travelrecord.productsList", "event.DataElement.travelrecord.passengersList").show(5, truncate=False)

        # Manually check if productsList is a JSON string that can be parsed
        df = df.withColumn("parsed_productsList", F.when(
            F.col("event.DataElement.travelrecord.productsList").rlike(r"^\[.*\]$"),  # Regex to match valid JSON arrays
            F.from_json("event.DataElement.travelrecord.productsList", products_schema)
        ).otherwise(None))  # If it's not a valid JSON array, set it as None

        #df.show(5, truncate=False)

        # Parse the passengersList into an array of structs
        df = df.withColumn("passengersList", F.from_json("event.DataElement.travelrecord.passengersList", passengers_schema))

        #df.printSchema()
        #df.show(5, truncate=False)

        passengers_df = df.select(
            'event.DataElement.travelrecord.envelopNumber',
            'event.DataElement.travelrecord.creationDate',
            'event.DataElement.travelrecord.nbPassengers',
            'event.DataElement.travelrecord.isMarketingBlockspace',
            'event.DataElement.travelrecord.isTechnicalLastUpdater',
            'passengersList'
        ).withColumn('passenger', F.explode('passengersList')).drop('passengersList')

        passengers_df = passengers_df.select(
            'envelopNumber',
            'nbPassengers',
            'passenger.uci',
            'passenger.age',
            'passenger.passengerType'
        )

        # Exploding products list
        products_df = df.select(
            'event.DataElement.travelrecord.envelopNumber',
            'parsed_productsList'
        ).withColumn('product', F.explode('parsed_productsList')).drop('parsed_productsList')

        # Extracting flight-related details
        products_df = products_df.select(
            'envelopNumber',
            'product.bookingStatus',
            'product.flight.originAirport',
            'product.flight.destinationAirport',
            'product.flight.departureDate',
            'product.flight.arrivalDate',
            'product.flight.operatingAirline',
            'product.flight.marketingAirline',
        )

        # Join the passenger and flight details on envelopNumber
        df_flat = passengers_df.join(products_df, 'envelopNumber', 'inner')

        return df_flat

    except Exception as e:
        logging.error(f"Error reading or processing JSON data: {e}")
        return None

def read_airports_data(spark, file_path):
    """
    Read and flats airports data
    """
    # Defined schema from md file
    schema = StructType([
        StructField("AirportID", IntegerType(), True),
        StructField("Name", StringType(), True),
        StructField("City", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("IATA", StringType(), True),
        StructField("ICAO", StringType(), True),
        StructField("Timezone", StringType(), True)

    ])

    df = spark.read.csv(file_path, schema=schema, header=False, sep=",", nullValue="")

    df = df.select(
        F.col("IATA"),
        F.col("Name"),
        F.col("City"),
        F.col("Country"),
        F.col("ICAO"),
        F.col("Timezone"),
    )

    return df

def join_with_airports(bookings_df, airports_df):

    bookings_df_alias = bookings_df.alias("bookings")
    airports_df_alias = airports_df.alias("airports")

    # joins by airport code
    df = bookings_df_alias.join(airports_df_alias, bookings_df_alias.destinationAirport == airports_df_alias.IATA, "inner") \
        .select(
        bookings_df_alias["*"],
        airports_df_alias["Country"].alias("destination_country")
    )
    return df


def add_weekday_and_season(bookings_df):
    # Remove the 'Z' and reformat the timestamp into a parsable format
    bookings_df = bookings_df.withColumn(
        "departureDate",
        F.regexp_replace(bookings_df.departureDate, "Z$", "")
    )

    # Convert the departureDate to a date object and extract the weekday (0=Monday, 6=Sunday)
    bookings_df = bookings_df.withColumn(
        "departureDate", F.to_timestamp(bookings_df.departureDate, "yyyy-MM-dd'T'HH:mm:ss")
    )

    # Extract weekday (0 = Monday, 6 = Sunday)
    bookings_df = bookings_df.withColumn("weekday", F.dayofweek(bookings_df.departureDate))

    # Define the seasons based on the month of the departure date
    bookings_df = bookings_df.withColumn(
        "season",
        F.when((F.month(bookings_df.departureDate) >= 3) & (F.month(bookings_df.departureDate) <= 5), "Spring")
        .when((F.month(bookings_df.departureDate) >= 6) & (F.month(bookings_df.departureDate) <= 8), "Summer")
        .when((F.month(bookings_df.departureDate) >= 9) & (F.month(bookings_df.departureDate) <= 11), "Autumn")
        .otherwise("Winter")
    )

    return bookings_df

def add_passenger_counts(df):
    # Create columns for the number of adults and children
    df = df.withColumn(
        "num_adults",
        F.when(df["passengerType"] == "ADT", 1).otherwise(0)
    )
    df = df.withColumn(
        "num_children",
        F.when(df["passengerType"] == "CHD", 1).otherwise(0)
    )

    # Calculate total passengers as the sum of adults and children
    df = df.withColumn("total_passengers", df["num_adults"] + df["num_children"])

    # Aggregate by destination_country, weekday, and season, then sum adults/children
    aggregated_df = df.groupBy("destination_country", "weekday", "season") \
        .agg(
        F.sum("total_passengers").alias("num_passengers"),  # Sum of adults and children
        F.avg("age").alias("avg_age"),  # Calculate the average age per group
        F.sum("num_adults").alias("num_adults"),  # Sum of adults
        F.sum("num_children").alias("num_children")  # Sum of children
    ) \
        .orderBy(F.desc("num_passengers"))

    return aggregated_df
