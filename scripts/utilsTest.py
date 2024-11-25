from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
import logging, os
from pyspark.sql.functions import col, explode

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

        #df.printSchema()
        #df.show(5, truncate=False)

        df.select("event.DataElement.travelrecord.productsList", "event.DataElement.travelrecord.passengersList")

        # Manually check if productsList is a JSON string that can be parsed
        df = df.withColumn("parsed_productsList", F.when(
            F.col("event.DataElement.travelrecord.productsList").rlike(r"^\[.*\]$"),  # to match valid JSON arrays
            F.from_json("event.DataElement.travelrecord.productsList", products_schema)
        ).otherwise(None))  # If not a valid JSON array, set it as None

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

        products_df = df.select(
            'event.DataElement.travelrecord.envelopNumber',
            'parsed_productsList'
        ).withColumn('product', F.explode('parsed_productsList')).drop('parsed_productsList')

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

        # Join details on envelopNumber
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

def write_partitioned_data(bookings_df, output_folder):
    """
    Writes partitioned on departureDate.
    """
    try:
        # Convert departureDate to the NL timezone (from UTC)
        bookings_df = bookings_df.withColumn(
            "departureDateNL",
            F.from_utc_timestamp("departureDate", "Europe/Amsterdam")
        )

        # Ensure departureDateNL is of timestamp type
        bookings_df = bookings_df.withColumn("departureDateNL", F.col("departureDateNL").cast("timestamp"))

        # Extract year, month, and day of month and convert them to strings
        bookings_df = bookings_df.withColumn("year", F.year("departureDateNL"))
        bookings_df = bookings_df.withColumn("month", F.month("departureDateNL"))
        bookings_df = bookings_df.withColumn("day", F.dayofmonth("departureDateNL"))

        # Check if the columns for year, month, and day are correctly added
        bookings_df.select("year", "month", "day").show(5)

        # Write the data partitioned by year, month, and day as string (e.g., year=2019/month=01/day=01)
        bookings_df.write.partitionBy("year", "month", "day").parquet(output_folder, mode="overwrite")

    except Exception as e:
        logging.error(f"Error writing partitioned data: {e}")

def read_partitioned_data(spark, partitioned_data_path):
    try:
        print(f"Attempting to read partition from: {partitioned_data_path}")

        if os.path.exists(partitioned_data_path):
            files_in_path = os.listdir(partitioned_data_path)
            if files_in_path:
                print(f"Found files in partition: {files_in_path}")

                df = spark.read.option("mergeSchema", "true") \
                    .parquet(partitioned_data_path + "/*")
                return df
            else:
                print(f"No files found in the partition {partitioned_data_path}.")
                return None
        else:
            print(f"Partition {partitioned_data_path} does not exist.")
            return None
    except Exception as e:
        logging.error(f"Error reading partitioned data: {e}")
        return None


def add_weekday_and_season(bookings_df):
    """
    Adds weekday and season columns based on departureDate.
    Weekday from departureDate, season is determined manually based on the month range.
    """
    bookings_df = bookings_df.withColumn(
        "departureDate",
        F.to_timestamp(bookings_df.departureDate, "yyyy-MM-dd'T'HH:mm:ss")
    )
    bookings_df = bookings_df.withColumn("weekday", F.dayofweek(bookings_df.departureDate))

    # season's definitions
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