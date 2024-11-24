from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

def create_spark_session():
    from pyspark.sql import SparkSession
    spark = SparkSession.builder \
        .appName("KLM Booking Analysis") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    return spark

def read_bookings_data(spark, bookings_path):
    """
    Reads and flattens the booking data
    """
    df = spark.read.json(bookings_path)

    print('Schema of the raw json booking with some sample;')
    df.printSchema() #such a json!

    passengers_df = df.select(
        'event.DataElement.travelrecord.envelopNumber',
        'event.DataElement.travelrecord.creationDate',
        'event.DataElement.travelrecord.nbPassengers',
        'event.DataElement.travelrecord.isMarketingBlockspace',
        'event.DataElement.travelrecord.isTechnicalLastUpdater',
        'event.DataElement.travelrecord.passengersList'
    ).withColumn('passenger', F.explode('passengersList')).drop('passengersList')

    passengers_df = passengers_df.select(
        'envelopNumber',
        'nbPassengers',
        'passenger.uci',
        'passenger.tattoo',
        'passenger.age',
        'passenger.passengerType'
    )

    # Exploding the products list to flatten flight details
    products_df = df.select(
        'event.DataElement.travelrecord.envelopNumber',
        'event.DataElement.travelrecord.productsList'
    ).withColumn('product', F.explode('productsList')).drop('productsList')

    # Flatten product details
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

    # Join the passenger and product data on envelopNumber
    df_flat = passengers_df.join(products_df, 'envelopNumber', 'inner')

    return df_flat


def read_airports_data(spark, file_path):
    """
    Read the airports data and flatten the structure, renaming necessary columns.
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

    # Perform the join for origin airport
    df = bookings_df_alias.join(airports_df_alias, bookings_df_alias.destinationAirport == airports_df_alias.IATA, "inner") \
        .select(
        bookings_df_alias["*"],
        airports_df_alias["Country"].alias("destination_country")
    )
    return df

def add_weekday_and_season(df):
    """
    Add columns for the day of the week and season based on the departure date.
    """
    # Convert departureDate to timestamp to extract weekday
    df = df.withColumn("departureDate", F.to_timestamp("departureDate"))

    # Extract the weekday (0=Sunday, 1=Monday, ..., 6=Saturday)
    df = df.withColumn("weekday", F.dayofweek("departureDate"))

    # Assuming season is based on months (example for Northern Hemisphere):
    df = df.withColumn(
        "season",
        F.when((F.month("departureDate") >= 3) & (F.month("departureDate") <= 5), "Spring")
        .when((F.month("departureDate") >= 6) & (F.month("departureDate") <= 8), "Summer")
        .when((F.month("departureDate") >= 9) & (F.month("departureDate") <= 11), "Fall")
        .otherwise("Winter")
    )
    return df

def process_and_aggregate(bookings_df):
    # Ensure 'departureDate' is in the correct format
    bookings_df = bookings_df.withColumn('departureDate', F.to_date('departureDate', 'yyyy-MM-dd'))

    # Add weekday and season columns
    bookings_df = bookings_df.withColumn('weekday', F.dayofweek('departureDate')) \
        .withColumn('season',
                    F.when((F.month('departureDate') >= 3) & (F.month('departureDate') <= 5), 'Spring')
                    .when((F.month('departureDate') >= 6) & (F.month('departureDate') <= 8), 'Summer')
                    .when((F.month('departureDate') >= 9) & (F.month('departureDate') <= 11), 'Fall')
                    .otherwise('Winter'))

    # Group by origin_country, destination_country, weekday, and season, then count distinct passengers
    result = bookings_df.groupBy('origin_country', 'destination_country', 'weekday', 'season') \
        .agg(F.countDistinct('uci').alias('num_passengers')) \
        .orderBy(F.desc('num_passengers'))

    return result

def filter_confirmed_bookings(bookings_df):
    # Filter the bookings dataframe to include only those with confirmed booking status
    return bookings_df.filter(bookings_df.bookingStatus == 'CONFIRMED')

def filter_flights_from_netherlands(bookings_df):
    # Define a list of airport codes for the Netherlands
    netherlands_airports = ['AMS', 'RTM', 'EIN']

    # Filter the bookings to only include flights departing from these airports
    return bookings_df.filter(bookings_df.originAirport.isin(netherlands_airports))


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
    aggregated_df.show()
    return aggregated_df
