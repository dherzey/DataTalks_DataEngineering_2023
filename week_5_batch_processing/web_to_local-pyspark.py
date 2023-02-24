import os
from pathlib import Path
from pyspark.sql import SparkSession, types
from pyspark.sql.utils import AnalysisException

curr_dir = '/home/jdtganding/Documents/data-engineering-zoomcamp/week_5_batch_processing/'

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('test') \
    .getOrCreate()

green_schema = types.StructType([
    types.StructField("VendorID", types.IntegerType(), True),
    types.StructField("lpep_pickup_datetime", types.TimestampType(), True),
    types.StructField("lpep_dropoff_datetime", types.TimestampType(), True),
    types.StructField("store_and_fwd_flag", types.StringType(), True),
    types.StructField("RatecodeID", types.IntegerType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("passenger_count", types.IntegerType(), True),
    types.StructField("trip_distance", types.DoubleType(), True),
    types.StructField("fare_amount", types.DoubleType(), True),
    types.StructField("extra", types.DoubleType(), True),
    types.StructField("mta_tax", types.DoubleType(), True),
    types.StructField("tip_amount", types.DoubleType(), True),
    types.StructField("tolls_amount", types.DoubleType(), True),
    types.StructField("ehail_fee", types.DoubleType(), True),
    types.StructField("improvement_surcharge", types.DoubleType(), True),
    types.StructField("total_amount", types.DoubleType(), True),
    types.StructField("payment_type", types.IntegerType(), True),
    types.StructField("trip_type", types.IntegerType(), True),
    types.StructField("congestion_surcharge", types.DoubleType(), True)
])

yellow_schema = types.StructType([
    types.StructField("VendorID", types.IntegerType(), True),
    types.StructField("tpep_pickup_datetime", types.TimestampType(), True),
    types.StructField("tpep_dropoff_datetime", types.TimestampType(), True),
    types.StructField("passenger_count", types.IntegerType(), True),
    types.StructField("trip_distance", types.DoubleType(), True),
    types.StructField("RatecodeID", types.IntegerType(), True),
    types.StructField("store_and_fwd_flag", types.StringType(), True),
    types.StructField("PULocationID", types.IntegerType(), True),
    types.StructField("DOLocationID", types.IntegerType(), True),
    types.StructField("payment_type", types.IntegerType(), True),
    types.StructField("fare_amount", types.DoubleType(), True),
    types.StructField("extra", types.DoubleType(), True),
    types.StructField("mta_tax", types.DoubleType(), True),
    types.StructField("tip_amount", types.DoubleType(), True),
    types.StructField("tolls_amount", types.DoubleType(), True),
    types.StructField("improvement_surcharge", types.DoubleType(), True),
    types.StructField("total_amount", types.DoubleType(), True),
    types.StructField("congestion_surcharge", types.DoubleType(), True)
])

def web_to_local(service, year, schema):

    init_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/'
    
    # create parent directory if it does not exist
    path_dir = Path(f"data/{service}")
    path_dir.mkdir(parents=True, exist_ok=True)

    for month in range(1,13):

        file_name = f"{service}_tripdata_{year}-{month:02}.csv.gz"
        url = init_url + file_name

        try:
            #download data as a csv file
            os.system(f"wget {url} -O {path_dir/file_name}")

            #unzip and remove compressed file
            os.system(f"gzip -d {path_dir/file_name}")

            # read file as csv
            file_name = file_name.replace('csv.gz','csv')
            df_csv = spark.read \
                        .option('header','true') \
                        .schema(schema) \
                        .csv(f"{curr_dir}{path_dir/file_name}")

            #convert to parquet
            df_csv.repartition(4).write.parquet(f"data/pq/{service}/{year}/{month:02}")
        except AnalysisException:
            break

web_to_local('green', 2020, green_schema)
web_to_local('green', 2021, green_schema)
web_to_local('yellow', 2020, yellow_schema)
web_to_local('yellow', 2021, yellow_schema)