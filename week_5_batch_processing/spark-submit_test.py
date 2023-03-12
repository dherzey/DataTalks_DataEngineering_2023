import pyspark
import argparse
from pyspark.sql import SparkSession

#we specify our master options using spark-submit in the command line
spark =  SparkSession.builder \
                     .appName("test") \
                     .getOrCreate()

parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

#specify the needed arguments
parser.add_argument('--main_dir', required=True, help='main directory of file')
parser.add_argument('--input_green', required=True, help='input path file for green data')
parser.add_argument('--input_yellow', required=True, help='input path file for yellow data')

params = parser.parse_args()

main_dir = params.main_dir
input_green = params.input_green
input_yellow = params.input_yellow

df_green = spark.read.parquet(f'{main_dir}/{input_green}')
df_yellow = spark.read.parquet(f'{main_dir}/{input_yellow}')

print(f"Number of green records: {df_green.count()}")
print(f"Number of yellow records: {df_yellow.count()}")