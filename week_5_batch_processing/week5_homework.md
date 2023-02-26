# Week 5 - Homework

## Setup
We first read the June 2021 HVFHW data using the previous schema we have modified. 
```python
#schema for our dataframe:
schema = StructType([
         StructField('dispatching_base_num', StringType(), True), 
         StructField('pickup_datetime', TimestampType(), True), 
         StructField('dropoff_datetime', TimestampType(), True), 
         StructField('PULocationID', IntegerType(), True), 
         StructField('DOLocationID', IntegerType(), True), 
         StructField('SR_Flag', StringType(), True), 
         StructField('Affiliated_base_number', StringType(), True)
])

#reading the file:
df_csv = spark.read \
              .option('header','true') \
              .schema(schema) \
              .csv(f"{curr_dir}/data/fhv_tripdata_2021-06.csv")
```

## Question 1: Install Spark and PySpark
> After installing Spark and PySpark and adding them to path, we check if everything works by executing its version number.
```python
import pyspark

pyspark.__version__
```
From the above result, Spark is currently in version 3.3.2.

## Question 2: HVFHW June 2021
> Read it with Spark using the same schema as we did in the lessons. We will use this dataset for all the remaining questions. Repartition it to 12 partitions and save it to parquet. What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)?
```python
#writing file as parquet with 12 partitions
df_csv.repartition(12).write.parquet(f"{curr_dir}/data/fhv")
```
Looking at the parquet files, each file seems to be, on average, around 24MB in size.

## Question 3: Count records
> How many taxi trips were there on June 15? Consider only trips that started on June 15.
```python
#we can query this using PySpark SQL
df_csv.createOrReplaceTempView('fhv_06_2021')

spark.sql("""
SELECT 
    count(*) as taxi_trips_count
FROM 
    fhv_06_2021
WHERE 
    pickup_datetime >= '2021-06-15 00:00:00'
    AND pickup_datetime < '2021-06-16 00:00:00'
""").show()
```
```
output:
+----------------+
|taxi_trips_count|
+----------------+
|          452470|
+----------------+
```

## Question 4: Longest trip for each day
> Now calculate the duration for each trip. How long was the longest trip in Hours?
```python
spark.sql("""
SELECT 
   round((CAST(dropoff_datetime as numeric) -
   CAST(pickup_datetime as numeric))/3600, 2)
   AS trip_duration_hour
FROM 
    fhv_06_2021
ORDER BY
    trip_duration_hour DESC
LIMIT 5;
""").show()
```
```
output:
+------------------+
|trip_duration_hour|
+------------------+
|             66.88|
|             25.55|
|             19.98|
|             18.20|
|             16.47|
+------------------+
```
From the output, we see that the longest trip lasted for about 66.88 hours.

## Question 5: User interface
> Spark’s User Interface which shows application's dashboard runs on which local port?

Spark's UI runs by default in port 4040 locally.

## Question 6: Most frequent pickup location zone
> Load the zone lookup data into a temp view in Spark. Using the zone lookup data and the fhvhv June 2021 data, what is the name of the most frequent pickup location zone?
```python
#we read the zones file as parquet
df_zones = spark.read.parquet(f"{curr_dir}/data/zones")

#we perform the join for our FHV and zones data using the pickup location ID:
df_join = df_csv.join(df_zones, 
                      df_csv.PULocationID==df_zones.LocationID,
                      how = 'inner')

#create temporary table/view
df_join.createOrReplaceTempView('fhv_zones')

#use Spark SQL to perform groupby
spark.sql("""
SELECT 
    Zone, 
    count(*) as number_trips
FROM 
    fhv_zones
GROUP BY 
    Zone
ORDER BY 
    number_trips DESC
LIMIT 5
""").show()
```
```
output:
+-------------------+------------+
|               Zone|number_trips|
+-------------------+------------+
|Crown Heights North|      231279|
|       East Village|      221244|
|        JFK Airport|      188867|
|     Bushwick South|      187929|
|      East New York|      186780|
+-------------------+------------+
```
From the output, we see that Crown Heights North have the largest number of pickup trips in our data.