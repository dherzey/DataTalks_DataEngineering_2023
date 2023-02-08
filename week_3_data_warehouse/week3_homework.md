# Week 3 - Homework

## Setup
For this assignment, we will need to ingest FHV NY Taxi Data and load it into our Google Cloud Storage. We can use an orchestrator like Airflow or Prefect, or use the GCS client to load data to our bucket. For the latter, we make sure that the following are installed:
```bash
pip install pandas pyarrow google-cloud-storage
```
In order to use our storage, we need to authenticate our service account to our local machine:
```bash
#set environment variable to point to downloaded service account keys
export GOOGLE_APPLICATION_CREDENTIALS="/home/jdtganding/Documents/data-engineering-zoomcamp/week_2_workflow_orchestration/zoomcamp-user-bfecc07e3f66.json"

#verify authentication
gcloud auth application-default login
```
Next, we can create a script which loads our data from the web to GCS. See `web_to_gcs-gcloud.py` for the sample Python script. Finally, when the data is already loaded, we can create an external and an internal table in BigQuery using the FHV 2019 data.
```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `zoomcamp-user.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://dtc_data_lake_zoomcamp-user/data/fhv/fhv_tripdata_2019-*.csv']
);

-- Create non-partitioned table from external table
CREATE OR REPLACE TABLE zoomcamp-user.trips_data_all.fhv_tripdata_non_partitioned AS
SELECT * FROM zoomcamp-user.trips_data_all.external_fhv_tripdata;
```

## Question 1: 
> What is the count for fhv vehicle records for the year 2019?

```sql
SELECT COUNT(*) 
FROM trips_data_all.fhv_tripdata_non_partitioned;
```
From the result, there are 43,244,696 vehicle records in 2019.

## Question 2:
> Write a query to count the distinct number of affiliated_base_number for the entire dataset on both the tables.
What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?

```sql
SELECT DISTINCT Affiliated_base_number 
FROM `zoomcamp-user.trips_data_all.fhv_tripdata_non_partitioned`;

SELECT DISTINCT Affiliated_base_number 
FROM `zoomcamp-user.trips_data_all.external_fhv_tripdata`;
```
The BQ table processed 317.94 MB of data while the External table processed 0 MB of data.

## Question 3:
> How many records have both a blank (null) PUlocationID and DOlocationID in the entire dataset?

```sql
SELECT COUNT(*) 
FROM `zoomcamp-user.trips_data_all.external_fhv_tripdata` 
WHERE PUlocationID IS NULL 
AND DOlocationID IS NULL;
```
A total of 717,748 rows have NULL values for both columns

## Question 4:
> What is the best strategy to optimize the table if query always filter by pickup_datetime and order by affiliated_base_number?

For this case, it is best to partition our table by the pickup_datetime and cluster our table by the affiliated_base_number.

## Question 5:
> Implement the optimized solution you chose for question 4. Write a query to retrieve the distinct affiliated_base_number between pickup_datetime 03/01/2019 and 03/31/2019 (inclusive).
Use the BQ table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? Choose the answer which most closely matches.

Partitioning by pickup_datetime and clustering by affiliated_base_number:
```sql
CREATE OR REPLACE TABLE zoomcamp-user.trips_data_all.fhv_tripdata_partitioned_clustered
PARTITION BY DATE(pickup_datetime)
CLUSTER BY Affiliated_base_number AS
SELECT * FROM zoomcamp-user.trips_data_all.external_fhv_tripdata;
```
```sql
SELECT DISTINCT Affiliated_base_number
FROM `zoomcamp-user.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE pickup_datetime >= "2019-03-01" 
AND pickup_datetime <= "2019-03-31";

SELECT DISTINCT Affiliated_base_number
FROM `zoomcamp-user.trips_data_all.fhv_tripdata_non_partitioned`
WHERE pickup_datetime >= "2019-03-01" 
AND pickup_datetime <= "2019-03-31";
```
From the queries, the paritioned and clustered table processed around 24 MB of data while the non-partitioned table processed around 648 MB of data.

## Question 6:
> Where is the data stored in the External Table you created?

Data for the external table is located in Google Cloud Storage Bucket

## Question 7:
> Is it a best practice in Big Query to always cluster your data?

To optimize our query performance, it is a best practice to partition or cluster our data in BigQuery.