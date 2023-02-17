import os
from google.cloud import bigquery

BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_zoomcamp-user")

def gcs_to_bq(service, year, project='zoomcamp-user', dataset='trips_data_all'):
    # Construct a BigQuery client object.
    client = bigquery.Client()

    table_id = f"{project}.{dataset}.{service}_taxi_data_2019"
    uri = f"gs://{BUCKET}/data/{service}/{service}_tripdata_{year}*.parquet"

    # to append, replace WRITE_TRUNCATE with WRITE_APPEND
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        source_format=bigquery.SourceFormat.PARQUET,
    )

    load_job = client.load_table_from_uri(
        uri, table_id, job_config=job_config
    )  # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

if __name__=="__main__":
    gcs_to_bq(service='green', year=2019)