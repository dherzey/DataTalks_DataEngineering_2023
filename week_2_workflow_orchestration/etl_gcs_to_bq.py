from pathlib import Path
import pandas as pd
from prefect import task, flow
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials

@task(log_prints=True, retries=3)
def extract_from_gcs(color: str, year: int, month: int, dataset_file: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"data/{color}/{dataset_file}.parquet"
    gcs_block = GcsBucket.load("zoom-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path="")
    return Path(f"{gcs_path}")

@task(log_prints=True)
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)
    na_counts = df['passenger_count'].isna().sum()
    print(f"(PRE-TRANSFORM) Missing data count: {na_counts}")
    df['passenger_count'].fillna(0, inplace=True)

    na_counts = df['passenger_count'].isna().sum()
    print(f"(POST-TRANSFORM) Missing data count: {na_counts}")
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write Dataframe to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")   

    #using pandas to load data to BigQuery
    df.to_gbq(
        destination_table="trips_data_all.yellow_taxi_data",
        project_id="zoomcamp-user",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(name="Load Data From GCS To BigQuery")
def etl_gcs_to_bq():
    """Main ETL flow to load data to BigQuery Datawarehouse"""  
    color = "yellow"
    year = 2021
    month = 1
    dataset_file = f"{color}_tripdata_{year}-{month:02}"

    path = extract_from_gcs(color, year, month, dataset_file)
    df = transform(path)
    write_bq(df)

if __name__=="__main__":
    etl_gcs_to_bq()