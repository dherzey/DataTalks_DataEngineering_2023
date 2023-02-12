import os
import pyarrow
import argparse
import pandas as pd
from pathlib import Path
from google.cloud import storage

BUCKET = os.environ.get("GCP_GCS_BUCKET", "dtc_data_lake_zoomcamp-user")

def upload_to_gcs(bucket, object_name, local_file):

    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)

def web_to_gcs(params):

    service = params.service
    year = params.year

    init_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/'

    # create parent directory if it does not exist
    path_dir = Path(f"data/{service}")
    path_dir.mkdir(parents=True, exist_ok=True)

    for month in range(12):

        file_name = f"{service}_tripdata_{year}-{month+1:02}.csv.gz"
        url = init_url + file_name

        # read file and convert to csv/parquet
        df_iter = pd.read_csv(url, iterator=True, chunksize=1000000)

        count = 0
        while True:
            try:
                df = next(df_iter)

                if service=="yellow":
                    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
                    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
                elif service=="green":
                    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
                    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
                else:
                    pass
        
                new_file = file_name.replace('.csv.gz',f'_part{count+1:02}.parquet')
                local_file = path_dir/new_file            
                # df.to_csv(local_file)
                df.to_parquet(local_file, compression="gzip", engine='pyarrow')
                print(f"Dataset {new_file} read with shape {df.shape}")
                count += 1
            except pd.errors.EmptyDataError:
                pass
            except StopIteration:
                break

            # upload file to gcs 
            upload_to_gcs(BUCKET, f"data/{service}/{new_file}", local_file)
            print(f"File uploaded to GCS with path {local_file}")

if __name__=="__main__":
    parser = argparse.ArgumentParser(description='Ingest CSV data to GCS')

    #specify the needed arguments
    parser.add_argument('--service', required=True, help='service or NY taxi data type')
    parser.add_argument('--year', required=True, help='NY taxi data year')

    args = parser.parse_args()
    web_to_gcs(args)