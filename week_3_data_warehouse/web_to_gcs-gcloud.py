import os
import pyarrow
import pandas as pd
from pathlib import Path
from google.cloud import storage

init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/fhv/'
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

def web_to_gcs(service, year):

    # create parent directory if it does not exist
    path_dir = Path(f"data/{service}")
    path_dir.mkdir(parents=True, exist_ok=True)

    for month in range(12):

        file_name = f"{service}_tripdata_{year}-{month+1:02}.csv.gz"
        url = init_url + file_name
        local_file = path_dir/file_name

        # download data as a csv file or just proceed to read it directly
        os.system(f"wget {url} -O {local_file}")

        try:
            # read file and convert to csv/parquet
            df = pd.read_csv(local_file, compression="gzip")
            file_name = file_name.replace('.csv.gz','.csv')
            local_file = path_dir/file_name
            df.to_csv(local_file)
            # df.to_parquet(local_file, engine='pyarrow')
            print(f"Dataset {file_name} read with shape {df.shape}")
        except pd.errors.EmptyDataError:
            os.remove(local_file)
            print(f"Removed {local_file}")
        except FileNotFoundError:
            pass

        # upload parquet file to gcs 
        upload_to_gcs(BUCKET, f"data/{service}/{file_name}", local_file)
        print(f"File uploaded to GCS with path {local_file}")

web_to_gcs('fhv', 2019)