import os
import pyarrow
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

def web_to_gcs(service, year):

    init_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{service}/'

    # create parent directory if it does not exist
    path_dir = Path(f"data/{service}")
    path_dir.mkdir(parents=True, exist_ok=True)

    for month in range(12):

        file_name = f"{service}_tripdata_{year}-{month+1:02}.csv.gz"
        url = init_url + file_name

        try:
            # read file and convert to csv/parquet
            df = pd.read_csv(url)
            file_name = file_name.replace('.csv.gz','.csv')
            local_file = path_dir/file_name            
            df.to_csv(local_file)
            # df.to_parquet(local_file, compression="gzip", engine='pyarrow')
            print(f"Dataset {file_name} read with shape {df.shape}")
        except pd.errors.EmptyDataError:
            pass

        # upload file to gcs 
        upload_to_gcs(BUCKET, f"data/{service}/{file_name}", local_file)
        print(f"File uploaded to GCS with path {local_file}")

# web_to_gcs('fhv', 2019)
web_to_gcs('green', 2019)