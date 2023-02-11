from prefect_gcp import GcpCredentials
from prefect_gcp.cloud_storage import GcsBucket

# import json
# service_info_json = json.load(open("zoomcamp-user-bfecc07e3f66.json"))

credentials_block = GcpCredentials(
    service_account_file = '/home/jdtganding/Documents/data-engineering-zoomcamp/service_account_key.json'
)
credentials_block.save("zoom-gcp-creds", overwrite=True)


bucket_block = GcsBucket(
    gcp_credentials = GcpCredentials.load("zoom-gcp-creds"),
    bucket = "dtc_data_lake_zoomcamp-user",  # insert your  GCS bucket name
)

bucket_block.save("zoom-gcs", overwrite=True)