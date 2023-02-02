# Week 2 - Homework 

## Question 1: Load January 2020 data
>Using the `etl_web_to_gcs.py` flow that loads taxi data into GCS as a guide, create a flow that loads the green taxi CSV dataset for January 2020 into GCS and run it. Look at the logs to find out how many rows the dataset has.
>
>How many rows does that dataset have?

Since we already have the flow, we can just change our parameters when deploying in Prefect:

```bash
prefect deployment run etl-parent-flow/docker-flow --params '{"months":[1], "year":2020, "color":"green"}'
```

The total number of rows is 447,770.

>NOTE: Make sure `data/green` folder is available locally. I also encounetered an error which states that there are no `tpep_pickup_datetime` and `tpep_dropoff_datetime` columns. Checking the data, the columns are named as `lpep_pickup_datetime` and `lpep_dropoff_datetime` for the green taxi data.

## Question 2: Scheduling with Cron
>Cron is a common scheduling specification for workflows.
>
>Using the flow in `etl_web_to_gcs.py`, create a deployment to run on the first of every month at 5am UTC. What’s the cron schedule for that?

```python
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parameterized_flow import etl_parent_flow
from prefect.orion.schemas.schedules import CronSchedule

docker_block = DockerContainer.load("zoomcamp-docker-container")
docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="sample-flow",
    infrastructure=docker_block
    schedule=(CronSchedule(cron="0 5 1 * *", timezone="UTC"))
)

if __name__=="__main__":
    docker_dep.apply()
```

The cron schedule for running our deployment at 5AM UTC every month is `0 5 1 * *`.

## Question 3: Loading data into BigQuery
>Using `etl_gcs_to_bq.py` as a starting point, modify the script for extracting data from GCS and loading it into BigQuery. This new script should not fill or remove rows with missing values. (The script is really just doing the E and L parts of ETL).
>
>The main flow should print the total number of rows processed by the script. Set the flow decorator to log the print statement.
>
>Parametrize the entrypoint flow to accept a list of months, a year, and a taxi color.
>
>Make any other necessary changes to the code for it to function as required.
>
>Create a deployment for this flow to run in a local subprocess with local flow code storage (the defaults).
>
>Make sure you have the parquet data files for Yellow taxi data for Feb. 2019 and March 2019 loaded in GCS. Run your deployment to append this data to your BiqQuery table. How many rows did your flow code process?

Before running our deployment, we first ingest yellow taxi data for Feb 2019 and March 2019 and load them into GCS.
```bash
prefect deployment run etl-parent-flow/docker-flow --params '{"months":[2,3], "year":2019, "color":"yellow"}'
```

From the terminal, February 2019 yellow taxi data has 7,019,375 rows while March 2019 yellow taxi data has 7,832,545 rows for a total of 14,851,920 rows ingested for both.

>NOTE: Encountered <i>"Flow run infrastructure exited with non-zero status code 137"</i> error when I initially try to ingest both Feb and Mar data. It seems like Docker does not have enough RAM. It worked when I finally ingest each file separately.

Next, we parameterized the script of `etl_gcs_to_bq.py`:
```python
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
    counts = df['passenger_count'].shape[0]
    print(f"Number of row: {counts}")
    return df

@task(log_prints=True)
def write_bq(df: pd.DataFrame) -> None:
    """Write Dataframe to BigQuery"""
    gcp_credentials_block = GcpCredentials.load("zoom-gcp-creds")   

    df.to_gbq(
        destination_table="trips_data_all.yellow_taxi_data",
        project_id="zoomcamp-user",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append"
    )

@flow(name="Load Data From GCS To BigQuery")
def etl_gcs_to_bq(year, month, color):
    """Main ETL flow to load data to BigQuery Datawarehouse"""  
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    path = extract_from_gcs(color, year, month, dataset_file)
    df = transform(path)
    write_bq(df)

@flow()
def etl_parent_flow(
    months: list[int] = [2,3], year: int = 2019, color: str = "yellow"
):
    for month in months:
        etl_gcs_to_bq(year, month, color)

if __name__=="__main__":
    etl_parent_flow()
```

To run this, we will create a new deployment using Docker for running the flows:
```python
from prefect.infrastructure.docker import DockerContainer
from prefect.deployments import Deployment
from parameterized_gcs_to_bq import etl_parent_flow

docker_block = DockerContainer.load("zoomcamp-docker-container")
docker_dep = Deployment.build_from_flow(
    flow=etl_parent_flow,
    name="docker-bq-flow",
    infrastructure=docker_block
)

if __name__=="__main__":
    docker_dep.apply()
```

We then run this script to create our new Prefect deployment. Afterwards, we add any additional files in our Dockerfile, rebuild our Docker image, and push the image to our Docker repo. Finally, we can run our deployment using Prefect CLI:

```bash
prefect deployment run etl-parent-flow/docker-bq-flow
```