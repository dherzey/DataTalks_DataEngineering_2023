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

To run this, we will create a new deployment with a Docker infrastructure for running the flows. Note that instead of running our dpeloyment in Docker, we can run it in a local subprocess without any infrastructure.
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

## Question 4: Github Storage Block
>Using the `web_to_gcs` script from the videos as a guide, you want to store your flow code in a GitHub repository for collaboration with your team. Prefect can look in the GitHub repo to find your flow code and read it. Create a GitHub storage block from the UI or in Python code and use that in your Deployment instead of storing your flow code locally or baking your flow code into a Docker image.
>
>Note that you will have to push your code to GitHub, Prefect will not push it for you.
>
>Run your deployment in a local subprocess (the default if you don’t specify an infrastructure). Use the Green taxi data for the month of November 2020.
>
>How many rows were processed by the script?

Since I already have my repo in Github, I proceeded to create a new Github block using Python. 

```bash
pip install prefect-github
```

```bash
prefect block register -m prefect_github
```

```python
from prefect_github.repository import GitHubRepository

github_block = GitHubRepository(
    repository_url="https://github.com/dherzey/DataTalks_DataEngineering_2023.git"
)

github_block.save("zoomcamp-github", overwrite=True)
```

We can then create our deployment by directing our deployment storage to our Github repo:

```python
from prefect.deployments import Deployment
from prefect_github.repository import GitHubRepository

import sys
sys.path.insert(0, "/home/jdtganding/Documents/data-engineering-zoomcamp")

from week_2_workflow_orchestration.parameterized_flow import etl_parent_flow

github_block = GitHubRepository.load("zoomcamp-github")
github_dep = Deployment.build_from_flow(
    flow = etl_parent_flow,
    name = "flow-github-storage",
    storage = github_block
)

if __name__=="__main__":
    github_dep.apply()
```
>NOTE: I am not sure how to direct my deployment specifically to the `week_2_workflow_orchestration` folder. I tried adding the `path` argument in `build_from_flow()` but I still receive the `parameterized_flow.py` not found error for some reason.

We run this Python script to create our deployment and run the deployment itself.

```bash
prefect deployment run etl-parent-flow/flow-github-storage -p "months=[11]" -p "year=2020" -p "color=green"
```

There are 88,605 rows for green taxi data within November 2020.

>NOTE: I kept encountering an "<i>OSError: Cannot save file into a non-existent directory: 'data/green'</i>." To resolve this, I changed the path in the `write_local()` function of the parameterized_flow script such that it will create the parent directory if it does not exist. See modified part below:
```python
@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write Dataframe out locally as parquet file"""
    path_dir = Path(f"data/{color}")
    filename = f"{dataset_file}.parquet"
    path_dir.mkdir(parents=True, exist_ok=True)
    path = path_dir/filename
    df.to_parquet(path, compression="gzip")
    return path
```

## Question 5: Email or Slack notification

>It’s often helpful to be notified when something with your dataflow doesn’t work as planned. Choose one of the options below for creating email or slack notifications.
>
>The hosted Prefect Cloud lets you avoid running your own server and has Automations that allow you to get notifications when certain events occur or don’t occur.
>
>Create a free forever Prefect Cloud account at app.prefect.cloud and connect your workspace to it following the steps in the UI when you sign up.
>
>Set up an Automation that will send yourself an email when a flow run completes. Run the deployment used in Q4 for the Green taxi data for April 2019. Check your email to see the notification.
>
>Alternatively, use a Prefect Cloud Automation or a self-hosted Orion server Notification to get notifications in a Slack workspace via an incoming webhook.
>
>In the Prefect Cloud UI create an Automation or in the Prefect Orion UI create a Notification to send a Slack message when a flow run enters a Completed state.
>
>How many rows were procesed by the script?

Since I opted to use the Prefect cloud, I will need to re-create my blocks and deployments to my cloud workspace. First, I made sure that my Prefect profile is interacting with my cloud account via API:

```bash
#switch prefect profile if you want to use another profile
prefect profile use cloud-user

#configure settings for this profile using workspace API
prefect config set PREFECT_API_URL="https://api.prefect.cloud/api/accounts/[ACCOUNT-ID]/workspaces/[WORKSPACE-ID]"

prefect config set PREFECT_API_KEY="[API-KEY]"

#login to Prefect cloud
prefect cloud login

#re-run Prefect agent
prefect agent start -q default
```

Next, we can re-create all our needed blocks and deployments. In particular, I re-created the GCP and Github blocks, and the Github deployment for this exercise.
<ul>
<li><b>gcp_blocks.py</b>: create both GCP credential block and GCS bucket block</li>
<li><b>github_block.py</b>: create Github repo block</li>
<li><b>github_deploy.py</b>: create our web to GCS ingestion deployment which takes in file from our Github repo</li>
</ul>

In the Prefect cloud, we can add a block called email to send notifications externally. Then, we can attach this block to our Automation. For this, we automate the sending of email when our `etl-parent-flow` has completed its run successfully.

Testing our setup, we run our deployment to ingest green taxi data for April 2019:

```bash
prefect deployment run etl-parent-flow/flow-github-storage -p "months=[4]" -p "year=2019" -p "color=green"
```

The data ingested has a total of 514,392 rows. The email notification has also been successfully setup:

![Email notification](/week_2_workflow_orchestration/img_md/notif_week2.PNG "Email notification")

## Question 6: Secrets

>Prefect Secret blocks provide secure, encrypted storage in the database and obfuscation in the UI. Create a secret block in the UI that stores a fake 10-digit password to connect to a third-party service. Once you’ve created your block in the UI, how many characters are shown as asterisks (*) on the next page of the UI?

The 10-digit fake password is shown to have 8 asterisks in the UI:

![Secret Block](/week_2_workflow_orchestration/img_md/secret-block-sample.PNG "Secret Block in UI")