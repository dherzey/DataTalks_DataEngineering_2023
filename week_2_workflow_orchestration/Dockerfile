FROM prefecthq/prefect:2.7.8-python3.9

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

COPY data /opt/prefect/data
COPY etl_web_to_gcs.py /opt/prefect/flows/etl_web_to_gcs.py
COPY etl_gcs_to_bq.py /opt/prefect/flows/etl_gcs_to_bq.py
COPY parameterized_flow.py /opt/prefect/flows/parameterized_flow.py
COPY parameterized_gcs_to_bq.py /opt/prefect/flows/parameterized_gcs_to_bq.py
COPY docker_deploy_bq.py /opt/prefect/flows/docker_deploy_bq.py