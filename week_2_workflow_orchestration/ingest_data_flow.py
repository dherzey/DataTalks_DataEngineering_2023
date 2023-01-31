#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from time import time
from datetime import timedelta
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector

#create caching using prefect task_input_hash
@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(params):
    url = params.url

    if url.endswith('.csv.gz'):
        filename = 'output.csv.gz'
    else:
        filename = 'output.csv'

    #download data as a csv file
    os.system(f"wget {url} -O {filename}")

    #To read and load the whole data, we read and split our dataset into chunks of n rows
    trips_iter = pd.read_csv(filename, iterator=True, chunksize=100000)
    trips = next(trips_iter)

    return trips

@task(log_prints=True)
def transform_data(df):
    passenger_count = df["passenger_count"].isin([0]).sum()
    print(f"(PRE-TRANSFORMED) Missing passenger count: {passenger_count}")

    #get only rows with nonzero passenger count
    df = df[df['passenger_count'] != 0]
    passenger_count = df["passenger_count"].isin([0]).sum()
    print(f"(POST-TRANSFORMED) Missing passenger count: {passenger_count}")

    return df

@task(log_prints=True, retries=3)
def main(df, params):
    table_name = params.table_name
    
    #connect to Postgres database
    connector_block = SqlAlchemyConnector.load('postgres-connector')
    with connector_block.get_connection(begin=False) as engine:
        #Insert table to our database. For this, we will initially insert only the table columns. 
        #Afterwards, we append the first chunk of data
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')

@flow(name="Subflow", log_prints=True)
def log_subflow(params):
    #Test subflow
    table_name = params.table_name
    print(f"Logging subflow for {table_name}")

@flow(name="Ingest Data")
def main_flow():

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    #specify the needed arguments
    parser.add_argument('--table_name', required=True, help='table name where ingested file is to be loaded in postgres')
    parser.add_argument('--url', required=True, help='file url to be ingested to postgres')

    args = parser.parse_args()

    log_subflow(args)
    raw_data = extract_data(args)
    data = transform_data(raw_data)
    main(data, args)

if __name__ == "__main__":
    main_flow()