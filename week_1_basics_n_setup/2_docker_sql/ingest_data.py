#!/usr/bin/env python
# coding: utf-8

import os
import argparse
import pandas as pd
from time import time
from sqlalchemy import create_engine

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url

    if url.endswith('.csv.gz'):
        filename = 'output.csv.gz'
    else:
        filename = 'output.csv'

    #download data as a csv file
    os.system(f"wget {url} -O {filename}")

    #connect to Postgres database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    #To read and load the whole data, we read and split our dataset into chunks of n rows
    trips_iter = pd.read_csv(filename, iterator=True, chunksize=100000)
    trips = next(trips_iter)

    #Insert table to our database. For this, we will initially insert only the table columns. 
    #Afterwards, we append the first chunk of data
    trips.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    trips.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:            
            time_start = time()
            
            trips = next(trips_iter)
            trips.to_sql(name=table_name, con=engine, if_exists='append')
            
            time_end = time()
            
            print("inserted another chunk, took {:.3f} seconds".format(time_end-time_start))
            
        except StopIteration as e:
            print("Ingestion of data to Postgres database is now done.")
            break

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    #specify the needed arguments
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='hostname for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='table name where ingested file is to be loaded in postgres')
    parser.add_argument('--url', required=True, help='file url to be ingested to postgres')

    args = parser.parse_args()
    main(args)
