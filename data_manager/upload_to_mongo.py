from pymongo import MongoClient
from dotenv import load_dotenv

import os
import pandas as pd
import pyarrow.parquet as pq

load_dotenv()

username = os.getenv('MONGODB_USER')
password = os.getenv('MONGODB_PASSWORD')
database = os.getenv('MONGODB_DATABASE')
collection_name = os.getenv('MONGODB_COLLECTION')
data_path = os.getenv('DATA_STAGING')

def read_from_parquet(table_path, filter_day, reference_column):
    tables = []
    
    for file in os.listdir(table_path):
        table = pq.read_table(table_path+file)
        table = table.to_pandas()

        table["datetime"] = pd.to_datetime(table[reference_column], format = "%Y-%m-%d %H:%M:%S")
        filtered = table.loc[(table['datetime'].dt.day == filter_day)]
        tables.append(filtered)

    return pd.concat(tables, axis=0)

def load_to_mongodb(username: str, password: str, database: str,
                     collection_name: str, table_path: str, filter_day: int, reference_column = int):

    # Replace with your MongoDB Atlas connection string
    mongo_uri = f'mongodb+srv://{username}:{password}@{database}.hi7evkw.mongodb.net/'

    # Connect to MongoDB Atlas
    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[collection_name]

    table = read_from_parquet(table_path, filter_day, reference_column)

    # Insert the data into MongoDB
    collection.insert_many(table.to_dict(orient = 'records'))

    # Close the MongoDB connection
    client.close()

import argparse

if __name__ == "__main__":
    # Define and parse command-line arguments
    parser = argparse.ArgumentParser(description="Filtering using day")
    parser.add_argument("--day", default=1, help="Filter by day")
    args = parser.parse_args()
    
    filter_day = int(args.day)

    load_to_mongodb(username, password, database, collection_name+"_"+str(filter_day), data_path,filter_day, reference_column = "tpep_pickup_datetime")