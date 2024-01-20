import os
import pandas as pd

from pymongo import MongoClient
from evidently.pipeline.column_mapping import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataQualityPreset
from dotenv import load_dotenv

load_dotenv()

username = os.getenv('MONGODB_USER')
password = os.getenv('MONGODB_PASSWORD')
database = os.getenv('MONGODB_DATABASE')
collection_name = os.getenv('MONGODB_COLLECTION')

def retrieve_from_mongodb(username: str, password: str, database: str,
                     collection_name: str):

    mongo_uri = f'mongodb+srv://{username}:{password}@{database}.hi7evkw.mongodb.net/'

    client = MongoClient(mongo_uri)
    db = client[database]
    collection = db[collection_name]

    documents = collection.find()
    
    return pd.DataFrame(documents)

import argparse

if __name__ == "__main__":
    # Define and parse command-line arguments
    parser = argparse.ArgumentParser(description="reference data for comparing")
    parser.add_argument("--reference", default=1, help="reference")
    parser.add_argument("--current", default=2, help="reference")
    args = parser.parse_args()
    
    reference_day = int(args.reference)
    current_day = int(args.current)
    
    reference = retrieve_from_mongodb(username, password, database, collection_name+"_"+str(reference_day))
    reference = reference[['passenger_count', 'trip_distance', 'RatecodeID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']]

    current = retrieve_from_mongodb(username, password, database, collection_name+"_"+str(current_day))
    current = current[['passenger_count', 'trip_distance', 'RatecodeID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']]

    numerical_features = ['passenger_count', 'trip_distance', 'RatecodeID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']

    column_mapping = ColumnMapping()
    column_mapping.numerical_features = numerical_features

    data_quality_report = Report(metrics=[DataQualityPreset()])
    data_quality_report.run(
        reference_data=reference,
        current_data=current,
        column_mapping=column_mapping
    )

    data_quality_report_path = f'reports/nyc_taxi/results/2023_01_0{reference_day}__2023_01_0{current_day}/'
    try:
        os.makedirs(data_quality_report_path)
    except:
        pass
    data_quality_report.save_html(data_quality_report_path+'data_quality.html')