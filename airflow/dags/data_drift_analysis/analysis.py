import os
import pandas as pd

from pymongo import MongoClient
from evidently.pipeline.column_mapping import ColumnMapping
from evidently.report import Report
from evidently.metric_preset import DataDriftPreset, DataQualityPreset
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

def analysis(username, password, database, collection_name, reference_day, current_day):

    numerical_features = ['passenger_count', 'trip_distance', 'RatecodeID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']

    reference = retrieve_from_mongodb(username, password, database, collection_name+'_'+str(reference_day))
    reference = reference[numerical_features]

    current = retrieve_from_mongodb(username, password, database, collection_name+'_'+str(current_day))
    current = current[numerical_features]

    column_mapping = ColumnMapping()
    column_mapping.numerical_features = numerical_features

    data_drift_report = Report(metrics=[DataDriftPreset()])
    data_drift_report.run(
        reference_data=reference,
        current_data=current,
        column_mapping=column_mapping
    )

    data_drift_report_path = f'airflow/dags/reports/nyc_taxi/results/2023_01_0{reference_day}__2023_01_0{current_day}/'
    try:
        os.makedirs(data_drift_report_path)
    except:
        pass
    data_drift_report.save_html(data_drift_report_path+'data_drift.html')
    return data_drift_report.as_dict()

def quality_report(username, password, database, collection_name, reference_day, current_day):

    numerical_features = ['passenger_count', 'trip_distance', 'RatecodeID', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount', 'congestion_surcharge', 'airport_fee']

    reference = retrieve_from_mongodb(username, password, database, collection_name+'_'+str(reference_day))
    reference = reference[numerical_features]

    current = retrieve_from_mongodb(username, password, database, collection_name+'_'+str(current_day))
    current = current[numerical_features]

    column_mapping = ColumnMapping()
    column_mapping.numerical_features = numerical_features

    data_quality_report = Report(metrics=[DataQualityPreset()])
    data_quality_report.run(
        reference_data=reference,
        current_data=current,
        column_mapping=column_mapping
    )

    data_quality_report_path = f'airflow/dags/reports/nyc_taxi/results/2023_01_0{reference_day}__2023_01_0{current_day}/'
    try:
        os.makedirs(data_quality_report_path)
    except:
        pass
    data_quality_report.save_html(data_quality_report_path+'data_quality.html')
    return data_quality_report.as_dict()

reference_day, current_day = 1, 2
print(analysis(username, password, database, collection_name, reference_day, current_day))
