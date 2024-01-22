from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

from dotenv import load_dotenv

from data_drift_analysis.notification import email_notification
from data_drift_analysis.analysis import analysis, quality_report

import os

load_dotenv()

username = os.getenv('MONGODB_USER')
password = os.getenv('MONGODB_PASSWORD')
database = os.getenv('MONGODB_DATABASE')
collection_name = os.getenv('MONGODB_COLLECTION')

reference_day = 1
current_day = 3

default_args = {
    'owner': 'diogenes',
    'start_date': days_ago(1),
}

@task()
def data_drift_analysis(username, password, database, collection_name, reference_day, current_day):
    return analysis(username, password, database, collection_name, reference_day, current_day)

@task()
def bad_notification():
    return

@task()
def good_notification():
    return

@task()
def data_quality(username, password, database, collection_name, reference_day, current_day):
    return quality_report(username, password, database, collection_name, reference_day, current_day)

with DAG(dag_id = 'data_drift', schedule_interval='@once',
         default_args=default_args, catchup=False) as dag:

    results_data_drift_analysis = data_drift_analysis(username, password, database,
                                                       collection_name, reference_day, current_day)

    if results_data_drift_analysis['metrics'][0]['result']['dataset_drift'] == True:
        bad_notification(results_data_drift_analysis)
        data_quality(username, password, database,
                          collection_name, reference_day, current_day)

    else:
        good_notification(results_data_drift_analysis)
