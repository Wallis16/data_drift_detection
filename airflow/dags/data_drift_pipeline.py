from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago

from dotenv import load_dotenv
from data_drift_analysis.notification import notification
from data_drift_analysis.analysis import analysis, quality_report

import os

load_dotenv()

username = os.getenv('MONGODB_USER')
password = os.getenv('MONGODB_PASSWORD')
database = os.getenv('MONGODB_DATABASE')
collection_name = os.getenv('MONGODB_COLLECTION')

sender_email = os.getenv('SENDER_EMAIL')
receiver_email = os.getenv('RECEIVER_EMAIL')
email_password = os.getenv('EMAIL_PASSWORD')

reference_day = 1
current_day = 4

default_args = {
    'owner': 'diogenes',
    'start_date': days_ago(1),
}

@task()
def data_drift_analysis_task(username, password, database, collection_name, reference_day, current_day):
    return analysis(username, password, database, collection_name, reference_day, current_day)

@task()
def bad_notification_task(sender_email, receiver_email, password, subject, body, mode, reference_day, current_day):
    notification(sender_email, receiver_email, password, subject, body, mode, reference_day, current_day)

@task()
def good_notification_task(sender_email, receiver_email, password, subject, body, mode, reference_day, current_day):
    notification(sender_email, receiver_email, password, subject, body, mode, reference_day, current_day)

@task()
def data_quality_task(username, password, database, collection_name, reference_day, current_day):
    quality_report(username, password, database, collection_name, reference_day, current_day)

# Define the condition function
def condition_function(**kwargs):
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='data_drift_analysis_task')
    return 'bad_notification_task' if result else 'good_notification_task'

with DAG(dag_id='data_drift', schedule_interval='@once', default_args=default_args, catchup=False) as dag:

    # Execute the data drift analysis task
    results_data_drift_analysis = data_drift_analysis_task(username, password, database, collection_name, reference_day, current_day)

    # Use BranchPythonOperator to create a conditional branch
    branching_task = BranchPythonOperator(
        task_id='branching_task',
        python_callable=condition_function,
        provide_context=True,
    )

    # Define downstream tasks for each branch
    subject = 'Data drift detected'
    body = 'Please, check the data drift analysis attached to this email.'
    mode = 'data_drift.html'
    bad_notification_task = bad_notification_task(sender_email, receiver_email, email_password, subject, body, mode, reference_day, current_day)

    subject = 'Data drift not detected'
    body = 'Check the data drift analysis attached to this email.'
    mode = 'data_drift.html'
    good_notification_task = good_notification_task(sender_email, receiver_email, email_password, subject, body, mode, reference_day, current_day)

    data_quality_task = data_quality_task(username, password, database, collection_name, reference_day, current_day)

    # Set up the DAG structure
    results_data_drift_analysis >> branching_task >> [bad_notification_task, good_notification_task]
    bad_notification_task >> data_quality_task
