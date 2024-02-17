"""<>"""
import os
from dotenv import load_dotenv
from data_drift_analysis.notification import notification
from data_drift_analysis.analysis import analysis, quality_report
from ml.train import skelarn_model
from airflow import DAG
from airflow.operators.python_operator import BranchPythonOperator
from airflow.decorators import task
from airflow.utils.dates import days_ago


load_dotenv()

username_ = os.getenv('MONGODB_USER')
password_ = os.getenv('MONGODB_PASSWORD')
database_ = os.getenv('MONGODB_DATABASE')
collection_name_ = os.getenv('MONGODB_COLLECTION')

sender_email_ = os.getenv('SENDER_EMAIL')
receiver_email_ = os.getenv('RECEIVER_EMAIL')
email_password_ = os.getenv('EMAIL_PASSWORD')

REFERENCE_DAY = 1
CURRENT_DAY = 4

default_args = {
    'owner': 'diogenes',
    'start_date': days_ago(1),
}

@task()
def data_drift_analysis_task(username, password, database,
        collection_name, reference_day, current_day):
    """<>"""
    return analysis(username, password, database, collection_name,
                     reference_day, current_day)

@task()
def bad_notification_task(sender_email, receiver_email, password,
        subject, body, mode, reference_day, current_day):
    """<>"""
    notification(sender_email, receiver_email, password,
                  subject, body, mode, reference_day, current_day)

@task()
def good_notification_task(sender_email, receiver_email, password,
                           subject, body, mode, reference_day, current_day):
    """<>"""
    notification(sender_email, receiver_email, password, subject,
                 body, mode, reference_day, current_day)

@task()
def data_quality_task(username, password, database, collection_name,
                       reference_day, current_day):
    """<>"""
    quality_report(username, password, database, collection_name,
                    reference_day, current_day)

@task()
def model_retraining_task(username: str, password: str, database: str,
                          collection_name: str, reference_day: int, current_day: int):
    """<>"""
    skelarn_model(username, password, database, collection_name,
                   reference_day, current_day)

# Define the condition function
def condition_function(**kwargs):
    """<>"""
    ti = kwargs['ti']
    result = ti.xcom_pull(task_ids='data_drift_analysis_task')
    return 'good_notification_task' if result else 'bad_notification_task'

with DAG(dag_id='data_drift', schedule_interval='@once',
         default_args=default_args, catchup=False) as dag:

    # Execute the data drift analysis task
    results_data_drift_analysis = data_drift_analysis_task(username_, password_,
            database_, collection_name_, REFERENCE_DAY, CURRENT_DAY)

    # Use BranchPythonOperator to create a conditional branch
    branching_task = BranchPythonOperator(
        task_id='branching_task',
        python_callable=condition_function,
        provide_context=True,
    )

    # Define downstream tasks for each branch
    SUBJECT_ = 'Data drift detected'
    BODY_ = 'Please, check the data drift analysis attached to this email.'
    MODE_ = 'data_drift.html'
    BAD_NOTIFICATION_TASK = bad_notification_task(sender_email_, receiver_email_,
                email_password_, SUBJECT_, BODY_, MODE_, REFERENCE_DAY, CURRENT_DAY)

    _SUBJECT = 'Data drift not detected'
    _BODY = 'Check the data drift analysis attached to this email.'
    _MODE = 'data_drift.html'
    GOOD_NOTIFICATION_TASK = good_notification_task(sender_email_, receiver_email_,
        email_password_, _SUBJECT, _BODY, _MODE, REFERENCE_DAY, CURRENT_DAY)

    DATA_QUALITY_TASK = data_quality_task(username_, password_, database_,
                                    collection_name_, REFERENCE_DAY, CURRENT_DAY)

    MODEL_RETRAINING_TASK = model_retraining_task(username_, password_, database_,
        collection_name_, REFERENCE_DAY, CURRENT_DAY)

    # Set up the DAG structure
    results_data_drift_analysis >> branching_task >> [GOOD_NOTIFICATION_TASK,
                                                       BAD_NOTIFICATION_TASK]
    BAD_NOTIFICATION_TASK >> DATA_QUALITY_TASK >> MODEL_RETRAINING_TASK
