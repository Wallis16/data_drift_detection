FROM apache/airflow:2.8.1
USER root
RUN apt-get update -y && apt-get install -y gcc && apt-get install -y git

USER ${AIRFLOW_UID}
COPY requirements.txt .
RUN pip install -r requirements.txt
