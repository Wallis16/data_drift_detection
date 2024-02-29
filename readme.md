## Run airflow

cd airflow

docker-compose build

docker-compose up -d

## Run streamlit

docker build -t visualize_drift:0.0.1 .

docker-compose up

## Run mlflow
cd mlflow

docker build -t mlflow_data_drift:0.0.1 .

docker-compose up
