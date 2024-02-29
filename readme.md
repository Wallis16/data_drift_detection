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

Please, check the Medium post related to this repository: [data monitoring](https://medium.com/@diogeneswallis/pipeline-for-data-drift-detection-and-retraining-bd083221320b)

<div align="center">
  
![GIFMaker_me](https://github.com/Wallis16/data_monitoring/assets/26671424/1b2686b0-9023-4918-b2fb-5707866d6d37)

</div>
