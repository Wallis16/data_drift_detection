FROM python:3.8.10

WORKDIR /data_monitoring

COPY requirements.txt /data_monitoring/requirements.txt

RUN pip install -r requirements.txt

COPY . /data_monitoring

RUN cd streamlit-app/

ENTRYPOINT ["streamlit", "run", "app.py", "--server.port=8501", "--server.address=0.0.0.0"]
