FROM python:3.8.10

WORKDIR /data_monitoring

COPY requirements.txt /data_monitoring/requirements.txt

RUN pip install -r requirements.txt

COPY . /data_monitoring

CMD ["cd streamlit-app/","streamlit run app.py"]