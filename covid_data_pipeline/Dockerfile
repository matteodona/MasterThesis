FROM apache/airflow:2.8.3

USER root

RUN apt update -y && apt install -y openjdk-17-jdk 

USER airflow

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir --user -r requirements.txt
