FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y gcc
USER airflow

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
