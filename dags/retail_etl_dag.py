from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import json

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

def check_for_files():
    files = os.listdir('/opt/airflow/data/incoming')
    if not files:
        raise FileNotFoundError("No files found in /opt/airflow/data/incoming")
    return files

def process_files():
    files = os.listdir('/opt/airflow/data/incoming')
    summaries = []

    for file in files:
        df = pd.read_csv(f'/opt/airflow/data/incoming/{file}')
        summary = {
            'file': file,
            'rows': len(df),
            'columns': list(df.columns)
        }
        summaries.append(summary)

    with open('/opt/airflow/data/outgoing/summary.json', 'w') as f:
        json.dump(summaries, f)

def mock_upload():
    print("Simulating upload to cloud... done.")

with DAG('retail_etl_dag', schedule_interval=None, default_args=default_args, catchup=False) as dag:
    t1 = PythonOperator(
        task_id='check_files',
        python_callable=check_for_files
    )

    t2 = PythonOperator(
        task_id='process_files',
        python_callable=process_files
    )

    t3 = PythonOperator(
        task_id='upload_to_cloud',
        python_callable=mock_upload
    )

    t1 >> t2 >> t3

