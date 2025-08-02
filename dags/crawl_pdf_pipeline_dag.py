from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pendulum import timezone

import sys
import os

# src 모듈 import 위해 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from main import main as run_etl_batch

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_batch_pipeline',
    default_args=default_args,
    description='크롤링 → OpenAI 추출 → 전처리 → MySQL 업로드',
    schedule_interval='0 20 * * *',  # 매일 오후 8시
    start_date=datetime(2025, 8, 1, tzinfo=timezone("Asia/Seoul")),
    catchup=False,
    tags=['etl', 'openai', 'crawler'],
) as dag:

    run_main = PythonOperator(
        task_id='run_main_pipeline',
        python_callable=run_etl_batch,
    )

    run_main


# with DAG("crawl_pdf_pipeline", start_date=datetime(2023, 1, 1), schedule_interval="@daily", catchup=False) as dag:

#     crawl = PythonOperator(
#         task_id="crawl_data",
#         python_callable=crawl_data,
#     )

#     extract = PythonOperator(
#         task_id="extract_text",
#         python_callable=extract_text,
#     )

#     parse = PythonOperator(
#         task_id="parse_text",
#         python_callable=parse_text,
#     )

#     clean = PythonOperator(
#         task_id="clean_data",
#         python_callable=clean_data,
#     )

#     load = PythonOperator(
#         task_id="load_to_mysql",
#         python_callable=load_to_mysql,
#     )

#     crawl >> extract >> parse >> clean >> load
