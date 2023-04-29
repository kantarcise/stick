""" This script contains a DAG that ecexutes sentiment analysis using Spark & SparkNLP. 
It's meant to be run after the crawler dag. Results will be in /results folder.
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from stick.spark import keyword_extraction_insights
import datetime as dt

default_arguments = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sezaiburakkantarci@gmail.com'],
    # we can set this up later: https://stackoverflow.com/a/61885444
    'email_on_failure': False,
    'email_on_retry': False,
}

spark_dag = DAG(
    dag_id="Keyword_Extraction_Insights",
    description="This DAG executes sentiment analysis on the content that is scraped from Reddit.",
    default_args=default_arguments,
    start_date= dt.datetime(2023, 4, 27 ),
    # every day at 9:40 pm GMT +03:00
    schedule_interval = "40 18 * * *",
    # schedule_interval = None,
)

starter = EmptyOperator(
    task_id= "Starter",
    dag=spark_dag,
)

sentiment_analysis = PythonOperator(
    task_id= "Extract_Valueable_Info",
    python_callable= keyword_extraction_insights.nlp_etl_prime,
    dag=spark_dag,
)

ending = EmptyOperator(
    task_id= "Ending",
    dag=spark_dag,
)

starter >> sentiment_analysis >> ending
