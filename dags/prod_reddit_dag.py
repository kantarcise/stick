""" This script contains a really simple DAG that fetches data
from the most popular subreddits in the world. It runs everyday at 10 pm, GMT+3. The saved content
 will be in data/raw and data/comments
"""

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from stick.crawler import reddit_crawler
import datetime as dt

default_arguments = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['sezaiburakkantarci@gmail.com'],
    # we can set this up later: https://stackoverflow.com/a/61885444
    'email_on_failure': False,
    'email_on_retry': False,
}

reddit_dag = DAG(
    dag_id="Reddit_Scraper",
    description="This DAG fetches data from the most popular  subreddits in the world, runs daily.",
    default_args=default_arguments,
    start_date= dt.datetime(2023, 4, 27 ),
    # every day at 9 pm GMT +03:00
    schedule_interval= "0 18 * * *",
    # schedule_interval = None,
)

starter = EmptyOperator(
    task_id= "Starter",
    dag=reddit_dag,
)

scrape_data = PythonOperator(
    task_id= "Scrape_Data",
    python_callable= reddit_crawler.prime,
    dag=reddit_dag,
)

ending = EmptyOperator(
    task_id= "Ending",
    dag=reddit_dag,
)

starter >> scrape_data >> ending