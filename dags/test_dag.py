# import tweepy
import pandas as pd
from datetime import datetime, timedelta
import csv
from pathlib import Path
import json
from urllib.request import urlopen

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

csv_path = Path("/opt/airflow/data/tweets.csv")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 11, 28),
    'email': ['daffaromero@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'schedule_interval': '@once',
}

dag = DAG(
    'twitter_dag1',
    default_args=default_args,
    description='Twitter scraping DAG, presumably',
)

# Define task1
task1 = BashOperator(
    task_id="echo1",
    bash_command="echo Start scraping.",
    dag=dag,
)


def run_twitter_etl():
    # Twitter authentication
    url = "https://ibnux.github.io/BMKG-importer/cuaca/501190.json"
    # response = requests.get(url)
    response = urlopen(url)
    data_json = json.loads(response.read())
    with open("weatherDIY.json", "w") as outfile:
        json.dump(data_json, outfile)

    print(data_json)
    return data_json
    # df.to_csv(r'C:\Users\daffa\docker\airflow\refined_tweets.csv')


task2 = PythonOperator(
    task_id='tweet_scrape_etl',
    python_callable=run_twitter_etl,
    dag=dag,
)


def write_csv(**kwargs):
    # Xcoms to get the list
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='tweet_scrape_etl')
    df = pd.DataFrame(df)

    try:
        print(df)
        df.to_csv(csv_path, index=False, header=True)
        return True
    except OSError as e:
        print(e)
        return False


task3 = PythonOperator(
    task_id="writing_csv",
    python_callable=write_csv,
    provide_context=True,
    dag=dag)


def confirmation(**kwargs):
    """
    If everything is done properly, print "Done!!!!!!"
    Otherwise print "Failed."
    """

    # Xcoms to get status which is the return value of write_csv().
    ti = kwargs['ti']
    status = ti.xcom_pull(task_ids='writing_csv')

    if status:
        print("Done!!!!!!")
    else:
        print("Failed.")


task4 = PythonOperator(
    task_id="confirmation",
    python_callable=confirmation,
    provide_context=True,
    dag=dag
)

task1 >> task2 >> task3 >> task4