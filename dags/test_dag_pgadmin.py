# import tweepy
# import pandas as pd
from datetime import datetime, timedelta
# import csv
# from pathlib import Path
import json
from urllib.request import urlopen

import pandas as pd
import csv
from pathlib import Path
from datetime import timedelta
import glob
import psycopg2 as pg

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['email@mail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    # 'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'Weather_ETL_DAG_test',
    default_args=default_args,
    description='twitter crawling DAG',
    schedule_interval=timedelta(days=1),
)



def extract_data(**kwargs):
    url = "https://ibnux.github.io/BMKG-importer/cuaca/501190.json"
    # response = requests.get(url)
    response = urlopen(url)
    data_json = json.loads(response.read())
    with open("weatherDIY.json", "w") as outfile:
        json.dump(data_json, outfile)

    print(data_json)
    return data_json


csv_path = Path("/opt/airflow/data/weather_crawl.csv")


def transform_save_data(**kwargs):
    # Xcoms to get the list
    ti = kwargs['ti']
    value = ti.xcom_pull(task_ids='crawling_data')
    df = pd.DataFrame(value)
    df1 = df.dropna()

    try:
        print(df1)
        df1.to_csv(csv_path, index=False, header=True)
        return True
    except OSError as e:
        print(e)
        return False


def LoadDB():
    try:
        conn = pg.connect(
            "dbname='airflow' user='airflow' host='airflow-postgres-1' password='airflow'"
        )
    except Exception as error:
        print(error)

    path = "/opt/airflow/data/*.csv"
    glob.glob(path)
    for fname in glob.glob(path):
        fname = fname.split('/')
        csvname = fname[-1]
        csvname = csvname.split('.')
        tablename = str(csvname[0])

    # jamCuaca,kodeCuaca,cuaca,humidity,tempC,tempF
    # create the table if it does not already exist
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_""" + tablename + """ (
                jamCuaca varchar(50),
                kodeCuaca varchar(50),
                cuaca varchar(50),
                humidity varchar(50),
                tempC varchar(50),
                tempF varchar(50)
            );
        """
                       )
        conn.commit()

    # insert each csv row as a record in our database
    with open('/opt/airflow/data/weather_crawl.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader)
        for row in reader:
            cursor.execute(
                "INSERT INTO weather_weather_crawl VALUES (%s, %s, %s, %s, %s, %s)",
                row
            )
    conn.commit()


t1 = PythonOperator(
    task_id='crawling_data',
    python_callable=extract_data,
    # provide_context=True,
    dag=dag)

# t2 = PythonOperator(
#     task_id='parsing_data',
#     python_callable=parse_data,
#     provide_context=True,
#     dag=dag)
t2 = PythonOperator(
    task_id='transform_save_data',
    python_callable=transform_save_data,
    provide_context=True,
    dag=dag)
t3 = PythonOperator(
    task_id='Load_to_PostgreSQL_DB',
    python_callable=LoadDB,
    dag=dag)

t1 >>  t2 >> t3
