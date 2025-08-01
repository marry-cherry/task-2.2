from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import psycopg2
from io import StringIO

DB_PARAMS = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'dwh',
    'user': 'postgres',
    'password': 'LinUb20'
}

CSV_PATH = "/home/marinaub/PycharmProjects/Task2.2/product_info.csv"

def load_product():
    df = pd.read_csv(CSV_PATH, encoding='cp1251', parse_dates=["effective_from_date", "effective_to_date"])

    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    cur.execute("SET search_path TO rd")
    cur.execute("TRUNCATE TABLE product")

    output = StringIO()
    df.to_csv(output, sep='\t', header=False, index=False, na_rep='\\N')
    output.seek(0)

    cur.copy_from(output, 'product', sep='\t', null='\\N')

    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id="load_product_dag",
    start_date=datetime(2025, 7, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    run_load_product = PythonOperator(
        task_id="load_product_task",
        python_callable=load_product
    )

