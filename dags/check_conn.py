from airflow import DAG
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

data_file = '/opt/airflow/data/raw/motorcycle.csv'

dag = DAG(
    'print_variables',
    default_args=default_args,
    description='Print details of Spark and PostgreSQL connections',
    schedule_interval=timedelta(days=1),
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

def head_file():
    df = pd.read_csv(data_file)
    print(df.head())


def print_variables_func():
    db_variables = Variable.get("postgres", deserialize_json=True)
    finance_db = db_variables["finance_db"]
    motorcycle_db = db_variables["motorcycle_db"]
    print(finance_db)
    print(motorcycle_db)

print_variables = PythonOperator(
    task_id='print_variables',
    python_callable=print_variables_func,
    dag=dag
)

head_file = PythonOperator(
    task_id='head_file',    
    python_callable=head_file,
    dag=dag
)

start >> print_variables >> head_file >> end