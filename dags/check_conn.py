from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta

def print_connection_info(conn_id):
    conn = BaseHook.get_connection(conn_id)
    print(f"Connection ID: {conn_id}")
    print(f"Connection Type: {conn.conn_type}")
    print(f"Host: {conn.host}")
    print(f"Schema: {conn.schema}")
    print(f"Login: {conn.login}")
    print(f"Port: {conn.port}")
    print(f"Extra: {conn.extra}")
    print("-" * 50)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'print_connection_details',
    default_args=default_args,
    description='Print details of Spark and PostgreSQL connections',
    schedule_interval=timedelta(days=1),
)

connections_to_check = ['spark_default', 'postgres_finance_db', 'postgres_motorcycle_db']

for conn_id in connections_to_check:
    task = PythonOperator(
        task_id=f'print_{conn_id}_info',
        python_callable=print_connection_info,
        op_kwargs={'conn_id': conn_id},
        dag=dag,
    )
