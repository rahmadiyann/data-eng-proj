import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'example_spark_submit_dag',
    default_args=default_args,
    description='A DAG with start and end Python tasks and a Spark Submit task',
    schedule_interval="@daily",
    catchup=False,
)

def start_task():
    print("Starting the DAG")

def end_task():
    print("DAG execution completed")

start = PythonOperator(
    task_id='start_task',
    python_callable=start_task,
    dag=dag,
)

spark_submit = SparkSubmitOperator(
    task_id='spark_submit_task',
    application='/app/submit.py',
    conn_id='spark_default',
    dag=dag,
)

end = PythonOperator(
    task_id='end_task',
    python_callable=end_task,
    dag=dag,
)

start >> spark_submit >> end
