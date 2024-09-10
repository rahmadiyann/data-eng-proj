import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator

email_receiver = ["rahmadiyan.m@gmail.com", "riansshole123@gmail.com"]

args = {
    "owner": "Rahmadiyan M",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 11, tzinfo=pendulum.timezone("Asia/Jakarta")),
    "email": email_receiver,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="dlk_to_dtm_oscar",
    description="(DAILY) ETL Datalake to Datamart NawaData Finance Oscar",
    default_args=args,
    catchup=False,
    schedule_interval="@daily"
)

start = DummyOperator(
    task_id="start",
    dag=dag
)

wait = DummyOperator(
    task_id="wait",
    dag=dag
)

end = DummyOperator(
    task_id="end",
    dag=dag
)

def spark_job(task_id, name, application, sql_file, sql_path, *extra_args):
    if not sql_file and not sql_path:
        return SparkSubmitOperator(
            task_id=task_id,
            application=str(application),
            conn_id='spark-conn',
            name=name,
        )
    return SparkSubmitOperator(
        task_id=task_id,
        application=str(application),
        application_args=[
            "--sql_file", str(sql_file),
            "--sql_path", str(sql_path),
            *extra_args
        ],
        conn_id='spark-conn',
        name=name,
    )
    
