import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

email_receiver = ["rahmadiyan.m@gmail.com", "riansshole123@gmail.com"]
proj = Variable.get("nawadata", deserialize_json=True)
proj_dir = proj['proj_dir']

def print_proj_dir():
    print(proj_dir)

args = {
    "owner": "Rahmadiyan M",
    "depends_on_past": False,
    "start_date": datetime(2024, 9, 11, tzinfo=pendulum.timezone("Asia/Jakarta")),
    "email": email_receiver,
    "email_on_failure": False,
    "email_on_retry": False,
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

print_proj = PythonOperator(
    task_id="print_proj_dir",
    python_callable=print_proj_dir,
    dag=dag
)

def sensor(task_id, filepath):
    return FileSensor(
        task_id=task_id,
        filepath=filepath,
        poke_interval=10,
        timeout=600,
        mode="poke",
        dag=dag
    )
      
sensor_dlk = sensor(
    task_id="sensor_dlk",
    filepath=f'{proj_dir}/data/raw/FinancialSample.xlsx',
)

items_list = [
    {
        "uid": "01",
        "task_id": "dlk_segment_to_staging",
        "table_name": "segment"
    },
    {
        "uid": "02",
        "task_id": "dlk_country_to_staging",
        "table_name": "country"
    },
    {
        "uid": "03",
        "task_id": "dlk_product_to_staging",
        "table_name": "product"
    },
    {
        "uid": "04",
        "task_id": "dlk_discount_to_staging",
        "table_name": "discount"
    }
]

items_list_2 = [
    {
        "uid": "05",
        "task_id": "generate_fact",
        "table_name": "sales"
    }
]

def spark_job(task_id, application, **kwargs):
    return SparkSubmitOperator(
        task_id=task_id,
        application=str(application),
        application_args=kwargs['extra_args'],  # Remove the extra list wrapping
        conn_id='spark-conn',
        dag=dag
    )

for item in items_list:
    dlk_to_staging_dim = spark_job(
        task_id=item["task_id"],
        application=f"{proj_dir}/jobs/ndt_oscar/spark/dlk_to_staging_dim.py",
        extra_args=item["table_name"]  # Pass the string directly, not as a list
    )
    
    sensor_dlk >> dlk_to_staging_dim >> wait

for item in items_list_2:
    dlk_to_staging_fact = spark_job(
        task_id=item["task_id"],
        application=f"{proj_dir}/jobs/ndt_oscar/spark/dlk_to_staging_fact.py",
        extra_args=item["table_name"]  # Pass the string directly, not as a list
    )
    wait >> dlk_to_staging_fact

# Update the load2hist task as well
load2hist = spark_job(
    task_id="load2hist",
    application=f"{proj_dir}/jobs/ndt_oscar/spark/stg_to_dtm_cleanup.py",
    extra_args=[]  # If no args are needed, pass an empty list
)

cleanup = BashOperator(
    task_id="cleanup",
    bash_command=f"rm -rf {proj_dir}/data/staging/*"
)

start >> print_proj >> sensor_dlk 
dlk_to_staging_fact >> load2hist >> cleanup >> end