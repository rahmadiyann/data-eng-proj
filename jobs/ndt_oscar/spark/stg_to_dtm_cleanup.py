from pyspark.sql import SparkSession
import findspark
from datetime import datetime
import os
buss_date = datetime.now().strftime("%Y%m%d")

def load_hist(df, table_name):
    df.write.mode('overwrite').parquet(f"{data_dir}/hist/{table_name}_{buss_date}.parquet")
    
if __name__ == "__main__":
    spark_home = '/opt/bitnami/spark'
    jars = f'{spark_home}/jars'
    findspark.init(spark_home)
    data_dir = '/opt/airflow/data'

    spark = SparkSession.builder \
        .appName('stg_to_dtm_cleanup') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.3.1') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    
    # get every parquet file inside the /staging folder
    staging_files = os.listdir(f"{data_dir}/staging")
    file_list = []
    for file in staging_files:
        if file.endswith(".parquet"):
            file_list.append(file)
        
    for file in file_list:
        df = spark.read.parquet(f"{data_dir}/staging/{file}")
        load_hist(df, file.split("_")[0])
    
    spark.stop()