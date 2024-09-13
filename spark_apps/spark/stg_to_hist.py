from pyspark.sql import SparkSession
import findspark
from datetime import datetime
import os
buss_date = datetime.now().strftime("%Y%m%d")


def load_hist_file(df, table_name):
    df.write.mode('overwrite').parquet(f"{data_dir}/hist/{table_name}_{buss_date}.parquet")
def load_hist_db(df, table_name):
    postgres_config = {
        'host': 'finance-raw.cp22uk6owxbs.us-west-2.rds.amazonaws.com',
        'port': 5432,
        'database': 'postgres',
        'user': 'postgres',
        'password': 'rahmadiyan'
    }
    df.write.jdbc(
        url=f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}",
        table=f'{table_name}_hist',
        mode='append',
        properties={
            'user': postgres_config['user'],
            'password': postgres_config['password'],
            'driver': 'org.postgresql.Driver'
        }
    )
def read_sales_fact():
    df = spark.read.format("jdbc")\
        .option("url", "jdbc:postgresql://finance-raw.cp22uk6owxbs.us-west-2.rds.amazonaws.com:5432/postgres")\
        .option("dbtable", "sales_fact")\
        .option("user", "postgres")\
        .option("password", "rahmadiyan")\
        .option("driver", "org.postgresql.Driver")\
        .load()
    return df

def main():
    df = read_sales_fact()
    load_hist_file(df, 'sales_fact')
    load_hist_db(df, 'sales_fact')

    df.show()


if __name__ == "__main__":

    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"  # For executors
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"  # For the driver
    data_dir='/opt/spark/data'
    spark_home = '/opt/spark'
    findspark.init(spark_home)

    spark = SparkSession.builder \
        .appName('stg_to_hist') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.3.1') \
        .config('spark.local.dir', '/tmp/spark-temp') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')
    
    main()
    
    spark.stop()