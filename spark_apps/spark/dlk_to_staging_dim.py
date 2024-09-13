from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import when, col
import findspark
import pandas as pd
from datetime import datetime
import os
import sys
    
def clean_df(df, data_dir):
    df = df.toDF(*[col_name.strip().lower().replace(' ', '_') for col_name in df.columns])
    df = df.withColumn('discount_band', when(col('discount_band') == 'NaN','No Discount').otherwise(col('discount_band')))
    try:
        df.write.parquet(f"{data_dir}/raw/raw_financial.parquet")
    except Exception:
        return df
    return df

class df_load_dim:
    def __init__(self, df):
        self.spark_df = df
        self.postgres_config = {
            'host': 'finance-raw.cp22uk6owxbs.us-west-2.rds.amazonaws.com',
            'port': 5432,
            'database': 'postgres',
            'user': 'postgres',
            'password': 'rahmadiyan'
        }
        self.table_name = {
            'segment': 'segment_dim',
            'country': 'country_dim',
            'product': 'product_dim',
            'discount': 'discount_dim'
        }
        
    def load(self, arg):
        if arg == 'segment':
            return self._load_segment(arg)
        elif arg == 'country':
            return self._load_country(arg)
        elif arg == 'product':
            return self._load_product(arg)
        elif arg == 'discount':
            return self._load_discount(arg)
        else:
            raise ValueError(f"Unsupported name: {arg}")
    
    def _export(self, df, table_name):
        df.write.parquet(f"{data_dir}/staging/{table_name}_staging.parquet")
        
    def _load_postgre(self, df, table_name):
        df.write.jdbc(
            url=f"jdbc:postgresql://{self.postgres_config['host']}:{self.postgres_config['port']}/{self.postgres_config['database']}",
            table=f'{table_name}_dim',
            mode='overwrite',
            properties={
                'user': self.postgres_config['user'],
                'password': self.postgres_config['password'],
                'driver': 'org.postgresql.Driver'
            }
        )
        
    def _load_segment(self, table_name):
        segment_table = self.spark_df.select('segment').distinct() \
            .withColumn('id', F.monotonically_increasing_id()) \
            .withColumnRenamed('segment', 'segment_name') \
            .select('id', 'segment_name')
        
        self._export(segment_table, table_name)
        self._load_postgre(segment_table, table_name)
        
        segment_table.show()
        return segment_table
        
    def _load_country(self, table_name):
        country_table = self.spark_df.select('country').distinct() \
            .withColumn('id', F.monotonically_increasing_id()) \
            .withColumnRenamed('country', 'country_name') \
            .select('id', 'country_name')
        
        self._export(country_table, table_name)
        self._load_postgre(country_table, table_name)
        
        country_table.show()
        return country_table
    
    def _load_product(self, table_name):
        product_table = self.spark_df.select('product').distinct() \
            .withColumn('id', F.monotonically_increasing_id()) \
            .withColumnRenamed('product', 'product_name') \
            .select('id', 'product_name')
        
        self._export(product_table, table_name)
        self._load_postgre(product_table, table_name)
        
        product_table.show()
        return product_table
    
    def _load_discount(self, table_name):
        discount_table = self.spark_df.select('discount_band').distinct() \
            .withColumn('id', F.monotonically_increasing_id()) \
            .withColumnRenamed('discount_band', 'discount_name') \
            .select('id', 'discount_name')
        
        self._export(discount_table, table_name)
        self._load_postgre(discount_table, table_name)
        
        discount_table.show()
        return discount_table
        
if __name__ == '__main__':

    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"  # For executors
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"  # For the driver

    spark_home = '/opt/spark'
    findspark.init(spark_home)
    data_dir = '/opt/spark/data'

    spark = SparkSession.builder \
        .appName(f'dlk_to_staging_{sys.argv[1]}_dim') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.3.1') \
        .config('spark.local.dir', '/tmp/spark-temp') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    panda_df = pd.read_excel(f'{data_dir}/raw/FinancialSample.xlsx', sheet_name='Sheet1')
    raw_df = spark.createDataFrame(panda_df)
    
    cleaned_df = clean_df(raw_df, data_dir)
    loader = df_load_dim(cleaned_df)
    
    if sys.argv[1] == 'sales':
        print("Sales table not supported")
        sys.exit()
    else:
        loader.load(sys.argv[1])
        
    spark.stop()