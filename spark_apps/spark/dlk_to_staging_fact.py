from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import when, col
from pyspark.sql.types import IntegerType
import findspark
import pandas as pd
from datetime import datetime
import sys

postgres_config = {
    'host': 'finance-raw.cp22uk6owxbs.us-west-2.rds.amazonaws.com',
    'port': 5432,
    'database': 'postgres',
    'user': 'postgres',
    'password': 'rahmadiyan'
}

def create_dict(df, table_name):
    dict = df.select(f'{table_name}_name', 'id').rdd.collectAsMap()
    return dict

def create_udf(dict):
    return F.udf(lambda x: dict.get(x), IntegerType())

def create_fact(df, segment_udf, country_udf, product_udf, discount_udf):
    df = df.select('segment', 'country', 'product', 'discount_band', 'date', 'manufacturing_price', 'sale_price', 'units_sold', 'gross_sales', 'discounts', 'sales', 'cogs', 'profit')
    df = df.withColumn('segment_id', segment_udf(F.col('segment')))
    df = df.withColumn('country_id', country_udf(F.col('country')))
    df = df.withColumn('product_id', product_udf(F.col('product')))
    df = df.withColumn('discount_id', discount_udf(F.col('discount_band')))
    # recast units_sold to int
    df = df.withColumn('units_sold', F.col('units_sold').cast(IntegerType()))
    # gross_sales = units_sold * sale_price
    df = df.withColumn('gross_sales', F.col('units_sold') * F.col('sale_price'))
    # sales = gross_sales - discounts
    df = df.withColumn('sales', F.col('gross_sales') - F.col('discounts'))
    # cogs = units_sold * manufacturing_price
    df = df.withColumn('cogs', F.col('units_sold') * F.col('manufacturing_price'))
    # profit = sales - cogs
    df = df.withColumn('profit', F.col('sales') - F.col('cogs'))
    # date to date
    df = df.withColumn('date', F.to_date(F.col('date')))

    print('====FACT TABLE====')
    df.show()
    return df
    
def load_postgre_fact(df, table_name, postgres_config):
    df.write.jdbc(
        url=f"jdbc:postgresql://{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}",
        table=f'{table_name}',
        mode='overwrite',
        properties={
            'user': postgres_config['user'],
            'password': postgres_config['password'],
            'driver': 'org.postgresql.Driver'
        }
    )
    
def write_parquet(df, data_dir, table_name):
    df.write.parquet(f"{data_dir}/staging/{table_name}_staging.parquet")

def clean_fact(df):
    df = df.drop('segment', 'country', 'product', 'discount_band')
    df = df.select('segment_id', 'country_id', 'product_id', 'discount_id', 'date', 'units_sold', 'manufacturing_price', 'sale_price', 'gross_sales', 'discounts', 'sales', 'cogs', 'profit')
    return df

if __name__ == '__main__':
    import os

    os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"  # For executors
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/local/bin/python3"  # For the driver

    spark_home = '/opt/spark'
    findspark.init(spark_home)
    data_dir = '/opt/spark/data'
    raw_data_dir = f'{data_dir}/raw'

    spark = SparkSession.builder \
        .appName(f'dlk_to_staging_{sys.argv[1]}_fact') \
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.3.1') \
        .config('spark.local.dir', '/tmp/spark-temp') \
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    # read dlk
    segment_df = spark.read.parquet(f"{data_dir}/staging/segment_staging.parquet")
    country_df = spark.read.parquet(f"{data_dir}/staging/country_staging.parquet")
    product_df = spark.read.parquet(f"{data_dir}/staging/product_staging.parquet")
    discount_df = spark.read.parquet(f"{data_dir}/staging/discount_staging.parquet")
    raw_df = spark.read.parquet(f"{raw_data_dir}/raw_financial.parquet")
    # create dicts
    segment_dict = create_dict(segment_df, 'segment')
    country_dict = create_dict(country_df, 'country')
    product_dict = create_dict(product_df, 'product')
    discount_dict = create_dict(discount_df, 'discount')
    
    # create udfs
    segment_udf = create_udf(segment_dict)
    country_udf = create_udf(country_dict)
    product_udf = create_udf(product_dict)
    discount_udf = create_udf(discount_dict)
    
    #create fact
    fact_df = create_fact(raw_df, segment_udf, country_udf, product_udf, discount_udf)
    final_df = clean_fact(fact_df)
    load_postgre_fact(final_df, 'sales_fact', postgres_config)
    write_parquet(final_df, data_dir, 'sales_fact')
    
    spark.stop()