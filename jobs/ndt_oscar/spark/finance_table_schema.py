from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# Segment table
segment_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("segment_name", StringType(), False)
])

# Country table
country_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("country_name", StringType(), False)
])

# Product table
product_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("product_name", StringType(), False),
    StructField("manufacturing_price", IntegerType(), False),
    StructField("sale_price", IntegerType(), False)
])

# Discount band table
discount_band_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("discount_band_name", StringType(), False)
])

# Sales table
sales_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("date", DateType(), False),
    StructField("segment_id", IntegerType(), False),
    StructField("country_id", IntegerType(), False),
    StructField("product_id", IntegerType(), False),
    StructField("units_sold", IntegerType(), False),
    StructField("discount_band_id", IntegerType(), False)
])

# OLAP sales data table
olap_sales_data_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("sales_id", IntegerType(), False),
    StructField("gross_sales", IntegerType(), False),
    StructField("discounts", IntegerType(), False),
    StructField("sales", IntegerType(), False),
    StructField("cogs", IntegerType(), False),
    StructField("profit", IntegerType(), False)
])