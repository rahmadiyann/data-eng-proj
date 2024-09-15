from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, to_date, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType
from dotenv import load_dotenv
import os
import findspark

def get_schemas():
    album_schema = StructType([
        StructField("album_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("total_tracks", IntegerType(), True),
        StructField("release_date", StringType(), True),  # Initially as StringType
        StructField("external_url", StringType(), True),
        StructField("image_url", StringType(), True),
        StructField("label", StringType(), True),
        StructField("popularity", IntegerType(), True)
    ])

    artist_schema = StructType([
        StructField("artist_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("external_url", StringType(), True),
        StructField("follower_count", IntegerType(), True),
        StructField("image_url", StringType(), True),
        StructField("popularity", IntegerType(), True)
    ])

    item_schema = StructType([
        StructField("listening_id", StringType(), True),
        StructField("played_at", StringType(), True),  # Initially as StringType
        StructField("song_id", StringType(), True),
        StructField("album_id", StringType(), True),
        StructField("artist_id", StringType(), True)
    ])

    song_schema = StructType([
        StructField("song_id", StringType(), True),
        StructField("title", StringType(), True),
        StructField("disc_number", IntegerType(), True),
        StructField("duration_ms", IntegerType(), True),
        StructField("explicit", BooleanType(), True),
        StructField("external_url", StringType(), True),
        StructField("preview_url", StringType(), True),
        StructField("popularity", IntegerType(), True)
    ])

    return {
        "album_topic": album_schema,
        "artist_topic": artist_schema,
        "item_topic": item_schema,
        "song_topic": song_schema
    }

def write_to_postgres(batch_df, batch_id):
    # Write data to the appropriate table based on the topic
    for topic in ["album_topic", "artist_topic", "item_topic", "song_topic"]: 
        properties = {
            "user": 'default',
            "password": 'AVb2Io9pqyWm',
            "driver": "org.postgresql.Driver",
            'url': 'jdbc:postgresql://ep-super-shape-a12kklqk-pooler.ap-southeast-1.aws.neon.tech:5432/verceldb',
            "dbtable": {
                "album_topic": "dim_album",
                "artist_topic": "dim_artist",
                "song_topic": "dim_song",
                "item_topic": "fact_history"
            }[topic],
        }
        
        topic_df = batch_df.filter(col("topic") == topic).drop("topic")
        if not topic_df.isEmpty():
            topic_df.write.format('jdbc').options(**properties).mode('append').save()

def create_spark_session():
    return SparkSession.builder \
        .appName("KafkaSparkConsumer") \
        .config("spark.jars", jdbc_jar) \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2") \
        .getOrCreate()

def read_from_kafka(spark, kafka_bootstrap_servers, topics):
    return spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", topics) \
        .load()

def parse_json_data(df, schemas):
    df = df.withColumn("album_data", from_json(col("json_value"), schemas["album_topic"])) \
           .withColumn("artist_data", from_json(col("json_value"), schemas["artist_topic"])) \
           .withColumn("item_data", from_json(col("json_value"), schemas["item_topic"])) \
           .withColumn("song_data", from_json(col("json_value"), schemas["song_topic"]))

    df = df.select(
        col("topic"),
        col("album_data.*"),
        col("artist_data.*"),
        col("item_data.*"),
        col("song_data.*")
    )

    # Convert date fields to appropriate types
    df = df.withColumn("release_date", when(col("topic") == "album_topic", to_date(col("release_date"))).otherwise(col("release_date")))
    df = df.withColumn("played_at", when(col("topic") == "item_topic", to_timestamp(col("played_at"))).otherwise(col("played_at")))
    
    return df

def main():
    schemas = get_schemas()
    spark = create_spark_session()
    kafka_bootstrap_servers = "localhost:9092"
    topics = "album_topic,artist_topic,item_topic,song_topic"  # Comma-separated list of topics

    df = read_from_kafka(spark, kafka_bootstrap_servers, topics)
    df = df.selectExpr("CAST(value AS STRING) as json_value", "topic")
    df = parse_json_data(df, schemas)

    query = df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("Process interrupted by user.")
    finally:
        query.stop()
        print("Query stopped.")
        spark.stop()
        print("Spark session stopped.")

if __name__ == "__main__":
    spark_home = "/Users/rian/Desktop/spark-3.5.2-bin-hadoop3"
    jdbc_jar = f"{spark_home}/jars/PostgreSQL-42.7.0.jar"
    findspark.init(spark_home)
    load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))
    main()