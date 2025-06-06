from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, date_trunc
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("KafkaProcessor") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0") \
    .getOrCreate()

schema = StructType([
    StructField("meta", StructType([
        StructField("domain", StringType()),
        StructField("dt", StringType())
    ])),
    StructField("performer", StructType([
        StructField("user_is_bot", BooleanType()),
        StructField("user_id", StringType()),
        StructField("user_text", StringType())  
    ])),
    StructField("page_title", StringType()),
    StructField("page_id", StringType())
])

df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-server:9092") \
    .option("subscribe", "input") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

enriched = parsed.select(
    col("meta.domain").alias("domain"),
    col("meta.dt").alias("created_at"),
    col("meta.dt").alias("created_ts"),
    col("performer.user_id").alias("user_id"),
    col("performer.user_text").alias("user_name"),
    col("performer.user_is_bot").alias("is_bot"),
    col("page_title"),
    col("page_id"),
    
)


enriched = enriched.withColumn("created_hour", date_trunc("hour", col("created_ts")))

enriched.selectExpr("to_json(struct(*)) AS value") \
    .writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-server:9092") \
    .option("topic", "processed") \
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    .start().awaitTermination()
