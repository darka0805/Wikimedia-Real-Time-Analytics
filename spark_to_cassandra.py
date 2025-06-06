from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pyspark.sql.functions as F
spark = SparkSession.builder \
    .appName("KafkaToCassandra") \
    .master("spark://spark:7077") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2,"
            "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", StringType()),
    StructField("user_name", StringType()),  
    StructField("domain", StringType()),
    StructField("created_at", StringType()),
    StructField("created_ts", StringType()),
    StructField("page_title", StringType()),
    StructField("is_bot", BooleanType()),
    StructField("page_id", StringType()),
    
])


df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-server:9092") \
    .option("subscribe", "processed") \
    .option("startingOffsets", "earliest") \
    .load()

parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
    .withColumn("data", from_json(col("json_str"), schema)) \
    .select("data.*")


parsed_ts = parsed \
    .withColumn("created_ts", to_timestamp("created_at")) \
    .filter("domain IS NOT NULL AND created_ts IS NOT NULL") \
    .withColumn("hour_window_start", date_trunc("hour", col("created_ts")))

pages_per_domain_by_hour = parsed_ts \
    .groupBy("hour_window_start", "domain") \
    .agg(count("*").alias("page_count"))

def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "wikidata") \
        .option("table", "pages_per_domain_by_hour") \
        .save()

pages_per_domain_query = pages_per_domain_by_hour.writeStream \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/cassandra_chk/pages_per_domain_by_hour") \
    .foreachBatch(write_to_cassandra) \
    .start()

# pages_per_domain_query.awaitTermination()


from pyspark.sql.functions import col, to_timestamp, date_trunc, count, expr


bot_parsed_ts = parsed \
    .withColumn("created_ts", to_timestamp("created_at")) \
    .filter("domain IS NOT NULL AND created_ts IS NOT NULL AND is_bot = true") \
    .withColumn("hour_window_start", date_trunc("hour", col("created_ts"))) \
    .withColumn("hour_window_end", expr("hour_window_start + INTERVAL 1 HOUR"))

bot_pages_by_domain = bot_parsed_ts \
    .groupBy("hour_window_start", "hour_window_end", "domain") \
    .agg(count("*").alias("pages_by_bots")) \
    .withColumnRenamed("hour_window_start", "time_window_start") \
    .withColumnRenamed("hour_window_end", "time_window_end")

def write_bot_aggregates_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "wikidata") \
        .option("table", "bot_pages_per_domain_summary") \
        .save()

bot_pages_stream = bot_pages_by_domain.writeStream \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/cassandra_chk/bot_pages_per_domain") \
    .foreachBatch(write_bot_aggregates_to_cassandra) \
    .start()

# bot_pages_stream.awaitTermination()


from pyspark.sql.functions import to_timestamp, window, col, count

from pyspark.sql.functions import col, to_timestamp, current_timestamp, expr, count

parsed_3 = parsed \
    .withColumn("created_ts", to_timestamp("created_at")) \
    .filter("domain IS NOT NULL AND created_ts IS NOT NULL") \
    .withColumn("hour_window_start", date_trunc("hour", col("created_ts"))) \
    .withColumn("hour_window_end", expr("hour_window_start + INTERVAL 1 HOUR"))

user_activity = parsed_3.groupBy(
    "hour_window_start", "hour_window_end", "user_id"
).agg(
    count("*").alias("page_count"),
    first("user_name").alias("user_name")
).withColumnRenamed("hour_window_start", "time_window_start") \
 .withColumnRenamed("hour_window_end", "time_window_end")


def write_top_users_batch(batch_df, epoch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "wikidata") \
        .option("table", "top_users_page_creation") \
        .save()

top_users_query = user_activity.writeStream \
    .foreachBatch(write_top_users_batch) \
    .option("checkpointLocation", "/tmp/cassandra_chk/top_users") \
    .outputMode("complete") \
    .start()
# top_users_query.awaitTermination()





from pyspark.sql.functions import to_timestamp

domain_df = parsed.select("domain", "created_at", "page_id") \
    .withColumn("created_at", to_timestamp("created_at")) \
    .filter("domain IS NOT NULL AND page_id IS NOT NULL") \
    .dropDuplicates(["domain"])

def write_domains_batch(batch_df, epoch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "wikidata") \
        .option("table", "domains") \
        .save()

query_4 = domain_df.writeStream \
    .foreachBatch(write_domains_batch) \
    .option("checkpointLocation", "/tmp/checkpoints/domains") \
    .outputMode("append") \
    .start()

# query_4.awaitTermination()



pages_by_user = parsed.select("user_id", "page_id", "page_title", "domain", "is_bot") \
    .filter("user_id IS NOT NULL AND page_id IS NOT NULL")

def write_pages_by_user(batch_df, epoch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "wikidata") \
        .option("table", "pages_by_user") \
        .save()

query_5 = pages_by_user.writeStream \
    .foreachBatch(write_pages_by_user) \
    .option("checkpointLocation", "/tmp/checkpoints/pages_by_user") \
    .outputMode("append") \
    .start()

# query_5.awaitTermination()


page_count_by_domain = parsed.select("domain", "page_id") \
    .filter("domain IS NOT NULL AND page_id IS NOT NULL") \
    .groupBy("domain") \
    .agg(count("page_id").alias("page_count"))

def write_to_cassandra(batch_df, epoch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "wikidata") \
        .option("table", "page_count_by_domain") \
        .save()

query_6 = page_count_by_domain.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoints/page_count_by_domain") \
    .start()
    
# query_6.awaitTermination()

page_by_id = parsed.select("page_id", "domain", "page_title") \
    .filter("page_id IS NOT NULL").dropDuplicates(["page_id"])

query_7 = page_by_id.writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("checkpointLocation", "/tmp/checkpoints/page_by_id") \
    .option("keyspace", "wikidata") \
    .option("table", "page_by_id") \
    .outputMode("append") \
    .start()

# query_7.awaitTermination()


from pyspark.sql.functions import to_timestamp, col, date_trunc, expr, count

user_activity = parsed \
    .withColumn("created_ts", to_timestamp("created_at")) \
    .filter("user_id IS NOT NULL AND page_id IS NOT NULL") \
    .withColumn("hour_window_start", date_trunc("hour", col("created_ts"))) \
    .withColumn("hour_window_end", expr("hour_window_start + INTERVAL 1 HOUR")) \
    .groupBy("hour_window_start", "hour_window_end", "user_id", "user_name") \
    .agg(count("page_id").alias("page_count")) \
    .withColumnRenamed("hour_window_start", "time_window_start") \
    .withColumnRenamed("hour_window_end", "time_window_end")

def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode("append") \
        .option("keyspace", "wikidata") \
        .option("table", "user_activity") \
        .save()

query_8 = user_activity.writeStream \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoints/user_activity") \
    .foreachBatch(write_to_cassandra) \
    .start()

# query_8.awaitTermination()


queries = [
    pages_per_domain_query,
    bot_pages_stream,
    top_users_query,
    query_4,
    query_5,
    query_6,
    query_7,
    query_8
]

for q in queries:
    q.awaitTermination()