from pyspark.sql import SparkSession
from pyspark.sql.functions import split, concat, from_json, to_timestamp, window, lit, count, sum, approx_count_distinct, mean, col, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import sys
import logging
import socket

logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


def anomalies(ratingsDF, sliding_window_size_days, anomaly_rating_count_threshold, anomaly_rating_mean_threshold, kafka_bootstrap_servers, kafka_anomaly_topic, movies, groupId):
    watermarkedRatingsDF = ratingsDF.withWatermark("timestamp", "30 days")
    anomaliesDF = watermarkedRatingsDF \
        .groupBy(window("timestamp", f"{sliding_window_size_days} days", "1 day"), "movie_id") \
        .agg(
        count("*").alias("ratingCount"),
        mean("rating").alias("ratingMean")
    ) \
        .filter(f"ratingCount >= {anomaly_rating_count_threshold} AND ratingMean >= {anomaly_rating_mean_threshold}")

    anomalies_joined = anomaliesDF.join(movies, anomaliesDF.movie_id == movies["_c0"], "inner") \
        .select(col("window.end").alias("window_end"), col("window.start").alias("window_start"), "movie_id", col("_c2").alias("title"), col("_c1").alias("Year"),"ratingCount", "ratingMean")

    anomalies_formatted = anomalies_joined.select(concat(
        col("window_start"),
        lit(","),
        col("window_end"),
        lit(","),
        col("title"),
        lit(","),
        col("ratingCount"),
        lit(","),
        col("ratingMean"),
    ).alias("value"))

    host_name = socket.gethostname()
    anomaliesQuery = anomalies_formatted \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{host_name}:9092") \
        .option("topic", kafka_anomaly_topic) \
        .option("checkpointLocation", "/tmp/checkpoints/anomalies") \
        .start()


def real_time_processing(ratingsDF, movies, jdbc_url, jdbc_user, jdbc_password, processing_mode):
    aggregatedRatingsDF = ratingsDF
    if processing_mode == "C":
        aggregatedRatingsDF = ratingsDF.withWatermark("timestamp", "30 days")

    aggregatedRatingsDF = aggregatedRatingsDF.groupBy(window("timestamp", "30 days"), "movie_id") \
        .agg(
        count("*").alias("rating_count"),
        sum("rating").alias("rating_sum"),
        approx_count_distinct("rating").alias("unique_rating_count")
    ) \
        .join(movies, aggregatedRatingsDF.movie_id == movies["_c0"], "inner") \
        .select(col("window.start").alias("window_start"), "movie_id", col("_c2").alias("title"), "rating_count", "rating_sum", "unique_rating_count") \

    jdbcProperties = {
        "user": jdbc_user,
        "password": jdbc_password,
        "driver": "org.postgresql.Driver"
    }

    if processing_mode == "A":
        aggregatedRatingsQuery = aggregatedRatingsDF \
            .writeStream \
            .outputMode("update") \
            .foreachBatch(lambda batchDF, batchId: save_to_jdbc(batchDF, jdbc_url, jdbcProperties)) \
            .option("checkpointLocation", "/tmp/checkpoints/aggregatedRatings") \
            .trigger(processingTime="20 seconds") \
            .start()
    if processing_mode == "C":
        aggregatedRatingsQuery = aggregatedRatingsDF \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(lambda batchDF, batchId: save_to_jdbc(batchDF, jdbc_url, jdbcProperties)) \
            .option("checkpointLocation", "/tmp/checkpoints/aggregatedRatings") \
            .trigger(processingTime="20 second") \
            .start()


def save_to_jdbc(batchDF, jdbc_url, jdbcProperties, mode="append"):
    batchDF.write.mode(mode).jdbc(jdbc_url, "movie_ratings", properties=jdbcProperties)


def main():
    if len(sys.argv) != 13:
        raise Exception("Usage: script <input_file_path> <kafka_bootstrap_servers> <kafka_topic> <group_id> <jdbc_url> <jdbc_user> <jdbc_password> <sliding_window_size_days> <anomaly_rating_count_threshold> <anomaly_rating_mean_threshold> <kafka_anomaly_topic>")

    input_file_path = sys.argv[1]
    kafka_bootstrap_servers = sys.argv[2]
    kafka_topic = sys.argv[3]
    group_id = sys.argv[4]
    jdbc_url = sys.argv[5]
    jdbc_user = sys.argv[6]
    jdbc_password = sys.argv[7]
    sliding_window_size_days = int(sys.argv[8])
    anomaly_rating_count_threshold = int(sys.argv[9])
    anomaly_rating_mean_threshold = float(sys.argv[10])
    kafka_anomaly_topic = sys.argv[11]
    processing_mode = sys.argv[12]

    spark = SparkSession.builder \
        .appName("Netflix Prize Data processing") \
        .getOrCreate()

    host_name = socket.gethostname()
    kafkaDF = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", f"{host_name}:9092") \
        .option("subscribe", kafka_topic) \
        .option("group.id", group_id) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    movies = spark.read.option("header", False).csv(input_file_path)

    data = kafkaDF.selectExpr("CAST(value AS STRING)").alias("value")
    split_columns = split(data["value"], ",")
    ratingsDF = data.withColumn("timestamp", to_timestamp(split_columns[0], "yyyy-MM-dd")) \
        .withColumn("movie_id", split_columns[1].cast(IntegerType())) \
        .withColumn("user_id", split_columns[2].cast(StringType())) \
        .withColumn("rating", split_columns[3].cast(IntegerType())) \
        .drop("value")
    ratingsDF.printSchema()
    movies.printSchema()

    jdbc_url = f"jdbc:postgresql://{host_name}:8432/netflix_ratings"
    real_time_processing(ratingsDF,movies,  jdbc_url, jdbc_user, jdbc_password, processing_mode)
    anomalies(ratingsDF, sliding_window_size_days, anomaly_rating_count_threshold, anomaly_rating_mean_threshold, kafka_bootstrap_servers, kafka_anomaly_topic, movies, group_id)

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
