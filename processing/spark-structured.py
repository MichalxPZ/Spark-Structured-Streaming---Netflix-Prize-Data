from pyspark.sql import SparkSession
from pyspark.sql.functions import split, from_json, to_timestamp, window, count, sum, approx_count_distinct, mean, col, to_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
import sys

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

    kafkaDF = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
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

    aggregatedRatingsDF = ratingsDF
    if processing_mode == "C":
        aggregatedRatingsDF = ratingsDF.withWatermark("timestamp", "30 days")

    aggregatedRatingsDF = aggregatedRatingsDF.groupBy(window("timestamp", "30 days"), "movie_id") \
        .agg(
        count("*").alias("rating_count"),
        sum("rating").alias("rating_sum"),
        approx_count_distinct("rating").alias("unique_rating_count")
    ) \
        .select(col("window.start").alias("window_start"), "movie_id", "rating_count", "rating_sum", "unique_rating_count")

    jdbcProperties = {
        "user": jdbc_user,
        "password": jdbc_password,
        "driver": "org.postgresql.Driver"
    }

    if processing_mode == "A":
        aggregatedRatingsQuery = aggregatedRatingsDF \
            .writeStream \
            .outputMode("update") \
            .foreachBatch(lambda batchDF, _: batchDF.write.mode("append").jdbc(jdbc_url, "movie_ratings", properties=jdbcProperties)) \
            .option("checkpointLocation", "/tmp/checkpoints/aggregatedRatings") \
            .trigger(processingTime="1 day") \
            .start()
    if processing_mode == "C":
        aggregatedRatingsQuery = aggregatedRatingsDF \
            .writeStream \
            .outputMode("append") \
            .foreachBatch(lambda batchDF, _: batchDF.write.mode("append").jdbc(jdbc_url, "movie_ratings", properties=jdbcProperties)) \
            .option("checkpointLocation", "/tmp/checkpoints/aggregatedRatings") \
            .trigger(processingTime="1 day") \
            .start()


    anomaliesDF = aggregatedRatingsDF \
        .groupBy(window("timestamp", f"{sliding_window_size_days} days", "1 day"), "movie_id") \
        .agg(
        count("*").alias("ratingCount"),
        mean("rating").alias("ratingMean")
    ) \
        .filter(f"rating_count >= {anomaly_rating_count_threshold} AND ratingMean >= {anomaly_rating_mean_threshold}")

    anomalies_joined = anomaliesDF.join(movies, anomaliesDF.movie_id == movies["_c0"], "inner")

    anomaliesQuery = anomalies_joined \
        .selectExpr("CAST(movie_id AS STRING) AS key", "to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_anomaly_topic) \
        .option("checkpointLocation", "/tmp/checkpoints/anomalies") \
        .trigger(processingTime="1 day") \
        .start()

    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    main()
