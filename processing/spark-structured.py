from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_timestamp, window, count, sum, countDistinct, mean
from pyspark.sql.types import StructType, StringType, IntegerType
import sys

def main():
    if len(sys.argv) != 12:
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

    spark = SparkSession.builder \
        .appName("Netflix Prize Data processing") \
        .getOrCreate()

    schema = StructType() \
        .add("timestamp", StringType()) \
        .add("movieId", IntegerType()) \
        .add("userId", StringType()) \
        .add("rating", IntegerType())

    kafkaDF = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "latest") \
        .load()

    ratingsDF = kafkaDF.selectExpr("CAST(value AS STRING)") \
        .select(from_json("value", schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp("timestamp", "yyyy-MM-dd"))

    watermarkedRatingsDF = ratingsDF.withWatermark("timestamp", "30 days")

    aggregatedRatingsDF = watermarkedRatingsDF \
        .groupBy(window("timestamp", "30 days"), "movieId") \
        .agg(
            count("*").alias("ratingCount"),
            sum("rating").alias("ratingSum"),
            countDistinct("rating").alias("uniqueRatingCount")
        )

    jdbcProperties = {
        "user": jdbc_user,
        "password": jdbc_password,
        "driver": "org.postgresql.Driver"
    }

    aggregatedRatingsQuery = aggregatedRatingsDF \
        .writeStream \
        .outputMode("update") \
        .foreachBatch(lambda batchDF, _: batchDF.withColumn("window_start", batchDF["window"]["start"]).drop("window").write.mode("append").jdbc(jdbc_url, "movie_ratings", properties=jdbcProperties)) \
        .option("checkpointLocation", "/tmp/checkpoints/aggregatedRatings") \
        .trigger(processingTime="1 day") \
        .start()

    anomaliesDF = watermarkedRatingsDF \
        .groupBy(window("timestamp", f"{sliding_window_size_days} days", "1 day"), "movieId") \
        .agg(
            count("*").alias("ratingCount"),
            mean("rating").alias("ratingMean")
        ) \
        .filter(f"ratingCount >= {anomaly_rating_count_threshold} AND ratingMean >= {anomaly_rating_mean_threshold}")

    anomaliesQuery = anomaliesDF \
        .selectExpr("CAST(movieId AS STRING) AS key", "to_json(struct(*)) AS value") \
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
