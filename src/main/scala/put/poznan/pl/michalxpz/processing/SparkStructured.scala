package put.poznan.pl.michalxpz.processing

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

import java.util.Properties

object SparkStructured {
  def main(args: Array[String]): Unit = {
    if (args.length != 11)
      throw new NoSuchElementException

    val spark = SparkSession.builder()
      .appName("Netflix Prize Data processing")
      .getOrCreate()

    import spark.implicits._

    val kafkaBootstrapServers = args(1)
    val kafkaTopic = args(2)
    val groupId = args(3)

    val jdbcUrl = args(4)
    val jdbcUser = args(5)
    val jdbcPassword = args(6)

    val slidingWindowSizeDays = args(7).toInt
    val anomalyRatingCountThreshold = args(8).toInt
    val anomalyRatingMeanThreshold = args(9).toLong
    val kafkaAnomalyTopic = args(10)

    val schema = new StructType()
      .add("timestamp", StringType)
      .add("movieId", IntegerType)
      .add("userId", StringType)
      .add("rating", IntegerType)

    val kafkaDF = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("subscribe", kafkaTopic)
      .option("startingOffsets", "latest")
      .load()
    val ratingsDF = kafkaDF.selectExpr("CAST(value AS STRING)")
      .select(from_json($"value", schema).as("data"))
      .select("data.*")
      .withColumn("timestamp", to_timestamp($"timestamp", "yyyy-MM-dd"))
    val watermarkedRatingsDF = ratingsDF.withWatermark("timestamp", "30 days")
    // Aggregated ratings calculation
    val aggregatedRatingsDF = watermarkedRatingsDF
      .groupBy(window($"timestamp", "30 days"), $"movieId")
      .agg(
        count("*").as("ratingCount"),
        sum("rating").as("ratingSum"),
        countDistinct("rating").as("uniqueRatingCount")
      )
    aggregatedRatingsDF.show()
    val jdbcProperties = new Properties()
    jdbcProperties.put("user", jdbcUser)
    jdbcProperties.put("password", jdbcPassword)
    jdbcProperties.put("driver", "org.postgresql.Driver")
    val aggregatedRatingsQuery = aggregatedRatingsDF
      .writeStream
      .outputMode("update")
      .foreachBatch { (batchDF: Dataset[Row], _: Long) =>
        batchDF.withColumn("window_start", $"window.start")
          .drop("window")
          .write
          .mode("append")
          .jdbc(jdbcUrl, "movie_ratings", jdbcProperties)
      }
      .option("checkpointLocation", "/tmp/checkpoints/aggregatedRatings")
      .trigger(Trigger.ProcessingTime("1 day"))
      .start()
    // Anomaly detection
    val anomaliesDF = watermarkedRatingsDF
      .groupBy(window($"timestamp", s"$slidingWindowSizeDays days", "1 day"), $"movieId")
      .agg(
        count("*").as("ratingCount"),
        mean("rating").as("ratingMean")
      )
      .filter($"ratingCount" >= anomalyRatingCountThreshold && $"ratingMean" >= anomalyRatingMeanThreshold)
    anomaliesDF.show()
    val anomaliesQuery = anomaliesDF
      .selectExpr("CAST(movieId AS STRING) AS key", "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaBootstrapServers)
      .option("topic", kafkaAnomalyTopic)
      .option("checkpointLocation", "/tmp/checkpoints/anomalies")
      .trigger(Trigger.ProcessingTime("1 day"))
      .start()

    spark.streams.awaitAnyTermination()
  }
}
