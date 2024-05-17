import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.net.InetAddress

object SparkStructured {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    val host_name = InetAddress.getLocalHost().getHostName()
    val ds1 = spark.readStream.
      format("kafka").
      option("kafka.bootstrap.servers", s"${host_name}:9092").
      option("subscribe", "kafka-input").
      load()

    import spark.implicits._
    val valueDF = ds1.select(expr("CAST(value AS STRING)").as("value"))

    val dataSchema = StructType(
      List(
        StructField("house", StringType, true),
        StructField("character", StringType, true),
        StructField("score", StringType, true),
        StructField("ts", TimestampType, true)
      )
    )

    val dataDF = valueDF.select(
        from_json($"value".cast(StringType), dataSchema).as("val")).
      select($"val.house",$"val.character",
        $"val.score".cast("int").as("score"),$"val.ts")

    val resultDF = dataDF.groupBy($"house").
      agg(count($"score").as("how_many"),
        sum($"score").as("sum_score"),
        approx_count_distinct("character", 0.1).as("no_characters"))

    val streamWriter = resultDF.writeStream.outputMode("complete").foreachBatch {
      (batchDF: DataFrame, batchId: Long) =>
        batchDF.write.
          format("jdbc").
          mode(SaveMode.Overwrite).
          option("url", s"jdbc:postgresql://${host_name}:8432/streamoutput").
          option("dbtable", "housestats").
          option("user", "postgres").
          option("password", "mysecretpassword").
          option("truncate", "true").
          save()
    }

    val query = streamWriter.start()
    query.awaitTermination()
  }
}