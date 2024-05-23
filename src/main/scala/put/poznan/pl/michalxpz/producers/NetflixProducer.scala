package put.poznan.pl.michalxpz.producers

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import java.io.File
import java.nio.file.{Files, Paths}
import java.util.{NoSuchElementException, Properties}
import java.util.concurrent.TimeUnit
import java.util.function.Consumer

object NetflixProducer extends App {
  if (args.length != 4)
    throw new NoSuchElementException

  val directory = args(0)
  val sleepTime = args(1)
  val topicName = args(2)
  val properties = new Properties();
  properties.put("bootstrap.servers", args(3))
  properties.put("acks", "all")
  properties.put("retries", "0")
  properties.put("batch.size", "16384")
  properties.put("linger.ms", "1")
  properties.put("buffer.memory", "33554432")
  properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  System.out.println("Starting producer")
  System.out.println("Directory: " + directory)
  System.out.println("Sleep time: " + sleepTime)
  System.out.println("Topic name: " + topicName)
  System.out.println("Kafka server properties: " + properties)
  val producer = new KafkaProducer[String, String](properties)
  val filePaths = new File(directory).listFiles().map(_.getAbsolutePath).sorted
  System.out.println("Files to process: " + filePaths.mkString(", "))
  filePaths.foreach(path => {
    try {
      System.out.println("Processing file: " + path)
      Files.lines(Paths.get(path)).
        skip(1).
        forEach(
          new Consumer[String] {
            override def accept(t: String): Unit = {
              producer.send(new ProducerRecord[String, String](topicName, t.split(',')(0), t))
            }
          })
      TimeUnit.SECONDS.sleep(sleepTime.toInt)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
  })
  System.out.println("Closing producer")
  producer.close()
}