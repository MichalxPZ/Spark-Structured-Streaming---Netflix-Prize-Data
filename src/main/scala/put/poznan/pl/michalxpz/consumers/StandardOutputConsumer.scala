package put.poznan.pl.michalxpz.consumers

import org.apache.kafka.clients.consumer.KafkaConsumer

import java.time.Duration
import java.util.Collections.singletonList
import java.util.{NoSuchElementException, Properties}
import scala.collection.JavaConverters.iterableAsScalaIterableConverter

object StandardOutputConsumer extends App {
  if (args.length != 3)
    throw new NoSuchElementException

  val properties = new Properties();
  properties.put("bootstrap.servers", args(0))
  properties.put("group.id", args(1))
  properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  System.out.println(properties)
  System.out.println("Starting consumer")
  val consumer = new KafkaConsumer[String, String](properties)
  System.out.println("Subscribing to: " + args(2))
  consumer.subscribe(singletonList(args(2)))
  while (true) {
    val results = consumer.poll(Duration.ofSeconds(6000)).asScala
    results.foreach( data => System.out.println(data.value())
    )
  }
  consumer.close()
}