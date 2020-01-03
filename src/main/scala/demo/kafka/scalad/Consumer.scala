package demo.kafka.scalad

import java.time.Duration
import java.util
import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.concurrent.Future
import scala.concurrent.forkjoin._

// the following is equivalent to `implicit val ec = ExecutionContext.global`
import scala.concurrent.ExecutionContext.Implicits.global

import scala.util.{Failure, Success}

case class KafkaRecord(topic: String, partition: Int, offset: Long, key: String, value: String)

class Consumer {

  var kafkaConsumer: Option[KafkaConsumer[String, String]] = None

  def initialise(groupName: String, clientName: String) = {

    val consumerProps = Map(
      "bootstrap.servers"  -> "localhost:9092",
      "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset"  -> "latest",
      "auto.commit.interval.ms" -> "100",
      "group.id"           -> groupName,
      "client.id"          -> clientName
    )

    val props = new Properties()

    props.putAll(consumerProps.mapValues(_.toString).asJava)
    kafkaConsumer = Some( new KafkaConsumer[String, String](props))
  }

  def subscribe(topic: String, func: KafkaRecord=>Unit) = {
    kafkaConsumer match {
      case None =>
      case Some(consumer) => {
        consumer.subscribe(util.Arrays.asList(topic))
        println("About to listen")
        val fut = Future {
          println("Listening")
          while (true) {
            val record = consumer.poll(Duration.ofSeconds(10)).asScala
            var scalaFormRecords = Array[KafkaRecord]()
            for (data <- record.iterator) {
              scalaFormRecords :+= KafkaRecord(data.topic(), data.partition(), data.offset(), data.key(), data.value())
            }
            scalaFormRecords.foreach(func(_))
          }
        }
      }
    }
  }
}

object Consumer {
  def apply(groupName: String, clientName: String): Consumer = {
    val consumer = new Consumer()
    consumer.initialise(groupName, clientName)
    consumer
  }
}