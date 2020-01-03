package demo.kafka.scalad

import java.util.Properties

import scala.collection.JavaConverters._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class Producer {

  var kafkaProducer: Option[KafkaProducer[String, String]] = None

  def initialise() = {
    val consumerProps = Map(
      "bootstrap.servers"  -> "localhost:9092",
      "key.serializer"   -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    )
    val props = new Properties()
    props.putAll(consumerProps.mapValues(_.toString).asJava)
    kafkaProducer = Some( new KafkaProducer[String, String](props))
  }

  def produce(kafkaRecord: KafkaRecord) = {
    val record = new ProducerRecord[String, String](kafkaRecord.topic, kafkaRecord.key, kafkaRecord.value)
    kafkaProducer match {
      case Some(producer) => producer.send(record)
      case None => throw UninitializedFieldError("must initialise")
    }
  }

}

object Producer {
  def apply(): Producer = {
    val producer = new Producer()
    producer.initialise()
    producer
  }
}
