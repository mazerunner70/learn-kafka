package demo.kafka.minimal

import java.time.Duration
import java.util.Properties
import java.util.concurrent.Executors

import com.fasterxml.jackson.module.scala.ser
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream

import scala.concurrent.{ExecutionContext, Future}
import scala.collection.JavaConverters._
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._

class Consumer {

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)
  def createConsumer(): KafkaConsumer[String, String] = {
    val consumerProps = Map(
      "bootstrap.servers" -> "localhost:9092",
      //      "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
      "auto.offset.reset" -> "latest",
      "auto.commit.interval.ms" -> "100",
      "group.id" -> "group1",
      "client.id" -> "client1")
    val props = new Properties()
    props.putAll(consumerProps.mapValues(_.toString).asJava)
    new KafkaConsumer[String, String](props)
  }

  def createProducer(): KafkaProducer[String, String] = {
    val consumerProps = Map(
      "bootstrap.servers"  -> "localhost:9092",
      "key.serializer"   -> "org.apache.kafka.common.serialization.StringSerializer",
      "value.serializer" -> "org.apache.kafka.common.serialization.StringSerializer",
    )
    val props = new Properties()
    props.putAll(consumerProps.mapValues(_.toString).asJava)
    new KafkaProducer[String, String](props)
  }

  def createStreams(topology: StreamsBuilder): KafkaStreams = {
    val streamProps = Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> "application1",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    )
    val props = new Properties()
    props.putAll(streamProps.mapValues(_.toString).asJava)
    val topology = topologyPlan.build()
    new KafkaStreams(topology, props)
  }

  def topologyPlan(): StreamsBuilder = {
    val builder = new StreamsBuilder
    val inputTopic: KStream[String, String] = builder.stream[String, String]("topic2")
    inputTopic.to("topic3")
    builder
  }

  def run() = {
    val kafkaStreams = createStreams(topologyPlan())
    kafkaStreams.start()

    val kafkaConsumer = createConsumer()
    val kafkaProducer = createProducer()
    kafkaConsumer.subscribe(List("topic1").asJava)
    while (true) {
      val record = kafkaConsumer.poll(Duration.ofSeconds(5)).asScala
      for (data <- record.iterator) {
        println( s"(key=${data.key()}, value=${data.value()}")
        kafkaProducer.send(new ProducerRecord[String, String]("topic2", data.value()))
      }
    }
  }

}

object Consumer {
  def main(args: Array[String]): Unit = {
    val consumer = new Consumer()
    consumer.run()

  }
}