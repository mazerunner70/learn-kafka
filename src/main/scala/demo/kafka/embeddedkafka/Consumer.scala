package demo.kafka.embeddedkafka

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.KafkaConsumer

import scala.collection.JavaConverters._

class Consumer {

  def consumeFromKafka(topic: String) = {
    println("listening")

    try {
      val props = new Properties()

      props.put("bootstrap.servers", "localhost:9092") //FROM LOCAL
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("auto.offset.reset", "latest")
      props.put("group.id", "tree")
      val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)

      consumer.subscribe(util.Arrays.asList(topic))
      println("listening")


      while (true) {
        println("listening")
        val record = consumer.poll(1000).asScala
        println(record)
        for (data <- record.iterator) {
          println("--", data.value())
          //        rec.createRecord(data.value())
          //probably going to be a seperate function later


        }
      }
    } catch {
      case e: Exception => println(e)
    }
  }
  def initialiseConsumer(topic:String): KafkaConsumer[String,String] = {
    val props = new Properties()


    props.put("bootstrap.servers", sys.env("KAFKA_CONSUMER_URL"))
    //    props.put("bootstrap.servers", "host.docker.internal:9092") //FROM DOCKER
    //    props.put("bootstrap.servers", "localhost:9092")//FROM LOCAL
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props.put("group.id", "NEW-GROUP")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(util.Arrays.asList(topic))
    consumer
  }



}
