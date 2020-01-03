package demo.kafka.embeddedkafka

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

class Producer {

  def writeToKafka(topic: String, key: String, value: String): Unit = {
    val props = new Properties()


    props.put("bootstrap.servers", "localhost:9092") //for local docker purposes
    //    props.put("bootstrap.servers", "host.docker.internal:9092") //for local docker purposes
    //    props.put("bootstrap.servers", "100.71.73.28:9092") //For deployment purposes
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val record = new ProducerRecord[String, String](topic, key, value)

    //println("inputs props into kafka producer function ", producer)
    println("topic= ", topic, "key =  ", key, "value = ", value)
    producer.send(record)
    producer.close()

  }

}
