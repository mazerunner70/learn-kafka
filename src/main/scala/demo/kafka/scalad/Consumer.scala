package demo.kafka.scalad

import java.time.Duration
import java.util
import java.util.Properties
import java.util.concurrent.Executors

import scala.collection.JavaConverters._
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.errors.WakeupException

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.forkjoin._

// the following is equivalent to `implicit val ec = ExecutionContext.global`
//import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.kafka.common.serialization._
import scala.reflect.runtime.universe.{typeOf, TypeTag}
import scala.util.{Failure, Success}
//import org.apache.kafka.common.serialization.IntegerDeserializer


case class KafkaRecord[K, V](topic: String, partition: Int, offset: Long, key: K, value: V)

class Consumer[K: TypeTag, V: TypeTag] {
  implicit val ec = ExecutionContext.fromExecutorService(Executors.newSingleThreadExecutor)
  val ser = Map(
    "String" -> "org.apache.kafka.common.serialization.StringDeserializer",
    "Int" -> "org.apache.kafka.common.serialization.IntegerDeserializer",
    "Long" -> "org.apache.kafka.common.serialization.LongDeserializer"
  )
  var kafkaConsumer: Option[KafkaConsumer[K, V]] = None

  def initialise(groupName: String, clientName: String) = {
    val consumerProps = Map(
      "bootstrap.servers"  -> "localhost:9092",
      //      "key.deserializer"   -> "org.apache.kafka.common.serialization.StringDeserializer",
      "key.deserializer"   -> ser.getOrElse(typeOf[K].toString, ""),
      "value.deserializer" -> ser.getOrElse(typeOf[V].toString, ""),
      "auto.offset.reset"  -> "latest",
      "auto.commit.interval.ms" -> "100",
      "group.id"           -> groupName,
      "client.id"          -> clientName
    )

    val props = new Properties()

    props.putAll(consumerProps.mapValues(_.toString).asJava)
    kafkaConsumer = Some( new KafkaConsumer[K, V](props))
  }

  def setShutdownHook() = {
    val mainThread = Thread.currentThread
    // Registering a shutdown hook so we can exit cleanly// Registering a shutdown hook so we can exit cleanly

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run(): Unit = {
        System.out.println("Starting exit...")
        // Note that shutdownhook runs in a separate thread, so the only thing we can safely do to a consumer is wake it up
        kafkaConsumer.foreach {println("found consumer");_.wakeup}
        try
          mainThread.join
        catch {
          case e: InterruptedException =>
            println("interrupted")
        }
      }
    })
  }

  def subscribe(topic: String, func: KafkaRecord[K, V]=>Unit) = {
    kafkaConsumer match {
      case None =>
      case Some(consumer) => {
        setShutdownHook()
        consumer.subscribe(util.Arrays.asList(topic))
        println("About to listen")
        val fut = Future {
          try {
            println("Listening")
            while (true) {
              val record = consumer.poll(Duration.ofSeconds(10)).asScala
              var scalaFormRecords = Array[KafkaRecord[K, V]]()
              for (data <- record.iterator) {
                scalaFormRecords :+= KafkaRecord(data.topic(), data.partition(), data.offset(), data.key(), data.value())
              }
              scalaFormRecords.foreach(func(_))
            }
          } catch {
            case e: Exception => println(e)
            case e: WakeupException => println(s" topic $topic listener woke up")
          } finally {
            println(s"topic '$topic' consumer closing down")
            kafkaConsumer.foreach(_.close)
            println(s"topic '$topic' consumer closed down")
          }
        }
      }
    }
  }
}

object Consumer {
  def apply[K :TypeTag, V: TypeTag](groupName: String, clientName: String): Consumer[K, V] = {
    val consumer = new Consumer[K, V]()
    consumer.initialise(groupName, clientName)
    consumer
  }
}