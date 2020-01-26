package demo.kafka.streamed

import java.time.Duration
import java.util.Properties
import java.util.concurrent.CountDownLatch

import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}

import scala.collection.JavaConverters._

class Stream {


  // https://kafka.apache.org/24/documentation/streams/developer-guide/config-streams.html#streams-developer-guide-configuration

  // Streams DSL
  // https://kafka.apache.org/24/documentation/streams/developer-guide/dsl-api.html#scala-dsl

  // Demo app
  // https://kafka.apache.org/documentation/streams/

  var kafkaStreams: Option[KafkaStreams] = None
  var countdownLatch = 0

  def initialise(topologyPlan: StreamsBuilder, applicationId: String): Unit = {
    val streamProps = Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> applicationId,
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    )
    val props = new Properties()
    props.putAll(streamProps.mapValues(_.toString).asJava)
    val topology = topologyPlan.build()
    println(topology.describe())
    kafkaStreams = Some( new KafkaStreams(topology, props))
    countdownLatch +=1
  }

  def start() = {

    sys.ShutdownHookThread {
      shutdown()
    }
    kafkaStreams.foreach(_.start())
    println(s"Stream started $countdownLatch")
  }

  def shutdown() = {
    println(s"stream shutdown $countdownLatch")
    kafkaStreams.foreach(_.close(Duration.ofSeconds(3)))
    countdownLatch -= 1
    if (countdownLatch <= 0) {
//      System.exit(0)
    }
  }

}

object Stream {
  def apply(topologyPlan: StreamsBuilder, applicationId: String = "my-first-streams-application"): Stream = {
    val stream = new Stream()
    stream.initialise(topologyPlan, applicationId: String)
    stream
  }
}
