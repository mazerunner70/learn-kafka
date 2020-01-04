package demo.kafka.streamed

import java.time.Duration
import java.util.Properties

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


  def initialise(topologyPlan: StreamsBuilder): Unit = {
    val streamProps = Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> "my-first-streams-application",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    )
    val props = new Properties()
    props.putAll(streamProps.mapValues(_.toString).asJava)
    val topology = topologyPlan.build()
    println(topology.describe())
    kafkaStreams = Some( new KafkaStreams(topology, props))
  }

  def start() = {
    kafkaStreams.foreach(_.start())

    sys.ShutdownHookThread {
      kafkaStreams.foreach(_.close(Duration.ofSeconds(10)))
    }
  }

}

object Stream {
  def apply(topologyPlan: StreamsBuilder): Stream = {
    val stream = new Stream()
    stream.initialise(topologyPlan)
    stream
  }
}
