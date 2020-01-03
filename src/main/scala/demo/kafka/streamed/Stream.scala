package demo.kafka.streamed

import java.util.Properties
import java.time.Duration
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.StreamsConfig
import org.apache.kafka.streams.kstream.{KStream, KTable, Materialized, Produced}
import org.apache.kafka.streams.StreamsConfig

import scala.collection.JavaConverters._

class Stream {

  // https://kafka.apache.org/24/documentation/streams/developer-guide/config-streams.html#streams-developer-guide-configuration

  // Streams DSL
  // https://kafka.apache.org/24/documentation/streams/developer-guide/dsl-api.html#scala-dsl

  // Demo app
  // https://kafka.apache.org/documentation/streams/

  var kafkaStreams: Option[KafkaStreams] = None

  def buildStreams(): StreamsBuilder = {
    val builder= new StreamsBuilder
    val fibValues:KStream[String, String] = builder.stream[String, String]("fib")
    fibValues.to("digitcounts")
    builder
  }

  def initialise(): Unit = {
    val streamProps = Map(
      StreamsConfig.APPLICATION_ID_CONFIG -> "my-first-streams-application",
      StreamsConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
    )
    val props = new Properties()
    props.putAll(streamProps.mapValues(_.toString).asJava)
    kafkaStreams = Some( new KafkaStreams(buildStreams().build(), props))
  }

  def start() = {
    kafkaStreams.foreach(_.start())

    sys.ShutdownHookThread {
      kafkaStreams.foreach(_.close(Duration.ofSeconds(10)))
    }
  }

}

object Stream {
  def apply(): Stream = {
    val stream = new Stream()
    stream.initialise()
    stream
  }
}
