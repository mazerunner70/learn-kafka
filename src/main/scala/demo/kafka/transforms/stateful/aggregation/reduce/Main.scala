package demo.kafka.transforms.stateful.aggregation.reduce

import demo.kafka.scalad.{Consumer, KafkaRecord, Producer}
import demo.kafka.streamed.Stream
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import scala.annotation.tailrec


// https://sachabarbs.wordpress.com/2019/01/28/kafkastreams-aggregating/

class Main {

  def topologyPlan(): StreamsBuilder = {
    import org.apache.kafka.streams.scala.Serdes._
    val builder= new StreamsBuilder
    val fibValues: KStream[String, String] = builder.stream[String, String]("fib")
    fibValues.to("unreduced")
    val groupedStream: KGroupedStream[String, String] = fibValues.groupBy((k, v) => (k.toInt/3).toString)
    val counted: KTable[String, String] = groupedStream.reduce((aggValue, newValue)=> aggValue + newValue)
    counted.toStream.to("reduced")
    builder
  }

  def runFibGenerater(producer: Producer[String, String]) = {
    @tailrec def fib(x: BigInt, xm1: BigInt, countdown: Int, func: (BigInt, Int) => Unit): Unit = {
      func(x, countdown)
      countdown match {
        case 0 =>
        case _ => fib(x + xm1, x, countdown - 1, func)
      }
    }

    fib(1, 0, 49, (x: BigInt, count: Int) => {
      producer.produce(KafkaRecord("fib", 0, 0, count.toString, x.toString()))
      Thread.sleep(400)
    })
  }

  def run() = {
    val fibProducer = Producer[String, String]()
    val rawConsumer = Consumer[String, String]("grp1", "776")
    val transformerStream = Stream(topologyPlan())
    val consumer2 = Consumer[String, String]("grp2", "554")
    val consumer3 = Consumer[String, String]("grp3", "544")

    rawConsumer.subscribe("fib", x=>println(s"raw= $x"))
    consumer2.subscribe("unreduced", x=>println(s"unreduced = $x"))
    consumer3.subscribe("reduced", x=>println(s"reduced = $x"))
    transformerStream.kafkaStreams.foreach(_.cleanUp())

    try {
      transformerStream.start()
      Thread.sleep(3000)
      runFibGenerater(fibProducer)
      println("Now waiting")
      Thread.sleep(2000)
    } catch {
      case e: Throwable =>
        System.exit(1)
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val main = new Main()
    main.run()
  }
}
