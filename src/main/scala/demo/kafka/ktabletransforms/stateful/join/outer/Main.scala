package demo.kafka.ktabletransforms.stateful.join.outer

import java.time.Duration

import demo.kafka.scalad.{Consumer, KafkaRecord, Producer}
import demo.kafka.streamed.Stream
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.kstream.{JoinWindows, Materialized}
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
    val fibValues: KTable[String, String] = builder.table[String, String]("fib")
    fibValues.toStream.to("unjoined")
    val filtered1 =fibValues.filter((k, v) => (v.last.toInt-48>2))
    val filtered2 =fibValues.filter((k, v) => (v.last.toInt-48<5))
    filtered1.toStream.to("filtered1")
    filtered2.toStream.to("filtered2")
    val joined = filtered1.outerJoin(filtered2)((lv, rv)=>"left=" + lv + ", right=" + rv)
    joined.toStream.to("joined")
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
    val consumer4 = Consumer[String, String]("grp4", "524")
    val consumer5 = Consumer[String, String]("grp5", "514")

    rawConsumer.subscribe("fib", x=>println(s"raw= $x"))
    consumer2.subscribe("unjoined", x=>println(s"unjoined = $x"))
    consumer3.subscribe("joined", x=>println(s"joined = $x"))
    consumer4.subscribe("filtered1", x=>println(s"filtered1 = $x"))
    consumer5.subscribe("filtered2", x=>println(s"filtered2 = $x"))
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
