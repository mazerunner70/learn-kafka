package demo.kafka.globalktabletransforms.stateful.join.left

import java.time.Duration

import demo.kafka.scalad.{Consumer, KafkaRecord, Producer}
import demo.kafka.streamed.Stream
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, Materialized}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.streams.scala.Serdes._

import scala.annotation.tailrec


// https://sachabarbs.wordpress.com/2019/01/28/kafkastreams-aggregating/

class KStreamMain {

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
      Thread.sleep(200)
    })
  }

  def topologyPlan1(): StreamsBuilder = {
    val builder= new StreamsBuilder
    val fibValues: KStream[String, String] = builder.stream[String, String]("fib")
    fibValues.to("unjoined")
    val filtered2 =fibValues.filter((k, v) => (v.last.toInt-48<5))
    filtered2.to("filtered2")
    builder
  }
  def topologyPlan2(): StreamsBuilder = {
    val builder= new StreamsBuilder
    val fibValues: KStream[String, String] = builder.stream[String, String]("fib")
    val filtered1 =fibValues.filter((k, v) => v.last.toInt-48>2)
    filtered1.to("filtered1")
    val filtered2asGlobal:GlobalKTable[String, String] = builder.globalTable("filtered2")
    val joined = filtered1.leftJoin(filtered2asGlobal)(
      (lk, lv) => lk,
      (lv, rv) => "left=" + lv + ", right=" + rv )
    joined.peek((k,v)=>println(s"$k, $v")).to("joined")
    builder
  }
  def stage2(): Unit = {
    val transformerStream2 = Stream(topologyPlan2(), "stream2")
    transformerStream2.kafkaStreams.foreach(_.cleanUp())
    val consumer4 = Consumer[String, String]("grp4", "524")
    consumer4.subscribe("filtered1", x=>{
      println(s"filtered1 = $x")
      if (x.key == 0) transformerStream2.shutdown()
    },x=>x.recordCounter == 38)
    try {
      transformerStream2.start()
    } catch {
      case e: Throwable => System.exit(1)
      case e: WakeupException => println(s" topic stream listener woke up")
    }
  }

  def run1() = {
    val fibProducer = Producer[String, String]()
    val rawConsumer = Consumer[String, String]("grp1", "776")
    val transformerStream = Stream(topologyPlan1())
    val consumer2 = Consumer[String, String]("grp2", "554")
    val consumer3 = Consumer[String, String]("grp3", "544")
    val consumer5 = Consumer[String, String]("grp5", "514")

    rawConsumer.subscribe("fib", x=>println(s"fib= $x"), x=>x.key == "0")
    consumer2.subscribe("unjoined", x=>println(s"unjoined = $x"), x=>x.key == "0")
    consumer3.subscribe("joined", x=>println(s"joined = $x"), x=>x.recordCounter == 100)
    consumer5.subscribe("filtered2", x=> {
      println(s"filtered2 = $x")
      if (x.key == "3") {
        stage2()
        transformerStream.shutdown()
      }
    }, x=>x.key == "3")
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

object KStreamMain {
  def main(args: Array[String]): Unit = {
    val main = new KStreamMain()
    main.run1()
  }
}
