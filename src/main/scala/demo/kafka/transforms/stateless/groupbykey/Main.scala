package demo.kafka.transforms.stateless.groupbykey

import demo.kafka.scalad.{Consumer, KafkaRecord, Producer}
import demo.kafka.streamed.Stream
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.scala.ImplicitConversions._

import scala.annotation.tailrec

class Main {

  def topologyPlan(): StreamsBuilder = {
    import org.apache.kafka.streams.scala.Serdes._
    val builder= new StreamsBuilder
    val fibValues: KStream[String, String] = builder.stream[String, String]("fib")
    val mapped: KStream[String, String]  = fibValues.flatMap((k, v) => List((k.toLowerCase, v), (k.toLowerCase, v)))
    fibValues.to("uncounted")
    val groupedStream: KGroupedStream[String, String] = mapped.groupByKey
    groupedStream.count().toStream.to("counted")
    builder
  }

  def runFibGenerater(producer: Producer[String, String]) = {
    @tailrec def fib(x :BigInt, xm1: BigInt, countdown: Int, func: (BigInt, Int)=>Unit ): Unit = {
      func(x, countdown)
      countdown match {
        case 0 =>
        case _ => fib(x+xm1, x, countdown-1, func)
      }
    }
    fib(1, 0, 50, (x:BigInt, count: Int)=> {
      producer.produce(KafkaRecord("fib", 0, 0, "Key"+count, x.toString()))
      Thread.sleep(400)
    })
  }



  def run() = {
    val fibProducer = Producer[String, String]()
    val rawConsumer = Consumer[String, String]("grp1", "776")
    val transformerStream = Stream(topologyPlan())
    val consumer2 = Consumer[String, String]("grp2", "554")
    val consumer3 = Consumer[String, Long]("grp3", "554")

    rawConsumer.subscribe("fib", x=>println(s"raw= $x"))
    consumer2.subscribe("uncounted", x=>println(s"digits = $x"))
    consumer3.subscribe("counted", x=>println(s"digits2 = $x"))
    transformerStream.kafkaStreams.foreach(_.cleanUp())
    transformerStream.start()
    Thread.sleep(3000)
    runFibGenerater(fibProducer)
    println("Now waiting")
    Thread.sleep(10000)
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    val main = new Main()
    main.run()
  }
}
