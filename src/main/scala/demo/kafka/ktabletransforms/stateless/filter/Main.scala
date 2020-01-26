package demo.kafka.ktabletransforms.stateless.filter

import demo.kafka.scalad.{Consumer, KafkaRecord, Producer}
import demo.kafka.streamed.Stream
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.{KStream, KTable}
import org.apache.kafka.streams.scala.ImplicitConversions._

import scala.annotation.tailrec

class Main {

  def topologyPlan(): StreamsBuilder = {
    import org.apache.kafka.streams.scala.Serdes._
    val builder= new StreamsBuilder
    val fibValues: KTable[String, String] = builder.table[String, String]("fib")
    val filtered = fibValues.filter((k, v) => (v.last.toInt-48>4))
    fibValues.toStream.to("unfiltered")
    filtered.toStream.to("filtered")
   builder
  }

  def runFibGenerater(producer: Producer[String, String]) = {

    @tailrec
    def fib(x :BigInt, xm1: BigInt, countdown: Int, func: (BigInt, Int)=>Unit ): Unit = {
      func(x, countdown)
      countdown match {
        case 0 =>
        case _ => fib(x+xm1, x, countdown-1, func)
      }
    }
    fib(1, 0, 50, (x:BigInt, count: Int)=> {
      producer.produce(KafkaRecord("fib", 0, 0, "key"+count, x.toString()))
      Thread.sleep(400)
    })
  }



  def run() = {
    val fibProducer = Producer[String, String]()
    val rawConsumer = Consumer[String, String]("grp1", "776")
    val transformerStream = Stream(topologyPlan())
    val consumer2 = Consumer[String, String]("grp2", "554")
    val consumer3 = Consumer[String, String]("grp3", "524")

    rawConsumer.subscribe("fib", x=>println(s"raw= $x"))
    consumer2.subscribe("unfiltered", x=>println(s"unfiltered = $x"))
    consumer3.subscribe("filtered", x=>println(s"filtered = $x"))
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
