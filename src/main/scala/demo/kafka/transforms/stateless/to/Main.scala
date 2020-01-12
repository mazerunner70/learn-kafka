package demo.kafka.transforms.stateless.to

import demo.kafka.scalad.{Consumer, KafkaRecord, Producer}
import demo.kafka.streamed.Stream
import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream.KStream
import org.apache.kafka.streams.scala.ImplicitConversions._

import scala.annotation.tailrec

class Main {

  def topologyPlan(): StreamsBuilder = {
    import org.apache.kafka.streams.scala.Serdes._
    val builder= new StreamsBuilder
    val fibValues: KStream[String, String] = builder.stream[String, String]("fib")
    fibValues.to("unchanged")

    //    fibValues.groupBy((key:String, value:String)=> value.last.toString).count().toStream().to("digitcounts2")
//    fibValues.map((k, v) => (v.last.toString, v)).to("digitcounts2")
    //    fibValues.map((k, v) => (v.last.toString, v)).groupBy((k, v) => k).count().to("digitcounts2")
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
      producer.produce(KafkaRecord("fib", 0, 0, "Key"+count, x.toString()))
      Thread.sleep(400)
    })
    producer.produce(KafkaRecord("fib", 0, 0, "END", ""))
  }



  def run() = {
    val fibProducer = Producer[String, String]()
    val rawConsumer = Consumer[String, String]("grp1", "776")
    val transformerStream = Stream(topologyPlan())
    val consumer2 = Consumer[String, String]("grp2", "554")


    rawConsumer.subscribe("fib", x=>println(s"raw= $x"))
    consumer2.subscribe("unchanged", x=>println(s"unchanged = $x"))

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
