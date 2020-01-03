package demo.kafka.streamed

import demo.kafka.scalad.{Consumer, KafkaRecord, Producer}

import scala.annotation.tailrec

class Main {

  def run() = {
    val fibProducer = Producer()
    val rawConsumer = Consumer("grp1", "776")
    val transformerStream = Stream()
    val consumer2 = Consumer("grp2", "554")

    def runFibGenerater(producer: Producer) = {

      @tailrec
      def fib(x :BigInt, xm1: BigInt, countdown: Int, func: (BigInt, Int)=>Unit ): Unit = {
        func(x, countdown)
        countdown match {
          case 0 =>
          case _ => fib(x+xm1, x, countdown-1, func)
        }
      }
      fib(1, 0, 100, (x:BigInt, count: Int)=> {
        producer.produce(KafkaRecord("fib", 0, 0, "key"+count, x.toString()))
        Thread.sleep(300)
      })
    }

    rawConsumer.subscribe("fib", x=>println(s"raw= $x"))
    consumer2.subscribe("digitcounts", x=>println(s"digits = $x"))
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
