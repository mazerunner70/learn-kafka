package demo.kafka.javakafka

import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.ExecutionContext.Implicits.global


object Main {

  def main(args: Array[String]): Unit = {
    val consumer = new Consumer()
    Future {
//      println("listening")

      consumer.consumeFromKafka("test-1")
    }
    Thread.sleep(2500)
    val producer = new Producer()
    producer.writeToKafka("test-1", "key2", "value5")
    Thread.sleep(1000)
    producer.writeToKafka("test-1", "key2", "value6")
    Thread.sleep(1000)
    producer.writeToKafka("test-1", "key2", "value7")
    Thread.sleep(1000)
  }


}
