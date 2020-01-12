package demo.kafka.transforms.stateless.branch

object Test {
  def main(args: Array[String]): Unit = {
    val bigint = BigInt("09876543245567788999").toString()
    println(bigint.last.toInt-48)
  }
}
