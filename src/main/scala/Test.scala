import scala.collection.mutable.ArrayBuffer

object Test {
  val n = 6
  val post = new Array[ArrayBuffer[Int]](n + 1)
  val pre = new Array[ArrayBuffer[Int]](n + 1)

  def main(args: Array[String]): Unit = {
    post(1) = ArrayBuffer(2, 3)
    post(2) = ArrayBuffer(4, 5)
    post(3) = ArrayBuffer(6)
    post(4) = ArrayBuffer(3)
    post(5) = ArrayBuffer(6)
    post(6) = ArrayBuffer()

    for (i <- 1 to n) {
      pre(i) = ArrayBuffer()
      for (j <- 1 to n) {
        if (post(j).contains(i)) {
          pre(i) += j
        }
      }
    }

    for (i <- 1 to n) {
      println("pre(" + i + "):" + pre(i))
    }
  }
}
