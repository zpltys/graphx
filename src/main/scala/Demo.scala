import org.apache.spark._

object Demo {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext()

    val input = sc.textFile("file:////home/zpltys/graphx/a.out")
    val result = input.map(s => 1).reduce(_ + _)
    println(result)
  }
}