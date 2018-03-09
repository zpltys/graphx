// $example off$
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._


/**
  * A PageRank example on social network dataset
  * Run with
  * {{{
  * bin/run-example graphx.PageRankExample
  * }}}
  */
object PageRank {
  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val startTime = System.currentTimeMillis()

    val spark = SparkSession
      .builder
      .appName(s"${this.getClass.getSimpleName}")
      .getOrCreate()
    val sc = spark.sparkContext

    println("start")
    val partition = args(0).toInt
    // $example on$
    // Load the edges as a graph
    val source = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/soc-LiveJournal1.txt", minPartitions = partition * 4).map(line => {
      line.split(" ")
      (line(0).toLong, line(1).toLong)
    }).cache()

    val vertex = source.flatMap(u => {
      val tmp = Array((u._1, 0), (u._2, 0))
      tmp
    }).distinct()
    val edge = source.map(l => Edge(l._1, l._2, 0))
    val graph = Graph(vertex, edge)

    val initialTime = System.currentTimeMillis()
    println("zs-log: finish load graph, load time:" + (initialTime - startTime) / 1000 + "s")
    // Run PageRank
    val ranks = graph.pageRank(0.01)
    // Join the ranks with the usernames
    ranks.vertices.saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/PageRankResult")
    // $example off$

    val stopTime = System.currentTimeMillis()
    println("zs-log: finish calculated, iteration time:" + (stopTime - initialTime) / 1000 + "s")

    spark.stop()
  }
}
