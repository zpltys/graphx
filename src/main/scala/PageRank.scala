// $example off$
import org.apache.spark.{SparkConf, SparkContext}
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

    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    println("start")
    val partition = args(0).toInt
    // $example on$
    // Load the edges as a graph
    val graph = GraphLoader.edgeListFile(sc, "alluxio://hadoopmaster:19998/zpltys/graphData/soc-LiveJournal1.txt")

    val initialTime = System.currentTimeMillis()
    println("zs-log: finish load graph, load time:" + (initialTime - startTime) / 1000 + "s")
    // Run PageRank
    val ranks = graph.pageRank(0.01)
    // Join the ranks with the usernames
    ranks.vertices.saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/PageRankResult")
    // $example off$

    val stopTime = System.currentTimeMillis()
    println("zs-log: finish calculated, iteration time:" + (stopTime - initialTime) / 1000 + "s")

    sc.stop()
  }
}