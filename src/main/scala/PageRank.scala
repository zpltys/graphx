import org.apache.spark.graphx.GraphLoader
// $example off$
import org.apache.spark.sql.SparkSession


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
    spark.stop()
  }
}
