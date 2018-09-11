import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._

object CC {
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    println("start")
    val partition = args(0).toInt
    // $example on$
    // Load the edges as a graph
    val source = sc.textFile("file:///mnt/nfs/zpltys/GRAPE_DATA/liveJournal/soc-LiveJournal1.txt", minPartitions = partition * 4).map(line => {
      val l = line.split("\t")
      (l(0).toLong, l(1).toLong)
    }).cache()

    val vertex = source.flatMap(u => {
      val tmp = Array((u._1, 0), (u._2, 0))
      tmp
    }).distinct()
    val edge = source.map(l => Edge(l._1, l._2, 0))
    println("zs-log: vertex sizs:" + vertex.count())
    println("zs-log: edge sizes:" + edge.count())
    val graph = Graph(vertex, edge)

    val initialTime = System.currentTimeMillis()
    println("zs-log: finish load graph, load time:" + (initialTime - startTime) / 1000 + "s")
    // Run CC
    val cc = graph.connectedComponents().vertices
    val size = cc.vertices.count()
    println("zs-log: ans size:" + size)

    val stopTime = System.currentTimeMillis()
    println("zs-log: finish calculated, iteration time:" + (stopTime - initialTime) / 1000 + "s")
    // $example off$
    sc.stop()
  }
}
