import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object SSSP {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val conf = new SparkConf()
    val sc = new SparkContext()
    val partition = args(0).toInt

    val start = System.currentTimeMillis()

    val vertex = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/LiveJournalNodes/*", minPartitions = partition).map(v => (v.toLong, 1L)).cache()
    println("zs-log: vertex size:" + vertex.count())

    val edge = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/soc-LiveJournal1.txt", minPartitions = partition).map(s => {
      val d = s.split('\t')
      val u = d(0).toLong
      val v = d(1).toLong
      Edge(u, v, 1)
    }).cache()
    println("zs-log: edge size:" + edge.count())

    val loadOk = System.currentTimeMillis()
    println("zs-log: load time:" + (loadOk - start) / 1000 + "s")



    // $example on$
    // A graph with edge attributes containing distances
    val graph: Graph[Long, Int] = Graph(vertex, edge)
    vertex.unpersist()
    edge.unpersist()

    val sourceId: VertexId = 1 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0 else Int.MaxValue)

    val sssp = initialGraph.pregel(Int.MaxValue)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    ).cache()
    sssp.vertices.map( vertex => (vertex._1, vertex._2)).saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/ssspResult/result" + partition + ".txt")
    val stopTime = System.currentTimeMillis()

    println("zs-log: total time:" + (stopTime - start) / 1000 + "s")

    sc.stop()
  }
}
