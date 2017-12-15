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

    sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/USA-road-d.USA.gr", minPartitions = partition).map(v => v.split(" ")(1).toLong).saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/graphNodes")
    /*
    println("zs-log: vertex size:" + vertex.count())

    val edge = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/USA-road-d.USA.gr", minPartitions = partition).map(s => {
      val d = s.split(' ')
      val u = d(1).toLong
      val v = d(2).toLong
      val dis = d(3).toInt
      Edge(u, v, dis)
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
    val totalLen = sssp.vertices.map( v => {
      var a = 0
      if(v._2 == Int.MaxValue) {
        a = 100
      } else {
        a = v._2
      }
      a
    }).reduce(_ + _)
    println("zs-log: totalLen:" + totalLen)

    val stopTime = System.currentTimeMillis()

    println("zs-log: run time" + (stopTime - loadOk) / 1000 + "s")

    println("zs-log: total time:" + (stopTime - start) / 1000 + "s")
*/
    sc.stop()
  }
}
