import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object Demo {

  def main(args: Array[String]): Unit = {
    // Creates a SparkSession.
    val conf = new SparkConf()
    val sc = new SparkContext()

    val data: RDD[(VertexId, VertexId)] = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/com-lj.ungraph.txt", minPartitions = 6).map(s => {
      val d = s.split('\t')
      val u = d(0).toLong
      val v = d(1).toLong
      (u, v)
    }).cache()

    val vertex = data.flatMap(e => {
      Seq((e._1, 1L), (e._2, 1L))
    }).distinct()

    val edge = data.map(e => {
      Edge(e._1, e._2, 1.0)
    })

    // $example on$
    // A graph with edge attributes containing distances
    val graph: Graph[Long, Double] = Graph(vertex, edge)

    val sourceId: VertexId = 0 // The ultimate source
    // Initialize the graph such that all vertices except the root have distance infinity.
    val initialGraph = graph.mapVertices((id, _) =>
      if (id == sourceId) 0.0 else Double.PositiveInfinity)
    val sssp = initialGraph.pregel(Double.PositiveInfinity)(
      (id, dist, newDist) => math.min(dist, newDist), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr + triplet.attr < triplet.dstAttr) {
          Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
        } else {
          Iterator.empty
        }
      },
      (a, b) => math.min(a, b) // Merge Message
    )
    sssp.vertices.map( vertex => (vertex._1, vertex._2)).saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/result")

    sc.stop()
  }
}
