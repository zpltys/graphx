import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphSim {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext()
    val data: RDD[(VertexId, VertexId)] = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/com-lj.ungraph.txt", minPartitions = 10).map(s => {
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

    val graph = Graph(vertex, edge)
    
    graph.vertices.map(_._1).saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/vertices")

  }
}
