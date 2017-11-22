import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable

object GraphSim {
  val n = 6
  val post = new Array[ArrayBuffer[Int]](n + 1)
  val pre = new Array[ArrayBuffer[Int]](n + 1)

  def generatePattern(): Unit = {
    post(1) = ArrayBuffer(2, 3)
    post(2) = ArrayBuffer(4, 5)
    post(3) = ArrayBuffer(6)
    post(4) = ArrayBuffer(3)
    post(5) = ArrayBuffer(6)
    post(6) = ArrayBuffer()

    for(i <- 1 to n) {
      pre(i) = ArrayBuffer()
      for(j <- 1 to n) {
        if(post(j).contains(i)) {
          pre(i) += j
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    generatePattern()

    val sc = new SparkContext()
    val data: RDD[(VertexId, VertexId)] = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/com-lj.ungraph.txt", minPartitions = 10).map(s => {
      val d = s.split('\t')
      val u = d(0).toLong
      val v = d(1).toLong
      (u, v)
    }).cache()

    println("zs-log: finish load data")
    val vertex = data.flatMap(e => {
      Array((e._1, mutable.Set[VertexId]()), (e._2, mutable.Set[VertexId]()))
    }).distinct()

    val edge = data.map(e => {
      Edge(e._1, e._2, 1.0)
    })

    val graph = Graph(vertex, edge).cache()

    val postGraph = graph.edges.map(e => {
      (e.srcId, mutable.Set[VertexId](e.dstId))
    }).reduceByKey(_ ++ _)

    graph.unpersist()

    val tempG = graph.joinVertices(postGraph)((_, postSet, buffer) => {
      buffer ++ postSet
    }).mapVertices((id, postSet) => {
      val array = Array[Int](n + 1)
      for (i <- 1 to n) {
        if(post(i).isEmpty) array(i) = 1
        else array(i) = if(postSet.nonEmpty) 1 else 0
      }
      array
    }).cache()
    val postCount = tempG.triplets.map(triplets => {
      (triplets.srcId, triplets.dstAttr)
    }).reduceByKey((a, b) => {
      for (i <- 1 to n) {
        a(i) += b(i)
      }
      a
    })

    val initialGraph = tempG.mapVertices((_, array) => {
      val nowSet = mutable.Set[VertexId]()
      for(i <- 1 to n) {
        if(array(i) == 1) nowSet.add(i)
      }
      (nowSet, mutable.Set[VertexId](), new Array[Int](n + 1))
    }).joinVertices(postCount)((_, attr, msg) => {
      val deleteSet = mutable.Set[VertexId]()
      for(i <- 1 to n) {
        if(msg(i) == 0) {
          for(j <- pre(i)) {
            if(attr._1.contains(j))
              deleteSet += j
          }
        }
      }
      (attr._1, deleteSet, msg)
    })

    initialGraph.pregel(new Array[VertexId](n + 1), Int.MaxValue, EdgeDirection.In)

    sc.stop()
  }
}
