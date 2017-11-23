import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object GraphSim {
  val n = 6
  val post = new Array[mutable.Set[VertexId]](n + 1)
  val pre = new Array[mutable.Set[VertexId]](n + 1)

  def generatePattern(): Unit = {
    post(1) = mutable.Set[VertexId](2L, 3L)
    post(2) = mutable.Set[VertexId](4L, 5L)
    post(3) = mutable.Set[VertexId](6L)
    post(4) = mutable.Set[VertexId](3L)
    post(5) = mutable.Set[VertexId](6L)
    post(6) = mutable.Set[VertexId]()

    for (i <- 1 to n) {
      pre(i) = mutable.Set[VertexId]()
      for (j <- 1 to n) {
        if (post(j).contains(i.toLong)) {
          pre(i) += j.toLong
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    generatePattern()

    val sc = new SparkContext()
    val data: RDD[(VertexId, VertexId)] = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/test.txt", minPartitions = 10).map(s => {
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
      val array = new Array[Int](n + 1)
      for (i <- 1 to n) {
        if (post(i).isEmpty) array(i) = 1
        else array(i) = if (postSet.nonEmpty) 1 else 0
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
      for (i <- 1 to n) {
        if (array(i) == 1) nowSet.add(i)
      }
      (nowSet, mutable.Set[VertexId](), new Array[Int](n + 1))
    }).joinVertices(postCount)((_, attr, msg) => {
      val deleteSet = mutable.Set[VertexId]()
      for (i <- 1 to n) {
        if (msg(i) == 0) {
          for (j <- pre(i)) {
            if (attr._1.contains(j))
              deleteSet += j
          }
        }
      }
      (attr._1, deleteSet, msg)
    })

    val finalGraph = initialGraph.pregel(new Array[Int](n + 1), Int.MaxValue, EdgeDirection.In)(
      (id, attr, msg) => {
        val nowSet = attr._1
        var deleteSet = attr._2
        val postArray = attr._3
        nowSet --= deleteSet
        deleteSet = mutable.Set[VertexId]()
        for (i <- 1 to n) {
          if (msg(i) != 0) {
            postArray(i) -= msg(i)
            if (postArray(i) == 0) {
              deleteSet ++= nowSet & pre(i)
            }
          }
        }
        (nowSet, deleteSet, postArray)
      }, triplet => {
        if (triplet.dstAttr._2.isEmpty) {
          Iterator.empty
        } else {
          val msg = new Array[Int](n + 1)
          for (s <- triplet.dstAttr._2) {
            msg(s.toInt) = 1
          }
          Iterator((triplet.srcId, msg))
        }
      }, (a, b) => {
        for (i <- 1 to n) {
          a(i) += b(i)
        }
        a
      })

    finalGraph.vertices.flatMap(v => {
      val buffer = new mutable.ArrayBuffer[(VertexId, mutable.Set[VertexId])]()
      for (s <- v._2._1) {
        buffer.append((s, mutable.Set(v._1)))
      }
      buffer
    }).reduceByKey(_ ++ _).saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/sim")

    sc.stop()
  }
}