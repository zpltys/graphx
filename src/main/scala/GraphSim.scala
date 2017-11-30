import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import scala.collection.mutable

object GraphSim {
  val n = 8
  val post = new Array[mutable.Set[VertexId]](n + 1)
  val pre = new Array[mutable.Set[VertexId]](n + 1)
  val vecType = new Array[Int](n + 1)

  def generatePattern(): Unit = {
    post(1) = mutable.Set[VertexId](2L, 3L)
    post(2) = mutable.Set[VertexId](4L, 8L)
    post(3) = mutable.Set[VertexId](4L, 7L)
    post(4) = mutable.Set[VertexId](1L, 8L)
    post(5) = mutable.Set[VertexId](3L, 6L, 8L)
    post(6) = mutable.Set[VertexId](2L, 7L)
    post(7) = mutable.Set[VertexId](4L)
    post(8) = mutable.Set[VertexId](4L)

    for (i <- 1 to n) {
      vecType(i) = i
      pre(i) = mutable.Set[VertexId]()
      for (j <- 1 to n) {
        if (post(j).contains(i.toLong)) {
          pre(i) += j.toLong
        }
      }
    }
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    generatePattern()

    val condMsg = conf.getAll

    println("zs-log:" + condMsg)

    val startTime = System.currentTimeMillis()

    val partition = args(0).toInt

    println("zs-log: partition:" + partition)

 /*
    val vertex = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/label.txt", minPartitions = 10).flatMap(line => {
      val msg = line.split('\t')
      val id = msg(0).toLong
      val label = msg(1).toInt
      if(label <= n) {
        Some((id, (label, mutable.Set[VertexId]())))
      } else None
    }).cache()
   */

    val vertex = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/label.txt", minPartitions = partition).map(line => {
      val msg = line.split('\t')
      val id = msg(0).toLong
      val label = msg(1).toInt

      (id, (label, mutable.Set[VertexId]()))
    }).cache()


    println("zs-log: vertex.size:" + vertex.count())


    val tmpPair = sc.textFile("alluxio://hadoopmaster:19998/zpltys/graphData/soc-LiveJournal1.txt", minPartitions = partition).map(s => {
      val d = s.split('\t')
      val u = d(0).toLong
      val v = d(1).toLong
      (u, v)
    })

   // val filterU = tmpPair.join(vertex).map(tuple => (tuple._2._1, tuple._1))
   // val filterV = filterU.join(vertex).map(tuple => (tuple._2._1, tuple._1))
   // val edge = filterV.map(e => Edge(e._1, e._2, 0))

    val edge = tmpPair.map(edge => Edge(edge._1, edge._2, 0)).cache()

    println("zs-log: edge.size:" + edge.count())

    val graph = Graph(vertex, edge).cache()

    val loadOk = System.currentTimeMillis()
    println("zs-log: load graph ok, load graph time:" + (loadOk - startTime) / 1000 + "s")

    edge.unpersist()
    vertex.unpersist()

    //calculate post set of data graph
    val postGraph = graph.edges.map(e => {
      (e.srcId, mutable.Set[VertexId](e.dstId))
    }).reduceByKey(_ ++ _).cache()

    println("zs-log: postGraph:" + postGraph.count())
   // postGraph.saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/postGraph")

    //initial sim
    val tempG = graph.joinVertices(postGraph)((_, postSet, buffer) => {
      (postSet._1, buffer ++ postSet._2)
    }).mapVertices((_, postSet) => {
      val array = new Array[Int](n + 1)
      for (i <- 1 to n) {
        if (vecType(i) != postSet._1) array(i) = 0
        else {
          if (post(i).isEmpty) array(i) = 1
          else array(i) = if (postSet._2.nonEmpty) 1 else 0
        }
      }
      array
    }).cache()

    postGraph.unpersist()
    graph.unpersist()


    println("zs-log: tempG.vertices:" + tempG.vertices.count())


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
    }).joinVertices(postCount)((_, attr, message) => {
      val deleteSet = mutable.Set[VertexId]()
      for (i <- 1 to n) {
        if (message(i) == 0) {
          for (j <- pre(i)) {
            if (attr._1.contains(j))
              deleteSet += j
          }
        }
      }
      (attr._1, deleteSet, message)
    })

    tempG.unpersist()

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

    println("zs-log: final graph calculated, vertex size:" + finalGraph.vertices.count())
    val stopTime = System.currentTimeMillis()
    println("zs-log: finish calculated, calculate time:" + (stopTime - loadOk) / 1000 + "s")

    finalGraph.vertices.flatMap(v => {
      val buffer = new mutable.ArrayBuffer[(VertexId, VertexId)]()
      for (s <- v._2._1) {
        buffer.append((s, v._1))
      }
      buffer
    }).saveAsTextFile("alluxio://hadoopmaster:19998/zpltys/graphData/sim" + partition)

   sc.stop()
  }
}