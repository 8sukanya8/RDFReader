package RdfReader

import org.apache.spark.SparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object RDFReader {

  def readRdf(sc:SparkContext, filename:String) = {

    val lines = sc.textFile(filename).map(_.split(" "))
    val linesContents = lines.collect()

    val triple = lines.map(x=>(x(0),x(1),x(2)))
    val tripleContents = triple.collect()

    val subjects: RDD[String] = triple.map(triple => triple._1)
    val subjectsContents = subjects.collect()

    val objects: RDD[String] = triple.map(triple => triple._3)
    val objectsContents = objects.collect()


    val distinctNodes: RDD[String] = sc.union(subjects, objects).distinct
    val distinctNodesContents = distinctNodes.collect()

    val zippedNodes: RDD[(String, Long)] = distinctNodes.zipWithUniqueId
    val zippedNodesContents = zippedNodes.collect()

    //val edges1 = r.map( x=> (x(0),(x(2),x(1)))).join(zippedNodes).map( x=> (x._2._1._1,(x._2._1._2))).collect()
    val edges = lines.map( x=> (x(0),(x(2),x(1)))).join(zippedNodes).
      map( x=> (x._2._1._1,(x._2._1._2,x._2._2))).join(zippedNodes).
      map( x=> new Edge(x._2._1._2,x._2._2,x._2._1._1))

    val edgesContents = edges.collect()

    val nodes: RDD[(VertexId, Any)] = zippedNodes.map(node => {
      val id = node._2
      val attribute = node._1
      (id, attribute)
    })

    Graph(nodes, edges)
  }

/*
  def main(args: Array[String]) {
    val sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName("readrdf"))
    val t0 = System.currentTimeMillis
    val graph = readRdf(sc, args(0))
    val graphEdges = graph.edges.collect()
    val graphVertices = graph.vertices.collect()

    println("Book example #edges=" + graph.edges.count +
      " #vertices=" + graph.vertices.count)

    val t1 = System.currentTimeMillis
    println("Elapsed: " + ((t1-t0) / 1000) + "sec")
    sc.stop

    /*
    * @prefix ppl: <http://example.org/people#>.
      @prefix foaf: <http://xmlns.com/foaf/0.1/>.
    */
  }*/
}
