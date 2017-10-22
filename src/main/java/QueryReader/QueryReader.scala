package QueryReader

import com.hp.hpl.jena.query.Query
import com.hp.hpl.jena.sparql.algebra.Op
import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph, VertexId}
import org.apache.spark.rdd.RDD

object QueryReader {



  def readQuery(sc:SparkContext, patterns:RDD[Array[String]]) = {


    val triple = patterns.map(x=>(x(0),x(1),x(2)))
    val tripleContents = triple.collect()


    val subjects: RDD[String] = triple.map(triple => triple._1)
    val subjectsContents = subjects.collect()

    val objects: RDD[String] = triple.map(triple => triple._3)
    val objectsContents = objects.collect()

    val distinctNodes: RDD[String] = sc.union(subjects, objects).distinct
    val distinctNodesContents = distinctNodes.collect()

    val zippedNodes: RDD[(String, Long)] = distinctNodes.zipWithUniqueId
    val zippedNodesContents = zippedNodes.collect()

    val edges = triple.map( x=> (x._1,(x._2,x._3))).join(zippedNodes).map( x=> (x._2._1._2,(x._2._1._1,x._2._2))).join(zippedNodes).map( x=> new Edge(x._2._1._2,x._2._2,x._2._1._1))

    val edgesContents = edges.collect()
    val x= 3
    val nodes: RDD[(VertexId, Any)] = zippedNodes.map(node => {
      val id = node._2
      val attribute = node._1
      (id, attribute)
    })

    Graph(nodes, edges)
  }

  def readQueryOp(sc:SparkContext, opBGP: Op) = {


     //patterns.map(x=>(x(0),x(1),x(2)))
    //val tripleContents = triples.collect()

    /*
    val subjects: RDD[String] = triples.
    val subjectsContents = subjects.collect()

    val objects: RDD[String] = triple.map(triple => triple._3)
    val objectsContents = objects.collect()

    val distinctNodes: RDD[String] = sc.union(subjects, objects).distinct
    val distinctNodesContents = distinctNodes.collect()

    val zippedNodes: RDD[(String, Long)] = distinctNodes.zipWithUniqueId
    val zippedNodesContents = zippedNodes.collect()

    val edges = triple.map( x=> (x._1,(x._2,x._3))).join(zippedNodes).map( x=> (x._2._1._2,(x._2._1._1,x._2._2))).join(zippedNodes).map( x=> new Edge(x._2._1._2,x._2._2,x._2._1._1))

    val edgesContents = edges.collect()
    val x= 3
    val nodes: RDD[(VertexId, Any)] = zippedNodes.map(node => {
      val id = node._2
      val attribute = node._1
      (id, attribute)
    })

    Graph(nodes, edges)*/
  }


  def parseQuery(sc:SparkContext, query: Query) = {

    //val op = Algebra.compile(query)

    var patternInitial = query.getQueryPattern.toString.filterNot(_ == ' ')

    if (patternInitial.contains('{')){
      patternInitial = patternInitial.replaceAll("\\{","")
    }
    if (patternInitial.contains('}')){
      patternInitial = patternInitial.replaceAll("\\}","")
    }
    if (patternInitial.contains('.')){
      patternInitial = patternInitial.replaceAll("\\.","")
    }

    val patternsSplit: RDD[String] = sc.parallelize(patternInitial.split("\n"))
    val patternRefined = patternsSplit.map(pattern => pattern.split("<|>"))
    //val patternRefinedContents = patternRefined.collect()
    patternRefined

  }

  /*def main(args: Array[String]) {
    val sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName("readrdf"))
    val t0 = System.currentTimeMillis

    val query = QueryFactory.read(args(1).trim)

    val patternRefined = parseQuery(sc,query)

    val queryGraph = readQuery(sc, patternRefined)
    val graphEdges = queryGraph.edges.collect()
    val graphVertices = queryGraph.vertices.collect()

    val prefixes = query.getPrefixMapping
    val opRoot = Algebra.compile(query)
    val op: OpBGP = new OpBGP()
    //op = query.

    println("Book example #edges=" + queryGraph.edges.count +
      " #vertices=" + queryGraph.vertices.count)

    val t1 = System.currentTimeMillis
    println("Elapsed: " + ((t1-t0) / 1000) + "sec")
    sc.stop
  }*/
}
