import QueryReader.QueryReader._
import RdfReader.RDFReader.readRdf
import com.hp.hpl.jena.query.QueryFactory
import com.hp.hpl.jena.sparql.algebra.Algebra
import org.apache.spark.{SparkConf, SparkContext}


object main {

  def main(args: Array[String]) {
    val sc = new SparkContext(
      new SparkConf().setMaster("local").setAppName("readrdf"))

    val t0 = System.currentTimeMillis

    val RDFGraph = readRdf(sc, args(0))
    val RDFGraphEdges = RDFGraph.edges.collect()
    val RDFGraphVertices = RDFGraph.vertices.collect()

    println("RDF Graph #edges=" + RDFGraph.edges.count +
      " #vertices=" + RDFGraph.vertices.count)
    val query = QueryFactory.read(args(1).trim)
    val patternRefined = parseQuery(sc,query)

    val op = Algebra.compile(query)

    val prefixes = query.getPrefixMapping

    val opVB = new OperationVisitorBase(prefixes)

    val visitor=  new OperationVisitorByType(opVB)
    op.visit(visitor)




    val queryGraph = readQuery(sc, patternRefined)

    val queryGraphEdges = queryGraph.edges.collect()
    val queryGraphVertices = queryGraph.vertices.collect()

    println("Query Graph #edges=" + queryGraph.edges.count +
      " #vertices=" + queryGraph.vertices.count)

    val t1 = System.currentTimeMillis
    println("Elapsed: " + ((t1-t0) / 1000) + "sec")
    sc.stop


  }

}
