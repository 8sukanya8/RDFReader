import java.util

import com.hp.hpl.jena.reasoner.TriplePattern
import com.hp.hpl.jena.shared.PrefixMapping
import com.hp.hpl.jena.sparql.algebra.OpVisitorBase
import com.hp.hpl.jena.sparql.algebra.op.OpBGP

import scala.collection.JavaConverters._

class OperationVisitorBase extends OpVisitorBase {
  private var prefixes: PrefixMapping = null
  def this(prefix: PrefixMapping) {
    this()
    this.prefixes = prefixes
  }

  override def visit(opBGP: OpBGP): Unit = {
    print("from inside visitor base")
    val patterns = opBGP.getPattern.getList
    patterns.iterator()

    val triples = new util.ArrayList[TriplePattern]()

    for (pattern <- patterns.asScala){
    val subjects = pattern.getSubject
      val objects  = pattern.getObject
      val predicate = pattern.getPredicate
    val triple = new TriplePattern(subjects, predicate,objects)
      triples.add(triple)
    }

    val x = 2
  }
}
