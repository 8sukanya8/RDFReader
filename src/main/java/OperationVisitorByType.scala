import com.hp.hpl.jena.sparql.algebra.{OpVisitor, OpVisitorByType}
import com.hp.hpl.jena.sparql.algebra.op._


class OperationVisitorByType extends OpVisitorByType {

  private var visitor: OpVisitor = null

  def this(visitor: OpVisitor) {
    this()
    this.visitor = visitor
  }


  override def visitFilter(opFilter: OpFilter): Unit = ???

  override def visitExt(opExt: OpExt): Unit = ???

  override def visitN(opN: OpN): Unit = ???

  override def visit0(op0: Op0): Unit ={
    print("inside visitor by type")
    op0.visit(visitor)
  }

  override def visitLeftJoin(opLeftJoin: OpLeftJoin): Unit = ???

  override def visit2(op2: Op2): Unit = ???

  override def visit1(op1: Op1): Unit = ???
}
