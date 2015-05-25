package io.flink.ddf.content

import io.ddf.DDF
import io.flink.ddf.utils._
import org.apache.flink.api.table.expressions.{And, Expression, Or}
import org.apache.flink.api.table.parser.ExpressionParser
import org.apache.flink.api.table.parser.ExpressionParser._


class SchemaHandler(theDDF: DDF) extends io.ddf.content.SchemaHandler(theDDF) {

  val parser = new TableDdlParser

  def parse(input: String): Function = parser.parse(input)

  trait ExprParser extends ExpressionParser.PackratParser[Function] {
    lazy val expression: ExpressionParser.PackratParser[Expression] = alias
    lazy val expressionList: Parser[List[Expression]] = repsep(expression, ",")

    def select: Parser[Select] =
      project ~ tableNames ~ join.? ~ where.? ~ group.? ^^ {
        case p ~ t ~ j ~ w ~ g => {
          val iProject = Project(p.toArray: _*)
          val iFrom = From(t.toArray: _*)
          val iWhere = w map {
            case e: Expression => Where(e)
            case _ => null
          }
          val iGroup = g map {
            case e: List[Expression] => Group(e.toArray: _*)
            case _ => null
          }

          val iJoin = j map {
            case e: List[Expression] => Join(e)
            case _ => null
          }

          Select(iProject, iFrom, iWhere, iGroup, iJoin)
        }
      }

    def nestedExpr: ExpressionParser.PackratParser[BinaryExpr] = "(".? ~> binExpr <~ ")".? ^^ { case e => new BinaryExpr(e, true)}

    def andExpr: ExpressionParser.PackratParser[AndExpr] = AND ~> nestedExpr ^^ { case e => AndExpr(e.expr)}

    def orExpr: ExpressionParser.PackratParser[OrExpr] = OR ~> nestedExpr ^^ { case e => OrExpr(e.expr)}

    def binExpr: ExpressionParser.PackratParser[Expression] = expression ~ repsep(andExpr | orExpr, " ").? ^^ { case e1 ~ e2 =>
      expr(e1,e2.getOrElse(List()))
    }

    def project: ExpressionParser.PackratParser[List[Expression]] = SELECT ~> expressionList ^^ { e => e}

    def tableNames: ExpressionParser.PackratParser[List[String]] = FROM ~> repsep(tableName, ",") ^^ {
      List() ++ _
    }

    def join: ExpressionParser.PackratParser[Expression] = JOIN ~> ON ~> binExpr ^^ { e => e}

    def group: ExpressionParser.PackratParser[List[Expression]] = GROUP ~> BY ~> expressionList ^^ { e => e}

    def where: ExpressionParser.PackratParser[Expression] = WHERE ~> binExpr ^^ { e => e}

    def tableName: ExpressionParser.PackratParser[String] = ident ^^ { case ident => ident}

    def columns: ExpressionParser.PackratParser[List[(String, String)]] = "(" ~> repsep(column, ",") <~ ")" ^^ {
      List() ++ _
    }

    def create: ExpressionParser.PackratParser[Create] =
      ((CREATE ~> TABLE) ~> tableName) ~ columns ^^ { case name ~ contents => Create(name, contents)}

    def load: ExpressionParser.PackratParser[Load] =
      (LOAD ~> quotedStr) ~ (INTO ~> tableName) ^^ { case url ~ name => Load(name, url)}


    def column: ExpressionParser.PackratParser[(String, String)] =
      columnName ~ dataType ^^ { case columnName ~ dataType => (columnName, dataType)}

    def columnName: ExpressionParser.PackratParser[String] = ident ^^ { case ident => ident}

    def dataType: ExpressionParser.PackratParser[String] = VARCHAR | INTEGER | INT | FLOAT | DOUBLE | DATE | TIMESTAMP | BOOLEAN | BOOL | STRING

    def quotedStr =
      "'" ~> ("""([^']|(?<=\\)')*""".r ^^ ((_: String).replace("\\'", "'"))) <~ "'"

    protected val VARCHAR = ExpressionParser.Keyword("VARCHAR")
    protected val INTEGER = ExpressionParser.Keyword("INTEGER")
    protected val INT = ExpressionParser.Keyword("INT")
    protected val FLOAT = ExpressionParser.Keyword("FLOAT")
    protected val DOUBLE = ExpressionParser.Keyword("DOUBLE")
    protected val DATE = ExpressionParser.Keyword("DATE")
    protected val STRING = ExpressionParser.Keyword("STRING")
    protected val TIMESTAMP = ExpressionParser.Keyword("TIMESTAMP")
    protected val BOOLEAN = ExpressionParser.Keyword("BOOLEAN")
    protected val BOOL = ExpressionParser.Keyword("BOOL")

    protected val TABLE = ExpressionParser.Keyword("TABLE")

    protected val SELECT = ExpressionParser.Keyword("SELECT")
    protected val FROM = ExpressionParser.Keyword("FROM")
    protected val WHERE = ExpressionParser.Keyword("WHERE")
    protected val JOIN = ExpressionParser.Keyword("JOIN")
    protected val GROUP = ExpressionParser.Keyword("GROUP")
    protected val BY = ExpressionParser.Keyword("BY")
    protected val ON = ExpressionParser.Keyword("ON")
    protected val AND = ExpressionParser.Keyword("AND")
    protected val OR = ExpressionParser.Keyword("OR")

    protected val CREATE = ExpressionParser.Keyword("CREATE")
    protected val LOAD = ExpressionParser.Keyword("LOAD")
    protected val INTO = ExpressionParser.Keyword("INTO")

    protected implicit def asParser(k: Keyword): ExpressionParser.PackratParser[String] = allCaseVersions(k.str).map(x => x: Parser[String]).reduce(_ | _)

    /** Generate all variations of upper and lower case of a given string */
    def allCaseVersions(s: String, prefix: String = ""): Stream[String] = {
      if (s == "") {
        Stream(prefix)
      } else {
        allCaseVersions(s.tail, prefix + s.head.toLower) ++
          allCaseVersions(s.tail, prefix + s.head.toUpper)
      }
    }

  }


  class TableDdlParser extends ExprParser {
    def createOrLoadOrSelect: ExpressionParser.PackratParser[Function] = create | load | select

    override def apply(in: Input): ParseResult[Function] = createOrLoadOrSelect(new PackratReader[ExpressionParser.Elem](in))

    def parse(input: String) = parseAll(createOrLoadOrSelect, input) match {
      case s: Success[Function] => s.get
      case f: Failure =>
        val msg = "Cannot parse [" + input + "] because " + f.msg
        throw new IllegalArgumentException(msg)
    }

  }

  case class Keyword(val str: scala.Predef.String) extends scala.AnyRef with scala.Product with scala.Serializable

  def expr(e:Expression,l: List[BinaryExpr]): Expression = {
    val expression: Expression =
      if (!l.isEmpty) {
        var booleanExpr: Expression = if(l.head.isAnd) new And(e,l.head.expr) else new Or(e,l.head.expr)
          l.tail.foreach { i =>
            i match {
              case o: OrExpr => booleanExpr = new Or(booleanExpr, o.expr)
              case a: BinaryExpr => booleanExpr = new And(booleanExpr, a.expr)
            }
        }
        booleanExpr
      } else e
    expression
  }
}

object SchemaHandler extends SchemaHandler(null) {
  def main(args: Array[String]) {
    val create =
      """CREATE TABLE person (first_name VARCHAR, last_name VARCHAR, age INTEGER, expYear INT, married BOOLEAN) """
    println(parser.parse(create))
    val load =
      """LOAD 'file:///usrs/juin/io/persons' into person"""
    println(parser.parse(load))
    val select = """SELECT a,b,c,d from e,f join on (a=c) where b=1"""
    val select2 = """SELECT a,b,c,d from e,f join on (a=c) where a=2 OR (b=1)"""
    val select3 = """SELECT a,b,c,d from e,f join on (a=c) where a=2 OR (b=1 AND c=2)"""
    val select4 = """SELECT a,b,c,d from e,f join on (a=c) where a=2 AND d=1 OR (b=1 AND c=2) group by a"""
    println(parser.parse(select))
    println(parser.parse(select2))
    println(parser.parse(select3))
    println(parser.parse(select4))
  }

}

