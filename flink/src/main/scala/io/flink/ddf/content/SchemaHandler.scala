package io.flink.ddf.content

import io.ddf.DDF
import io.flink.ddf.utils._

class SchemaHandler(theDDF: DDF) extends io.ddf.content.SchemaHandler(theDDF) {

  val parser = new TableDdlParser

  def parse(input: String): Function = parser.parse(input)


  import scala.util.parsing.combinator._

  class TableDdlParser extends JavaTokenParsers {

    def createOrLoad = create | load

    def create: Parser[Create] =
      ((CREATE ~> TABLE) ~> tableName) ~ columns ^^ { case name ~ contents => Create(name, contents)}

    def load: Parser[Load] =
      (LOAD ~> quotedStr) ~ (INTO ~> tableName) ^^ { case url ~ name => Load(name, url)}

    def tableName: Parser[String] = ident ^^ { case ident => ident}

    def columns: Parser[List[(String, String)]] = "(" ~> repsep(column, ",") <~ ")" ^^ {
      List() ++ _
    }

    def column: Parser[(String, String)] =
      columnName ~ dataType ^^ { case columnName ~ dataType => (columnName, dataType)}

    def columnName: Parser[String] = ident ^^ { case ident => ident}

    def dataType: Parser[String] = VARCHAR | INTEGER | INT | FLOAT | DOUBLE | DATE | TIMESTAMP | BOOLEAN | BOOL | STRING

    def quotedStr =
      "'" ~> ("""([^']|(?<=\\)')*""".r ^^ ((_: String).replace("\\'", "'"))) <~ "'"

    protected val VARCHAR = Keyword("VARCHAR")
    protected val INTEGER = Keyword("INTEGER")
    protected val INT = Keyword("INT")
    protected val FLOAT = Keyword("FLOAT")
    protected val DOUBLE = Keyword("DOUBLE")
    protected val DATE = Keyword("DATE")
    protected val STRING = Keyword("STRING")
    protected val TIMESTAMP = Keyword("TIMESTAMP")
    protected val BOOLEAN = Keyword("BOOLEAN")
    protected val BOOL = Keyword("BOOL")


    protected val CREATE = Keyword("CREATE")
    protected val LOAD = Keyword("LOAD")
    protected val INTO = Keyword("INTO")
    protected val TABLE = Keyword("TABLE")

    protected implicit def asParser(k: Keyword): Parser[String] = allCaseVersions(k.str).map(x => x: Parser[String]).reduce(_ | _)

    /** Generate all variations of upper and lower case of a given string */
    def allCaseVersions(s: String, prefix: String = ""): Stream[String] = {
      if (s == "") {
        Stream(prefix)
      } else {
        allCaseVersions(s.tail, prefix + s.head.toLower) ++
          allCaseVersions(s.tail, prefix + s.head.toUpper)
      }
    }

    def parse(input: String) = parseAll(createOrLoad, input) match {
      case s: Success[Function] => s.get
      case f: Failure =>
        val msg = "Cannot parse [" + input + "] because [" + f.msg + "]"
        throw new IllegalArgumentException(msg)
    }

  }

  case class Keyword(val str: scala.Predef.String) extends scala.AnyRef with scala.Product with scala.Serializable

}
