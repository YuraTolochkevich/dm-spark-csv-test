package dm

import Models.CastColumn
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.to_date
import io.circe._, io.circe.generic.semiauto._
import io.circe.syntax._


object Utils {

  val dateTypeName = "date"

  def castAllColumns(sourceDf: DataFrame, castColumn: List[CastColumn]): DataFrame = castColumn match {
    case hd :: tl =>
      val res = castColumnTo(sourceDf, hd)
      castAllColumns(res, tl)
    case Nil => sourceDf
  }

  def castColumnTo(df: DataFrame, castColumn: CastColumn): DataFrame = {
    castColumn.newDataType match {
      case `dateTypeName` =>
        df.withColumn(castColumn.newColName, to_date(df(castColumn.existingColName), castColumn.dateExpression))
      case _ =>
        df.withColumn(castColumn.newColName, df(castColumn.existingColName).cast(castColumn.newDataType))
    }
  }

  def filterSpaceAndEmpty(df: DataFrame, columns: Seq[String]) = {

    val regex = "'^ *$'"

    def buildFilterCase(cols: List[String]): String = {
      cols match {
        case col :: xf if xf != Nil => s"($col is null or $col  not rlike $regex) and " + buildFilterCase(xf)
        case col :: _ => s"($col is null  or $col not rlike $regex)"
        case Nil => ""
      }
    }
    df.where(buildFilterCase(columns.toList))
  }

  def getProfiling(df: DataFrame, columns: List[String]) = {

    val profilingData = columns.map { n =>

      val grouped = df.groupBy(n).count.na.drop
      val kv = grouped.collect.map { case Row(cn: Any, num: Long) => Map(cn.toString -> num) }
      Output(n, grouped.count(), kv)
    }

    implicit val encoder: Encoder[Output] = deriveEncoder

    profilingData.asJson
  }

  case class Output(Column: String, Unique_values: Long, Values: Array[Map[String, Long]])
}
