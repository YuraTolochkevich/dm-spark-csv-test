package dm

import Models.CastColumn
import io.circe.generic.extras.{Configuration}
import org.apache.spark.sql.{SparkSession}

import org.apache.log4j.Logger
import org.apache.log4j.Level


object Models {
  implicit val customConfig: Configuration =
    Configuration.default.withDefaults
   case class CastColumn(existingColName: String, newColName: String, newDataType: String, dateExpression: String="")

}

object SparkProcessor {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  def getStats(path: String ,columnsToCast: List[CastColumn]) {

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("localSparkApp")
      .config("spark.some.config.option", "config-value")
      .getOrCreate()

    val dataFrame = sparkSession.read.option("header", "true").csv(path)

    println("Initial data from cvs:")

    dataFrame.show()

    val columnsNames = dataFrame.schema.fields.map(_.name).toList

    val filtered = Utils.filterSpaceAndEmpty(dataFrame, columnsNames)

    println("Filtered empty rows:")
    filtered.show()

    val castedDf = Utils.castAllColumns(filtered,
      columnsToCast).drop( columnsNames diff columnsToCast.map(_.newColName) : _*)

    println("Cast to new types:")
    castedDf.show


    val prifilingCols = Utils.getProfiling(castedDf, columnsToCast.map(_.newColName))
    println("Profiling columns")
    println(prifilingCols)
  }


}
object main extends App {

  val parser = new scopt.OptionParser[Config]("scopt") {

    opt[String]('p', "path").required().action( (p, c) =>
      c.copy(path = p) ).text("Input CSV file path")

    opt[Seq[String]]('c', "column-update")
      .valueName("existingColName|name|newColName|newDataType,existingColName|name|newColName|newDataType|dateExpression")
      .action({
        case(seq, config) =>
          val decoded = seq.map { s =>
            val splitted = s.split('|')
            if (splitted.size==3) CastColumn(splitted(0), splitted(1),splitted(2))
            else CastColumn(splitted(0), splitted(1),splitted(2),splitted(3))
          }
        config.copy(castColumnsList = decoded.toList)})
      .validate( s =>
       if (s.forall(_.split("|").length>=3)) success
       else failure("Unable to parse input `column-update`." +
         " Use next format:existingColName|name|newColName|newDataType," +
         "existingColName|name|newColName|newDataType|dateExpression"))
  }

  case class Config(path: String = "", castColumnsList:  List[CastColumn] = List[CastColumn]())
  parser.parse(args, Config()) match {
    case Some(config) =>
    SparkProcessor.getStats(config.path, config.castColumnsList)
    case None =>
  }

}
