import java.nio.file.Files

import fr.esme.gdpr.configuration.JsonConfigProtocol._
import fr.esme.gdpr.configuration.{ConfigReader, JsonConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import spray.json._

object Launcher {

  def main(args: Array[String]): Unit = {

    //Add Scopt command line
    Logger.getLogger("org").setLevel(Level.OFF)

    println("hello")
    //    implicit val sparkSession = SparkSession.builder().master("local").getOrCreate()


    //create sparksession object
    implicit val sparkSession = SparkSession.builder().master("local").getOrCreate()

    val files =  new java.io.File("C:/Users/Victor HENRIO/Documents/ESME/ESME Ingé 3/Framework_Big_data/TP2/data").listFiles.filter(_.getName.endsWith(".csv"))

    files.foreach{println}

    val schema = StructType(
      StructField("amount", IntegerType, true) ::
        StructField("base_currency", StringType, true) ::
        StructField("currency", StringType, true) ::
        StructField("exchange_rate", DoubleType, true) ::
        StructField("date", StringType, true) :: Nil)

    val dataRDD = sparkSession.sparkContext.emptyRDD[Row]

    var alldf = sparkSession.createDataFrame(dataRDD,schema)

    for (file <- files ) {
      val df:DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", false).csv(file.toString)
      val date = Files.getLastModifiedTime(file.toPath)
      val dfwithdate = df.withColumn("date",lit(date.toString))
      dfwithdate.show()
      println(dfwithdate.schema)
      alldf = alldf.union(dfwithdate)
      alldf.show()
    }


    //val df:DataFrame = sparkSession.read.option("delimiter", ",").option("inferSchema", true).option("header", false).csv("data/*.csv")


    //    //Methode 1 :
    //    val pathStr = "data/echanges_2.csv"
    //    val date2 = Files.readAttributes(Paths.get(pathStr), classOf[BasicFileAttributes]).creationTime
    //    println(date2)

    //    //Methode 2 :
    //    val file = "data/echanges_2.csv"
    //    val filepath = Paths.get(file)
    //    val date3 = Files.getLastModifiedTime(filepath)
    //    println(date3)




  }
}