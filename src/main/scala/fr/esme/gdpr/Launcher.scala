import fr.esme.gdpr.DataFrameReader
import fr.esme.gdpr.configuration.{ConfigReader, JsonConfig}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{lit}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import spray._
import spray.json._
import fr.esme.gdpr.configuration.JsonConfig._
import fr.esme.gdpr.configuration.JsonConfigProtocol._
import fr.esme.gdpr.utils.schemas.DataFrameSchema
import java.nio.file._
import java.time._
import java.time.temporal.ChronoUnit.DAYS
import java.nio.file.{Files, Paths, Path}
import java.nio.file.attribute.BasicFileAttributes
import java.io.File
import java.nio.file.Path
import java.io._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.apache.commons.io.FilenameUtils

import scala.reflect.io.Path



object Launcher {


  def main(args: Array[String]): Unit = {
    //Add Scopt command line
    Logger.getLogger("org").setLevel(Level.OFF)

    println("hello")

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
      val date = file.getName()
      val filenamewithoutext = FilenameUtils.removeExtension(date)
      val dfwithdate = df.withColumn("date",lit(filenamewithoutext))
      alldf = alldf.union(dfwithdate)
      alldf.show()




    }



  }
}

