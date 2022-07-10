package Project1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark extends App {
  def sparkMaker(): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    val spark = SparkSession
      .builder
      .appName("Does this even matter?")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    spark.sparkContext.setLogLevel("ERROR")
    println("Spark Session created")
  }

}
