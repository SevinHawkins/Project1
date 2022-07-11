package Project1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Spark {
  private var bool : Boolean = false
  private var spark : SparkSession = _

  def sparkConnect(): Unit = {
    spark = SparkSession
      .builder
      .appName("idk im not ur dad")
      .config("spark.master", "local[*]")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Connected to Spark")

    spark.SparkContext.setLogLevel("ERROR")
    val df = spark.read.csv("hdfs://localhost:9000/user/sevin/airports.csv")




}
