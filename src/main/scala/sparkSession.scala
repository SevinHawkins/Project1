import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

class sparkSession {
  def makeIt(): Unit = {
    val spark = SparkSession
      .builder
      .config("spark.master", "local[*]")
      .appName("Av Nerd Network")
      .enableHiveSupport()
      .getOrCreate()
    Logger.getLogger("org").setLevel(Level.ERROR)
    println("Spark Session created")
  }
  def closeIt(): Unit = {
    SparkSession.clearActiveSession()
    SparkSession.clearDefaultSession()
    println("Spark Session closed")
  }

}
