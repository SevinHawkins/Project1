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

    spark.sparkContext.setLogLevel("ERROR")
    val df = spark.read.csv("hdfs://localhost:9000/user/sevin/airports.csv")
    df.createOrReplaceTempView("airports")

    // query 1
    spark.sql("DROP TABLE IF EXISTS query1")
    spark.sql("create table query1('id' int, 'name' string, 'continent' string) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("insert into table query1(select id, name, continent from airports)")

    // query 2
    spark.sql("DROP TABLE IF EXISTS query2")
    spark.sql("create table query2('id' int, 'name' string, 'type' string, 'continent' string) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("insert into table query2(select id, name, type, continent from airports)")

    // query 3
    spark.sql("DROP TABLE IF EXISTS query3")
    spark.sql("create table query3('id' int, 'name' string, 'type' string, 'continent' string, 'scheduled_service' string) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("insert into table query3(select id, name, type, continent, scheduled_service from airports)")

    // query 4
    spark.sql("DROP TABLE IF EXISTS query4")
    spark.sql("create table query4('id' int, 'name' string, 'type' string, 'continent' string, 'iso_country' string, 'iso_region' string, 'municipality' string) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("insert into table query4(select id, name, type, continent, iso_country, iso_region, municipality from airports)")

    // query 5
    spark.sql("DROP TABLE IF EXISTS query5")
    spark.sql("create table query5('id' int, 'name' string, 'type' string, 'continent' string, 'iso_country' string, 'iso_region' string, 'municipality' string, row format delimited fields terminated by ',' stored as textfile")
    spark.sql("insert into table query5(select id, name, type, continent, iso_country, iso_region, municipality from airports)")

    // query 6
    spark.sql("DROP TABLE IF EXISTS query6")
    spark.sql("create table query6('id' int, 'ident' string, 'name' string, 'type' string, 'continent' string, 'iso_country' string, 'iso_region' string, 'municipality' string) row format delimited fields terminated by ',' stored as textfile")
    spark.sql("insert into table query6(select id, ident, name, type, continent, iso_country, iso_region, municipality from airports)")

  }

  // query 1: sort by id
  def query1(): Unit = {
    spark.sql("select * from query1 order by id").show()
  }

  // query 2: select and sort by airport type
  def query2(): Unit = {
    var userInput = ""
    val typeList = List("balloonport", "closed", "heliport", "seaplane_base", "small_airport", "medium_airport", "large_airport")
    do {
      println("Enter the type of airport you want to see:")
      println {
        for (i <- 0 to typeList.length - 1)
          print(i + ": " + typeList(i) + "\n")
      }
      userInput = scala.io.StdIn.readLine()
      bool = typeList.contains(userInput)
    } while (!bool)
    spark.sql("select * from query2 where type = '" + userInput + "' order by id").show()
  }

  // query 3: select and sort by scheduled service
  def query3(): Unit = {
    var userInput = ""
    val serviceList = List("no", "yes")
    do {
      println("Enter the scheduled service of airport you want to see:")
      println {
        for (i <- 0 to serviceList.length - 1)
          print(i + ": " + serviceList(i) + "\n")
      }
      userInput = scala.io.StdIn.readLine()
      bool = serviceList.contains(userInput)
    } while (!bool)
    spark.sql("select * from query3 where scheduled_service = '" + userInput + "' order by id").show()
  }

  // query 4: select and sort by continent
  def query4(): Unit = {
    var userInput = ""
    val continentList = List("africa", "antarctica", "asia", "australia", "europe", "north_america", "south_america")
    do {
      println("Enter the continent of airport you want to see:")
      println {
        for (i <- 0 to continentList.length - 1)
          print(i + ": " + continentList(i) + "\n")
      }
      userInput = scala.io.StdIn.readLine()
      bool = continentList.contains(userInput)
    } while (!bool)
    spark.sql("select * from query4 where continent = '" + userInput + "' order by id").show()
  }

  // query 5: select and sort by iso_country
  def query5(): Unit = {
    var userInput = ""
    val countryList = List("AF", "AX", "AL", "DZ", "AS", "AD", "AO", "AI", "AQ", "AG", "AR", "AM", "AW", "AU", "AT", "AZ", "BS", "BH", "BD", "BB", "BY", "BE", "BZ", "BJ", "BM", "BT", "BO", "BQ", "BA", "BW", "BV", "BR", "IO", "BN", "BG", "BF", "BI", "KH", "CM", "CA", "CV", "KY", "CF", "TD", "CL", "CN", "CX", "CC", "CO", "KM", "CG", "CD", "CK", "CR", "CI", "HR", "CU", "CW", "CY", "CZ", "DK", "DJ", "DM", "DO", "EC", "EG", "SV", "GQ", "ER", "EE", "ET", "FK", "FO", "FJ", "FI", "FR", "GF", "PF", "TF", "GA", "GM", "GE", "DE", "GH", "GI", "GR", "GL", "GD", "GP", "GU", "GT", "GG", "GN", "GW", "GY", "HT", "HM", "VA", "HN", "HK", "HU", "IS", "IN", "ID", "IR", "IQ", "IE", "IM", "IL", "IT", "JM", "JP", "JE", "JO", "KZ", "KE", "KI", "KP", "KR", "KW", "KG", "LA", "LV", "LB", "LS", "LR", "LY", "LI", "LT", "LU", "MO", "MK", "MG", "MW", "MY", "MV", "ML", "MT", "MH", "MQ", "MR", "MU", "YT", "MX", "FM", "MD", "MC", "MN", "ME", "MS", "" +
      "MA", "MZ", "MM", "NA", "NR", "NP", "NL", "NC", "NZ", "NI", "NE", "NG", "NU", "NF", "MP", "NO", "OM", "PK", "PW", "PS", "PA", "PG", "PY", "PE", "PH", "PN", "PL", "PT", "PR", "QA", "RE", "RO", "RU", "RW", "BL", "SH", "KN", "LC", "MF", "PM", "VC", "WS", "SM", "ST", "SA", "SN", "RS", "SC", "SL", "SG", "SK", "SI", "SB", "SO", "ZA", "GS", "SS", "ES", "LK", "SD", "SR", "SJ", "SZ", "SE", "CH", "SY", "TW", "TJ", "TZ", "TH", "TL", "TG", "TK", "TO", "TT", "TN", "TR", "TM", "TC", "TV", "UG", "UA", "AE", "GB", "US", "UM", "UY", "UZ", "VU", "VE", "VN", "VG", "VI", "WF", "EH", "YE", "ZM", "ZW")
    do {
      println("Enter the country of airport(s) you want to see:")
      println {
        for (i <- 0 to countryList.length - 1)
          print(i + ": " + countryList(i) + "\n")
      }
      userInput = scala.io.StdIn.readLine()
      bool = countryList.contains(userInput)
    } while (!bool)
    spark.sql("select * from query5 where iso_country = '" + userInput + "' order by id").show()
  }

  // query 6: sort by name
  def query6(): Unit = {
    spark.sql("select * from query6 order by name").show()
  }

  // close spark
  def close(): Unit = {
    spark.stop()
  }
}





















































































































































































































































































































































  