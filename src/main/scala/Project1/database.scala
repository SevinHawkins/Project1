package Project1

import org.apache.hadoop.shaded.org.eclipse.jetty.util.security.Password
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

import java.sql.{Connection, DriverManager, SQLException}
import scala.collection.mutable.ListBuffer

object database extends App{
  private var connection: Connection = _

  def databaseConnect(): Unit = {
    val url = "jdbc:mysql://localhost:3306/project1"
    val driver = "com.mysql.jdbc.Driver"
    val username = "root"
    val password = "JewelGote@2021"

    try {
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)
    } catch {
      case e: SQLException =>
        println("Connection Failed! Check output console")
        e.printStackTrace()
        return
    }
    println("Connection to database successful")
  }

  def createUser(username: String, password: String, admin: Int): Int = {
    databaseConnect()
    var result = 0
    val statement = connection.prepareStatement(s"INSERT INTO users (username, password, admin) VALUES ('$username', '$password', $admin)")
    try{
      statement.setString(1, username)
      statement.setString(2, password)
      statement.setInt(3, admin)
      result = statement.executeUpdate()
      print("User created successfully")
      result
    }
    catch {
      case e: SQLException =>
        println("User creation failed! Check output console")
        e.printStackTrace()
        return 0
    }
    finally {
      connection.close()
    }



  }
  def displayUsername(username: String): Unit = {
    databaseConnect()
    val statement = connection.prepareStatement(s"SELECT * FROM users WHERE username = '$username'")
    try{
      val resultSet = statement.executeQuery()
      while (resultSet.next()) {
        println(resultSet.getString("username"))
      }
    }
    catch {
      case e: SQLException =>
        println("User creation failed! Check output console")
        e.printStackTrace()
        return
    }
    finally {
      connection.close()
    }
  }

  def checkIfexsists(username: String): Boolean = {
    databaseConnect()
    val statement = connection.prepareStatement(s"SELECT * FROM users WHERE username = '$username'")
    try{
      val resultSet = statement.executeQuery()
      if(resultSet.next()){
        return true
      }
      else{
        return false
      }
    }
    catch {
      case e: SQLException =>
        println("User creation failed! Check output console")
        e.printStackTrace()
        return false
    }
    finally {
      connection.close()
    }
  }

  def adminElevation(username: String): Unit = {
    databaseConnect()
    val statement = connection.prepareStatement(s"UPDATE users SET admin = 1 WHERE username = '$username'")
    try{
      statement.executeUpdate()
      println("Admin elevation successful")
    }
    catch {
      case e: SQLException =>
        println("Admin elevation failed! Check output console")
        e.printStackTrace()
        return
    }
    finally {
      connection.close()
    }
  }

  def deleteUser(username: String): Unit = {
    databaseConnect()
    val statement = connection.prepareStatement(s"DELETE FROM users WHERE username = '$username'")
    try{
      statement.executeUpdate()
      println("User deletion successful")
    }
    catch {
      case e: SQLException =>
        println("User deletion failed! Check output console")
        e.printStackTrace()
        return
    }
    finally {
      connection.close()
    }
  }

  def validateLogin(username: String, password: String): Unit = {
    databaseConnect()
    val statement = connection.prepareStatement(s"SELECT * FROM users WHERE username = '$username' AND password = '$password'")
    try{
      val resultSet = statement.executeQuery()
      if(resultSet.next()){
        println("Login successful")
      }
      else{
        println("Login failed")
      }
    }
    catch {
      case e: SQLException =>
        println("Login failed! Check output console")
        e.printStackTrace()
        return
    }
    finally {
      connection.close()
    }
  }

  def validateAdmin(username: String): Boolean = {
    databaseConnect()
    val statement = connection.prepareStatement("SELECT * FROM users WHERE username = '$username' AND admin = 1")
    var validUsername = statement.executeQuery()
    try{
      if(!validUsername.next()){
        return false
      }
      else{
        return true
      }
    }
    catch {
      case e: SQLException =>
        println("User creation failed! Check output console")
        e.printStackTrace()
        return false
    }
    finally {
      connection.close()
    }

  }
  def disconnect(): Unit = {
    connection.close()

  }
}
