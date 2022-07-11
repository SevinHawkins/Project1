package Project1

import java.sql.{Connection, DriverManager, SQLException}
import scala.collection.mutable.ListBuffer

object database extends App {
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

  def createUser(username: String, password: String, admin: Boolean): Int = {
    val statement = connection.prepareStatement("INSERT INTO users (username, password, admin) VALUES (?, ?, ?)")
    var result = 0
    try {
      statement.setString(1, username)
      statement.setString(2, password)
      statement.setBoolean(3, admin)
      result = statement.executeUpdate()
      result
    } catch {
      case e: SQLException =>
        println("Error creating user")
        e.printStackTrace()
        result
    }
  }


  def displayUsername(username: String): Unit = {
    databaseConnect()
    val statement = connection.prepareStatement(s"SELECT * FROM users WHERE username = '$username'")
    try {
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
  }

  def updateUsername(username: String, newUsername: String): Int = {
    databaseConnect()
    var result = 0
    val statement = connection.prepareStatement(s"UPDATE users SET username = '$newUsername' WHERE username = '$username'")
    try{
      result = statement.executeUpdate()
      result
    }
    catch {
      case e: SQLException =>
        println("User creation failed! Check output console")
        e.printStackTrace()
        return 0
    }
  }

  def updatePassword(username: String, password: String): Int = {
    databaseConnect()
    var result = 0
    val statement = connection.prepareStatement(s"UPDATE users SET password = '$password' WHERE username = '$username'")
    try{
      result = statement.executeUpdate()
      result
    }
    catch {
      case e: SQLException =>
        println("User creation failed! Check output console")
        e.printStackTrace()
        return 0
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
  }

  def validateLogin(username: String, password: String): Boolean = {
    val statement = connection.prepareStatement(s"SELECT * FROM users WHERE username = '$username' AND password = '$password'")
    val valid = statement.executeQuery()
    try{
      if(valid.next()){
        true
      }
      else{
        false
      }
    }
    catch {
      case e: SQLException =>
        println("User creation failed! Check output console")
        e.printStackTrace()
        return false
    }
  }


  def validateAdmin(username: String): Boolean = {
    databaseConnect()
    val statement = connection.prepareStatement(s"SELECT * FROM users WHERE username = '$username' AND admin = 1")
    val validUsername = statement.executeQuery()
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
  }

  def ShowNonAdmins(): ListBuffer[String] = {
    databaseConnect()
    val statement = connection.prepareStatement(s"SELECT * FROM users WHERE admin = 0")
    val validUsername = statement.executeQuery()
    val nonAdmins = new ListBuffer[String]()
    try{
      while(validUsername.next()){
        nonAdmins += validUsername.getString("username")
      }
      return nonAdmins
    }
    catch {
      case e: SQLException =>
        println("User creation failed! Check output console")
        e.printStackTrace()
        return nonAdmins
    }
  }
  def disconnect(): Unit = {
    connection.close()

  }
}
