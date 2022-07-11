package Project1

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer
import scala.sys.exit

object userInterface {

  private var admin: Boolean = false
  private var bool: Boolean = false
  private var username: String = ""


  def main(args: Array[String]): Unit = {
    Spark.sparkConnect()
    database.databaseConnect()
    startup()
  }

  def startup(): Unit = {
    println("Welcome to my Av Nerd App!")
    println("Are you a:" + "\n" + "[1] New User" + "\n" + "[2] Existing User" + "\n" + "[3] Prefer to leave")
    val input = scala.io.StdIn.readLine()
    input match {
      case "1" => newAccount()
      case "2" => userLogin()
      case "3" => closeApp()
      case _ => println("Invalid input, please try again")
        startup()
    }
  }

  def newAccount(): Unit = {
    println("Let's create a new account!")
    println("What do you want your username to be?")
    username = scala.io.StdIn.readLine()
    println("What do you want your password to be?")
    val password = scala.io.StdIn.readLine()
    admin = false
    val creation = database.createUser(username, password, admin)
    if (creation == 1) {
      println("Account created!")
      startup()
    }
    else {
      println("Account creation failed!")
      startup()
    }
    newAccount()
  }

  @tailrec
  def userLogin(): Unit = {
    println("What is your username?")
    username = scala.io.StdIn.readLine()
    println("What is your password?")
    val password = scala.io.StdIn.readLine()
    val valid = database.validateLogin(username, password)
    if (valid) {
      println("Welcome " + username + "!")
      admin = database.validateAdmin(username)
      if (admin) {
        println("Booting to admin panel...")
        adminPanel()
      }
      else {
        println("Booting to user panel...")
        userPanel()
      }
    }
    else {
      println("Invalid username or password. Please try again.")
      userLogin()
    }

  }

  def userPanel(): Unit = {
    println {
      "Av Nerd App: User panel" + "\n" +
        "[1] Data Queries" + "\n" +
        "[2] Change username" + "\n" +
        "[3] Change password" + "\n" +
        "[4] Logout" + "\n"
    }
    val userInput = scala.io.StdIn.readLine()
    userInput match {
      case "1" => dataQueries()
      case "2" => changeUsername()
      case "3" => changePassword()
      case "4" => logout()
      case _ => println("Invalid input")
        userPanel()
    }
  }

  def adminPanel(): Unit = {
    println {
      "Av Nerd App: Admin panel" + "\n" +
        "[1] Data Queries" + "\n" +
        "[2] Change username" + "\n" +
        "[3] Change password" + "\n" +
        "[4] Delete user" + "\n" +
        "[5] Admin elevation" + "\n" +
        "[6] Logout" + "\n"
    }
    val adminInput = scala.io.StdIn.readLine()
    adminInput match {
      case "1" => dataQueries()
      case "2" => changeUsername()
      case "3" => changePassword()
      case "4" => deleteUser()
      case "5" => adminElevation()
      case "6" => logout()
      case _ => println("Invalid input")
        adminPanel()
    }
  }

  def dataQueries(): Unit = {
    println("Here is the nerdy stuff!")
    println("Select an option to start a query")
    println{
      "[1] Sort airports by airport ID" + "\n" +
      "[2] Sort by airport type" + "\n" +
      "[3] Sort by scheduled services" + "\n" +
      "[4] Sort by airport continent" + "\n" +
      "[5] Sort by airport country" + "\n" +
      "[6] Sort by airport region" + "\n" +
      "[7] Back to panel" + "\n"
    }
    val queryInput = scala.io.StdIn.readLine()
    queryInput match {
      case "1" => Spark.query1()
      case "2" => Spark.query2()
      case "3" => Spark.query3()
      case "4" => Spark.query4()
      case "5" => Spark.query5()
      case "6" => Spark.query6()
      case "7" => toTheMenus()
      case _ => println("Invalid input")
    }
    dataQueries()
  }

  def toTheMenus(): Unit = {
    if (admin) {
      adminPanel()
    }
    else {
      userPanel()
    }
  }

  def changeUsername(): Unit = {
    println("What do you want your new username to be?")
    val newUsername = scala.io.StdIn.readLine()
    val change = database.updateUsername(username, newUsername)
    if (change == 1) {
      println("Username changed!")
    }
    else {
      println("Username change failed!")
    }
    if(admin) {
      adminPanel()
    }
    else {
      userPanel()
    }
  }

  def changePassword(): Unit = {
    println("What do you want your new password to be?")
    val newPassword = scala.io.StdIn.readLine()
    val change = database.updatePassword(username, newPassword)
    if (change == 1) {
      println("Password changed!")
    }
    else {
      println("Password change failed!")
    }
    if(admin) {
      adminPanel()
    }
    else {
      userPanel()
    }
  }

  def deleteUser(): Unit = {
    println("Are you sure you want to delete your account?")
    val delete = scala.io.StdIn.readLine()
    if (delete == "yes") {
      val delete = database.deleteUser(username)
      if (delete == 1) {
        println("Account deleted!")
        startup()
      }
      else {
        println("Account deletion failed!")
        userPanel()
      }
    }
    else {
      println("Account deletion cancelled!")
      userPanel()
    }
  }

  def adminElevation(): Unit = {
    println(" List of current non admin users: ")
    var users: ListBuffer[String] = database.ShowNonAdmins()
    for (i <- users) {
      println(i)
    }
    println("Which user do you want to elevate to admin?")
    val user = scala.io.StdIn.readLine()
    val change = database.adminElevation(user)
    if (change == 1) {
      println("User elevated to admin!")
      adminPanel()
    }
    else {
      println("User elevation failed!")
      adminPanel()
    }
  }

  def logout(): Unit = {
    println("Logging out...")
    startup()
  }

  def closeApp(): Unit = {
    println("Closing app...")
    println("Goodbye!")
    Spark.close()
    database.disconnect()
    exit(0)
  }
}