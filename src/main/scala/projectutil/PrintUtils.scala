package projectutil


import scala.Console.{BOLD, RESET}

trait PrintUtils {
  def printBoldMessage(message: String): Unit = {
    println(BOLD + message + RESET)
  }
}