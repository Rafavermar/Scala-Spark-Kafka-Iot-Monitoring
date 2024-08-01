package projectutil


import org.apache.log4j.Logger

import scala.Console.{BOLD, RESET}

trait PrintUtils {
  def printBoldMessage(message: String): Unit = {
    println(BOLD + message + RESET)
  }
  def printAndLog(message: String, logLevel: String = "INFO")(implicit log: Logger) : Unit = {
    println(message)
    logLevel match {
      case "ERROR" => log.error(message)
      case "WARN" => log.warn(message)
      case "INFO" => log.info(message)
      case "DEBUG" => log.debug(message)
      case _ => log.info(message)
    }
  }
}