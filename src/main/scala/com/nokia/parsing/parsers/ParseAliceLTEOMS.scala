package com.nokia.parsing.parsers

import java.util.Scanner

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParseAliceLTEOMS {

  /**
    * lte_oms__df__df_out
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */

  def lte_oms__df__df_out(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    var line = txt.nextLine()
    while (txt.hasNextLine) {
      var logEntry = Map[String, String]()
      while (line.contains("Filesystem") |
        line.contains("Executing: df") | line.contains("##########")) {
        line = txt.nextLine()
        linNum += 1
      }
      if (line.trim.length != 0) {

        var line_split = line.split("\\s+")
        logEntry += ("line_number" -> linNum.toString,
          "FILESYSTEM" -> line_split(0).trim,
          "1K_BLOCKS" -> line_split(1).trim,
          "USED" -> line_split(2).trim,
          "AVAILABLE" -> line_split(3).trim,
          "USEDPERCENTAGE" -> line_split(4).replace("%", " ").trim,
          "MOUNTED_PATH" -> line_split(5).trim)
        logEntries += logEntry

      }
      line = txt.nextLine()
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * lte_oms__cat__ioms_mirroring_error
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def lte_oms__cat__ioms_mirroring_error(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = mutable.ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.contains("blocks super 1.2 [2/1]")) {
        logEntry += ("line_number" -> linNum.toString,
          "MIRROR_STATE" -> line.split("\\s+").last.trim)
        linNum += 1
        logEntries += logEntry.toMap
      }
      linNum += 1
    }
    logEntries.toList

  }

  /**
    * lte_oms__zstatus__zstatus_out_alarmprocessor_check
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def lte_oms__zstatus__zstatus_out_alarmprocessor_check(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("***************************")) {
        line = txt.nextLine()
        linNum += 1
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("alarm_id" -> ( line.stripPrefix("                   AlarmId:").trim))
        line = txt.nextLine()
        linNum += 1
        var mid = line.stripPrefix("                      MOId:")
        if (mid.contains(",fsAlarmProcessorId=") && mid.contains(",fsFragmentId=")) {
          logEntry += ("mo" -> ( mid.split(",fsAlarmProcessorId=")(0).trim))
          logEntry += ("fs_alarm_processor_id" -> ( mid.split(",fsAlarmProcessorId=")(1).split(",")(0).trim))
          logEntry += ("fs_fragment_id" -> ( mid.split(",fsFragmentId=")(1).split(",")(0).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("specific_problem" -> ( line.stripPrefix("           SpecificProblem:").trim))
          for (i <- 1 to 3) {
            line = txt.nextLine()
          }
          linNum += 3
          logEntry += ("alarm_time" -> ( line.stripPrefix("                 AlarmTime:").trim))
          linNum += 1
          logEntry += ("local_alarm_time" -> ( txt.nextLine().stripPrefix("           LOCAL_AlarmTime:").trim))
          logEntries += logEntry
        }
      }
      linNum += 1
    }

    logEntries.toList
  }

  /**
    * lte_oms__alarms__alarms_alarmprocessor_check
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def lte_oms__alarms__alarms_alarmprocessor_check(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 7
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("SP=") && line.contains("MO=") && line.contains("IINFO=") && line.contains("NINFO") && line.contains("TIME=")) {

        var split = line.split("\\s+")
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("month" -> ( split(0).trim))
        logEntry += ("day" -> ( split(1).trim))
        logEntry += ("time" -> ( split(2).trim))
        logEntry += ("specific_problem" -> ( line.split("SP=")(1).split("MO=")(0).trim))
        logEntry += ("mo" -> ( line.split("MO=")(1).split("\\s+")(0).trim))
        logEntry += ("iinfo" -> ( line.split("IINFO=")(1).split("\"")(1).trim))
        logEntry += ("alarm_time" -> ( line.split("TIME=")(1).split("\\s+")(0).trim))
        logEntries += logEntry


      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * lte_oms__syslog__spontaneous_ioms_reboot
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def lte_oms__syslog__spontaneous_ioms_reboot(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 7
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      var split = line.split("\\s+")
      logEntry += ("line_number" -> linNum.toString)
      logEntry += ("date_time" -> ( split(0).trim + " " + split(1).trim + " " + split(2).trim))
      logEntry += ("severity" -> ( split(3).trim))
      logEntry += ("unit" -> ( split(4).trim))
      logEntry += ("source" -> ( split(5).stripSuffix(":") trim))
      var msg = ""
      for (i <- 6 to split.length - 1) {
        msg = msg + " " + split(i)
      }
      logEntry += ("message" -> ( msg.trim))
      logEntries += logEntry


      linNum += 1
    }

    logEntries.toList


  }

}
