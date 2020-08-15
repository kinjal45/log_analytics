package com.nokia.parsing.parsers

import java.util.Scanner
import Utils.Util._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParseAlicemcBSC {

  /**
    * mcbsc__mcbc___zw6t_flowcontrol
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def mcbsc__mcbc___zw6t_flowcontrol(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = mutable.ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.startsWith("storm-control")) {
        println(line)
        logEntry += ("line_number" -> linNum.toString,
          "FLOWCONTROL" -> line.split("storm-control")(1).trim)
        linNum += 1
        logEntries += logEntry.toMap

      }
      linNum += 1
    }

    logEntries.toList

  }

  /**
    * mcbsc__bsc_alarms__zaho_bsc_alarm_check
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def mcbsc__bsc_alarms__zaho_bsc_alarm_check(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("          ")) {

        logEntry += ("line_number" -> linNum.toString)

        logEntry ++= mcBscZahoBscAlarmsFirstLine(line, linNum)
        linNum += 1
        logEntry ++= mcBscZahoBscAlarmsSecondLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= mcBscZahoBscAlarmsThirdLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= mcBscZahoBscAlarmsFourthLine(txt.nextLine, linNum)
        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList


  }

  def mcBscZahoBscAlarmsFirstLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    var split = line.split("\\s")
    split = line.trim.split("\\s+")
    logEntry += ("history" -> ( line.substring(4, 10)))
    logEntry += ("bsc" -> ( split(0)))
    logEntry += ("unit" -> ( split(1)))
    logEntry += ("alart_type" -> ( split(2)))
    logEntry += ("date_time" -> ( split(3) + split(4)))
    logEntry

  }

  def mcBscZahoBscAlarmsSecondLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("param" -> ( line.substring(3, 10)))
    logEntry += ("type" -> ( line.substring(11, 17)))
    logEntry += ("issuer" -> ( line.substring(22, 30)))
    logEntry += ("cart" -> ( line.substring(34, 40)))
    logEntry

  }

  def mcBscZahoBscAlarmsThirdLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("notification_id" -> ( line.substring(4, 10).trim))
    logEntry += ("alarm_no" -> ( line.substring(11, 15).trim))
    logEntry += ("alarm_txt" -> ( line.substring(16, 70).trim))
    logEntry

  }

  def mcBscZahoBscAlarmsFourthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("supp" -> ( line.trim))
    logEntry

  }

  /**
    * mcbsc__io_system__zw7i_fea_state
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def mcbsc__io_system__zw7i_fea_state(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("FEATURE CODE:..............")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("feature_code" -> (line.split("FEATURE CODE:..............")(1).trim))
        line= txt.nextLine()
        linNum+=1
        logEntry += ("feature_name" -> (line.split("FEATURE NAME:..............")(1).trim))
        line= txt.nextLine()
        linNum+=1
        logEntry += ("feature_state" -> (line.split("FEATURE STATE:.............")(1).trim))
        line= txt.nextLine()
        linNum+=1
        if(line.contains("FEATURE CAPACITY:..........")) {
          logEntry += ("feature_capacity" -> (line.split("FEATURE CAPACITY:..........")(1).trim))
        }else{logEntry += ("feature_capacity" -> "")}
        logEntries += logEntry

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * mcbsc__bts_seg_and_trx_parameters__zeqo_bts_ibho
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def mcbsc__bts_seg_and_trx_parameters__zeqo_bts_ibho(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("BCF-") && line.contains("BTS-")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bcf" -> line.split("BTS-")(0).stripPrefix("BCF-").trim)
        logEntry += ("bts" -> line.split("BTS-")(1).split("\\s+")(0).trim)
         logEntry += ("name"->line.split("BTS-")(1).split("\\s+").lift(1).getOrElse("").trim)
        while (txt.hasNext() && !line.contains("IBHO GSM ENABLED .....................(IGE)...")) {
          line = txt.nextLine()
          linNum += 1
        }
        logEntry += ("param" -> line.trim.stripPrefix("IBHO GSM ENABLED .....................(IGE)...").trim)
        line = txt.nextLine();
        linNum += 1
        logEntry += ("param1" -> line.trim.stripPrefix("IBHO WCDMA ENABLED ...................(IWE)...").trim)
        logEntries += logEntry
      }
      linNum += 1
    }
    logEntries.toList
  }



}
