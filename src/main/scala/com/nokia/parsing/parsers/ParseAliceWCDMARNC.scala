package com.nokia.parsing.parsers

import java.util.Scanner

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParseAliceWCDMARNC {

  /**
    *
    * wcdma_rnc__omu_computer_logs__omu_computer_logs
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

/* def wcdma_rnc__omu_computer_logs__omu_computer_logs(logfilecontent: String): List[Map[String, String]] = {
    println("Executing wcdma_rnc__omu_computer_logs__omu_computer_logs")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.contains("CALLER:")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {
        if (line.contains("CALLER:")) {
          my_map += ("CALLER" -> (line.substring(12, 27)).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("WRITE TIME:")) {
          my_map += ("WRITE_TIME" -> (line.substring(12, line.length)).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("PARAMETERS:")) {
          my_map += ("PARAMETERS" -> (line.substring(12, line.length)).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("USER TEXT:")) {
          my_map += ("USER_TEXT" -> (line.substring(12, line.length)).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("USER DATA:")) {
          my_map += ("USER_DATA" -> (line.substring(12, line.length)).replaceAll("""(?m)\s+$""", ""))
        }
        if (my_map.nonEmpty && line == "") {
          my_map += "line_number" -> start_log.toString
          log_started = false
          val colsList = List("CALLER", "WRITE_TIME", "PARAMETERS", "USER_TEXT", "USER_DATA")
          colsList.foreach(x => {
            if (!my_map.keys.toList.contains(x)) my_map += x -> ""
          })
          list_map += my_map.toMap
        }
      }
      lineNum += 1
    }
    list_map.toList.filter(_.size>1)
  }
*/
def wcdma_rnc__omu_computer_logs__omu_computer_logs(logfilecontent: String): List[Map[String, String]] = {
    println("Executing wcdma_rnc__omu_computer_logs__omu_computer_logs")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var userdatalineno=0
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.contains("CALLER    :")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {
        if (line.contains("CALLER    :")) {
          my_map += ("CALLER" -> (line.substring(12, 27)).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("WRITE TIME:")) {
          my_map += ("WRITE_TIME" -> (line.substring(12, line.length)).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("PARAMETERS:")) {
          my_map += ("PARAMETERS" -> (line.substring(12, line.length)).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("USER TEXT :")) {
          my_map += ("USER_TEXT" -> (line.substring(12, line.length)).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("USER DATA :")) {
          userdatalineno=lineNum
          my_map += ("USER_DATA" -> (line.substring(11, line.length)).replaceAll("""(?m)\s+$""", ""))

        }
        else if((lineNum==userdatalineno+1) && (!line.contains(":"))){
          my_map += ("USER_DATA1" -> (line).replaceAll("""(?m)\s+$""", ""))
        }
        else if((lineNum==userdatalineno+2) && (!line.contains(":"))){
          my_map += ("USER_DATA2" -> (line).replaceAll("""(?m)\s+$""", ""))
        }

        if (my_map.nonEmpty && line == "") {
          my_map += "line_number" -> start_log.toString
          log_started = false
          val colsList = List("CALLER", "WRITE_TIME", "PARAMETERS", "USER_TEXT", "USER_DATA","USER_DATA1","USER_DATA2")
          colsList.foreach(x => {
            if (!my_map.keys.toList.contains(x)) my_map += x -> ""
          })
          list_map += my_map.toMap
        }
      }
      lineNum += 1
    }
    list_map.toList.filter(_.size>1)
  }


  /**
    * wcdma_rnc__functional_units__zusi_full
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def wcdma_rnc__functional_units__zusi_full(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.endsWith(" UNIT            PHYS   LOG    STATE  INFO")) {
        line = txt.nextLine()
        linNum += 1
        logEntry += ("line_number" -> linNum.toString)

        while (line.length != 0) {
          logEntry += ("unit" -> ( line.substring(0, 17).trim))
          logEntry += ("phys" -> ( line.substring(17, 24).trim))
          logEntry += ("log" -> ( line.substring(24, 31).trim))
          logEntry += ("state" -> ( line.substring(31, 38).trim))
          logEntry += ("info" -> ( line.splitAt(38)._2.trim))
          logEntries += logEntry
          line = txt.nextLine()
          linNum += 1
        }
      }

      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * wcdma_rnc__diagnostics__diagnostics_report_history_unit_diagnose_status
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def wcdma_rnc__diagnostics__diagnostics_report_history_unit_diagnose_status(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line == "DIAGNOSTIC REPORT") {
        txt.nextLine()
        line = txt.nextLine()
        linNum += 2
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("unit" -> ( line.split("\\s+")(1).trim))

        while (!line.contains("DIAGNOSIS EXECUTED - ")) {
          line = txt.nextLine()
          linNum += 1
        }
        logEntry += ("execution_status" -> ( line.split("-")(1).trim))
        logEntries += logEntry

      }

      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * wcdma_rnc__active_alarms__rnc_active_alarm
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def wcdma_rnc__active_alarms__rnc_active_alarm(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("    (")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("alarm_consecutive_number" -> ( line.substring(0, 12).stripPrefix("    (").stripSuffix(") ").trim))
        logEntry += ("rnc_id" -> ( line.substring(12, 29).trim))
        logEntry += ("dumm1" -> ( line.substring(29, 45).trim))
        logEntry += ("alarm_label" -> ( line.splitAt(45)._2.split("\\s+")(0).trim))
        logEntry += ("date_time" -> ( line.substring(58, 80).trim))
        line = txt.nextLine()
        linNum += 1
        logEntry += ("severity" -> ( line.substring(0, 4).trim))
        logEntry += ("notification_type" -> ( line.substring(4, 12).trim))
        logEntry += ("unit" -> ( line.substring(12, 22).trim))
        logEntry += ("wcel" -> ( line.splitAt(22)._2.trim))
        line = txt.nextLine()
        linNum += 1
        if (line.trim.split("\\s+").length == 1) {
          line = txt.nextLine();
          linNum += 1
          var split = line.trim.split("\\s+")
          logEntry += ("alarm_number" -> ( split(0).trim))
          var alarm_description = ""
          for (i <- 1 to split.length - 1) {
            alarm_description = alarm_description + " " +split(i)
          }
          logEntry += ("alarm_description" -> ( alarm_description.trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("set_by" -> ( line.split("\\s+")(0).trim))
          logEntry += ("set_at" -> ( line.split("\\s+")(1).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("suppl_info1" -> ( line.trim))
          line = txt.nextLine()
          linNum += 1
          if (line.isEmpty) {
            logEntry += ("suppl_info2" -> "")
          } else {
            logEntry += ("suppl_info2" -> ( line.trim))
          }
          logEntries += logEntry
        }else{

          var split = line.trim.split("\\s+")
          logEntry += ("alarm_number" -> ( split(0).trim))
          var alarm_description = ""
          for (i <- 1 to split.length - 1) {
            alarm_description = alarm_description + " " + split(i)
          }
          logEntry += ("alarm_description" -> ( alarm_description.trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("set_by" -> ( line.split("\\s+")(0).trim))
          logEntry += ("set_at" -> ( line.split("\\s+")(1).trim))
          line = txt.nextLine()
          linNum += 1
          if(line.isEmpty){
            logEntry += ("suppl_info1" -> "")
            logEntry += ("suppl_info2" -> "")
          }else{
            logEntry += ("suppl_info1" -> ( line.trim))
            line = txt.nextLine()
            linNum += 1
            if (line.isEmpty) {
              logEntry += ("suppl_info2" -> "")
            } else {
              logEntry += ("suppl_info2" -> ( line.trim))
            }

          }
          logEntries += logEntry

        }

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * wcdma_rnc__alarm_history__rnc_alarm_history
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def wcdma_rnc__alarm_history__rnc_alarm_history(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("HST (")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("alarm_consecutive_number" -> ( line.substring(0, 12).stripPrefix("HST (").stripSuffix(") ").trim))
        logEntry += ("rnc_id" -> ( line.substring(12, 29).trim))
        logEntry += ("dumm1" -> ( line.substring(29, 45).trim))
        logEntry += ("alarm_label" -> ( line.splitAt(45)._2.split("\\s+")(0).trim))
        logEntry += ("date_time" -> ( line.substring(58, 80).trim))
        line = txt.nextLine()
        linNum += 1
        logEntry += ("severity" -> ( line.substring(0, 4).trim))
        logEntry += ("notification_type" -> ( line.substring(4, 12).trim))
        logEntry += ("unit" -> ( line.substring(12, 22).trim))
        logEntry += ("wcel" -> ( line.splitAt(22)._2.trim))
        line = txt.nextLine()
        linNum += 1
        if (line.trim.split("\\s+").length == 1) {
          line = txt.nextLine();
          linNum += 1
          var split = line.trim.split("\\s+")
          logEntry += ("alarm_number" -> ( split(0).trim))
          var alarm_description = ""
          for (i <- 1 to split.length - 1) {
            alarm_description = alarm_description + " " + split(i)
          }
          logEntry += ("alarm_description" -> ( alarm_description.trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("set_by" -> ( line.split("\\s+")(0).trim))
          logEntry += ("set_at" -> ( line.split("\\s+")(1).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("suppl_info1" -> ( line.trim))
          line = txt.nextLine()
          linNum += 1
          if (line.isEmpty) {
            logEntry += ("suppl_info2" -> "")
          } else {
            logEntry += ("suppl_info2" -> ( line.trim))
          }
          logEntries += logEntry
        }else{

          var split = line.trim.split("\\s+")
          logEntry += ("alarm_number" -> ( split(0).trim))
          var alarm_description = ""
          for (i <- 1 to split.length - 1) {
            alarm_description = alarm_description + " " + split(i)
          }
          logEntry += ("alarm_description" -> ( alarm_description.trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("set_by" -> ( line.split("\\s+")(0).trim))
          logEntry += ("set_at" -> ( line.split("\\s+")(1).trim))
          line = txt.nextLine()
          linNum += 1
          if(line.isEmpty){
            logEntry += ("suppl_info1" -> "")
            logEntry += ("suppl_info2" -> "")
          }else{
            logEntry += ("suppl_info1" -> ( line.trim))
            line = txt.nextLine()
            linNum += 1
            if (line.isEmpty) {
              logEntry += ("suppl_info2" -> "")
            } else {
              logEntry += ("suppl_info2" -> ( line.trim))
            }

          }
          logEntries += logEntry

        }

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * wcdma_rnc__dsp_mml_data__cells_configured_in_dsp_pool
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def wcdma_rnc__dsp_mml_data__cells_configured_in_dsp_pool(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 7
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("CCH POOL CONFIGURATION: POOL 3     CONFIGURED CELLS:")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("configured_cells" -> ( line.split("CCH POOL CONFIGURATION: POOL 3     CONFIGURED CELLS:")(1).trim))
        logEntries += logEntry
      }
      linNum += 1
    }
    logEntries.toList
  }


  /**
    * wcdma_rnc__wbts_info__wbts_wcell_count
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def wcdma_rnc__wbts_info__wbts_wcell_count(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.startsWith("Connecting to RNW database...")) {

        while (txt.hasNextLine && !(line.startsWith("Handled successfully") && (line.contains("WBTSs,") && line.contains("matching WCELs")))) {
          line = txt.nextLine()
          linNum += 1
        }

        if (line.startsWith("Handled successfully") && line.contains("WBTSs,") && line.contains("matching WCELs")) {
          logEntry += ("line_number" -> linNum.toString)
          logEntry += ("wbts_count" -> ( line.stripPrefix("Handled successfully ").split("WBTSs,")(0).trim))
          logEntry += ("wcell_count" -> ( line.split("WBTSs,")(1).split("matching WCELs")(0).trim))
          logEntries += logEntry
        }

      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * wcdma_rnc__ip_configuration_txt__zqri_network_if_data
    * @param logfilecontent
    * @return
    */

  def wcdma_rnc__ip_configuration_txt__zqri_network_if_data(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZQRI")) {

        while(!line.contains("------------------------------------------------------------------")){
          line = txt.nextLine()
          linNum+=1
        }

        line = txt.nextLine;
        linNum+=1

        logEntry += ("line_number" -> linNum.toString)

        while (!line.startsWith("COMMAND EXECUTED")) {
          if(!line.isEmpty && !((line.contains(" P ") && line.contains("/")) || (line.contains(" L ") && line.contains("/")))) {
            println(line)
            logEntry += ("unit" -> line.substring(0, 10).trim)
            logEntry += ("if_name" -> line.substring(10, 20).trim)
            logEntry += ("adm_state" -> line.substring(20, 29).trim)
            logEntry += ("mtu" -> line.substring(29, 37).trim)
            logEntry += ("frag" -> line.substring(37, 44).trim)
            logEntry += ("if_priority" -> line.substring(44, 57).trim)
            logEntry += ("if_type" -> line.splitAt(57)._2.trim)
            txt.nextLine;
            line = txt.nextLine()
            linNum += 2
            if (line.contains(" P ") && line.contains("/")) {
              var value = line.trim.split("\\s+")
              logEntry += ("p_add_type" -> value(0).trim)
              logEntry += ("p_source_ip" -> value(1).split("/")(0).trim)
              logEntry += ("p_subnet" -> value(1).split("/")(1).trim)
              logEntry += ("p_dest_ip" -> value.lift(2).getOrElse("").trim)
              logEntry += ("l_add_type" -> "")
              logEntry += ("l_source_ip" -> "")
              logEntry += ("l_subnet" -> "")
              logEntry += ("l_dest_ip" -> "")
              logEntries += logEntry
              txt.nextLine;
              line = txt.nextLine()
              linNum += 2
            } else if (line.contains(" L ") && line.contains("/")) {
              println(line)
              var value = line.trim.split("\\s+")
              logEntry += ("l_add_type" -> value(0).trim)
              logEntry += ("l_source_ip" -> value(1).split("/")(0).trim)
              logEntry += ("l_subnet" -> value(1).split("/")(1).trim)
              logEntry += ("l_dest_ip" -> value.lift(2).getOrElse("").trim)
              logEntry += ("p_add_type" -> "")
              logEntry += ("p_source_ip" -> "")
              logEntry += ("p_subnet" -> "")
              logEntry += ("p_dest_ip" -> "")
              logEntries += logEntry
              txt.nextLine;
              line = txt.nextLine()
              linNum += 2
            } else {
              logEntry += ("l_add_type" -> "")
              logEntry += ("l_source_ip" -> "")
              logEntry += ("l_subnet" -> "")
              logEntry += ("l_dest_ip" -> "")
              logEntry += ("p_add_type" -> "")
              logEntry += ("p_source_ip" -> "")
              logEntry += ("p_subnet" -> "")
              logEntry += ("p_dest_ip" -> "")
              logEntries += logEntry
              txt.nextLine;
              line = txt.nextLine()
              linNum += 2
            }
          }else{
            println("else")
            line = txt.nextLine()
            linNum += 1
          }

        }

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * wcdma_rnc__mxu_10_computer_logs_txt__mxu_computer_logs
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def wcdma_rnc__mxu_10_computer_logs_txt__mxu_computer_logs(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if(line.startsWith("CALLER:")&& line.contains("TYPE:")&&line.contains("DATE:")&&line.contains("TIME:")){

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("caller" -> line.stripPrefix("CALLER:").split("TYPE:")(0).trim)
        logEntry += ("type" -> line.split("TYPE:")(1).split("DATE:")(0).trim)
        logEntry += ("date" -> line.split("DATE:")(1).split("TIME:")(0).trim)
        logEntry += ("time" -> line.split("TIME:")(1).trim)
        line = txt.nextLine()
        linNum+=1
        logEntry += ("user_text" -> line.stripPrefix("USER TEXT:").trim)
        line = txt.nextLine()
        linNum+=1
        logEntry += ("user_data" -> line.stripPrefix("USER DATA:").trim)
        logEntries += logEntry
      }
      linNum += 1
    }

    return logEntries.toList
  }





}
