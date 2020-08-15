package com.nokia.parsing.parsers

import java.util.Scanner

import Utils.Util._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex

object ParseAliceSRANBTS {

  /**
    * sran_bts__scf__x2_link_status
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def sran_bts__scf__x2_link_status(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    var another_dest_field_present = false
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.contains("<managedObject class=\"NOKLTE:LNADJ\"")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {

        if (line.contains("<p name=\"adjEnbId\">")) {
          my_map += ("LNADJ" -> line.substring(line.indexOf(">") + 1, line.indexOf("/") - 1).trim)
        }
        if (line.contains("<p name=\"xLinkStatus\">")) {
          my_map += ("LINK_STATUS" -> line.substring(line.indexOf(">") + 1, line.indexOf("/") - 1).trim)
        }

        if (lineNum != (start_log + 1) && line.contains("</managedObject>")) {
          my_map += "line_number" -> start_log.toString
          log_started = false

          val cols_list = List("LNADJ", "LINK_STATUS")
          cols_list.foreach(x => {
            if (!my_map.keys.toList.contains(x)) {
              my_map += (x -> "")
            }
          })

          list_map += my_map.toMap
        }
      }
      lineNum += 1
    }

    list_map.toList.filter(_.size > 1)
  }

  /**
    * sran_bts__snapshot_file_list__dsp_crash
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def sran_bts__snapshot_file_list__dsp_crash(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = mutable.ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.contains("MEMDUM")) {

        logEntry += ("line_number" -> linNum.toString,
          "PARAM1" -> line.split("_")(3).trim,
          "PARAM2" -> line.split("_").last.split(".bin")(0).trim)

        linNum += 1
        logEntries += logEntry.toMap

      }
      linNum += 1
    }

    logEntries.toList

  }

  /**
    * sran_bts__011_rawalarmhistory__rawalarmhistory
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def sran_bts__011_rawalarmhistory__rawalarmhistory(logfilecontent: String): List[Map[String, String]] = {
    println("sran_bts__011_rawalarmhistory__rawalarmhistory")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var secondlineno, thirdlineno, endlineno: Int = 0
    var endlinelist, secondlinelist, thirdlinelist = ListBuffer[Int]()
    var firstline_dttm_text = txt.nextLine().split("\\s+").last
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.length == 0) {
        endlinelist += lineNum
      }
      if (line.contains("faults")) {
        secondlinelist += lineNum
      }
      if (line.contains("=======================")) {
        thirdlinelist += lineNum
      }

      lineNum += 1

    }
    endlinelist += lineNum

    for (startline <- secondlinelist) {
      val linedata = dataArray(startline - 1)
      var alarm_type_text = (linedata.split("//s+")(0).trim)
      if(!endlinelist.toList.filter(_ > startline + 4).isEmpty) {
        endlineno = endlinelist.toList.filter(_ > startline + 4).min
      }
      val regex=new Regex("[0-9a-zA-Z]+")
      
      for (linenno <- startline + 2 until endlineno) {
        my_map.clear()
        val tabledata = dataArray(linenno - 1)
        if ((tabledata.trim.length != 0) && tabledata.contains("Z\t(")) {

          var line_split = tabledata.split("\\s+")
          if (line_split(4).length < 3) {
            line_split(4) = ""
          }
          my_map += (
            "line_number" -> linenno.toString,
            "DTTM" -> firstline_dttm_text.trim,
            "TYPE" -> alarm_type_text,
            "FAULT_DTTM" -> line_split(0).trim,
            "FAULT_ID" -> regex.findFirstIn(line_split(1).trim).getOrElse().toString.trim,
            "FAULT_NAME" -> line_split(2).trim,
            "SOURCE" -> line_split(3).trim,
            "SOURCE_ADDITIONAL" -> line_split(4).trim
          )
        }
        list_map += my_map.toMap
      }
    }

    list_map.toList.filter(_.size > 1)
  }

  /**
    * sran_bts__scf__scf_dlmimotype
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def sran_bts__scf__scf_dlmimotype(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.contains("<managedObject class=\"NOKLTE:LNCEL_FDD\"")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {

        if (line.contains("<p name=\"dlMimoMode\">")) {

          my_map += ("MIMO_TYPE" -> line.substring(line.indexOf(">") + 1, line.indexOf("/") - 1).trim)
        }
        if (line.contains("<managedObject class=\"NOKLTE:LNCEL_FDD\"")) {
          my_map += ("MRBTS_ID" -> line.split("MRBTS-")(1).split("/LNBTS")(0).trim,
            "LNBTS_ID" -> line.split("LNBTS-")(1).split("/LNCEL")(0).trim,
            "LNCEL_ID" -> line.split("LNCEL-")(1).split("/LNCEL_FDD")(0).trim)
        }

        if (lineNum != (start_log + 1) && line.contains("</managedObject>")) {
          my_map += "line_number" -> start_log.toString
          log_started = false

          val cols_list = List("MIMO_TYPE", "MRBTS_ID", "LNBTS_ID", "LNCEL_ID")
          cols_list.foreach(x => {
            if (!my_map.keys.toList.contains(x)) {
              my_map += (x -> "")
            }
          })

          list_map += my_map.toMap
        }
      }
      lineNum += 1
    }

    list_map.toList.filter(_.size > 1)
  }

  /**
    * sran_bts__tas_extended_startup__tas_extended_startup_lmpipcheck
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def sran_bts__tas_extended_startup__tas_extended_startup_lmpipcheck(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = mutable.ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.startsWith("INF/tas/  out: [<address")) {
        logEntry += ("line_number" -> linNum.toString,
          "PARAM" -> line.split("=")(1).split("\\s+")(0).trim)
        linNum += 1
        logEntries += logEntry.toMap

      }
      linNum += 1
    }

    logEntries.toList

  }

  /**
    *
    * sran_bts__scf__scf_parameters_all
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def sran_bts__scf__scf_parameters_all(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("<cmData")) {

        while (!line.contains("</cmData>")) {
          logEntry = Map[String, String]()
          if (line.contains("<p name=\"") && line.contains("</p>")) {
            logEntry += "line_number" -> linNum.toString
            logEntry += ("param" -> line.split("<p name=\"")(1).split(">")(0).stripSuffix("\"").trim)
            logEntry += ("value" -> line.split("<p name=\"")(1).split(">")(1).split("<")(0).trim)
            logEntries += logEntry
          }
          line = txt.nextLine()
          linNum += 1
        }

      }

      linNum += 1
    }

    logEntries.toList
  }

  /**
    * sran_bts__startup_log__startup_rmod_check
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */


  def sran_bts__startup_log__startup_rmod_check(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.contains("Eeprom: Serial number")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {

        if (line.contains("Eeprom: Serial number")) {
          my_map += ("SN" -> line.split(", Eeprom: Serial number").last.replace(":", " ").trim)
        }
        if (line.contains("Eeprom: Manufacture id")) {
          my_map += ("Manufacturer" -> line.split("Eeprom: Manufacture id").last.replace(":", " ").trim)
        }
        if (line.contains("Eeprom: Product code")) {
          my_map += ("ProductCode" -> line.split("Eeprom: Product code").last.replace(":", " ").trim)
        }
        if (line.contains("| HW version :")) {
          my_map += ("HW_Version" -> line.split("HW version")(1).split("|")(0).replace(":", " ").trim,
            "RMOD_Type" -> line.split("| Proto module :")(1).trim)
        }
        if (line.contains("Software::") & line.contains("request header: to")) {
          my_map += ("RMOD_LETTER_ID" -> line.split("/RMOD_")(1).split("_")(0).trim,
            "RMOD_ID" -> line.split("/RMOD_")(1).split("_")(1).trim)
        }

        if (lineNum != (start_log + 1) && line.contains("ReadUpf")) {
          my_map += "line_number" -> start_log.toString
          log_started = false
          val cols_list = List("SN", "Manufacturer", "ProductCode", "HW_Version", "RMOD_Type", "RMOD_LETTER_ID", "RMOD_ID")
          cols_list.foreach(x => {
            if (!my_map.keys.toList.contains(x)) {
              my_map += (x -> "")
            }
          })

          list_map += my_map.toMap
        }
      }
      lineNum += 1
    }

    list_map.toList.filter(_.size > 1)
  }

  /**
    * sran_bts__extendedsysteminfo__extendedsysteminfo_diskusage
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def sran_bts__extendedsysteminfo__extendedsysteminfo_diskusage(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("disk usage")) {
        txt.nextLine();
        txt.nextLine()
        line = txt.nextLine()
        linNum += 3
        logEntry += ("line_number" -> linNum.toString)
        while (line.length != 0) {
          logEntry += ("file_system" -> ( line.split("\\s+")(0).trim))
          logEntry += ("1k_blocks" -> ( line.split("\\s+")(1).trim))
          logEntry += ("used" -> ( line.split("\\s+")(2).trim))
          logEntry += ("available" -> ( line.split("\\s+")(3).trim))
          logEntry += ("use_percentage" -> ( line.split("\\s+")(4).trim))
          logEntry += ("mount_on" -> ( line.split("\\s+")(5).trim))
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
    * sran_bts__scf__sbtstimezone
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def sran_bts__scf__sbtstimezone(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("<p name=\"timeZone\">") && line.endsWith("</p>")) {
        var value = line.split("<p name=\"timeZone\">")(1).stripSuffix("</p>")
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("offset" -> ( value.split("\\s+")(0).trim))
        logEntry += ("country" -> ( value.split("\\s+")(1).trim))
        logEntries += logEntry
      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * sran_bts__scf__scf_pmax
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def sran_bts__scf__scf_pmax(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("<managedObject class=\"NOKLTE:LNCEL\" distName=\"") && line.contains("version=")) {
        var value = line.split("<managedObject class=\"NOKLTE:LNCEL\" distName=\"")(1).split("\\s+")(0).split("/")
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("mrbts_id" -> ( value(0).split("-")(0).trim))
        logEntry += ("lnbts_id" -> ( value(1).split("-")(0).trim))
        logEntry += ("lncel_id" -> ( value(2).split("-")(0).trim))
        logEntries += logEntry
      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * sran_bts__dump_routing_table__routingtablenew
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def sran_bts__dump_routing_table__routingtablenew(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("----------------------------------------------------------------------------------------------------------------------------------------------------------------------")) {
        line = txt.nextLine()
        linNum += 1
        if (line.contains("|     SRC IP      |     DEST IP     |     GATEWAY     |  RMTU  |   PMTU  |      OIF    |  USED FLAG  |  PROTO  |SRC PORT MIN|SRC PORT MAX| DST PORT MIN | DST PORT MAX |")) {
          line = txt.nextLine()
          linNum += 1
          if (line.contains("------------------+-----------------+-----------------+--------+---------+-------------+-------------+---------+------------+------------+--------------+--------------")) {
            line = txt.nextLine()
            linNum += 1
            logEntry += ("line_number" -> linNum.toString)

            while (line.length != 0) {
              val value = line.split('|')
              logEntry += ("param1" -> ( value(1).split('.')(0).trim))
              logEntry += ("param2" -> ( value(1).split('.')(1).trim))
              logEntry += ("param3" -> ( value(1).split('.')(2).trim))
              logEntry += ("param4" -> ( value(1).split('.')(3).trim))
              logEntry += ("param5" -> ( value(2).split('.')(0).trim))
              logEntry += ("param6" -> ( value(2).split('.')(1).trim))
              logEntry += ("param7" -> ( value(2).split('.')(2).trim))
              logEntry += ("param8" -> ( value(2).split('.')(3).trim))
              logEntry += ("gateway_ip" -> ( value(3).trim))
              logEntry += ("rmtu" -> ( value(4).trim))
              logEntry += ("pmtu" -> ( value(5).trim))
              logEntry += ("otf" -> ( value(6).trim))
              logEntry += ("used_flag" -> ( value(7).trim))
              logEntries += logEntry
              line = txt.nextLine()
              linNum += 1
            }

          }
        }
      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * sran_bts__snapshot_file_list__snapshot_file_list_countofmodules
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def sran_bts__snapshot_file_list__snapshot_file_list_countofmodules(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.endsWith("_RawAlarmHistory.txt.xz")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("param" -> ( line.stripSuffix("_RawAlarmHistory.txt.xz").split("_")(1).trim))
        logEntries += logEntry
      }

      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * sran_bts__dump_routing_table__routingtable
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def sran_bts__dump_routing_table__routingtable(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("----------------------------------------------------------------------------------------------------------------------------------------------------------------------")) {
        line = txt.nextLine()
        linNum += 1
        if (line.contains("|     SRC IP      |     DEST IP     |     GATEWAY     |  RMTU  |   PMTU  |      OIF    |  USED FLAG  |  PROTO  |SRC PORT MIN|SRC PORT MAX| DST PORT MIN | DST PORT MAX |")) {
          line = txt.nextLine()
          linNum += 1
          if (line.contains("------------------+-----------------+-----------------+--------+---------+-------------+-------------+---------+------------+------------+--------------+--------------")) {
            line = txt.nextLine()
            linNum += 1
            logEntry += ("line_number" -> linNum.toString)

            while (line.length != 0) {
              val value = line.split('|')
              logEntry += ("src_ip" -> ( value(1).trim))
              logEntry += ("dst_ip" -> ( value(2).trim))
              logEntry += ("gateway_ip" -> ( value(3).trim))
              logEntry += ("rmtu" -> ( value(4).trim))
              logEntry += ("pmtu" -> ( value(5).trim))
              logEntry += ("otf" -> ( value(6).trim))
              logEntry += ("used_flag" -> ( value(7).trim))
              logEntry += ("proto" -> ( value(8).trim))
              logEntry += ("src_port_min" -> ( value(9).trim))
              logEntry += ("src_port_max" -> ( value(10).trim))
              logEntry += ("dst_port_min" -> ( value(11).trim))
              logEntry += ("dst_port_max" -> ( value(12).trim))
              logEntries += logEntry
              line = txt.nextLine()
              linNum += 1
            }

          }
        }
      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * sran_bts__1011_rawalarmhistory__bts_1011_rawalarmhistory
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def sran_bts__1011_rawalarmhistory__bts_1011_rawalarmhistory(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    var line = txt.nextLine()
    var logEntry = Map[String, String]()

    logEntry += ("line_number" -> linNum.toString)
    while(txt.hasNextLine) {

      while (txt.hasNextLine && !line.contains("faults")) {
        linNum += 1
        line = txt.nextLine()
      }
      logEntry += ("type" -> (line.split("\\s+")(1).trim))

      while (txt.hasNextLine && !(line.contains("TimeStamp") && line.contains("FaultId") && line.contains("FaultSource") && line.contains("Current 10's") &&
        line.contains("Last 10's") && line.contains("Current Min") && line.contains("Last Min") && line.contains("Current Hour") && line.contains("Last Hour") &&
        line.contains("Current Day") && line.contains("Last Day") && line.contains("All"))) {
        line = txt.nextLine()
        linNum += 1
      }

      if (line.contains("TimeStamp") && line.contains("FaultId") && line.contains("FaultSource") && line.contains("Current 10's") &&
        line.contains("Last 10's") && line.contains("Current Min") && line.contains("Last Min") && line.contains("Current Hour") && line.contains("Last Hour") &&
        line.contains("Current Day") && line.contains("Last Day") && line.contains("All")) {
        txt.nextLine();
        line = txt.nextLine();
        linNum += 2
        while (line.length != 0) {
          if (line.substring(0, 20).contains("T") && line.substring(0, 20).contains("Z")) {
            logEntry += ("date" -> (line.substring(0, 20).split("T")(0).trim))
            logEntry += ("time" -> (line.substring(0, 20).split("T")(1).stripSuffix("Z").trim))
            logEntry += ("fault_id" -> (line.split("\\s+")(1).
              stripSuffix(")").stripPrefix("(").trim))
            logEntry += ("fault_name" -> (line.split("\\s+")(2).trim))
            logEntry += ("source" -> (line.split("\\s+")(3).split("/")(1).trim))
            var index = line.indexOf('/', 1)
            logEntry += ("source_additional" -> (line.split("\\s+")(3).splitAt(index)._2.trim))

          } else {
            logEntry += ("date" -> (line.substring(0, 20).split(" ")(0).trim))
            logEntry += ("time" -> (line.substring(0, 20).split(" ")(1).trim))
            logEntry += ("fault_id" -> (line.split("\\s+")(2).
              stripSuffix(")").stripPrefix("(").trim))
            logEntry += ("fault_name" -> (line.split("\\s+")(3).trim))
            logEntry += ("source" -> (line.split("\\s+")(4).split("/")(1).trim))
            var index = line.indexOf('/', 1)
            logEntry += ("source_additional" -> (line.split("\\s+")(4).splitAt(index)._2.trim))

          }
          logEntries += logEntry
          line = txt.nextLine()
          linNum += 1
        }
      }
      linNum+=1
    }

    return logEntries.toList
  }



  /**
    * sran_bts__extendedsystem__cp_traffic_wcdma
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def sran_bts__extendedsystem__cp_traffic_wcdma(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("Chain CP_TRAFFIC_WCDMA")) {
        txt.nextLine()
        line = txt.nextLine()
        var value = line.split("\\s+")
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("packets" -> ( value(1).trim))
        logEntry += ("bytes" -> ( value(2).trim))
        logEntry += ("target" -> ( value(3).trim))
        logEntry += ("protocol" -> ( value(4).trim))
        logEntry += ("opt" -> ( value(5).trim))
        logEntry += ("in" -> ( value(6).trim))
        logEntry += ("out" -> ( value(7).trim))
        logEntry += ("source" -> ( value(8).trim))
        logEntry += ("destination" -> ( value(9).trim))
        var additional_info = ""
        for (i <- 10 to value.length - 1) {
          additional_info = additional_info + value(i)
        }
        logEntry += ("additional_info" -> ( additional_info.trim))
        logEntries += logEntry
      }

      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * sran_bts__runtime__fsp_136a_runtime
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  /* def sran_bts__runtime__fsp_136a_runtime(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
 if(line.trim.length>0){
 
      var logEntry = Map[String, String]()
      val split = line.split("\\s+")
      logEntry += ("line_number" -> linNum.toString)
      logEntry += ("n_a" -> ( split(0).trim))
      logEntry += ("cpu" -> ( split(1).trim))
      logEntry += ("date_time" -> ( split(2).trim))
      logEntry += ("message" -> ( split(3).trim))
      logEntry += ("info" -> ( split(4).split("/")(0).trim))
      logEntry += ("message1" -> ( split(4).split("/")(1).trim))
      var msg2 = ""
      for (i <- 5 to split.length - 1) {
        msg2 = msg2 + " " + split(i)
      }
      logEntry += ("message2" -> ( msg2.trim))
      logEntries += logEntry
      linNum += 1
    }
    }
    logEntries.toList
  }
  */
  def sran_bts__runtime__fsp_136a_runtime(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    var logcompleted=false
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      if(line.contains("------  -------  ------     ------"))
        logcompleted=true
      var logEntry = Map[String, String]()
      val split = line.split("\\s+")
      if(!logcompleted && split.size>4 && line.length>50){
        val date_time = split(2).trim.replaceAll("[<>]", "").split("T")
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("n_a" -> ( split(0).trim))
        logEntry += ("cpu" -> ( split(1).trim))
        logEntry += ("date" -> ( date_time.lift(0).getOrElse("")))
        logEntry += ("time" -> ( date_time.lift(1).getOrElse("").split("\\.").lift(0).getOrElse("")))
        logEntry += ("message" -> ( split(3).trim))
        logEntry += ("info" -> ( split(4).split("/").lift(0).getOrElse("")))
        logEntry += ("message1" -> ( split(4).split("/").lift(1).getOrElse("")))
        var msg2 = ""
        for (i <- 5 to split.length - 1) {
          msg2 = msg2 + " " + split(i)
        }
        logEntry += ("message2" -> ( msg2.trim))
        logEntries += logEntry
      }
      linNum += 1
    }
    logEntries.toList
  }

  
  /**
    * sran_bts__runtime_btsom__x011_runtime_btsom
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def sran_bts__runtime_btsom__x011_runtime_btsom(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("-") && line.contains("T") && line.contains("/")&&line.contains(">")&& line.contains("<")) {
        logEntry += ("line_number" -> linNum.toString)
        var value = line.split("\\s+")
        logEntry += ("runtime_line_id" -> value(0).trim)
        if (value(1).split("-").length == 4 && value.length == 6) {
          logEntry += ("board" -> (value(1).split("-")(0).trim + "-" + value(1).split("-")(1).trim))
          logEntry += ("board_id" -> value(1).split("-")(2).trim)
          logEntry += ("mo" -> value(1).split("-")(3).trim)
          if (value(2).contains("T") && value(2).contains("<") && value(2).contains(">")) {
            logEntry += ("date" -> value(2).split("T")(0).stripPrefix("<").trim)
            logEntry += ("time" -> value(2).split("T")(1).stripSuffix(">").trim)
            logEntry += ("mo_class" -> value(3).trim)
            logEntry += ("severity" -> value(4).split("/")(0).trim)
            logEntry += ("path" -> value(4).splitAt(value(4).indexOf("/"))._2.trim)
            var msg = ""
            for (i <- 5 to value.length - 1) {
              msg = msg + " " + value(i)
            }
            logEntry += ("message" -> msg.trim)
            logEntries += logEntry
          }
        }
      }
      linNum += 1
    }

    logEntries.toList
  }


  /**
    * sran_bts__011_filelistinfo_log__filelistinfo
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def sran_bts__011_filelistinfo_log__filelistinfo(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("/")) {

        line = txt.nextLine()
        linNum+=1
        if(line.startsWith("total")){
          line = txt.nextLine()
          linNum+=1
          while(!line.isEmpty && txt.hasNextLine){
            var value = line.split("\\s+")
            logEntry += ("line_number" -> linNum.toString)
            logEntry += ("file_permition" -> value(0).trim)
            logEntry += ("links" -> value(1).trim)
            logEntry += ("owner" -> value(2).trim)
            logEntry += ("group" -> value(3).trim)
            logEntry += ("file_size" -> value(4).trim)
            logEntry += ("month" -> value(5).trim)
            logEntry += ("day" -> value(6).trim)
            logEntry += ("time_year" -> value(7).trim)
            logEntry += ("file_name" -> value(8).trim)
            logEntries += logEntry
            line = txt.nextLine()
            linNum += 1
          }
        }
      }
      linNum += 1
    }

    logEntries.toList
  }

  /**
    * btsmed__imp_upgrade__imp_upgrade_check_rpm_info
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def btsmed__imp_upgrade__imp_upgrade_check_rpm_info(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains(".noarch.zip:")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("rpm_info" -> line.split(".noarch.zip:")(1).split(":")(0).trim)
        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList
  }

  /**
    * btsmed__imp__imp_check_extra_info_alarm_8524
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def btsmed__imp__imp_check_extra_info_alarm_8524(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("<notification>")) {

        txt.nextLine();line = txt.nextLine()
        linNum+=2
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("date" -> line.stripPrefix("        <eventTime>").split("</eve")(0).split("T")(0).trim)
        logEntry += ("time" -> line.stripPrefix("        <eventTime>").split("</eve")(0).split("T")(1).trim)
        while(!(line.contains("        <additionalText1>8524:")||line.contains("</notification>"))){
          line=txt.nextLine()
          linNum+=1
        }
        logEntry += ("alarm8524_additional_text" -> line.stripPrefix("        <additionalText1>8524:").split("</ad")(0).trim)

        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList
  }

  /**
    * ******************************************************
    * sran_bts__lrm_dump__lrm_dump
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def sran_bts__lrm_dump__lrm_dump(logfilecontent: String): List[Map[String, String]] = {
    println("sran_bts__lrm_dump__lrm_dump")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, dashlineno, endlineno, lastlineno, tableendline, alllinecount: Int = 0
    var endlinelist, startlinelist, secondlinelist, tablelist, dashlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("LCG Resource")) {
        startlinelist += lineNum
      }

      if (line.trim.size == 0) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (startline <- startlinelist) {
      var my_map = mutable.Map[String, String]()
      endlineno = getClosestLineNumber(startline, endlinelist.toList.sorted)
      for (i <- startline + 3 to endlineno - 1) {
        my_map += (
          "line_number"->i.toString.trim,
          "LCG_ID" -> dataArray(i - 1).split("\\|")(0).trim.trim,
          "CaGroup_ID" -> dataArray(i - 1).split("\\|")(1).trim.trim,
          "HspaConfig" -> dataArray(i - 1).split("\\|")(2).trim.trim,
          "MulticastPSId" -> dataArray(i - 1).split("\\|")(3).trim.trim,
          "MulticastSMId" -> dataArray(i - 1).split("\\|")(4).trim.trim,
          "MulticastEVAMId" -> dataArray(i - 1).split("\\|")(5).trim.trim,
          "StartAllDone" -> dataArray(i - 1).split("\\|")(6).trim.trim,
          "HSUPA_Res_Allocation_Blocked" -> dataArray(i - 1).split("\\|")(7).trim.trim,
          "MinNbrOfHsRachCfs" -> dataArray(i - 1).split("\\|")(8).trim.trim,
          "ChannelCapacityExceeded_DCH_UL" -> dataArray(i - 1).split("\\|")(9).trim.trim,
          "ChannelCapacityExceeded_DCH_DL" -> dataArray(i - 1).split("\\|")(10).trim.trim,
          "ChannelCapacityExceeded_HSUPA_SM_1" -> dataArray(i - 1).split("\\|")(11).trim.trim,
          "ChannelCapacityExceeded_HSUPA_SM_2" -> dataArray(i - 1).split("\\|")(12).trim.trim,
          "ChannelCapacityExceeded_HSDPA" -> dataArray(i - 1).split("\\|")(13).trim.trim
        )
      }
      list_map += my_map.toMap
    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    * ******************************************************
    * sran_bts__scf__scf_channelgrouping
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def sran_bts__scf__scf_channelgrouping(logfilecontent: String): List[Map[String, String]] = {
    println("sran_bts__scf__scf_channelgrouping")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, dashlineno, endlineno, lastlineno, tableendline, alllinecount: Int = 0
    var endlinelist, startlinelist, secondlinelist, tablelist, dashlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("managedObject class") && line.contains("/CELLMAPPING") && line.contains("/LCEL") && line.contains("/CHANNELGROUP")
        && line.contains("/CHANNEL-")) {
        startlinelist += lineNum
      }

      if (line.contains("/managedObject")) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (startline <- startlinelist) {
      var my_map = mutable.Map[String, String]()
      endlineno = getClosestLineNumber(startline, endlinelist.toList.sorted)
      val data_part = dataArray(startline - 1).replaceAll("\"", "")
      val data_part_arr = data_part.substring(data_part.indexOf("distName") + 10, data_part.indexOf("version")).split("/")
      for (i <- 0 to data_part_arr.length - 1) {
        if (data_part_arr(i).contains("CELLMAPPING")) {
          my_map += "CELLMAPPING" -> data_part_arr(i).split("-")(1).trim
        }
        if (data_part_arr(i).contains("LCEL")) {
          my_map += "LCEL" -> data_part_arr(i).split("-")(1).trim
        }
        if (data_part_arr(i).contains("CHANNELGROUP")) {
          my_map += "CHANNELGROUP" -> data_part_arr(i).split("-")(1).trim
        }
        if (data_part_arr(i).contains("CHANNEL")) {
          my_map += "CHANNEL" -> data_part_arr(i).split("-")(1).trim
        }
      }

      if (dataArray(startline).contains("direction")) {
        val directiondata = dataArray(startline).replaceAll("\"", "")
        my_map += "DIRECTION" -> directiondata.substring(directiondata.indexOf(">") + 1, directiondata.lastIndexOf("<"))
      }

      if (dataArray(startline + 1).contains("antlDN")) {
        val antdata = dataArray(startline + 1).replaceAll("\"", "")
        val antdataarr = antdata.substring(antdata.indexOf(">") + 1, antdata.lastIndexOf("<")).split("/")
        for (i <- 0 to antdataarr.length - 1) {
          my_map += "line_number" ->startline.toString.trim
          if (antdataarr(i).contains("MRBTS")) {
            my_map += "MRBTS" -> (antdataarr(i).split("-")(1).trim)
          }
          if (antdataarr(i).contains("EQM")) {
            my_map += "EQM" -> (antdataarr(i).split("-")(1).trim)
          }
          if (antdataarr(i).contains("APEQM")) {
            my_map += "APEQM" -> (antdataarr(i).split("-")(1).trim)
          }
          if (antdataarr(i).contains("RMOD")) {
            my_map += "RMOD" -> (antdataarr(i).split("-")(1).trim)
          }
          if (antdataarr(i).contains("ANTL")) {
            my_map += "ANTL" -> (antdataarr(i).split("-")(1).trim)
          }
        }
      }
      list_map += my_map.toMap
    }
    list_map.toList.filter(_.size > 1)
  }
  /**
    * flexi_mr_bts_lte__1011_rawalarmhistory__parse_rawalarmhistory
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */
  /*def flexi_mr_bts_lte__1011_rawalarmhistory__parse_rawalarmhistory(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_mr_bts_lte__1011_RawAlarmHistory__parse_rawalarmhistory")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var secondlineno, endlineno: Int = 0

    var endlinelist = ListBuffer[Int]()
    var startlinelist = ListBuffer[Int]()
    var secondlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("faults")) {
        startlinelist += lineNum
      }

      secondlinelist = startlinelist.map(_ + 4)

      if (line.trim.length == 0) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (linenno <- startlinelist) {
      secondlineno = getClosestLineNumber(linenno, secondlinelist.toList.sorted)
      endlineno = getClosestLineNumber(secondlineno, endlinelist.toList.sorted)
      val firstlinedata = dataArray(linenno - 1)
      for (tableline <- secondlineno until endlineno) {
        val tablelinedata = dataArray(tableline - 1)
        my_map+="line_number"->linenno.toString.trim
        my_map += "Faults" -> firstlinedata.substring(0, firstlinedata.length).trim
        my_map += (
          "FAULT_ID" -> tablelinedata.substring(25, tablelinedata.indexOf(")")).trim,
          "FAULT_NAME" -> tablelinedata.substring(tablelinedata.indexOf(")"), tablelinedata.indexOf(" ")).trim,
          "SOURCE" -> tablelinedata.substring(tablelinedata.substring(77, 201).indexOf("/"), tablelinedata.substring(77, 201).indexOf(" ")).trim,
          "SOURCE_ADDITIONAL" -> tablelinedata.substring(tablelinedata.substring(77, 201).indexOf(" "), tablelinedata.substring(77, 201).length).trim
        )
      }
      list_map += my_map.toMap
    }
    list_map.toList.filter(x => x.size >1)
  }
  */
  def flexi_mr_bts_lte__1011_rawalarmhistory__parse_rawalarmhistory(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_mr_bts_lte__1011_RawAlarmHistory__parse_rawalarmhistory")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var secondlineno, endlineno: Int = 0

    var endlinelist = ListBuffer[Int]()
    var startlinelist = ListBuffer[Int]()
    var secondlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("faults")) {
        startlinelist += lineNum
      }

      secondlinelist = startlinelist.map(_ + 4)

      if (line.trim.length == 0) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (linenno <- startlinelist) {
      secondlineno = getClosestLineNumber(linenno, secondlinelist.toList.sorted)
      endlineno = getClosestLineNumber(secondlineno, endlinelist.toList.sorted)
      val firstlinedata = dataArray(linenno - 1)
      for (tableline <- secondlineno until endlineno) {
        val tablelinedata = dataArray(tableline - 1)
        if (tablelinedata.contains(")") && tablelinedata.contains("(") && tablelinedata.contains("/"))
        {
          val splitdata=tablelinedata.split("\\s+")
          val date_time = splitdata(0).replaceAll("[<>]", "").split("T")
          my_map += (
            "date" ->  date_time.lift(0).getOrElse(""),
            "time" ->   date_time.lift(1).getOrElse("").stripSuffix("Z"),
            "line_number" -> linenno.toString.trim,
            "Faults" -> firstlinedata.substring(0, firstlinedata.length).trim,
            "FAULT_ID" -> splitdata(2).replace("(","").replace(")","").trim,
            "FAULT_NAME" -> splitdata(3).trim,
            "SOURCE" -> splitdata(4).trim,
            "SOURCE_ADDITIONAL" -> splitdata(5).replace("(","").replace(")","").trim
          )
        }
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(x => x.size >1)
  }

  /**
    *******************************************************
    * sran_bts__runtime__rfmodulefaults
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    ****************************************************
    */

  /*def sran_bts__runtime__rfmodulefaults(logfilecontent: String): List[Map[String, String]] = {
    println("sran_bts__runtime__rfmodulefaults")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno,dashlineno,endlineno,lastlineno,tableendline: Int = 0
    var endlinelist,startlinelist,secondlinelist,dashlinelist,emptylinelist = ListBuffer[Int]()
    var firstlinetext=""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if(line.contains("received Fault") ) {
        startlinelist+=lineNum
      }

      lineNum += 1
    }

    for (startline <- startlinelist) {
      var my_map = mutable.Map[String, String]()

      val tabledata=dataArray(startline-1)
      if(tabledata.trim.length!=0){
        val tablearr=tabledata.split(",")
        for(i<-0 to tablearr.length-1) {
          my_map+="line_number"->startline.toString.trim
          if(tablearr(i).trim.startsWith("received Fault")) {
            my_map += "FaultName" -> (tablearr(i).split(" ",-1).filter(_.trim.length>0)(2)).trim
          }
          if(tablearr(i).trim.startsWith("Id")) {
            my_map += "FaultId" -> (tablearr(i).split(":",-1).filter(_.trim.length>0)(1)).trim
          }
          if(tablearr(i).trim.startsWith("Source")) {
            my_map += "Source" -> (tablearr(i).split(":",-1).filter(_.trim.length>0)(1)).trim
          }
          if(tablearr(i).trim.startsWith("Severity")) {
            my_map += "Severity" -> (tablearr(i).split(":",-1).filter(_.trim.length>0)(1)).trim
          }
          if(tablearr(i).trim.startsWith("State")) {
            my_map += "State" -> (tablearr(i).split(":",-1).filter(_.trim.length>0)(1)).trim
          }
          if(tablearr(i).trim.startsWith("faultCause")) {
            my_map += "FaultCause" -> (tablearr(i).split("=",-1).filter(_.trim.length>0)(1)).trim
          }
        }
        list_map+=my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)
  }*/
  
  
 /* def sran_bts__runtime__rfmodulefaults(logfilecontent: String): List[Map[String, String]] = {
    println("sran_bts__runtime__rfmodulefaults")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno,dashlineno,endlineno,lastlineno,tableendline: Int = 0
    var endlinelist,startlinelist,secondlinelist,dashlinelist,emptylinelist = ListBuffer[Int]()
    var firstlinetext=""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if(line.contains("received Fault") ) {
        startlinelist+=lineNum
      }

      lineNum += 1
    }

    for (startline <- startlinelist) {
      var my_map = mutable.Map[String, String]()

      val tabledata=dataArray(startline-1)
      if(tabledata.trim.length!=0){
        val tablearr=tabledata.split(",")
        for(i<-0 to tablearr.length-1) {
          my_map+="line_number"->startline.toString.trim
          if(tablearr(i).trim.startsWith("received Fault")) {
            my_map += "FaultName" -> (tablearr(i).split(" ",-1).filter(_.trim.length>0)(2)).trim
          }
          if(tablearr(i).trim.startsWith("Id")) {
            my_map += "FaultId" -> (tablearr(i).split(":",-1).filter(_.trim.length>0)(1)).trim
          }
          if(tablearr(i).trim.startsWith("Source")) {
            my_map += "Source" -> (tablearr(i).split(":",-1).filter(_.trim.length>0)(1)).trim
          }
          if(tablearr(i).trim.startsWith("Severity")) {
            my_map += "Severity" -> (tablearr(i).split(":",-1).filter(_.trim.length>0)(1)).trim
          }
          if(tablearr(i).trim.startsWith("State")) {
            my_map += "State" -> (tablearr(i).split(":",-1).filter(_.trim.length>0)(1)).trim
          }
          if(tablearr(i).trim.startsWith("faultCause")) {
            my_map += "FaultCause" -> (tablearr(i).split("=",-1).filter(_.trim.length>0)(1)).trim
          }
        }
        list_map+=my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)
  }*/

 def sran_bts__runtime__rfmodulefaults(logfilecontent: String): List[Map[String, String]] = {
   println("Executing sran_bts__runtime__rfmodulefaults")
   var lineNum = 1
   val txt = new Scanner(logfilecontent)
   var list_map = mutable.ListBuffer[Map[String, String]]()
   var logEntry = mutable.Map[Int, String]()
   var dataArray = mutable.ArrayBuffer[String]()

   /*var secondlineno, dashlineno, endlineno, lastlineno, tableendline: Int = 0
   var endlinelist, startlinelist, secondlinelist, dashlinelist, emptylinelist = ListBuffer[Int]()
   var firstlinetext = ""
 */

   while (txt.hasNext) {
     var my_map = mutable.Map[String, String]()
     val line = txt.nextLine()
     dataArray += line
     if (line.contains("received Fault")) {
       // startlinelist+=lineNum
       my_map += "line_number" -> lineNum.toString.trim
       val tablearr = line.split(",", -1)
       for (i <- 0 to tablearr.length - 1) {
         if (tablearr(i).trim.startsWith("received Fault")) {
           my_map += "FaultName" -> (tablearr(i).split(" ", -1).filter(_.trim.length > 0)(2)).trim
         }
         if (tablearr(i).trim.startsWith("Id")) {
           my_map += "FaultId" -> (tablearr(i).split(":", -1).filter(_.trim.length > 0)(1)).trim
         }
         if (tablearr(i).trim.startsWith("Source")) {
           my_map += "Source" -> (tablearr(i).split(":", -1).filter(_.trim.length > 0)(1)).trim
         }
         if (tablearr(i).trim.startsWith("Severity")) {
           my_map += "Severity" -> (tablearr(i).split(":", -1).filter(_.trim.length > 0)(1)).trim
         }
         if (tablearr(i).trim.startsWith("State")) {
           my_map += "State" -> (tablearr(i).split(":", -1).filter(_.trim.length > 0)(1)).trim
         }
         if (tablearr(i).trim.startsWith("faultCause")) {
           my_map += "FaultCause" -> (tablearr(i).split("=", -1).filter(_.trim.length > 0)(1)).trim
         }
       }
       list_map += my_map.toMap
      }
     lineNum += 1
     }
   list_map.toList
 }

  /**
    * sran_bts__1011_blackbox__1011_blackbox
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def sran_bts__1011_blackbox__1011_blackbox(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.startsWith("RESET Reason and timestamp:")) {
        line = txt.nextLine()
        linNum += 1
        logEntry += ("line_number" -> linNum.toString)
        while (!line.contains("SW VERSION")) {
          var split = line.split("\\s+")
          logEntry += ("date_time" -> split(0).trim)
          var event = ""
          for (i <- 1 to split.length - 1) {
            event = event + " " + split(i)
          }

          logEntry += ("reason" ->  event.split(":")(0).trim)
          logEntry += ("reason_additional" ->  event.split(":").lift(1).getOrElse("").trim)

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
    * sran_bts__runtime_btsom__runtime_btsom_only_wrn
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def sran_bts__runtime_btsom__runtime_btsom_only_wrn(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("-") && line.contains("WRN/")) {
        logEntry += ("line_number" -> linNum.toString)
        var value = line.split("\\s+")
        logEntry += ("runtime_line_id" -> value(0).trim)
        logEntry += ("board" -> (value(1).split("-")(0).trim + "-" + value(1).split("-")(1).trim))
        logEntry += ("board_id" -> value(1).split("-")(2).trim)
        logEntry += ("mo" -> value(1).split("-")(3).trim)
        logEntry += ("date_time" -> value(2).trim)
        logEntry += ("mo_class" -> value(3).trim)
        logEntry += ("path" -> value(4).splitAt(value(4).indexOf("/"))._2.trim)
        var msg = ""
        for (i <- 5 to value.length - 1) {
          msg = msg + " " + value(i)
        }
        logEntry += ("message" -> (msg.trim))
        logEntries += logEntry
      }


      linNum += 1
    }

    return logEntries.toList
  }




}
