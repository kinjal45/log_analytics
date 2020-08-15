package com.nokia.parsing.parsers

import java.util.Scanner
import Utils.Util._

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParseAliceFlexiBSC {
  /**
    * flexi_bsc__OEM_network__zqdi_max_osi_data
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__oem_network__zqdi_max_osi_data(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__oem_network__zqdi_max_osi_data")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var startlinelist, endlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    var firstlinedata, secondlinedata, thirdlinedata, fourthlinedata, fifthlinedata, sixthlinedata, seventhlinedata = ""

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }

      if (line.contains("AE-NAME          APPL   NET ADDR  STATE   UNIT        FAM ID PROC ID")) {
        startlinelist += lineNum
      }
      lineNum += 1
    }

    for (startline <- startlinelist) {
      firstlinedata = dataArray(startline + 1)
      secondlinedata = dataArray(startline + 3)
      thirdlinedata = dataArray(startline + 4)
      fourthlinedata = dataArray(startline + 5)
      fifthlinedata = dataArray(startline + 7)
      sixthlinedata = dataArray(startline + 8)
      seventhlinedata = dataArray(startline + 9)

      my_map += (
        "line_number"->startline.toString.trim,
        "AE_NAME" -> firstlinedata.substring(Math.min(0, firstlinedata.length), Math.min(15, firstlinedata.length)).trim,
        "APPL" -> firstlinedata.substring(Math.min(15, firstlinedata.length), Math.min(23, firstlinedata.length)).trim,
        "NETADDR" -> firstlinedata.substring(Math.min(23, firstlinedata.length), Math.min(32, firstlinedata.length)).trim,
        "STATE" -> firstlinedata.substring(Math.min(32, firstlinedata.length), Math.min(42, firstlinedata.length)),
        "UNIT" -> firstlinedata.substring(Math.min(42, firstlinedata.length), Math.min(52, firstlinedata.length)),
        "FAMID" -> firstlinedata.substring(Math.min(52, firstlinedata.length), Math.min(62, firstlinedata.length)),
        "PROCID" -> firstlinedata.substring(Math.min(62, firstlinedata.length), Math.min(firstlinedata.length, firstlinedata.length)),
        "AP_TYPE" -> secondlinedata.substring(Math.min(12, secondlinedata.length), Math.min(secondlinedata.length, secondlinedata.length)).trim,
        "AP_TITLE" -> thirdlinedata.substring(Math.min(12, thirdlinedata.length), Math.min(thirdlinedata.length, thirdlinedata.length)).trim,
        "AEQ" -> fourthlinedata.substring(Math.min(12, fourthlinedata.length), Math.min(fourthlinedata.length, fourthlinedata.length)).trim,
        "P_SELECTOR" -> fifthlinedata.substring(Math.min(11, fifthlinedata.length), Math.min(fifthlinedata.length, fifthlinedata.length)).trim,
        "S_SELECTOR" -> sixthlinedata.substring(Math.min(11, sixthlinedata.length), Math.min(sixthlinedata.length, sixthlinedata.length)).trim,
        "T_SELECTOR" -> seventhlinedata.substring(Math.min(11, secondlinedata.length), Math.min(seventhlinedata.length, seventhlinedata.length)).trim
      )
      list_map += my_map.toMap
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * flexi_bsc__utpfil_and_memory_files__fb_ipc_obsolete_utpfil
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__utpfil_and_memory_files__fb_ipc_obsolete_utpfil(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__utpfil_and_memory_files__fb_ipc_obsolete_utpfil")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var startlinelist, endlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    var tabledata = ""
    var endlinenumber: Int = 0

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }

      if (line.contains("MCMU-")) {
        startlinelist += lineNum
      }

      lineNum += 1
    }
    for (startline <- startlinelist) {
      endlinenumber = getClosestLineNumber(startline, endlinelist.toList.sorted)
      val firstlinedata = dataArray(startline - 1)
      for (tableline <- (startline + 1).until(endlinenumber)) {
        tabledata = dataArray(tableline - 1)
        my_map += (
          "line_number"->startline.toString.trim,
          "MCMU" -> firstlinedata.substring(Math.min(0, firstlinedata.length), firstlinedata.length).trim,
          "FAMILY" -> tabledata.split("\\s")(0).trim,
          "UTPFIL" -> tabledata.split("\\s")(1).trim,
          "UTPFILval1" -> tabledata.split("\\s")(2).trim,
          "UTPFILval2" -> tabledata.split("\\s")(3).trim
        )
      }
      list_map += my_map.toMap
    }
    list_map.toList.filter(x => x.size > 1)
  }
  /**
    * *****************************************************
    * flexi_bsc__unit_information__zudh_diagnosis_history
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ****************************************************
    */
  def flexi_bsc__unit_information__zudh_diagnosis_history(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__unit_information__zudh_diagnosis_history")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var secondlineno: Int = 0
    var endlineno: Int = 0

    var endlinelist = ListBuffer[Int]()
    var startlinelist = ListBuffer[Int]()
    var secondlinelist = ListBuffer[Int]()
    var spacelinelist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }

      if (line.equals("DIAGNOSTIC REPORT")) {
        secondlinelist += lineNum
      }

      startlinelist = secondlinelist.map(_ - 2)

      if (line.contains("END OF REPORT")) {
        endlinelist += lineNum
      }
      if (line.trim.length == 0) {
        spacelinelist += lineNum
      }

      lineNum += 1
    }

    for (startline <- startlinelist) {
      var unitdata, messagedata, firstlinedata, unitloopdata, messagloopedata = ""

      firstlinedata = dataArray(startline - 1)
      if (firstlinedata.contains("FlexiBSC") || firstlinedata.contains("FlexiBSC3i")) {

        secondlineno = getClosestLineNumber(startline, secondlinelist.toList.sorted)
        endlineno = getNextClosestLineNumber(secondlineno, spacelinelist.toList.sorted)

        val unitlinestart = secondlineno + 2
        val unitlineend = unitlinestart

        for (j <- unitlinestart to unitlineend) {
          unitloopdata += dataArray(j - 1)
        }
        unitdata = unitloopdata.substring(0, Math.min(12, unitloopdata.length)).concat("\n")


        val tablelinestartline = getClosestLineNumber(unitlineend, spacelinelist.toList.sorted) + 1
        val tablelineendline = getClosestLineNumber(tablelinestartline, spacelinelist.toList.sorted) - 1

        val messagestartline = tablelineendline + 2
        val messageendline = getNextClosestLineNumber(messagestartline, endlinelist.toList.sorted) - 1

        for (u <- messagestartline to messageendline) {
          messagloopedata += dataArray(u - 1).concat("\n")
        }
        messagedata += messagloopedata
        for (h <- tablelinestartline to tablelineendline) {
          var param1,param2=""
          if(dataArray(h - 1).size>25){
            param1=dataArray(h - 1).substring(0, 24)
            param2=dataArray(h - 1).substring(25, dataArray(h - 1).length)
          }
          my_map += (
            "line_number"->startline.toString.trim,
            "BSC" -> firstlinedata.substring(10, 25),
            "Unit" -> unitdata,
            "Param1" -> param1,
            "Param2" -> param2,
            "Message" -> messagedata
          )
          list_map += my_map.toMap
        }
      }
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    *
    * flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewl_bts_sw_pack
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    *
    */
  def flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewl_bts_sw_pack(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewl_bts_sw_pack")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var parsedstringMap = mutable.Map[Int, String]()
    var logEntry = mutable.Map[Int, String]()
    var firsttextlinelist = ListBuffer[Int]()
    var secondtextlinelist = ListBuffer[Int]()
    var endtextlinelist = ListBuffer[Int]()

    def parserequiredLine(logEntry: mutable.Map[Int, String], lineno: Int) = {
      val logEntrytoparse = logEntry(lineno).toString
      lineno -> logEntrytoparse
    }

    def generateMap(parsedstringMap: mutable.Map[Int, String]): List[Map[String, String]] = {

      var list_map = mutable.ListBuffer[Map[String, String]]()
      parsedstringMap.foreach {
        value => {
          val my_map = Map("line_number" -> value._1.toString,
            "BUILD_ID" -> value._2.substring(0, 18).replaceAll("""(?m)\s+$""", ""),
            "TYPE" -> value._2.substring(19, 24).replaceAll("""(?m)\s+$""", ""),
            "REL_VER" -> value._2.substring(25, 37).replaceAll("""(?m)\s+$""", ""),
            "INITIAL" -> value._2.substring(38, 47).replaceAll("""(?m)\s+$""", ""),
            "MASTER_FILE" -> value._2.substring(48, 62).replaceAll("""(?m)\s+$""", ""),
            "SUBDIR" -> value._2.substring(63, 75).replaceAll("""(?m)\s+$""", ""),
            "CONNECTED_SITES" -> value._2.substring(76, value._2.length).replaceAll("""(?m)\s+$""", ""))
          list_map += my_map
        }
      }
      list_map.toList
    }

    while (txt.hasNext) {
      val line = txt.nextLine()
      logEntry += lineNum -> line
      if (line.contains("ZEWL;")) {
        firsttextlinelist += lineNum
      }

      if (line.contains("BUILD-ID          TYPE   REL VER     INITIAL    MF NAME        SUBDIR      CONN")) {
        secondtextlinelist += lineNum
      }

      if (line.contains("COMMAND EXECUTED")) {
        endtextlinelist += lineNum
      }
      lineNum += 1
    }
    for (line <- firsttextlinelist) {
      val secondline = getClosestLineNumber(line, secondtextlinelist.toList)
      val lastline = getClosestLineNumber(secondline, endtextlinelist.toList)
      for (lineno <- secondline + 2 to lastline - 2) {
        parsedstringMap += parserequiredLine(logEntry, lineno.toInt)
      }

    }
    generateMap(parsedstringMap)
  }

  /**
    *
    * flexi_bsc__switch info__training_switch_info
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__switch_info__training_switch_info(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var parsedstringMap = mutable.Map[Int, String]()
    var logEntry = mutable.Map[Int, String]()
    var firsttextlinelist = ListBuffer[Int]()
    var secondtextlinelist = ListBuffer[Int]()
    var endtextlinelist = ListBuffer[Int]()

    def parserequiredLine(logEntry: mutable.Map[Int, String], lineno: Int) = {
      val logEntrytoparse = logEntry(lineno).toString
      lineno -> logEntrytoparse
    }

    def generateMap(parsedstringMap: mutable.Map[Int, String]): List[Map[String, String]] = {
      var list_map = mutable.ListBuffer[Map[String, String]]()
      parsedstringMap.foreach {
        value => {
          val my_map = Map("line_number" -> value._1.toString,
            "Unit" -> value._2.substring(1, 16).replaceAll("""(?m)\s+$""", ""),
            "Phy_State" -> value._2.substring(17, 23).replaceAll("""(?m)\s+$""", ""),
            "Location" -> value._2.substring(24, 45).replaceAll("""(?m)\s+$""", ""),
            "Info" -> value._2.substring(46, value._2.length - 1).replaceAll("""(?m)\s+$""", ""))
          list_map += my_map
        }
      }
      list_map.toList
    }
    while (txt.hasNext) {
      val line = txt.nextLine()
      logEntry += lineNum -> line
      if (line.contains("WORKING STATE OF UNITS")) {
        firsttextlinelist += lineNum
      }

      if (line.contains(" UNIT       PHYS STATE LOCATION              INFO")) {
        secondtextlinelist += lineNum
      }

      if (line.contains("COMMAND EXECUTED")) {
        endtextlinelist += lineNum
      }
      lineNum += 1
    }
    for (line <- firsttextlinelist) {
      val secondline = getClosestLineNumber(line, secondtextlinelist.toList)
      val lastline = getClosestLineNumber(secondline, endtextlinelist.toList)
      for (lineno <- secondline + 1 to lastline - 4) {
        parsedstringMap += parserequiredLine(logEntry, lineno.toInt)
      }
    }
    generateMap(parsedstringMap)
  }

  /**
    *
    * flexi_bsc__bsc_alarm_parameters__zabo_bsc_blk_alarm
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    *
    */
 def flexi_bsc__bsc_alarm_parameters__zabo_bsc_blk_alarm(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var parsedstringMap = mutable.Map[Int, String]()
    var logEntry = mutable.Map[Int, String]()
    var firsttextlinelist = ListBuffer[Int]()
    var secondtextlinelist = ListBuffer[Int]()
    var endtextlinelist = ListBuffer[Int]()

    def parserequiredLine(logEntry: mutable.Map[Int, String], lineno: Int) = {
      val logEntrytoparse = logEntry(lineno).toString
      lineno -> logEntrytoparse
    }

    def generateMap(parsedstringMap: mutable.Map[Int, String]): List[Map[String, String]] = {

      var list_map = mutable.ListBuffer[Map[String, String]]()
      parsedstringMap.foreach {
        value => {
          if(value._2.length>71) {
            val my_map = Map("line_number" -> value._1.toString,
              "AlarmNo" -> value._2.substring(0, 5).replaceAll("""(?m)\s+$""", ""),
              "AlarmText" -> value._2.substring(Math.min(value._2.length, 6), Math.min(70, value._2.length)).replaceAll("""(?m)\s+$""", ""),
              "Units" -> value._2.substring(Math.min(value._2.length, 70), value._2.length).replaceAll("""(?m)\s+$""", ""))
            /*  "AlarmText" -> value._2.substring(6, 69).replaceAll("""(?m)\s+$""", ""),
            "Units" -> value._2.substring(70, value._2.length - 1).replaceAll("""(?m)\s+$""", ""))
          */ list_map += my_map
          }
        }
      }
      list_map.toList
    }
    while (txt.hasNext) {
      val line = txt.nextLine()
      logEntry += lineNum -> line
      if (line.contains("ZABO;")) {
        firsttextlinelist += lineNum
      }

      if (line.contains("VTP       ALL ALARMS")) {
        secondtextlinelist += lineNum
      }

      if (line.contains("COMMAND EXECUTED")) {
        endtextlinelist += lineNum
      }
      lineNum += 1
    }
    for (line <- firsttextlinelist) {
      val secondline = getClosestLineNumber(line, secondtextlinelist.toList)
      val lastline = getClosestLineNumber(secondline, endtextlinelist.toList)
      for (lineno <- secondline + 2 to lastline - 2) {
        parsedstringMap += parserequiredLine(logEntry, lineno.toInt)
      }

    }
    generateMap(parsedstringMap)
  }


  /**
    * ******************************************************
    * flexi_bsc__bsc_alarm__zahp_zaho_bsc_alarms
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    * ****************************************************
    */
  def flexi_bsc__bsc_alarm__zahp_zaho_bsc_alarms(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var log_start_line = 0
    var list_map = mutable.ListBuffer[Map[String, String]]()
    val log_starting_text = "ALARM"
    val log_ending_text = "END OF ALARM"
    var line = ""
    var log_started = false
    var skip_nextline = false
    while (txt.hasNext) {
      if(skip_nextline) skip_nextline = false
      else {
        line = txt.nextLine()
      }
      //      logEntry += lineNum -> line
      if (!log_started && line.contains(log_starting_text)) {
        log_started = true
        log_start_line = lineNum + 3
      }
      if (line.contains(log_ending_text)) {
        log_start_line = -1
      }
      if (lineNum == log_start_line) {
        var inner_index = 0
        var my_map = mutable.Map[String, String]()
        my_map += "line_number" -> lineNum.toString
        while (inner_index < 4) {
          inner_index match {
            case 0 =>
              if (line.contains("HIST")) {
                my_map += ("BSC" -> line.substring(11, 33).replaceAll("""(?m)\s+$""", ""),
                  "Unit" -> line.substring(34, 45).replaceAll("""(?m)\s+$""", ""),
                  "Alarm_Type" -> line.substring(46, 55).replaceAll("""(?m)\s+$""", ""),
                  "DTTM" -> line.substring(56, line.length).replaceAll("""(?m)\s+$""", ""))
              }
              else {
                my_map += ("BSC" -> line.substring(10, 33).replaceAll("""(?m)\s+$""", ""),
                  "Unit" -> line.substring(34, 45).replaceAll("""(?m)\s+$""", ""),
                  "Alarm_Type" -> line.substring(46, 55).replaceAll("""(?m)\s+$""", ""),
                  "DTTM" -> line.substring(56, line.length).replaceAll("""(?m)\s+$""", ""))
              }
              line = txt.nextLine()
              lineNum += 1
            case 1 =>
              my_map += ("Severity" -> line.substring(0, 3).replaceAll("""(?m)\s+$""", ""),
                "Not_type" -> line.substring(11, 21).replaceAll("""(?m)\s+$""", ""),
                "Param1" -> line.substring(22, 33).replaceAll("""(?m)\s+$""", ""),
                "Issuer" -> line.substring(34, line.length).replaceAll("""(?m)\s+$""", ""))
              line = txt.nextLine()
              lineNum += 1
            case 2 =>
              my_map += ("Trans_id" -> line.substring(4, 10).replaceAll("""(?m)\s+$""", ""),
                "Alarm_id" -> line.substring(11, 15).replaceAll("""(?m)\s+$""", ""),
                "Alarm_text" -> line.substring(16, line.length).replaceAll("""(?m)\s+$""", ""))
              line = txt.nextLine()
              lineNum += 1
            case 3 =>
              if (line == ""){
                var index_loop = 1
                while (index_loop < 10) {
                  my_map += (s"Supp$index_loop" -> "")
                  index_loop += 1
                }
                lineNum = lineNum - 1
                skip_nextline = true
              }
              else {
                val complete_log_line = line.substring(4, line.length)
                val log_list = complete_log_line.split(" ")
                var index_loop = 1
                log_list.foreach(value => {
                  my_map += (s"Supp$index_loop" -> value)
                  index_loop += 1
                })
                while (index_loop < 10) {
                  my_map += (s"Supp$index_loop" -> "")
                  index_loop += 1
                }
              }
          }
          inner_index += 1
        }
        list_map += my_map.toMap
        log_start_line = lineNum + 2
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * ******************************************************
    * flexi_bsc__bsc_alarm__zahp_zaho_bsc_alarms
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    * ****************************************************
    */
  def flexi_bsc__bts_alarm_history__zeoh_bts_alarm_history(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var log_start_line = 0
    var list_map = mutable.ListBuffer[Map[String, String]]()
    val log_starting_text = "BTS ALARM HISTORY LISTING"
    val log_ending_text = "END OF BTS ALARM HISTORY LISTING"
    while (txt.hasNext) {
      var line = txt.nextLine()
      //      logEntry += lineNum -> line
      if (line.equals(log_starting_text)) {
        log_start_line = lineNum + 2
      }
      if (line.equals(log_ending_text)) {
        log_start_line = -1
      }
      if (line.contains("<HIST>")) {
        var inner_index = 0
        var my_map = mutable.Map[String, String]()
        my_map += "line_number" -> lineNum.toString
        while (inner_index < 5) {
          inner_index match {
            case 0 =>
              my_map += ("Type" -> line.substring(4, 10).replaceAll("""(?m)\s+$""", ""),
                "BSC_Name" -> line.substring(11, 24).replaceAll("""(?m)\s+$""", ""),
                "BCF_ID" -> line.substring(24, 34).replaceAll("""(?m)\s+$""", ""),
                "BTS_ID" -> line.substring(34, 45).replaceAll("""(?m)\s+$""", ""),
                "Tag" -> line.substring(45, 53).replaceAll("""(?m)\s+$""", ""),
                "DTTM" -> line.substring(53, line.length - 1).replaceAll("""(?m)\s+$""", ""))
              line = txt.nextLine()
              lineNum += 1
            case 1 =>
              my_map += ("Sev" -> line.substring(0, 3).replaceAll("""(?m)\s+$""", ""),
                "Alarm_or_Cancel" -> line.substring(4, 10).replaceAll("""(?m)\s+$""", ""),
                "TRX_ID" -> line.substring(11, 33).replaceAll("""(?m)\s+$""", ""),
                "BTS_NAME" -> line.substring(34, line.length - 1).replaceAll("""(?m)\s+$""", ""))
              line = txt.nextLine()
              lineNum += 1
            case 2 =>
              if (line.contains("(") && line.contains(")")) {
                my_map += ("Param" -> "")
                my_map += ("Notification_ID" -> line.substring(3, 10).replaceAll("""(?m)\s+$""", ""),
                  "Alarm_No" -> line.substring(11, 15).replaceAll("""(?m)\s+$""", ""),
                  "Alarm_text" -> line.substring(16, line.length - 1).replaceAll("""(?m)\s+$""", ""))
              }
              else {
                var log_line_list = line.split("\\s+")
                log_line_list = log_line_list.filter(x => x != "")
                my_map += ("Param" -> log_line_list.mkString)
              }
              line = txt.nextLine()
              lineNum += 1
            case 3 =>
              if (line != "") {
                my_map += ("Notification_ID" -> line.substring(3, 10).replaceAll("""(?m)\s+$""", ""),
                  "Alarm_No" -> line.substring(11, 15).replaceAll("""(?m)\s+$""", ""),
                  "Alarm_text" -> line.substring(16, line.length - 1).replaceAll("""(?m)\s+$""", ""))
              }
              line = txt.nextLine()
              lineNum += 1
            case 4 =>
              if (line != "") {
                my_map += ("Supp_Info" -> line.substring(16, line.length - 1).replaceAll("""(?m)\s+$""", ""))
              }
              else
                my_map += ("Supp_Info" -> "")
          }
          inner_index += 1
        }
        list_map += my_map.toMap
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexi_bsc__RNW_status_and_BSC_and_BCF_data__zego_bsc_timers
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__rnw_status_and_bsc_and_bcf_data__zego_bsc_timers(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__rnw_status_and_bsc_and_bcf_data__zego_bsc_timers")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var table_start_line = 0
    while (txt.hasNext) {
      var line = txt.nextLine()

      if (!log_started && line.contains("ZEGO;")) {
        log_started = true
      }
      if (log_started && line.contains("PAGE")) {
        table_start_line = lineNum + 4
      }
      if (line.contains("COMMAND EXECUTED")) {
        log_started = false
      }

      if (lineNum == table_start_line) {
        while (line != "") {
          var my_map = mutable.Map[String, String]()
          my_map += ("line_number" -> lineNum.toString,
            "PARAMETER" -> (line.substring(0, 45).replaceAll("""(?m)\s+$""", "")).trim,
            "VALUE" -> (line.substring(45, 60).replaceAll("""(?m)\s+$""", "")).trim,
            "PRESET_VALUE" -> (line.substring(60, 71).replaceAll("""(?m)\s+$""", "")).trim,
            "MODIFIABLE" -> (line.substring(71, line.length).replaceAll("""(?m)\s+$""", "")).trim)
          list_map += my_map.toMap
          line = txt.nextLine()
          if (line != "") lineNum += 1
        }
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexi_bsc__IO_system__zi2h_protocol_state
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__io_system__zi2h_protocol_state(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var startedLineNum = ""
    val cols_list = List("IPv4_SSH_ENABLED", "IPv4_SSH_PORT", "IPv6_SSH_ENABLED", "IPv6_SSH_PORT", "PRIVATE_RSA_KEY_NAME", "PRIVATE_DSA_KEY_NAME",
      "LOGIN_GRACE_TIME", "SFTP_ENABLED", "IPv4_FTP_ENABLED", "IPv4_FTP_PORT", "IPv6_FTP_ENABLED", "IPv6_FTP_PORT")
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext) {
      val line = txt.nextLine()
      if (!log_started && line.contains("ZI2H:PROTOCOL=")) {
        log_started = true
        startedLineNum = ""
      }
      if (log_started) {
        if (line.contains("IPv4 SSH ENABLED:")) {
          my_map += ("IPv4_SSH_ENABLED" -> line.substring(30, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("IPv4 SSH PORT:")) {
          my_map += ("IPv4_SSH_PORT" -> line.substring(30, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("IPv6 SSH ENABLED:")) {
          my_map += ("IPv6_SSH_ENABLED" -> line.substring(30, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("IPv6 SSH PORT:")) {
          my_map += ("IPv6_SSH_PORT" -> line.substring(30, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("PRIVATE RSA KEY NAME:")) {
          my_map += ("PRIVATE_RSA_KEY_NAME" -> line.substring(30, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("PRIVATE DSA KEY NAME:")) {
          my_map += ("PRIVATE_DSA_KEY_NAME" -> line.substring(30, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("LOGIN GRACE TIME:")) {
          my_map += ("LOGIN_GRACE_TIME" -> line.substring(30, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("SFTP ENABLED:")) {
          my_map += ("SFTP_ENABLED" -> line.substring(17, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("IPv4 FTP ENABLED:")) {
          my_map += ("IPv4_FTP_ENABLED" -> line.substring(21, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("IPv4 FTP PORT:")) {
          my_map += ("IPv4_FTP_PORT" -> line.substring(21, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("IPv6 FTP ENABLED:")) {
          my_map += ("IPv6_FTP_ENABLED" -> line.substring(21, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("IPv6 FTP PORT:")) {
          my_map += ("IPv6_FTP_PORT" -> line.substring(21, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (my_map.nonEmpty && (startedLineNum == "")) {
          startedLineNum = lineNum.toString
          my_map += "line_number" -> startedLineNum
        }
        if (line.contains("COMMAND EXECUTED") && my_map.nonEmpty) {
          cols_list.foreach(x => {
            if (!my_map.contains(x)) my_map += (x -> "")
          })
          list_map += my_map.toMap
          my_map.clear()
          log_started = false
        }
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_bsc_dfca
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_bsc_dfca(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (line.contains("EXPECTED BSC-BSC INTERFACE DELAY")) {
        var my_map = mutable.Map[String, String]()
        my_map += ("line_number" -> lineNum.toString,
          "EXPECTED_BSC_BSC_INTERFACE_DELAY" -> line.substring(63, line.length).replaceAll("""(?m)\s+$""", ""))
        list_map += my_map.toMap
      }
      lineNum += 1
    }
    list_map.toList
  }

/*
  def flexi_bsc__ip_configurations__zqri_ip_configuration(logfilecontent: String): List[Map[String, String]] = {
    var flag = 0
    var list_map = ListBuffer[Map[String, String]]()
    var data_map = mutable.Map[String, String]()
    var initial_tbl_map = mutable.Map[String, String]()
    var tbl_map = mutable.Map[String, String]()
    var all_val_map = mutable.Map[String, String]()
    var line_number = 1
    var flag_state = 0
    //List("line_number","BSC", "DTTM","Unit","UNIT_PIU","Interface", "Interface1", "Attribute", "IP_ADD", "Interface2", "VLAN_ID", "STATE", "MTU")
    val txt = new Scanner(logfilecontent)
    while (txt.hasNext) {
      val line = txt.nextLine();
      line match {
        case x if  x.contains("ZQRI") => flag=1;data_map+=("line_number"->line_number.toString, "BSC"->"", "DTTM"->"");tbl_map+=("Interface"->"", "Interface1"->"", "Attribute"->"", "IP_ADD"->"",  "VLAN_ID"->"", "STATE"->"", "MTU"->"");initial_tbl_map+=("Unit"->"","UNIT_PIU"->"")
        case x if x.contains("FlexiBSC") & flag ==1=>  val data_val=x.replace("FlexiBSC","").split("               ");data_map+=("BSC"->data_val.head.trim,"DTTM"->data_val.last.trim)
        case x if   x.split("\\s+").length ==2 & !x.contains("0/0")& !x.contains("IP ADDRESS") & flag==1=> val initial_val = x.split("\\s+"); initial_tbl_map+=("Unit"->initial_val(0),"UNIT_PIU"->initial_val(1));flag_state=1
        case x if x.contains("->IL") & flag==1 & flag_state==1 => val tbl_val=  x.replace("->IL","").replace("->","").trim.split("    ").filter(_!="").map(_.trim);tbl_map+=("Interface"->"", "Interface1"->"", "Attribute"->"", "IP_ADD"->"", "VLAN_ID"->"", "STATE"->"", "MTU"->"");tbl_map+=("Interface"->tbl_val(0),"Interface1"->tbl_val(1),"Attribute"->tbl_val(2),"IP_ADD"->tbl_val(3));all_val_map=initial_tbl_map++data_map++tbl_map;list_map+=all_val_map.toMap;all_val_map.clear()
        case x if x.trim.endsWith("0/0") & flag==1 & flag_state==1=> val tbl_val=x.replace("->","").trim.split("   ").filter(_!="").map(_.trim);tbl_map+=("Interface"->"", "Interface1"->"", "Attribute"->"", "IP_ADD"->"", "VLAN_ID"->"", "STATE"->"", "MTU"->"");tbl_map+=("Interface1"->tbl_val(0),"VLAN_ID"->tbl_val(1));all_val_map=initial_tbl_map++data_map++tbl_map;list_map+=all_val_map.toMap;all_val_map.clear()
        case x if x.trim.split("  ").filter(_!="").map(_.trim).length==5 & flag==1 & flag_state==1=>val tbl_val=x.trim.split("  ").filter(_!="").map(_.trim);tbl_map+=("Interface"->"", "Interface1"->"", "Attribute"->"", "IP_ADD"->"",  "VLAN_ID"->"", "STATE"->"", "MTU"->"");tbl_map+=("Interface"->tbl_val(0),"STATE"->tbl_val(1),"MTU"->tbl_val(2),"Attribute"->tbl_val(3),"IP_ADD"->tbl_val(4));all_val_map=initial_tbl_map++data_map++tbl_map;list_map+=all_val_map.toMap;all_val_map.clear()
        case x if   x.trim.length==0 => flag_state=0
        case x if   x.contains("COMMAND EXECUTED") & flag==1 => flag=0; flag_state==0
        case _ => None

      }
      line_number=line_number+1
    }
    list_map.toList
  }

*/
  /**
    * flexi_bsc__ip_configurations__zqri_ip_configuration
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  /*def flexi_bsc__ip_configurations__zqri_ip_configuration(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var unit_and_piu_line = 0
    var bsc_value = ""
    var dttm = ""
    var unit_value = ""
    var unit_piu_value = ""
    var actual_log_start_line = 0
    val col_list = List("Interface", "Interface1", "Attribute", "IP_ADD", "Interface2", "VLAN_ID", "Interface3", "STATE", "MTU",
      "Attr", "IP_ADDR")

    while (txt.hasNext) {
      val line = txt.nextLine()
      if (!log_started && line.contains("ZQRI;")) log_started = true
      if (line.contains("COMMAND EXECUTED")) log_started = false
      if (log_started) {
        if (line.contains("FlexiBSC")) {
          bsc_value = line.substring(10, 35).trim
          dttm = line.substring(36, line.length - 1).trim
          unit_and_piu_line = lineNum + 7
        }
        if (lineNum == unit_and_piu_line) {
          unit_value = line.substring(0, 9)
          unit_piu_value = line.substring(10, line.length)
          actual_log_start_line = lineNum + 1
        }
        if (lineNum == actual_log_start_line) {
          var my_map = mutable.Map[String, String]()
          my_map += "line_number" -> (actual_log_start_line-1).toString
          if (line.contains("0/0")) {
            my_map += ("Interface2" -> line.substring(3, 14).trim,
              "VLAN_ID" -> line.substring(15, 19).trim)
          }
          else if (line.contains("->IL")) {
            my_map += ("Interface" -> line.substring(5, 14).trim,
              "Interface1" -> line.substring(15, 29).trim,
              "Attribute" -> line.substring(30, 37).trim,
              "IP_ADD" -> line.substring(38, line.length).trim)
          }
          else if (line == "") {
            if (my_map.nonEmpty) {
              my_map += ("line_number" -> (unit_and_piu_line-1).toString,
                "BSC" -> bsc_value,
                "DTTM" -> dttm,
                "Unit" -> unit_value,
                "UNIT_PIU" -> unit_piu_value)
              col_list.foreach(x => {
                if (!my_map.keys.toList.contains(x)) {
                  my_map += (x -> "")
                }
              })
              list_map += my_map.toMap
              my_map.clear()
            }
            unit_and_piu_line = lineNum + 1
          }
          else {
            my_map += (
              "Interface3" -> line.substring(2, 12).trim,
              "STATE" -> line.substring(13, 18).trim,
              "MTU" -> line.substring(19, 24).trim,
              "Attr" -> line.substring(25, 32).trim,
              "IP_ADDR" -> line.substring(33, line.length).trim)
          }
          actual_log_start_line += 1
        }
      }
      lineNum += 1
    }
    list_map.toList
  }
*/
  /**
    * ******************************************************
    * flexi_bsc__locked_files__ziwx_alhistgx
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    * ****************************************************
    */
  def flexi_bsc__locked_files__ziwx_alhistgx(logfilecontent: String): List[Map[String, String]] = {

    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    while (txt.hasNext) {
      val line = txt.nextLine()
      if (line.contains("ALHISTGX   .ALF")) {
        var my_map = mutable.Map[String, String]()
        my_map += ("line_number" -> lineNum.toString,
          "ALHISTGX-FILE_VERSION" -> line.substring(19, 22).trim,
          "ALHISTGX-FILE_NO" -> line.substring(23, 29).trim,
          "ALHISTGX-FILE_LENGTH" -> line.substring(30, 39).trim)
        list_map += my_map.toMap
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexibsc__boot_info__zwdi_boot_image_of_units
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__boot_info_txt__zwdi_boot_image_of_units(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)
    var lineNum = 1
    var log_started = false
    var log_start_line = 0
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      if (!log_started && line.contains("ZWDI;")) log_started = true
      if (line == "") log_start_line = 0
      if (line.contains("COMMAND EXECUTED")) log_started = false
      if (log_started) {
        if (line.contains("FUNCTIONAL   PLUG-IN UNIT   BOOT PACKAGE: FLASH MEMORY")) log_start_line = lineNum + 3
      }
      if (log_start_line == lineNum) {
        if (line.substring(0, 29).trim == "") {
          my_map += ("Disk_File" -> line.substring(42, line.length).trim)
          list_map += my_map.toMap
          my_map.clear()
        }
        else {
          my_map += ("line_number" -> lineNum.toString,
            "Functional_Unit" -> line.substring(4, 15).trim,
            "Plug_In_Unit_Type_Index" -> line.substring(16, 28).trim,
            "Boot_Package" -> line.substring(29, 41).trim,
            "Flash_Memory" -> line.substring(42, line.length).trim)
        }
        log_start_line += 1
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexi_bsc__ip_configurations__zqkb_static_routes
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip_configurations__zqkb_static_routes(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)
    var lineNum = 1
    var log_started = false
    var log_start_line = 0
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      if (!log_started && line.contains("ZQKB;")) log_started = true
      if (line == "") log_start_line = 0
      if (line.contains("COMMAND EXECUTED")) log_started = false
      if (log_started) {
        if (line.contains("GATEWAY ADDRESS")) log_start_line = lineNum + 3
      }
      if (log_start_line == lineNum) {
        if (line.contains("->")) {
          my_map += ("DF" -> line.substring(24, line.length).trim)
          list_map += my_map.toMap
          my_map.clear()
        }
        else {
          my_map += ("line_number" -> lineNum.toString,
            "UNIT" -> line.substring(0, 22).trim,
            "IPROUTE" -> line.substring(23, 67).trim,
            "PHY" -> line.substring(68, 73).trim,
            "NUM" -> line.substring(74, line.length).trim)
        }
        log_start_line += 1
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexi_bsc__ip_configurations__zqkb_statroute
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip_configurations__zqkb_statroute(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)
    var lineNum = 1
    var log_started = false
    var log_start_line = 0
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      if (!log_started && line.contains("ZQKB;")) log_started = true
      if (line == "") log_start_line = 0
      if (line.contains("COMMAND EXECUTED")) log_started = false
      if (log_started) {
        if (line.contains("GATEWAY ADDRESS")) log_start_line = lineNum + 3
      }
      if (log_start_line == lineNum) {
        if (line.contains("->")) {
          if (line.contains("DEFAULT")) {
            my_map += ("DF" -> line.substring(24, line.length).trim,
              "DF_IP1" -> "", "DF_IP2" -> "", "DF_IP3" -> "", "DF_IP4" -> "")
          }
          else {
            val ip_str_list = line.substring(24, line.length).trim.split("/").take(1)(0).split("[.]")
            my_map += ("DF_IP1" -> ip_str_list(0).trim,
              "DF_IP2" -> ip_str_list(1).trim,
              "DF_IP3" -> ip_str_list(2).trim,
              "DF_IP4" -> ip_str_list(3).trim,
              "DF" -> "")
          }
          list_map += my_map.toMap
          my_map.clear()
        }
        else {
          val ip_string_list = line.substring(23, 67).trim.split("[.]")
          my_map += ("line_number" -> lineNum.toString,
            "UNIT" -> line.substring(0, 22).trim,
            "IP1" -> ip_string_list(0).trim,
            "IP2" -> ip_string_list(1).trim,
            "IP3" -> ip_string_list(2).trim,
            "IP4" -> ip_string_list(3).trim)
        }
        log_start_line += 1
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexibsc__ss7_network__znci_signalling_link_data
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ss7_network__znci_signalling_link_data(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)
    var lineNum = 1
    var log_started = false
    var log_start_line = 0
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      if (!log_started && line.contains("INTERROGATING SIGNALLING LINK DATA")) {
        log_started = true
        log_start_line = lineNum + 5
      }
      if (line.contains("COMMAND EXECUTED")) {
        log_started = false
        log_start_line = 0
      }
      if (log_start_line == lineNum) {
          if (line != ""&& line.length>77) {
          my_map += ("line_number" -> lineNum.toString,
            "LINK" -> line.substring(3, 6).trim,
            "LINK_SET" -> line.substring(7, 26).trim,
            "PCM-TSL" -> line.substring(27, 38).trim,
            "UNIT" -> line.substring(39, 47).trim,
            "TERM" -> line.substring(48, 52).trim,
            "TF" -> line.substring(53, 55).trim,
            "LOG_UNIT" -> line.substring(56, 62).trim,
            "LOG_TERM" -> line.substring(63, 68).trim,
            "PAR_SET" -> line.substring(69, 70).trim,
            "BIT_RATE" -> line.substring(71, 76).trim,
            "MTP2_REQ" -> line.substring(77, line.length - 1).trim
          )
          list_map += my_map.toMap
          my_map.clear()
        }
        log_start_line += 1
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexi_bsc__swu__swu_monitor_esb24
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__swu__swu_monitor_esb24(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = mutable.ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.startsWith(" mirror")) {
        val flexi_bsc_swu_monitor_first_line = (line: String, linNum: Int) => {
          var logEntry = mutable.Map[String, String]()
          var split = line.split("\\s")

          split = line.trim.split("\\s+")
          logEntry += ("line_number" -> linNum.toString)
          logEntry += ("MIRROR" -> split(1))
          logEntry += ("PARAM1" -> split(2))
          logEntry
        }

        logEntry ++= flexi_bsc_swu_monitor_first_line(line, linNum)
        linNum += 1
        logEntries += logEntry.toMap

      }
      linNum += 1
    }

    logEntries.toList

  }

  /**
    * flexi_bsc_io_system_zw7i_fea_state
    *
    * @author Kinjal Singh.
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__io_system__zw7i_fea_state(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.contains("FEATURE CODE")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {

        if (line.contains("FEATURE CODE:")) {

          my_map += ("FEATURE_CODE" -> line.split("FEATURE CODE:")(1).replace('.', ' ').trim)
        }
        if (line.contains("FEATURE NAME:")) {

          my_map += ("FEATURE_NAME" -> line.split("FEATURE NAME:")(1).replace('.', ' ').trim)
        }
        if (line.contains("FEATURE STATE:")) {

          my_map += ("FEATURE_STATE" -> line.split("FEATURE STATE:")(1).replace('.', ' ').trim)
        }
        if (line.contains("FEATURE CAPACITY:")) {

          my_map += ("FEATURE_CAPACITY" -> line.split("FEATURE CAPACITY:")(1).replace('.', ' ').trim)
        }

        if (lineNum != (start_log + 1) && line.contains("----------------------------------------------")) {
          log_started = false
          my_map += "line_number" -> start_log.toString

          val cols_list = List("FEATURE_CODE", "FEATURE_NAME", "FEATURE_STATE", "FEATURE_CAPACITY")
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
    * flexi_bsc__ip_configurations__zqri_etpsig_m
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__ip_configurations__zqri_etpsig_m(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__ip_configurations__zqri_etpsig_m")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0

    var endlinelist, secondlinelist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()

    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.trim.length == 0) {
        endlinelist += lineNum
      }
      if (line.contains("MCMU-")) {
        secondlinelist += lineNum
      }

      lineNum += 1
    }
    for (startline <- secondlinelist) {
      val linedata = dataArray(startline - 1)
      val mcmu_text = linedata.trim
      endlineno = endlinelist.toList.filter(_ > startline).min
      for (linenno <- startline until endlineno) {
        var my_map = mutable.Map[String, String]()
        val tabledata = dataArray(linenno - 1)
        if ((tabledata.trim.length != 0) && tabledata.contains("VLAN")) {
          my_map += (
            "line_number" -> linenno.toString,
            "MCMU" -> mcmu_text,
            "VLAN" -> tabledata.substring(0, 10).trim,
            "STATE" -> tabledata.substring(11, 17).trim,
            "MTU" -> tabledata.substring(19, 25).trim,
            "ATTR" -> tabledata.substring(25, 29).trim,
            "IP_ADDR" -> tabledata.substring(30, 50).replace("(", "").replace(")", "").trim
          )
        }
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    * flexi_bsc__bsc_alarms__zaho_bsc_alarms
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__bsc_alarms__zahp_zaho_bsc_alarms(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = mutable.ListBuffer[Map[String, String]]()
    val flexi_bsc__bsc_alarms__zaho_bsc_alarms_first_line = (line: String, lineNum: Int) => {
      var logEntry = mutable.Map[String, String]()
      var split = line.split("\\s")
      split = line.trim.split("\\s+")

      logEntry += ("line_number" -> lineNum.toString)
      logEntry += ("BSC" -> split(0))
      logEntry += ("UNIT" -> split(1))
      logEntry += ("ALARM_TYPE" -> split(2))
      logEntry += ("DTTM" -> (split(3) + " " + split(4)))
      logEntry
    }
    val flexi_bsc__bsc_alarms__zaho_bsc_alarms_second_line = (line: String, _: Int) => {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("SEVERITY" -> line.substring(3, 10))
      logEntry += ("NOT_TYPE" -> line.substring(11, 17))
      logEntry += ("PARAM1" -> line.substring(22, 30))
      logEntry += ("ISSUER" -> line.substring(34, 40))
      logEntry
    }
    val flexi_bsc__bsc_alarms__zaho_bsc_alarms_third_line = (line: String, _: Int) => {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("TRANS_ID" -> line.substring(4, 10).trim)
      logEntry += ("ALARM_NO" -> line.substring(11, 15).trim)
      logEntry += ("ALARM_TXT" -> line.substring(16, 70).trim)
      logEntry
    }
    val flexi_bsc__bsc_alarms__zaho_bsc_alarms_fourth_line = (line: String, _: Int) => {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("SUPP" -> line.trim)
      logEntry
    }
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.startsWith("          ")) {

        logEntry ++= flexi_bsc__bsc_alarms__zaho_bsc_alarms_first_line(line, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarms__zaho_bsc_alarms_second_line(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarms__zaho_bsc_alarms_third_line(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarms__zaho_bsc_alarms_fourth_line(txt.nextLine, linNum)
        logEntries += logEntry.toMap

      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__etp_logs__etp_active_call
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__etp_logs__etp_active_call(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    var etp_id = "etp"
    val logEntries = new ListBuffer[Map[String, String]]()
    val flexi_bsc__etp_logs__etp_active_call_line = (line: String, lineNum: Int, etp_id: String) => {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("line_number" -> lineNum.toString)
      logEntry += ("NUM_CS_CALLS_ACTIVE" -> line.split("::")(1).trim)
      logEntry += ("ETP_ID" -> etp_id)
      logEntry
    }
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.startsWith("UNAUTHORIZED USAGE")) {
        linNum += 1
        etp_id =  txt.nextLine().trim
        linNum += 1
      }
      if (line.contains("Num CS calls Active ::")) {

        logEntry ++= flexi_bsc__etp_logs__etp_active_call_line(line, linNum, etp_id)
        linNum += 1
        logEntries += logEntry.toMap
      }

      linNum += 1
    }

    logEntries.toList
  }

  /**
    * flexi_bsc_bts_alarms_zeol_bts_alarms
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__bts_alarms__zeol_bts_alarms(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__bts_alarms__zeol_bts_alarms")
    def flexi_bsc__bts_alarms_zeol_bts_alarms_first_line(line: String, lineNum: Int): mutable.Map[String, String] = {
      var logEntry = mutable.Map[String, String]()

      logEntry += ("line_number" -> lineNum.toString)
      logEntry += ("BSC_NAME" -> line.substring(10, 19).trim)
      logEntry += ("BCF_ID" -> line.substring(20, 34).trim)
      logEntry += ("BTS_ID" -> line.substring(34, 44).trim)
      logEntry += ("TAG" -> line.substring(44, 53).trim)

      logEntry += ("DTTM" -> line.substring(53, line.length - 1).trim)

      logEntry

    }

    def flexi_bsc__bts_alarms_zeol_bts_alarms_second_line(line: String, lineNum: Int): mutable.Map[String, String] = {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("SEVERITY" -> line.substring(0, 4).trim)
      logEntry += ("ALARM_OR_CANCEL" -> line.substring(4, 10).trim)
      logEntry += ("TRX_ID" -> line.substring(10, 20).trim)
      logEntry += ("BTS_NAME" -> line.substring(24, line.length - 1).trim)
      logEntry

    }

    def flexi_bsc__bts_alarms_zeol_bts_alarms_third_line(line: String, lineNum: Int): mutable.Map[String, String] = {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("NOTIFICATION_ID" -> line.substring(4, 9).trim)
      logEntry += ("ALARM_NO" -> line.substring(11, 15).trim)
      logEntry += ("ALARM_TXT" -> line.substring(16, line.length - 1).trim)
      logEntry

    }

    def flexi_bsc__bts_alarms_zeol_bts_alarms_fourth_line(line: String, lineNum: Int): mutable.Map[String, String] = {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("SUPP_INFO" -> line.trim)
      logEntry

    }

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.startsWith("           KOG")) {

        logEntry ++= flexi_bsc__bts_alarms_zeol_bts_alarms_first_line(line, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bts_alarms_zeol_bts_alarms_second_line(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bts_alarms_zeol_bts_alarms_third_line(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bts_alarms_zeol_bts_alarms_fourth_line(txt.nextLine, linNum)
        logEntries += logEntry.toMap

      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    *
    * flexi_bscsw_configuration_zwqo_cr_omupack
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__sw_configuration__zwqo_cr_omupack(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0

    var endlinelist, secondlinelist, tableEndlist, tableStartlist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()

    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }
      if (line.trim.length == 0) {
        tableEndlist += lineNum
      }
      if (line.contains("TSLS")) {
        tableStartlist += lineNum
      }

      if (line.contains("PACKAGES CREATED IN OMU:")) {
        secondlinelist += lineNum
      }

      lineNum += 1
    }
    for (startline <- secondlinelist) {
      endlineno = endlinelist.toList.filter(_ > startline).min
      var linenno = startline
      while (linenno < (endlineno - 1)) {
        my_map.clear()
        val tabledata = dataArray(linenno - 1)
        val secondline = dataArray(linenno)
        val thirdline = dataArray(linenno + 1)
        if ((tabledata.length != 0) && (secondline.length != 0) && (thirdline.length != 0)) {
          linenno += 1
          my_map += ("line_number" -> linenno.toString,
            "SW_PACKAGE" -> tabledata.substring(2, 14).trim.concat(secondline.substring(2, 14).trim).concat(thirdline.substring(2, 14).trim),
            "STATUS" -> tabledata.substring(19, 22).trim.concat(" ").concat(secondline.substring(19, 23).trim).concat(" ").concat(thirdline.substring(19, 23).trim),
            "DIRECTORY" -> tabledata.substring(28, 38).trim.concat(" ").concat(secondline.substring(28, 38).trim).concat(" ").concat(thirdline.substring(28, 38).trim),
            "ENV" -> tabledata.substring(48, 64).trim.concat(" ").concat(secondline.substring(48, 64).trim).concat(" ").concat(thirdline.substring(48, thirdline.length - 1).trim),
            "DEF" -> tabledata.substring(69, 72).trim,
            "ACT" -> tabledata.substring(74, tabledata.length - 1).trim
          )

        }
        linenno += 3
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.nonEmpty)
  }

  /**
    * flexi_bsc__zwdi__boot_info
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__boot_info_txt__zwdi(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0

    var endlinelist, startlinelist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }
      if (line.startsWith(" UNIT")) {
        startlinelist += lineNum
      }

      lineNum += 1
    }
    for (startline <- startlinelist) {

      endlineno = endlinelist.toList.filter(_ > startline).min
      var linnum = startline + 1
      while (linnum < (endlineno - 1)) {
        my_map.clear()
        val firstrowdata = dataArray(linnum)
        val first_split = firstrowdata.trim.split("\\s+")

        val secondrowdata = dataArray(linnum + 1)
        val second_split = secondrowdata.trim.split("\\s+")

        if (firstrowdata.trim.length != 0) {
          my_map += (
            "line_number" -> linnum.toString,
            "UNIT" -> first_split(0).trim,
            "PIU_TYPE" -> first_split(1).trim,
            "BOOT_PKG_FLASH" -> first_split(2).trim,
            "FLASH_VERSION" -> first_split(3).trim,
            "DATE" -> first_split(4).trim,
            "RANDOM" -> first_split(5).trim,
            "BOOT_PKG_DISK" -> second_split(0).trim,
            "DISK_VERSION" -> second_split(1).trim,
            "DV_DATE" -> second_split(2).trim,
            "RANDOM1" -> second_split(3).trim
          )
        }
        linnum = linnum + 2
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)

  }

  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_dinho   *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_dinho(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = mutable.ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.contains("DISABLE INTERNAL HO")) {

        logEntry += ("line_number" -> linNum.toString,
          "DINHO" -> line.split("\\)..")(1).trim)
        linNum += 1
        logEntries += logEntry.toMap

      }
      linNum += 1
    }

    logEntries.toList

  }

  /**
    *flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_bsc_bcf_mc_sbnt
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_bsc_bcf_mc_sbnt(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    var bcf_data_text, c_plane_ip = "bcf"
    val logEntries = new ListBuffer[Map[String, String]]()
    def flexi_bsc__fb_bsc_bcf_mc_sbnt__rnw_status_and_bsc_and_bcf_data_line(line: String, lineNum: Int, bcf_data_text: String, c_plane_ip: String): mutable.Map[String, String] = {
      var logEntry = mutable.Map[String, String]()
      val ip1 = line.split("\\s+").last.split("/")
      val ip2 = c_plane_ip.toString.split("/")

      logEntry += ("line_number" -> lineNum.toString)
      logEntry += ("BCFDATA" -> bcf_data_text)
      logEntry += ("CUPLANE_IP1" -> ip2(0).split("\\.")(0))
      logEntry += ("CUPLANE_IP2" -> ip2(0).split("\\.")(1))
      logEntry += ("CUPLANE_IP3" -> ip2(0).split("\\.")(2))
      logEntry += ("CUPLANE_IP4" -> ip2(0).split("\\.")(3))
      logEntry += ("CUPLANE_SubnetMask" -> ip2(1))
      logEntry += ("MPLANE_IP1" -> ip1(0).split("\\.")(0))
      logEntry += ("MPLANE_IP2" -> ip1(0).split("\\.")(1))
      logEntry += ("MPLANE_IP3" -> ip1(0).split("\\.")(2))
      logEntry += ("MPLANE_IP4" -> ip1(0).split("\\.")(3))
      logEntry += ("MPLANE_SubnetMask" -> ip1(1))
      logEntry

    }
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.startsWith("BASE CONTROL FUNCTION")) {

        bcf_data_text =  line.split("\\s+")(3).trim
        linNum += 1
      }
      if (line.contains("BTS C/U-PLANE IP ADDRESS AND SUBNET MASK LENGTH")) {

        c_plane_ip = line.split("\\s+").last.trim
        linNum += 1

      }
      if (line.contains("BTS M-PLANE IP ADDRESS AND SUBNET MASK LENGTH")) {

        logEntry ++= flexi_bsc__fb_bsc_bcf_mc_sbnt__rnw_status_and_bsc_and_bcf_data_line(line, linNum, bcf_data_text, c_plane_ip)
        linNum += 1
        logEntries += logEntry.toMap
      }

      linNum += 1
    }

    logEntries.toList
  }

  /** flexi_bsc__ss7_network__zobl_ip_broadcast
    *
    * @author Kinjal singh
    *
    */
  def flexi_bsc__ss7_network__zobl_ip_broadcast(logfilecontent: String): List[Map[String, String]] = {

    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0

    var endlinelist, secondlinelist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }
      if (line.contains("INTERROGATING LOCAL BROADCAST STATUS OF SCCP SUBSYSTEMS")) {
        secondlinelist += lineNum
      }

      lineNum += 1
    }
    for (startline <- secondlinelist) {

      endlineno = endlinelist.toList.filter(_ > startline + 5).min
      for (linenno <- startline + 7 until endlineno) {
        my_map.clear()
        val tabledata = dataArray(linenno - 1)
        if ((tabledata.trim.length != 0) && tabledata.contains("/")) {
          my_map += (
            "line_number" -> linenno.toString,
            "BROADCAST_GROUPS" -> tabledata.split("\\s+/")(0).trim,
            "CONCERNED_LOCAL_SUBSYSTEMS" -> tabledata.split("\\s+/")(1).trim
          )
        }
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    *
    * flexi_bsc___ip_configurations
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */

  def flexi_bsc___ip_configurations(logfilecontent: String): List[Map[String, String]] = {

    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0

    var endlinelist, startlinelist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line


      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }
      if (line.startsWith("ZQHT:")) {
        startlinelist += lineNum
      }

      lineNum += 1
    }
    for (startline <- startlinelist) {
      val linedata = dataArray(startline - 1)
      val t = linedata.split(":")(1)
      endlineno = endlinelist.toList.filter(_ > startline).min
      var linnum = startline + 1
      while (linnum < (endlineno - 1)) {
        my_map.clear()

        val row1data = dataArray(linnum)
        val row2data = dataArray(linnum + 1)
        val row3data = dataArray(linnum + 2)
        val row4data = dataArray(linnum + 3)
        val row5data = dataArray(linnum + 4)

        if ((row1data.trim.length != 0) && (row5data.trim.length != 0)) {
          my_map += (
            "line_number" -> linnum.toString,
            "unit" -> t.trim,
            "Txpackets" -> row1data.trim,
            "RxPackets" -> row1data.trim,
            "BroadcastTX" -> row2data.trim,
            "BroadcastRX" -> row2data.trim,
            "VLANtx" -> row3data.trim,
            "VLANrx" -> row3data.trim,
            "Errors" -> row4data.trim,
            "TXcarrierSenseLost" -> row5data.trim
          )
        }
        linnum = linnum + 5
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)

  }

  /**
    * flexi_bsc__ip__fb_ipc_zqri_etpxunit
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip__fb_ipc_zqri_etpxunit(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0
    var endlinelist, secondlinelist, thirdlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.length == 0) {
        endlinelist += lineNum
      }
      if (line.startsWith("ETP") & line.split("\\s+").length == 1) {
        secondlinelist += lineNum
      }
      if (line.contains("->IL0")) {
        thirdlinelist += lineNum
      }

      lineNum += 1
    }

    for (startline <- secondlinelist) {
      val linedata = dataArray(startline - 1)
      val etp_text = (linedata.split("\\s+")(0).trim)
      endlineno = endlinelist.toList.filter(_ > startline).min
      for (linenno <- startline until endlineno) {
        my_map.clear()
        val tabledata = dataArray(linenno - 1)
        if ((tabledata.trim.length != 0) && tabledata.contains("->IL0")) {
          my_map += ("line_number" -> linenno.toString,
            "ETPxUNIT" -> etp_text,
            "PARAM1" -> tabledata.split("\\s+")(2).trim,
            "PARAM2" -> tabledata.split("\\s+")(3).trim,
            "IP_ADDR" -> tabledata.split("\\s+")(4).split("/")(0).replace("(", " ").trim,
            "SUBNET" -> tabledata.split("\\s+")(4).split("/")(1).replace(")", " ").trim)
        }
        list_map += my_map.toMap
      }
    }

    list_map.toList.filter(_.size > 1)
  }

  /**
    * flexi_bsc__bsc_alarm_history__zahp_bsc_alarm_histroy
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__bsc_alarm_history__zahp_bsc_alarm_histroy(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("    <HIST>")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry ++= flexiBscZahpBscAlarmHistoryFirstLine(line, linNum)
        linNum += 1
        logEntry ++= flexiBscZahpBscAlarmHistorySecondLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexiBscZahpBscAlarmHistoryThirdLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexiBscZahpBscAlarmHistoryFourthLine(txt.nextLine, linNum)
        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList

  }

  def flexiBscZahpBscAlarmHistoryFirstLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    var split = line.split("\\s")
    split = line.trim.split("\\s+")
    logEntry += ("history" -> ( split(0).trim))
    logEntry += ("bsc" -> ( split(1).trim))
    logEntry += ("unit" -> ( split(2).trim))
    logEntry += ("alart_type" -> ( split(3).trim))
    logEntry += ("date_time" -> ( split(4).trim + split(5).trim))
    logEntry

  }

  def flexiBscZahpBscAlarmHistorySecondLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("param" -> ( line.substring(3, 10).trim))
    logEntry += ("type" -> ( line.substring(11, 17).trim))
    logEntry += ("issuer" -> ( line.substring(22, 30).trim))
    logEntry += ("cart" -> ( line.substring(34, 40).trim))
    logEntry

  }

  def flexiBscZahpBscAlarmHistoryThirdLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("notification_id" -> ( line.substring(4, 10).trim))
    logEntry += ("alarm_no" -> ( line.substring(11, 15).trim))
    logEntry += ("alarm_txt" -> ( line.substring(16, 70).trim))
    logEntry

  }

  def flexiBscZahpBscAlarmHistoryFourthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("supp" -> ( line.trim))
    logEntry

  }

  /**
    * flexi_bsc__bsc_alarms__zaho_bsc_alarm_check
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__bsc_alarms__zaho_bsc_alarm_check(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("          ")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry ++= flexiBscZahoBscAlarmsFirstLine(line, linNum)
        linNum += 1
        logEntry ++= flexiBscZahoBscAlarmsSecondLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexiBscZahoBscAlarmsThirdLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexiBscZahoBscAlarmsFourthLine(txt.nextLine, linNum)
        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList


  }

  def flexiBscZahoBscAlarmsFirstLine(line: String, lineNum: Int): mutable.Map[String, String] = {
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

  def flexiBscZahoBscAlarmsSecondLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("param" -> ( line.substring(3, 10)))
    logEntry += ("type" -> ( line.substring(11, 17)))
    logEntry += ("issuer" -> ( line.substring(22, 30)))
    logEntry += ("cart" -> ( line.substring(34, 40)))
    logEntry

  }

  def flexiBscZahoBscAlarmsThirdLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("notification_id" -> ( line.substring(4, 10).trim))
    logEntry += ("alarm_no" -> ( line.substring(11, 15).trim))
    logEntry += ("alarm_txt" -> ( line.substring(16, 70).trim))
    logEntry

  }

  def flexiBscZahoBscAlarmsFourthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("supp" -> ( line.trim))
    logEntry

  }


  /**
    * flexi_bsc__measurement_status__zifi_event_sending_status
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__measurement_status__zifi_event_sending_status(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("    EVENT SENDING:")){

        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("event_sending" -> ( line.splitAt(18)._2.trim))
        logEntries += logEntry

      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__ip_configurations__zqri_bcsu_internal_ip
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip_configurations__zqri_bcsu_internal_ip(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1


    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("BCSU")) {

        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("bcsu" -> (line.substring(0, 7).trim))
        linNum += 1
        line = txt.nextLine()
        if (line.startsWith("  EL")) {

          logEntry ++= flexi_bsc__ip_configurations__zqri_bcsu_internal_ipSecondLine(line, linNum)
          linNum += 1
          logEntry ++= flexi_bsc__ip_configurations__zqri_bcsu_internal_ipThirdLine(txt.nextLine, linNum)
          linNum += 1
          logEntry ++= flexi_bsc__ip_configurations__zqri_bcsu_internal_ipFourthLine(txt.nextLine, linNum)
          txt.nextLine()
          linNum += 2
          logEntry ++= flexi_bsc__ip_configurations__zqri_bcsu_internal_ipFifthLine(txt.nextLine, linNum)
          txt.nextLine()
          linNum += 2
          logEntry ++= flexi_bsc__ip_configurations__zqri_bcsu_internal_ipSixthLine(txt.nextLine, linNum)

          logEntries += logEntry


        }
      }
      linNum += 1
    }

    logEntries.toList


  }

  def flexi_bsc__ip_configurations__zqri_bcsu_internal_ipSecondLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()

    logEntry += ("vlan1" -> ( line.substring(2, 9).trim))
    logEntry += ("vlan1_state" -> ( line.substring(13, 15).trim))
    logEntry += ("vlan1_mtu" -> ( line.substring(19, 23).trim))
    logEntry += ("vlan1_attr" -> ( line.substring(25, 29).trim))
    logEntry += ("vlan1_ip_subnet" -> ( line.substring(33, 79).trim))
    logEntry
  }

  def flexi_bsc__ip_configurations__zqri_bcsu_internal_ipThirdLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()

    logEntry += ("vlan2" -> ( line.substring(2, 9).trim))
    logEntry += ("vlan2_state" -> ( line.substring(13, 15).trim))
    logEntry += ("vlan2_mtu" -> ( line.substring(19, 23).trim))
    logEntry += ("vlan2_attr" -> ( line.substring(25, 29).trim))
    logEntry += ("vlan2_ip_subnet" -> ( line.substring(33, 79).trim))
    logEntry
  }

  def flexi_bsc__ip_configurations__zqri_bcsu_internal_ipFourthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()

    if(line.contains("VLAN")) {
      logEntry += ("vlan3" -> (line.substring(2, 9).trim))
      logEntry += ("vlan3_state" -> (line.substring(13, 15).trim))
      logEntry += ("vlan3_mtu" -> (line.substring(19, 23).trim))
      logEntry += ("vlan3_attr" -> (line.substring(25, 29).trim))
      logEntry += ("vlan3_ip_subnet" -> (line.substring(33, 79).trim))
      logEntry
    }else{
      logEntry += ("vlan3" -> "")
      logEntry += ("vlan3_state" -> "")
      logEntry += ("vlan3_mtu" -> "")
      logEntry += ("vlan3_attr" -> "")
      logEntry += ("vlan3_ip_subnet" -> "")
      logEntry
    }
  }

  def flexi_bsc__ip_configurations__zqri_bcsu_internal_ipFifthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()

    if(line.contains("VLAN")) {
      logEntry += ("vlan4" -> (line.substring(2, 9).trim))
      logEntry += ("vlan4_state" -> (line.substring(13, 15).trim))
      logEntry += ("vlan4_mtu" -> (line.substring(19, 23).trim))
      logEntry += ("vlan4_attr" -> (line.substring(25, 29).trim))
      logEntry += ("vlan4_ip_subnet" -> (line.substring(33, 79).trim))
      logEntry
    }else{
      logEntry += ("vlan4" -> "")
      logEntry += ("vlan4_state" -> "")
      logEntry += ("vlan4_mtu" -> "")
      logEntry += ("vlan4_attr" -> "")
      logEntry += ("vlan4_ip_subnet" -> "")
      logEntry

    }
  }

  def flexi_bsc__ip_configurations__zqri_bcsu_internal_ipSixthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    if(line.contains("VLAN")){
      logEntry += ("vlan5" -> ( line.substring(2, 9).trim))
      logEntry += ("vlan5_state" -> ( line.substring(13, 15).trim))
      logEntry += ("vlan5_mtu" -> ( line.substring(19, 23).trim))
      logEntry += ("vlan5_attr" -> ( line.substring(25, 29).trim))
      logEntry += ("vlan5_ip_subnet" -> ( line.substring(33, 79).trim))
      logEntry
    }else{
      logEntry += ("vlan5" -> "")
      logEntry += ("vlan5_state" -> "")
      logEntry += ("vlan5_mtu" -> "")
      logEntry += ("vlan5_attr" -> "")
      logEntry += ("vlan5_ip_subnet" -> "")
      logEntry
    }
  }


  /**
    * flexi_bsc__trx_deletion_error__zerd
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__trx_deletion_error__zerd(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 7
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("ZERD")) {

        for (_ <- 1 to skip) {
          line = txt.nextLine()
        }

        linNum += skip

        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("error" -> ( line.stripPrefix("/***").trim.stripSuffix("***/").trim))
        linNum += 1
        logEntry += ("error_desc" -> ( txt.nextLine.stripPrefix("/***").trim.stripSuffix("***/").trim))

        logEntries += logEntry


      }
      linNum += 1
    }

    logEntries.toList


  }

  /**
    * flexi_bsc__system_configuration__zwti_u_configuration
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__system_configuration__zwti_u_configuration(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 8
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZWTI:U;")) {

        for (_ <- 1 to skip) {
          line = txt.nextLine()
        }

        linNum += skip

        logEntry += ("line_number" -> linNum.toString)

        while (line.length != 0) {
          logEntry ++= flexi_bsc__system_configuration__zwti_u_configurationFirstLine(line, linNum)
          logEntries += logEntry
          line = txt.nextLine()
          linNum += 1

        }

      }
      linNum += 1
    }

    logEntries.toList


  }

  def flexi_bsc__system_configuration__zwti_u_configurationFirstLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    var split = line.split("\\s")
    split = line.trim.split("\\s+")

    logEntry += ("unit" -> ( split.lift(0).getOrElse("")))
    logEntry += ("param1" -> ( split.lift(1).getOrElse("")))
    logEntry += ("param2" -> ( split.lift(2).getOrElse("")))
    logEntry += ("location" -> ( split.lift(3).getOrElse("")))
    logEntry += ("master" -> ( split.lift(4).getOrElse("")))
    logEntry

  }

  /**
    * flexibsc__io_system__zw7i_fea_state
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexibsc__io_system__zw7i_fea_state(logfilecontent: String): List[mutable.Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[mutable.Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.contains("FEATURE CODE:..............")) {

        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("feature_code" -> ( line.split("FEATURE CODE:..............").lift(1).getOrElse("").trim))
        linNum += 1
        logEntry += ("feature_name" -> ( txt.nextLine().split("FEATURE NAME:..............").lift(1).getOrElse("").trim))
        linNum += 1
        logEntry += ("feature_state" -> ( txt.nextLine().split("FEATURE STATE:.............").lift(1).getOrElse("").trim))
        linNum += 1
        logEntry += ("feature_capacity" -> ( txt.nextLine().split("FEATURE CAPACITY:..........").lift(1).getOrElse("").trim))
        logEntries += logEntry


      }
      linNum += 1
    }

    logEntries.toList


  }

  /**
    * flexi_bsc__io_system__zw7i_fea_usage
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__io_system__zw7i_fea_usage(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 5
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("USED CAPACITY REPORT:")) {

        for (_ <- 1 to skip) {
          line = txt.nextLine()
        }

        linNum += skip

        logEntry += ("line_number" -> linNum.toString)

        while (line.length != 0) {
      //    println(line)
          logEntry ++= flexiBsc_IO_system_ZW7I_FEA_USAGE_parsedLine(line, linNum)
          logEntries += logEntry
          txt.nextLine()
          line = txt.nextLine()
          linNum += 2

        }
      }
      linNum += 1
    }

    logEntries.toList


  }

  def flexiBsc_IO_system_ZW7I_FEA_USAGE_parsedLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    var split = line.split("\\s")
    split = line.trim.split("\\s+")

    logEntry += ("feature_code" -> ( split.lift(0).getOrElse("").trim))
    logEntry += ("feature_usage" -> ( split.lift(1).getOrElse("").trim))
    logEntry

  }

  /**
    * flexi_bsc__switch_info__zw6g_topology
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__switch_info__zw6g_topology(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("T1 - ") && line.contains(", NETWORK ELEMENT") && line.contains("(CURRENT IS T")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("topology" -> ( line.split("CURRENT IS T")(1).split(",")(0).trim))
        logEntry += ("bsc_type" -> ( line.split(", NETWORK ELEMENT")(1).split("\\)")(0).trim))
        logEntries += logEntry
        txt.nextLine()
        line = txt.nextLine()


      }
      linNum += 1
    }

    logEntries.toList


  }

  /**
    * flexibsc__clock_and_lapd_status__zdti_lapd_check
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__clock_and_lapd_status__zdti_lapd_check(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZDTI;")) {
        line = txt.nextLine()
        linNum += 1

        while (txt.hasNextLine && !line.contains("INTERROGATING D-CHANNEL WORKING STATE")) {
          line = txt.nextLine()
          linNum += 1
        }
        if (txt.hasNextLine) {

          for (_ <- 1 to skip) {
            line = txt.nextLine()
          }

          linNum += skip
          logEntry += ("line_number" -> linNum.toString)

          while (line.length != 0) {
            logEntry ++= flexiBsc__clock_and_lapd_status__zdti_lapd_check_parsedline(line, linNum)
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

  def flexiBsc__clock_and_lapd_status__zdti_lapd_check_parsedline(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    var split = line.split("\\s")
    split = line.trim.split("\\s+")

    logEntry += ("name" -> ( split.lift(0).getOrElse("").trim))
    logEntry += ("num" -> ( split.lift(1).getOrElse("").trim))
    logEntry += ("unit" -> ( split.lift(2).getOrElse("").trim))
    logEntry += ("interface_side" -> ( split.lift(3).getOrElse("").trim))
    logEntry += ("pcm_tsl_tsl" -> ( split.lift(4).getOrElse("").trim))
    logEntry += ("sapi" -> ( split.lift(5).getOrElse("").trim))
    logEntry += ("working_state" -> ( split.lift(6).getOrElse("").trim))
    logEntry

  }

  /**
    * flexibsc__clock_and_lapd_status__zdri_sync_input
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__clock_and_lapd_status__zdri_sync_input(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val tblLen = 6
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZDRI;")) {

        while (!line.contains("----- --------------- ---------- ----------") && txt.hasNext()) {
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
          linNum += 1
        }
        linNum += 1
        logEntry += ("line_number" -> linNum.toString)
        for (_ <- 1 to tblLen) {
          line = txt.nextLine()
          logEntry += ("unit" -> ( line.substring(0, 6).trim))
          logEntry += ("state" -> ( line.substring(6, 22).trim))
          logEntry += ("used_input" -> ( line.substring(22, 34).trim))
          logEntry += ("priority" -> ( line.splitAt(34)._2.trim))
          logEntries += logEntry
          linNum += 1
        }

      }
      linNum += 1
    }

    logEntries.toList
  }

  /**
    * flexibsc__ip_configurations__zqvi_pad_parameters
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip_configurations__zqvi_pad_parameters(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZQVI;")) {

        while (!line.contains("=======================================================================")) {
          line = txt.nextLine()
          linNum += 1
        }
        linNum += 1
        line = txt.nextLine()
        logEntry += ("line_number" -> linNum.toString)
        while (line.length != 0) {
          logEntry += ("nr" -> ( line.substring(0, 4).trim))
          logEntry += ("padp_name" -> ( line.substring(4, 53).trim))
          logEntry += ("current_value" -> ( line.splitAt(53)._2.trim))
          logEntries += logEntry
          line = txt.nextLine()
          linNum += 1
        }

      }
      linNum += 1
    }

    logEntries.toList
  }

  /**
    * flexibsc__ss7_network_txt_znci_m3ua_based_signalling_links
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ss7_network_txt__znci_m3ua_based_signalling_links(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 5
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("M3UA BASED SIGNALLING LINKS")) {

        for (_ <- 1 to skip) {
          line = txt.nextLine()
        }

        linNum += skip
        logEntry += ("line_number" -> linNum.toString)
        while (line.length != 0) {
          logEntry += ("m3ua_link" -> ( line.substring(0, 11).trim))
          logEntry += ("m3ua_link_set" -> ( line.substring(11, 33).trim))
          logEntry += ("association_set" -> ( line.splitAt(33)._2.split("\\s+")(1).trim))
          logEntry += ("m3ua_param_set" -> ( line.splitAt(33)._2.split("\\s+")(2).trim))
          logEntries += logEntry
          line = txt.nextLine()
          linNum += 1
        }

      }
      linNum += 1
    }

    logEntries.toList


  }

  /**
    * flexibsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_m_
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_m_(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 4

    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZDOI:OMU&MCMU&BCSU:M;")) {

        for (_ <- 1 to skip) {
          line = txt.nextLine()
        }

        linNum += skip
        logEntry += ("line_number" -> linNum.toString)
        while (!line.contains("COMMAND EXECUTED")) {
          logEntry += ("unit" -> ( line.split(":")(1).trim))
          txt.nextLine()
          line = txt.nextLine()
          linNum += 2
          logEntry += ("pool" -> ( line.split("POOL")(1).split("\\s+").lift(1).getOrElse("").trim))
          logEntry += ("pool_percentage" -> ( line.split("POOL")(1).split("\\s+").lift(2).getOrElse("").trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("common_buffers" -> ( line.split("COMMON BUFFERS")(1).split("\\s+")(1).trim))
          logEntry += ("common_buffers_percentage" -> ( line.split("COMMON BUFFERS")(1).split("\\s+")(2).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("family_environment" -> ( line.split("FAMILY ENVIRONMENT")(1).split("\\s+")(1).trim))
          logEntry += ("family_environment_percentage" -> ( line.split("FAMILY ENVIRONMENT")(1).split("\\s+")(2).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("private_buffers" -> ( line.split("PRIVATE BUFFERS")(1).split("\\s+")(1).trim))
          logEntry += ("private_buffers_percentage" -> ( line.split("PRIVATE BUFFERS")(1).split("\\s+")(2).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("message_buffers" -> ( line.split("MESSAGE BUFFERS")(1).split("\\s+")(1).trim))
          logEntry += ("message_buffers_percentage" -> ( line.split("MESSAGE BUFFERS")(1).split("\\s+")(2).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("free_memory" -> ( line.split("FREE MEMORY")(1).split("\\s+")(1).trim))
          logEntry += ("free_memory_percentage" -> ( line.split("FREE MEMORY")(1).split("\\s+")(2).trim))
          line = txt.nextLine()
          //    println(line)
          linNum += 1
          logEntry += ("free_headers_count" -> ( line.split("FREE HEADERS ")(1).split("\\s+")(0).trim))
          logEntry += ("free_headers_count_percentage" -> ( line.split("FREE HEADERS ")(1).split("\\s+")(0).trim))
          logEntries += logEntry

          for (_ <- 0 to 2) {
            line = txt.nextLine()
          }
          //   println(line)
          linNum += 3
        }


      }
      linNum += 1
    }

    logEntries.toList
  }

  /**
    * flexi_bsc__bsc_alarm_history_txt_zahp_alarm_sup
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__bsc_alarm_history_txt__zahp_alarm_sup(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("    <HIST>")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry ++= flexi_bsc__bsc_alarm_history__zahp_alarm_supFirstLine(line, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarm_history__zahp_alarm_supSecondLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarm_history__zahp_alarm_supThirdLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarm_history__zahp_alarm_supFourthLine(txt.nextLine, linNum)
        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList


  }

  def flexi_bsc__bsc_alarm_history__zahp_alarm_supFirstLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    var split = line.split("\\s")
    split = line.trim.split("\\s+")
    logEntry += ("bsc" -> ( split(1)))
    logEntry += ("unit" -> ( split(2)))
    logEntry += ("alarm_type" -> ( split(3)))
    logEntry += ("date_time" -> ( split(4) + " " + split(5)))
    logEntry

  }

  def flexi_bsc__bsc_alarm_history__zahp_alarm_supSecondLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("severity" -> ( line.substring(3, 10)))
    logEntry += ("not_type" -> ( line.substring(11, 17)))
    logEntry += ("param1" -> ( line.substring(22, 30)))
    logEntry += ("issuer" -> ( line.substring(34, 40)))
    logEntry

  }

  def flexi_bsc__bsc_alarm_history__zahp_alarm_supThirdLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("trans_id" -> ( line.substring(4, 10).trim))
    logEntry += ("alarm_id" -> ( line.substring(11, 15).trim))
    logEntry += ("alarm_txt" -> ( line.substring(16, 70).trim))
    logEntry

  }

  def flexi_bsc__bsc_alarm_history__zahp_alarm_supFourthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("supp" -> ( line.trim))
    logEntry

  }

  /**
    * flexibsc__supplementary_ss7_network__zosk
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexibsc__supplementary_ss7_network__zosk(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZOSK") && line.endsWith(";")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("mtpsccp" -> ( line.split("ZOSK:")(1).trim.stripSuffix(";")))
        while (!line.contains("   ==================    ==================    ================")) {
          line = txt.nextLine()
          linNum += 1
        }
        linNum += 1
        line = txt.nextLine()
        while (line.length != 0) {
          logEntry += ("sp_code_hd" -> ( line.substring(0, 24).trim))
          logEntry += ("sp_name" -> ( line.substring(24, 47).trim))
          logEntry += ("reporting_status" -> ( line.splitAt(47)._2.trim))
          logEntries += logEntry
          line = txt.nextLine()
          linNum += 1
        }

      }
      linNum += 1
    }

    logEntries.toList
  }


  /**
    * flexiBsc__Supervision_and_disk_status__fb_unit_memory_check
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__supervision_and_disk_status_txt__fb_unit_memory_check(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("UNIT:") && retxt(linNum + 1).startsWith("POOL")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("unit" -> ( line.split(":")(1).trim))
        txt.nextLine()
        line = txt.nextLine()
        linNum += 2
        logEntry += ("pool" -> ( line.split("POOL")(1).split("\\s+").lift(1).getOrElse("").trim))
        while (!line.contains("FREE MEMORY")) {
          line = txt.nextLine()
          linNum += 1
        }
        logEntry += ("free_memory" -> ( line.split("FREE MEMORY")(1).split("\\s+")(1).trim))
        logEntries += logEntry
      }
      linNum += 1
    }

    logEntries.toList
  }

  /**
    * flexiBsc__RNW_status_and_BSC_and_BCF_data__zeoe_bts_blocked_alarms
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeoe_bts_blocked_alarms(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZEOE;")) {

        while (!line.contains("BSC") && txt.hasNext()) {
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
          linNum += 1
        }
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("unit" -> ( line.split("\\s+")(1).trim))
        logEntry += ("date_time" -> ( line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))
        while (!line.startsWith("BLOCKED ALARMS") && txt.hasNext()) {
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
          linNum += 1
        }
        while (!line.startsWith("------") && txt.hasNext()) {
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
          linNum += 1
        }
        line = txt.nextLine()
        line += 1
        while (line.length != 0) {
          logEntry += ("param1" -> ( line.substring(0, 8).trim))
          logEntry += ("param2" -> ( line.splitAt(8)._2.trim))
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
    * flexiBsc__unit_information__fb_mc_unit_status
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__unit_information__fb_mc_unit_status(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("WORKING STATE OF UNITS")) {
        txt.nextLine()
        line = txt.nextLine()
        linNum += 2
        logEntry += ("line_number" -> linNum.toString)
        while (line.trim.length != 0) {
          logEntry += ("unit" -> ( line.split("\\s+")(1).trim))
          logEntry += ("info" -> ( line.splitAt(12)._2.trim))
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
    * flexiBsc__BTS_SW_LMU_and_Abis_pool_data__zewo_bcf_swpack
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__bts_sm_lmu_and_abis_pool_data__zewo_bcf_swpack(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 3
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZEWO")) {

        while (!line.startsWith("BCF NUMBER      STATUS  BUILD-ID          VERSION    SUBDIR     STATE   SWMASTER")) {
          line = txt.nextLine()
          linNum += 1
        }

        for (i <- 1 to skip) {
          line = txt.nextLine()
        }
        linNum += skip
        logEntry += ("line_number" -> linNum.toString)

        while (!line.startsWith("COMMAND EXECUTED")) {
          logEntry += ("bcf_number" -> ( line.substring(0, 16).trim))
          logEntry += ("nw_status" -> ( line.substring(16, 24).trim))
          logEntry += ("nw_build_id" -> ( line.substring(24, 42).trim))
          logEntry += ("nw_version" -> ( line.substring(42, 53).trim))
          logEntry += ("nw_subdir" -> ( line.substring(53, 64).trim))
          logEntry += ("nw_state" -> ( line.substring(64, 71).trim))
          logEntry += ("nw_swmaster" -> ( line.splitAt(72)._2.trim))
          line = txt.nextLine()
          linNum += 1
          //println(line)
          logEntry += ("bu_status" -> ( line.substring(16, 24).trim))
          logEntry += ("bu_build_id" -> ( line.substring(24, 42).trim))
          logEntry += ("bu_version" -> ( line.substring(42, 53).trim))
          logEntry += ("bu_subdir" -> ( line.substring(53, 64).trim))
          logEntry += ("bu_state" -> ( line.substring(64, 71).trim))
          logEntry += ("bu_swmaster" -> ( line.splitAt(72)._2.trim))
          line = txt.nextLine()
          linNum += 1
          //println(line)
          logEntry += ("fb_status" -> ( line.substring(16, 24).trim))
          logEntry += ("fb_build_id" -> ( line.substring(24, 42).trim))
          logEntry += ("fb_version" -> ( line.substring(42, 53).trim))
          logEntry += ("fb_subdir" -> ( line.substring(53, 64).trim))
          logEntry += ("fb_state" -> ( line.substring(64, 71).trim))
          logEntry += ("fb_swmaster" -> ( line.splitAt(72)._2.trim))
          logEntries += logEntry
          txt.nextLine()
          line = txt.nextLine()
          linNum += 2
          //println(line)
        }
      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexiBsc__SWU_7__tsn_0911_swu_7_edgeport
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__swu_7__tsn_0911_swu_7_edgeport(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("swu-") && line.contains("# sh running-config")) {
        while (!(line.contains("spanning-tree port 1/1/") && line.endsWith("edgeport enable")) && txt.hasNext()) {
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
          linNum += 1
        }
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("low_port" -> ( line.split("\\s+")(3).split("-")(0).stripPrefix("1/1/").trim))
        logEntry += ("high_port" -> ( line.split("\\s+")(3).split("-")(1).stripPrefix("1/1/").trim))
        logEntries += logEntry

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexiBsc__Switch_info__switch_working_state
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__switch_info__switch_working_state(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("WORKING STATE OF UNITS")) {
        txt.nextLine()
        line = txt.nextLine()
        linNum += 2
        logEntry += ("line_number" -> linNum.toString)
        while (line.length != 0) {
          logEntry += ("unit" -> ( line.substring(0, 17).trim))
          logEntry += ("phy_state" -> ( line.substring(17, 23).trim))
          logEntry += ("location" -> ( line.substring(23, 45).trim))
          logEntry += ("info" -> ( line.splitAt(45)._2.trim))
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
    * flexibsc__rnw_status_and_bsc_and_bcf_data__zefo_bcf_all_parameters1
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_bcf_all_parameters1(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("BASE CONTROL FUNCTION BCF-") && line.endsWith("DATA")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bcf_number" -> ( line.split("-")(1).split("\\s+")(0).trim))
        line = txt.nextLine()
        linNum += 1

        while (!line.startsWith("ANTENNA ID") && txt.hasNext()) {
          if (!line.isEmpty) {
            var value = ""
            var index = line.lastIndexOf(". ")
            var split = line.splitAt(index + 1)
            logEntry += ("param_name" -> ( split._1.replaceAll("\\.", "").trim))
            if (value.endsWith(")")) {

              logEntry += ("param_value" -> ( split._2.split('(')(0).trim))

            } else {

              logEntry += ("param_value" -> ( split._2.trim))
            }
            logEntries += logEntry
          }
          linNum += 1
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
        }

      }

      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_bsc__bts_seg_and_trx_parameters__zeqo_btsparameter
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__bts_seg_and_trx_parameters__zeqo_btsparameter(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("BCF-") && line.contains("BTS-")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("bcf" -> ( line.split("\\s+")(0).split("-")(1).trim))
        logEntry += ("bts" -> ( line.split("\\s+")(1).split("-")(1).trim))
        txt.nextLine()
        line = txt.nextLine()
        linNum += 2
        logEntry += ("bts_adm_state" -> ( flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))
        line = txt.nextLine()
        linNum += 1
        logEntry += ("bts_state" -> ( flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))

        while (txt.hasNextLine && !line.startsWith("GPRS ENABLED.....................................(GENA)...")) {
          linNum += 1
          line = txt.nextLine()
        }
        logEntry += ("gena" -> ( flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))
        line = txt.nextLine()
        linNum += 1
        if(line.contains("PACKET SERVICE ENTITY IDENTIFIER..............(PSEI)...")) {
          logEntry += ("psei" -> (flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))
          txt.nextLine();
          txt.nextLine()
          line = txt.nextLine()
          linNum += 3
          logEntry += ("pcu_index" -> (flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("pcu_id" -> (flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("pcu_state" -> (flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("bvci" -> (flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))
          txt.nextLine()
          line = txt.nextLine()
          linNum += 2
          for (i <- 1 until 4) {
            if (line.startsWith("      (NSEI)... ") && line.contains("   (BVC)... ")) {
              logEntry += (s"nsei$i" -> (line.split("\\s+")(2).trim))
              logEntry += (s"bvc${i}_state" -> (flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line).trim))
            } else {
              logEntry += (s"nsei$i" -> "")
              logEntry += (s"bvc${i}_state" -> "")
            }
            line = txt.nextLine()
            linNum += 1
          }
          logEntries += logEntry
        }
      }
      linNum += 1
    }

    return logEntries.toList
  }

  def flexibsc__bts_seg_and_trx_parameters__zeqo_btsparameter_splitter(line: String): String = {
    var index = line.lastIndexOf(". ")
    line.splitAt(index + 1)._2

  }

  /**
    * flexibsc__bts_sw_lmu_and_abis_pool_data_txt_zewl_output
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewl_output(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZEWL;")) {
        txt.nextLine()
        line = txt.nextLine()
        linNum += 2
        if (line.contains("LOADING PROGRAM VERSION 21.1-0")) {
          txt.nextLine()
          line = txt.nextLine()
          linNum += 2
        }
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("bsc_name" -> ( line.split("\\s+")(1).trim))
        logEntry += ("date_time" -> ( line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))

        for (i <- 1 to skip) {
          line = txt.nextLine()
        }

        linNum += skip


        while (line.length != 0) {
          logEntry += ("build_id" -> ( line.substring(0, 18).trim))
          logEntry += ("type" -> ( line.substring(18, 25).trim))
          logEntry += ("rel_ver" -> ( line.substring(25, 37).trim))
          logEntry += ("initial" -> ( line.substring(37, 48).trim))
          logEntry += ("mf_name" -> ( line.substring(48, 63).trim))
          logEntry += ("subdir" -> ( line.substring(63, 75).trim))
          logEntry += ("conn" -> ( line.splitAt(75)._2.trim))
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
    * flexibsc__preprocessor_sw__zdpp_cls_preproinput
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__preprocessor_sw__zdpp_cls_preproinput(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1

    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZDPP:")) {
        for (i <- 1 to skip) {
          line = txt.nextLine()
        }
        linNum += skip
        if (line.startsWith("FlexiBSC")) {
          logEntry += ("line_number" -> linNum.toString)
          logEntry += ("bsc" -> ( line.split("\\s+")(1).trim))
          logEntry += ("date_time" -> ( line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))
          txt.nextLine()
          line = txt.nextLine()
          linNum += 2
          logEntry += ("cls" -> ( line.split("\\s+")(0).trim))
          logEntry += ("cls1" -> ( line.split("\\s+")(1).trim))

          for (i <- 1 to skip + 1) {
            line = txt.nextLine()
          }
          linNum += skip + 1

          logEntry += ("rom_sw" -> ( line.stripPrefix("ROM SW:").trim))
          line = txt.nextLine()
          linNum += 1
          logEntry += ("fpga_revision" -> ( line.stripPrefix("FPGA REVISION:").trim))
          txt.nextLine()
          line = txt.nextLine()
          linNum += 2
          logEntry += ("cpld_usercode" -> ( line.stripPrefix("CPLD USERCODE:").trim))
          logEntries += logEntry
        }
      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_bsc__adjacent_cell_data_check__zeat_adjaceny_check
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__adjacent_cell_data_check__zeat_adjaceny_check(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 7
    var logEntries = new ListBuffer[Map[String, String]]()

    var ls = List[Map[String, String]]()
    var bc = List[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var illegal_message = ""
      var same_bcch_message = ""

      var logEntry = Map[String, String]()
      if (line.contains("ZEAT;")) {

        while (!line.contains("BSC")) {
          line = txt.nextLine()
          linNum += 1
        }

        var bsc_dt = line.split("\\s+")
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bsc" -> ( bsc_dt(1).trim))
        logEntry += ("date_time" -> ( bsc_dt(2).trim + " " + bsc_dt(3).trim))
        txt.nextLine()
        line = txt.nextLine()
        linNum += 2
        if (line.startsWith("DUPLICATE")) {
          logEntry += ("duplicate_message" -> ( line.trim))
        }
        txt.nextLine()
        line = txt.nextLine()
        linNum += 2
        if (line.contains("ADJACENT CELL")) {
          while (!line.startsWith("====")) {
            line = txt.nextLine()
            linNum + 1
          }
          line = txt.nextLine()
          linNum += 1
          while (line.length != 0) {
            var value = line.split("\\s+")
            logEntry += ("seg_id" -> ( value(0).trim))
            logEntry += ("mcc_1" -> ( value(1).trim))
            logEntry += ("mnc_1" -> ( value(2).trim))
            logEntry += ("lac_1" -> ( value(3).trim))
            logEntry += ("ci_1" -> ( value(4).trim))
            logEntry += ("mcc_2" -> ( value(5).trim))
            logEntry += ("mnc_2" -> ( value(6).trim))
            logEntry += ("lac_2" -> ( value(7).trim))
            logEntry += ("ci_2" -> ( value(8).trim))
            line = txt.nextLine()
            linNum += 1
            logEntries += logEntry
          }
          line = txt.nextLine()
          linNum += 1
        } else {
          logEntry += ("seg_id" -> ( ""))
          logEntry += ("mcc_1" -> ( ""))
          logEntry += ("mnc_1" -> ( ""))
          logEntry += ("lac_1" -> ( ""))
          logEntry += ("ci_1" -> ( ""))
          logEntry += ("mcc_2" -> ( ""))
          logEntry += ("mnc_2" -> ( ""))
          logEntry += ("lac_2" -> ( ""))
          logEntry += ("ci_2" -> ( ""))
        }
        if (line.startsWith("SAME BCCH")) {
          same_bcch_message =  line.trim

        }
        txt.nextLine()
        line = txt.nextLine()
        linNum += 1
        if (line.contains("ADJACENT CELL")) {
          while (!line.startsWith("====")) {
            line = txt.nextLine()
            linNum + 1
          }
          line = txt.nextLine()
          linNum += 1
          while (line.length != 0) {
            var value = line.split("\\s+")
            logEntry += ("seg_id" -> ( value(0).trim))
            logEntry += ("mcc_1" -> ( value(1).trim))
            logEntry += ("mnc_1" -> ( value(2).trim))
            logEntry += ("lac_1" -> ( value(3).trim))
            logEntry += ("ci_1" -> ( value(4).trim))
            logEntry += ("mcc_2" -> ( ""))
            logEntry += ("mnc_2" -> ( ""))
            logEntry += ("lac_2" -> ( ""))
            logEntry += ("ci_2" -> ( ""))
            line = txt.nextLine()
            linNum += 1
            logEntries += logEntry
          }
        }

        line = txt.nextLine()
        linNum += 1

        if (line.startsWith("ILLEGAL")) {
          illegal_message =  line.trim
        }
        if (line.contains("ADJACENT CELL")) {
          while (!line.startsWith("====")) {
            line = txt.nextLine()
            linNum + 1
          }
          line = txt.nextLine()
          linNum += 1
          while (line.length != 0) {
            var value = line.split("\\s+")
            logEntry += ("seg_id" -> ( value(0).trim))
            logEntry += ("mcc_1" -> ( value(1).trim))
            logEntry += ("mnc_1" -> ( value(2).trim))
            logEntry += ("lac_1" -> ( value(3).trim))
            logEntry += ("ci_1" -> ( value(4).trim))
            line = txt.nextLine()
            linNum += 1
            logEntries += logEntry
          }
        }

        ls = List[Map[String, String]]()
        bc = logEntries.map { a =>
          val c = a + ("same_bcch_message" -> same_bcch_message, "illegal_message" -> illegal_message)
          c :: ls
        }.toList.flatMap(a => a)
      }
      linNum += 1
    }
    bc
  }

  /**
    * flexi_bsc____tsn_0911_swu_edgeport
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

 /* def flexi_bsc____tsn_0911_swu_edgeport(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("swu-") && line.contains("# sh running-config")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("swu_id" -> (line.stripPrefix("swu-").split("# sh running-config")(0).trim))
        while (!(line.contains("spanning-tree port 1/1/") && line.endsWith("edgeport enable"))) {
          line = txt.nextLine()
          linNum += 1
        }

        logEntry += ("low_port" -> (line.split("\\s+")(3).split("-")(0).stripPrefix("1/1/").trim))
        logEntry += ("high_port" -> (line.split("\\s+")(3).split("-")(1).stripPrefix("1/1/").trim))
        logEntries += logEntry

      }
      linNum += 1
    }

    return logEntries.toList
  }
*/
 def flexi_bsc____tsn_0911_swu_edgeport(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("swu-") && line.contains("# sh running-config")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("swu_id" -> (line.stripPrefix("swu-").split("# sh running-config")(0).trim))
        while (!(line.contains("spanning-tree port 1/1/") && line.endsWith("edgeport enable")) && txt.hasNext()) {
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
          linNum += 1
        }

        logEntry += ("low_port" -> (line.split("\\s+")(3).split("-")(0).stripPrefix("1/1/").trim))
        logEntry += ("high_port" -> (line.split("\\s+")(3).split("-")(1).stripPrefix("1/1/").trim))
        logEntries += logEntry

      }
      linNum += 1
    }

    return logEntries.toList
  }


  /**
    * flexi_bsc__bsc_alarm_history_txt__computer_logs   *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__bsc_alarm_history_txt__computer_logs(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("CALLER")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry ++= flexi_bsc____computer_logsFirstLine(line, linNum)
        linNum += 1
        logEntry ++= flexi_bsc____computer_logsSecondLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc____computer_logsThirdLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc____computer_logsFourthLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc____computer_logsFifthLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc____computer_logsSixthLine(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc____computer_logsSeventhLine(txt.nextLine, linNum)
        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList


  }


  def flexi_bsc____computer_logsFirstLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("caller" -> ( line.substring(12, 28)))
    logEntry
  }

  def flexi_bsc____computer_logsSecondLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("write_time" -> ( line.substring(12, 37)))
    logEntry
  }

  def flexi_bsc____computer_logsThirdLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("parameters" -> ( line.substring(12, 53)))
    logEntry
  }

  def flexi_bsc____computer_logsFourthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("user_text" -> ( line.splitAt(10)._2.trim))
    logEntry
  }

  def flexi_bsc____computer_logsFifthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("user_date" -> ( line.splitAt(10)._2.trim))
    logEntry
  }

  def flexi_bsc____computer_logsSixthLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("user_date1" -> ( line.splitAt(10)._2.trim))
    logEntry
  }

  def flexi_bsc____computer_logsSeventhLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    logEntry += ("user_date2" -> ( line.splitAt(10)._2.trim))
    logEntry
  }

  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeek_fbpp_status
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeek_fbpp_status(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("RNW PLAN DATABASE STATE")) {

        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("rnw_plan_database_state" -> ( line.splitAt(52)._2.trim))
        linNum += 1
        logEntry += ("rnw_configuration_id" -> ( txt.nextLine().splitAt(52)._2.trim))
        linNum += 1
        logEntry += ("rnw_plan_configuration_id" -> ( txt.nextLine().splitAt(52)._2.trim))
        linNum += 1
        logEntry += ("rnw_fallback_configuration_id" -> ( txt.nextLine().splitAt(52)._2.trim))
        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList


  }

  /**
    * flexi_bsc__finaletpaping__ip_ping_results
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__finaletpaping__ip_ping_results(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("--- ")) {

        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("ip" -> ( line.split("\\s")(1).trim))
        linNum += 1
        logEntry ++= flexi_bsc__finaletpaping__ip_ping_resultsSecondLine(txt.nextLine, linNum)

        logEntries += logEntry


      }
      linNum += 1
    }

    logEntries.toList


  }

  def flexi_bsc__finaletpaping__ip_ping_resultsSecondLine(line: String, lineNum: Int): mutable.Map[String, String] = {
    var logEntry = mutable.Map[String, String]()
    var split = line.split("\\s")
    split = line.trim.split("\\s+")
    logEntry += ("packets_transmitted" -> ( split(0).trim))
    logEntry += ("received" -> ( split(3).trim))
    logEntry += ("packet_loss" -> ( split(5).trim))
    logEntry += ("time" -> ( split(9).trim))
    logEntry
  }



  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_c_m_plane_ip
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_c_m_plane_ip(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("BTS C/U-PLANE IP ADDRESS AND SUBNET MASK LENGTH  ...........")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bts_cu_plane_ip_address" -> (line.stripPrefix("BTS C/U-PLANE IP ADDRESS AND SUBNET MASK LENGTH  ...........").split("/")(0).trim))
        logEntry += ("csubnet" -> (line.stripPrefix("BTS C/U-PLANE IP ADDRESS AND SUBNET MASK LENGTH  ...........").split("/")(1).trim))
        line=txt.nextLine()
        linNum += 1
        logEntry += ("bts_m_plane_ip_address" -> (line.stripPrefix("BTS M-PLANE IP ADDRESS AND SUBNET MASK LENGTH  .............").split("/")(0).trim))
        logEntry += ("msubnet" -> (line.stripPrefix("BTS M-PLANE IP ADDRESS AND SUBNET MASK LENGTH  .............").split("/")(1).trim))
        logEntries += logEntry

      }
      linNum += 1
    }

    logEntries.toList


  }

  /**
    *
    * flexi_bsc__boot_info__zwdi_bootimage
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__boot_info__zwdi_bootimage(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__boot_info__zwdi_bootimage")
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

      if (line.startsWith("ZWDI;")) {
        startlinelist += lineNum
      }
      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }
      if (line.contains("FUNCTIONAL   PLUG-IN UNIT   BOOT PACKAGE: FLASH MEMORY")) {
        secondlinelist += lineNum
      }
      lineNum += 1
    }
    for (startline <- startlinelist) {
      secondlineno = getClosestLineNumber(startline, secondlinelist.toList.sorted)
      endlineno = getClosestLineNumber(secondlineno, endlinelist.toList.sorted)
      for (linenno <- Range(secondlineno + 3, endlineno - 2, 2)) {
        val linecont = dataArray(linenno - 1)
        val linecont_1=dataArray(linenno)
        my_map += (
          "line_number" -> (startline.toString.trim),
          "FUNCTIONAL_UNIT" -> (linecont.substring(2, 14).trim),
          "PLUG_IN_UNIT_TYPE_INDEX" -> (linecont.substring(15, 29).trim),
          "BOOT_PACKAGE_FLASH_MEMORY" -> (linecont.substring(29, linecont.length).trim),
          "BOOT_PACKAGE_DISK_FILE" -> ((linecont_1).substring(29, (linecont_1).length - 1).trim)
        )
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(x => x.size > 1)
  }


  /**
    * ******************************************************
    * flexi_bsc__system_configuration__zwoi_pr_file
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def flexi_bsc__system_configuration__zwoi_pr_file(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__system_configuration__zwoi_pr_file")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0
    var endlinelist = ListBuffer[Int]()
    var startlinelist = ListBuffer[Int]()
    var secondlinelist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }

      if (line.contains("PARAMETER CLASS:")) {
        startlinelist += lineNum
      }

      if (line.contains("IDENTIFIER   NAME OF PARAMETER           VALUE  CHANGE POSSIBILITY")) {
        secondlinelist += lineNum
      }

      lineNum += 1
    }


    for (i <- startlinelist) {
      val paramdata = dataArray(i - 1)
      if (paramdata.toString.contains("PARAMETER CLASS:")) {
        endlineno = getNextClosestLineNumber(i + 2, endlinelist.toList.sorted)
        for (k <- i + 4 until endlineno) {
          val data = dataArray(k - 1)
          if (data.trim.length > 0) {
            my_map += (
              "line_number" -> i.toString.trim,
              "PARAMETER_CLASS" -> (paramdata.substring(17, 22)),
              "PARAMETER_NAME" -> (paramdata.substring(22, paramdata.length - 1)),
              "IDENTIFIER" -> (data.substring(0, 13)).replaceAll("""(?m)\s+$""", ""),
              "NAME_OF_PARAMETER" -> (data.substring(13, 39)).replaceAll("""(?m)\s+$""", ""),
              "VALUE" -> (data.substring(39, 51)).replaceAll("""(?m)\s+$""", ""),
              "CHANGE_POSSIBILITY" -> (data.substring(51, data.length - 1).replaceAll("""(?m)\s+$""", ""))
            )
          }
          else {
            my_map += (
              "line_number" -> i.toString.trim,
              "PARAMETER_CLASS" -> (paramdata.substring(17, 22)),
              "PARAMETER_NAME" -> (paramdata.substring(22, paramdata.length - 1)),
              "IDENTIFIER" -> "",
              "NAME_OF_PARAMETER" -> "",
              "VALUE" -> "",
              "CHANGE_POSSIBILITY" -> "")
          }
          list_map += my_map.toMap
        }
      }
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * flexi_bsc__computer_logs__zdde_computer_logs
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__computer_logs__zdde_computer_logs(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__computer_logs__zdde_computer_logs")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var startlinelist, endlinelist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    var endlinenumber: Int = 0

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.contains("CALLER    :")) {
        startlinelist += lineNum
      }
      if (line.trim.isEmpty) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (firstline <- startlinelist) {
      var firstlinedata, secondlinedata, thirdlinedata, fourthlinedata, fifthlinedata = ""
      endlinenumber = getClosestLineNumber(firstline, endlinelist.toList.sorted)
      firstlinedata = dataArray(firstline - 1).substring(12, Math.min(dataArray(firstline - 1).indexOf("RETURN ADDRESS"), dataArray(firstline - 1).length))
      secondlinedata = dataArray(firstline).substring(12, dataArray(firstline).length).trim
      thirdlinedata = dataArray(firstline + 1).substring(12, dataArray(firstline + 1).length).trim
      fourthlinedata = dataArray(firstline + 2).substring(12, dataArray(firstline + 2).length).trim
      for (lastline <- firstline + 4 until endlinenumber) {
        fifthlinedata += dataArray(lastline - 1).substring(12, dataArray(lastline - 1).length).trim
      }
      fifthlinedata =  fifthlinedata.trim
      my_map += "line_number" -> firstline.toString.trim
      my_map += "UNIT" -> (dataArray(0).split(":", -1)(1))
      my_map += "CALLER" -> firstlinedata
      my_map += "WRITE_TIME" -> secondlinedata
      my_map += "PARAMETERS" -> thirdlinedata
      my_map += "USER_TEXT" -> fourthlinedata
      my_map += "USER_DATA" -> fifthlinedata
      list_map += my_map.toMap
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    *
    * flexi_bsc__system_configuration__zwti_c
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    *
    */

  def flexi_bsc__system_configuration__zwti_c(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__system_configuration__zwti_c")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var startlinelist, secondlinelist, endlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    var secondlineno, endlineno: Int = 0

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }

      if (line.contains("ZWTI:C;")) {
        startlinelist += lineNum
      }

      if (line.contains("CART     HALL    LOC       P1        P2        P3        P4      AG    AL")) {
        secondlinelist += lineNum
      }
      lineNum += 1
    }

    for (startline <- startlinelist) {
      secondlineno = getClosestLineNumber(startline, secondlinelist.toList.sorted)
      endlineno = getClosestLineNumber(secondlineno, endlinelist.toList.sorted)
      for (k <- secondlineno + 2 until endlineno) {
        val tabledata = dataArray(k - 1)
        my_map += (
          "line_number" -> startline.toString.trim,
          "CART" -> (tabledata.substring(Math.min(0, tabledata.length), Math.min(9, tabledata.length)).trim),
          "HALL" -> (tabledata.substring(Math.min(9, tabledata.length), Math.min(15, tabledata.length)).trim),
          "LOC" -> (tabledata.substring(Math.min(15, tabledata.length), Math.min(25, tabledata.length)).trim),
          "P1" -> (tabledata.substring(Math.min(25, tabledata.length), Math.min(34, tabledata.length)).trim),
          "P2" -> (tabledata.substring(Math.min(34, tabledata.length), Math.min(44, tabledata.length)).trim),
          "P3" -> (tabledata.substring(Math.min(44, tabledata.length), Math.min(54, tabledata.length)).trim),
          "P4" -> (tabledata.substring(Math.min(54, tabledata.length), Math.min(65, tabledata.length)).trim),
          "AG" -> (tabledata.substring(Math.min(65, tabledata.length), Math.min(69, tabledata.length)).trim),
          "AL" -> (tabledata.substring(Math.min(69, tabledata.length), Math.min(tabledata.length, tabledata.length)).trim)
        )
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * ******************************************************
    * flexi_bsc__sw_configuration__zwqo_run_bscpack
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def flexi_bsc__sw_configuration__zwqo_run_bscpack(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__sw_configuration__zwqo_run_bscpack")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var secondlineno, thirdlineno, endlineno: Int = 0

    var endlinelist, startlinelist, secondlinelist, thirdlinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("ZWQO:RUN;")) {
        startlinelist += lineNum
      }
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }
      if (line.contains("WORKING PACKAGES IN UNITS PAGE")) {
        secondlinelist += lineNum
      }
      if (line.contains(" MBA UNIT         NAME       STATUS  PACKAGE_ID     (REP-ID)")) {
        thirdlinelist += lineNum
      }
      lineNum += 1
    }
    for (startline <- secondlinelist) {
      val linedata = dataArray(startline - 1)
      firstlinetext = linedata.substring(31, linedata.length).trim
      secondlineno = getClosestLineNumber(startline, thirdlinelist.toList.sorted)
      thirdlineno = secondlineno + 1
      endlineno = getClosestLineNumber(thirdlineno, endlinelist.toList.sorted)

      for (linenno <- (thirdlineno + 1).until(endlineno)) {
        val tabledata = dataArray(linenno - 1)
        if (tabledata.trim.length != 0 && !tabledata.contains("ERROR")) {
          my_map += (
            "line_number" -> startline.toString.trim,
            "WORKING_PACKAGES_IN_UNITS_PAGE" -> firstlinetext,
            "MBA" -> (tabledata.substring(0, 5)),
            "UNIT" -> (tabledata.substring(5, 18)),
            "NAME" -> (tabledata.substring(18, 27)),
            "STATUS" -> (tabledata.substring(27, 34)),
            "PACKAGE_ID" -> (tabledata.substring(34, 53)),
            "REP_ID" -> (tabledata.substring(53, tabledata.length - 1))
          )
        }
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)
  }
  /*
     *******************************************************
     * flexi_bsc__bts_seg_and_trx_parameters__zeqo_basic1
     * @author Rakesh Adhikari
     * @param logfilecontent
     * @return
     ****************************************************
     */

  def flexi_bsc__bts_seg_and_trx_parameters__zeqo_basic1(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__bts_seg_and_trx_parameters__zeqo_basic1")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, thirdlineno, endlineno, lastlineno: Int = 0
    var endlinelist, startlinelist, secondlinelist, thirdlinelist, dashlinelist, commandexeclist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("BCF-") && line.contains("BTS-")) {
        secondlinelist += lineNum
      }

      if (line.equals("================")) {
        startlinelist += lineNum
      }

      if (line.contains("------")) {
        dashlinelist += lineNum
      }

      if (line.contains("COMMAND EXECUTED")) {
        commandexeclist += lineNum
      }

      lineNum += 1
    }
    for (startline <- startlinelist) {
      endlineno = getClosestLineNumber(startline, startlinelist.toList.sorted)
      secondlineno = getClosestLineNumber(startline, secondlinelist.toList.sorted)

      if (endlineno == 0) {
        lastlineno = getClosestLineNumber(startline, commandexeclist.toList.sorted)
      }
      else lastlineno = endlineno

      val secondlinedata = dataArray(secondlineno - 1)
      val tablelineno = getClosestLineNumber(secondlineno, dashlinelist.toList.sorted) + 1

      for (linenno <- tablelineno to (lastlineno - 1)) {
        var my_map = mutable.Map[String, String]()
        val tabledata = dataArray(linenno - 1)
        if (tabledata.trim.length != 0 && tabledata.contains("....")) {
          var paramname, shortname, paramvalue = ""

          if (tabledata.contains("(") && tabledata.contains(")")) {
            paramname = tabledata.substring(0, tabledata.indexOf("(")).replace(".", "").trim
            shortname = tabledata.substring(tabledata.indexOf("(") + 1, tabledata.lastIndexOf(")")).replace(".", "").trim
            paramvalue = tabledata.substring(tabledata.lastIndexOf(".") + 1, tabledata.length).replace(".", "").trim
          }
          else {
            paramname = tabledata.substring(0, tabledata.indexOf(".")).replace(".", "").trim
            shortname = ""
            paramvalue = tabledata.substring(tabledata.lastIndexOf(".") + 1, tabledata.length).replace(".", "").trim
          }

          my_map += (
            "line_number" -> (startline.toString.trim),
            "BCFNAME" -> (secondlinedata.substring(0, 11)).trim,
            "BTSNAME" -> (secondlinedata.substring(11, 21)).trim,
            "BSCNAME" -> (secondlinedata.substring(21, secondlinedata.length)).trim,
            "PARAMNAME" -> paramname,
            "SHORTNAME" -> shortname,
            "PARAMVALUE" -> paramvalue
          )
        }
        list_map += my_map.toMap
      }

    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    *
    * flexi_bsc__BSC_Alarm_history_txt__bsc_alarm
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    *
    */

  /*def flexi_bsc__bsc_alarm_history_txt__bsc_alarm(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__bsc_alarm_history_txt__bsc_alarm")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var startlinelist, endlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    var firstlinedata, secondlinedata, thirdlinedata, fourthlinedata = ""

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }

      if (line.contains("<HIST>")) {
        startlinelist += lineNum
      }

      endlinelist = startlinelist.map(_ + 3)
      lineNum += 1
    }

    for (startline <- startlinelist) {
      firstlinedata = dataArray(startline - 1)
      secondlinedata = dataArray(startline)
      thirdlinedata = dataArray(startline + 1)
      fourthlinedata = dataArray(startline + 2)
      my_map += (
        "line_number" -> (startline.toString.trim),
        "BSC" -> (firstlinedata.substring(11, 22).trim),
        "UNIT" -> (firstlinedata.substring(23, 42).trim),
        "ALARM_TYPE" -> (firstlinedata.substring(43, 54).trim),
        "DTTM" -> (firstlinedata.substring(56, firstlinedata.length).trim),
        "SEVERITY" -> (secondlinedata.substring(0, 4)),
        "NOT_TYPE" -> (secondlinedata.substring(4, 11)),
        "PARAM1" -> (secondlinedata.substring(11, 22)),
        "PARAM2" -> (secondlinedata.substring(22, secondlinedata.length - 1)),
        "TRANS_ID" -> (thirdlinedata.substring(0, 11).trim),
        "ALARM_ID" -> (thirdlinedata.substring(11, 16).trim),
        "ALARM_TEXT" -> (thirdlinedata.substring(16, secondlinedata.length - 1).trim),
        "ALARM_SUP" -> (fourthlinedata).trim
      )
      list_map += my_map.toMap
    }
    list_map.toList.filter(x => x.size > 1)
  }
*/
def flexi_bsc__bsc_alarm_history_txt__bsc_alarm(logfilecontent: String): List[Map[String, String]]={
    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = mutable.ListBuffer[Map[String, String]]()
    val flexi_bsc__bsc_alarm_history_txt__bsc_alarm_first_line = (line: String, lineNum: Int) => {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("line_number" -> (lineNum.toString.trim))
      logEntry += ("BSC" -> (line.substring(11, 22).trim))
      logEntry += ("UNIT" -> (line.substring(23, 42).trim))
      logEntry += ("ALARM_TYPE" -> (line.substring(43, 54).trim))
      logEntry += ("DTTM" -> (line.substring(56, line.length).trim))
      logEntry

    }
    val flexi_bsc__bsc_alarm_history_txt__bsc_alarm_second_line = (line: String, _: Int) => {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("SEVERITY" -> (line.substring(0, 4)))
      logEntry += ("NOT_TYPE" -> (line.substring(4, 11)))
      logEntry += ("PARAM1" -> (line.substring(11, 22)))
      logEntry += ("PARAM2" -> (line.substring(22, line.length - 1)))
      logEntry
    }
    val flexi_bsc__bsc_alarm_history_txt__bsc_alarm_third_line = (line: String, _: Int) => {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("TRANS_ID" -> (line.substring(0, 11).trim))
      logEntry += ("ALARM_ID" -> (line.substring(11, 16).trim))
      logEntry += ("ALARM_TEXT" -> (line.substring(16, line.length - 1).trim))
      logEntry
    }
    val flexi_bsc__bsc_alarm_history_txt__bsc_alarm_fourth_line = (line: String, _: Int) => {
      var logEntry = mutable.Map[String, String]()
      logEntry += ("ALARM_SUP" -> line.trim)
      logEntry
    }
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = mutable.Map[String, String]()
      if (line.contains("<HIST>")) {

        logEntry ++= flexi_bsc__bsc_alarm_history_txt__bsc_alarm_first_line(line, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarm_history_txt__bsc_alarm_second_line(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarm_history_txt__bsc_alarm_third_line(txt.nextLine, linNum)
        linNum += 1
        logEntry ++= flexi_bsc__bsc_alarm_history_txt__bsc_alarm_fourth_line(txt.nextLine, linNum)
        logEntries += logEntry.toMap

      }
      linNum += 1
    }
    logEntries.toList
  }


  /**
    * ******************************************************
    * flexi_bsc__system_configuration__zwti_p_configuration
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def flexi_bsc__system_configuration__zwti_p_configuration(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__system_configuration__zwti_p_configuration")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, dashlineno, endlineno, lastlineno, tableendline: Int = 0
    var endlinelist, firstlinelist, startlinelist, secondlinelist, dashlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()
    var cardName= ""
    var cardIndex= ""
    var trackNum= ""

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.contains("ZWTI:P;")) {
        firstlinelist += lineNum
      }

      if (line.contains("IN LOC") && (line.split("\\s+").filter(_.trim.length > 0).size == 4) &&
        ((line.split("\\s+").filter(_.trim.length > 0)(1) == "IN" && line.split("\\s+").filter(_.trim.length > 0)(2) == "LOC"))) {
        startlinelist += lineNum
      }
      if (line.contains("TRACK:")) {
        secondlinelist += lineNum
      }
      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }

      if (line.trim.size == 0) {
        emptylinelist += lineNum
      }
      lineNum += 1
    }
    for (firstline <- firstlinelist) {
      var my_map = mutable.Map[String, String]()
      endlineno = getClosestLineNumber(firstline, endlinelist.toList.sorted)
      val completetablelist = startlinelist.filter(x => x > firstline && x < endlineno)
      for (startline <- completetablelist) {
        val loopstartlist = secondlinelist.filter(x => x > startline && x < endlineno)
        for (tableloop <- loopstartlist) {
          val loopend = Math.min(getClosestLineNumber(tableloop, emptylinelist.toList.sorted), getClosestLineNumber(tableloop, endlinelist.toList.sorted))
          for (i <- tableloop to loopend - 1) {
            my_map += ("line_number" -> (startline.toString.trim))
            val tablefirstline = dataArray(i - 1).split("\\s+").filter(_.trim.size > 0)
            my_map += (
              "UNIT" -> (dataArray(startline - 1).split("\\s+").filter(_.trim.length > 0)(0).trim),
              "Param1" -> (dataArray(startline - 1).split("\\s+").filter(_.trim.length > 0)(3).trim)
            )
            if (tablefirstline.size == 4 && tablefirstline(2).equals("TRACK:")) {
              cardName = (dataArray(i - 1).split("\\s+").filter(_.trim.size > 0)(0).trim)
              cardIndex = (dataArray(i - 1).split("\\s+").filter(_.trim.size > 0)(1).trim)
              trackNum = (dataArray(i - 1).split("\\s+").filter(_.trim.size > 0)(3).trim)
            }
            my_map += (
              "CARD_NAME" -> cardName,
              "CARD_INDEX" ->cardIndex,
              "TRACK_NO" -> trackNum
            )

            if (dataArray(i - 1).length > 31 && !dataArray(i - 1).contains("CONNECTOR SIDE:") &&
              (dataArray(i - 1).contains("PCM:") || dataArray(i - 1).substring(0, 37).trim.size == 0) &&
              (!dataArray(i - 1).contains("INT:") || (!dataArray(i - 1).contains("SW:")))) {
              my_map += (
                "PCU_CARD_INTERNAL_PCM_NAME" -> (dataArray(i - 1).substring(0, 18).trim),
                "PCM_TYPE" -> (dataArray(i - 1).substring(19, 24).trim),
                "PCM_NO" -> (dataArray(i - 1).substring(28, 36).trim)
              )
            }
            else {
              my_map += (

                "PCU_CARD_INTERNAL_PCM_NAME" -> (""),
                "PCM_TYPE" -> (""),
                "PCM_NO" -> ("")
              )
            }
            list_map += my_map.toMap
          }
        }
      }
    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    * flexi_bsc__cell_broadcast_status__zecp_cell_broadcast_msg
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__cell_broadcast_status__zecp_cell_broadcast_msg(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__cell_broadcast_status__zecp_cell_broadcast_msg")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var startlinelist, endlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    var endlinenumber: Int = 0

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }

      if (line.contains("ZECP;")) {
        startlinelist += lineNum
      }
      lineNum += 1
    }

    for (startline <- startlinelist) {
      endlinenumber = getClosestLineNumber(startline, endlinelist.toList.sorted)
      for (tableline <- (startline + 10).until(endlinenumber)) {
        val linedata = dataArray(tableline - 1)
        if (!linedata.contains("NO CELL BROADCAST MESSAGES EXIST") && linedata.trim.nonEmpty) {
          my_map += (
            "line_number" -> (startline.toString.trim),
            "MSG_INDEX" -> (linedata.substring(Math.min(0, linedata.length), Math.min(7, linedata.length))).trim,
            "MSG_ID" -> (linedata.substring(Math.min(7, linedata.length), Math.min(13, linedata.length))).trim,
            "MSG_CODE" -> (linedata.substring(Math.min(13, linedata.length), Math.min(18, linedata.length))).trim,
            "MSG_INFO" -> (linedata.substring(Math.min(18, linedata.length), Math.min(37, linedata.length))).trim,
            "REP_RATE" -> (linedata.substring(Math.min(37, linedata.length), Math.min(42, linedata.length))).trim,
            "GEP_SCOPE" -> (linedata.substring(Math.min(42, linedata.length), Math.min(59, linedata.length))).trim,
            "CODING_GROUP" -> (linedata.substring(Math.min(59, linedata.length), Math.min(64, linedata.length))).trim,
            "MC" -> (linedata.substring(Math.min(64, linedata.length), Math.min(68, linedata.length))).trim,
            "LANG_ALPH" -> (linedata.substring(Math.min(68, linedata.length), linedata.length)).trim
          )
          list_map += my_map.toMap
        }
      }

    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * ******************************************************
    * flexi_bsc__etxx_configuration__zwti_pp_etphotlink
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def flexi_bsc__etxx_configuration__zwti_pp_etphotlink(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__etxx_configuration__zwti_pp_etphotlink")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, dashlineno, endlineno, lastlineno, tableendline: Int = 0
    var endlinelist, startlinelist, secondlinelist, dashlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNextLine) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("EXECUTION STARTED")) {
        secondlinelist += lineNum
      }

      if (line.contains("ZWTI:PP;")) {
        startlinelist += lineNum
      }

      if (line.trim.size == 0) {
        emptylinelist += lineNum
      }

      if (line.contains("-------------- ----------  ---------")) {
        dashlinelist += lineNum
      }

      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }

      lineNum += 1
    }

    for (startline <- startlinelist) {
      secondlineno = getClosestLineNumber(startline, secondlinelist.toList.sorted)
      dashlineno = getClosestLineNumber(secondlineno, dashlinelist.toList.sorted)

      endlineno = getClosestLineNumber(dashlineno, emptylinelist.toList.sorted)

      for (linenno <- (dashlineno + 1) to (endlineno - 1)) {
        var my_map = mutable.Map[String, String]()
        val tabledata = dataArray(linenno - 1)
        if (tabledata.trim.length != 0) {
          my_map += (
            "line_number" -> linenno.toString.trim,
            "UNIT" -> (tabledata.substring(3, 16)).trim,
            "PIU" -> (tabledata.substring(16, 27)).trim,
            "PORT" -> (tabledata.substring(27, 46)).trim,
            "UNIT_1" -> (tabledata.substring(46, 59)).trim,
            "PIU_1" -> (tabledata.substring(59, 70)).trim,
            "PORT_1" -> (tabledata.substring(70, tabledata.length).trim)
          )
        }
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    * ******************************************************
    * flexi_bsc__etxx_configuration__zesl_pcu_pcuonetp
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def flexi_bsc__etxx_configuration__zesl_pcu_pcuonetp(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__etxx_configuration__zesl_pcu_pcuonetp")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, dashlineno, endlineno, thirdlineno, tableendline: Int = 0
    var endlinelist, startlinelist, secondlinelist, thirdlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNextLine) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("ETP GROUP ID ...")) {
        secondlinelist += lineNum
      }

      if (line.contains("ETP TYPE .......")) {
        startlinelist += lineNum
      }

      if (line.trim.size == 0) {
        emptylinelist += lineNum
      }

      if (line.contains("BCSU      PCU")) {
        thirdlinelist += lineNum
      }

      lineNum += 1
    }

    for (startline <- startlinelist) {
      secondlineno = getClosestLineNumber(startline, secondlinelist.toList.sorted)
      thirdlineno = getClosestLineNumber(secondlineno, thirdlinelist.toList.sorted)
      endlineno = getClosestLineNumber(thirdlineno, emptylinelist.toList.sorted)



      for (linenno <- (thirdlineno + 2) to (endlineno - 1)) {
        var my_map = mutable.Map[String, String]()
        val firstlinedata = dataArray(startline - 1)
        val secondlinedata = dataArray(secondlineno - 1)
        val tabledata = dataArray(linenno - 1)
        my_map += (
          "line_number"->startline.toString.trim,
          "ETPx" -> ( firstlinedata.substring(firstlinedata.lastIndexOf(".") + 1, firstlinedata.length).trim),
          "ETP_INDEX" -> ( secondlinedata.substring(secondlinedata.lastIndexOf(".") + 1, secondlinedata.length).trim),
          "BCSU" -> ( tabledata.substring(0, 11).trim),
          "PCU" -> ( tabledata.substring(11, tabledata.length).trim)
        )
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)
  }
  /**
    * ******************************************************
    * flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_p_
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_p_(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_p_")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, dashlineno, endlineno, thirdlineno, tableendline: Int = 0
    var endlinelist, startlinelist, secondlinelist, thirdlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNextLine) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("ZDOI:OMU&MCMU&BCSU:P;")) {
        startlinelist += lineNum
      }

      if (line.trim.size == 0) {
        emptylinelist += lineNum
      }


      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }

      lineNum += 1
    }

    for (startline <- startlinelist) {
      var my_map = mutable.Map[String, String]()
      var tablestartlist = ListBuffer[Int]()
      var endlineno = getClosestLineNumber(startline, endlinelist.toList.sorted)

      for (i <- startline to endlineno - 1) {
        if (dataArray(i - 1).contains("UNIT:")) {
          tablestartlist += i
        }
      }

      for (tablestartline <- tablestartlist) {
        my_map += (
          "line_number"->startline.toString.trim(),
          "UNIT" -> ( dataArray(tablestartline - 1).substring(dataArray(tablestartline - 1).indexOf(":") + 1, dataArray(tablestartline - 1).length).trim),
          "TIME_USAGE_ALLOWED" -> (dataArray(tablestartline + 2).substring(dataArray(tablestartline + 2).indexOf(":") + 1, dataArray(tablestartline + 2).length).trim),
          "LOAD_ALLOWED" ->  (dataArray(tablestartline + 4).substring(dataArray(tablestartline + 4).indexOf(":") + 1, dataArray(tablestartline + 4).length).trim),
          "LOAD_PERCENT" ->  (dataArray(tablestartline + 5).substring(dataArray(tablestartline + 5).indexOf(":") + 1, dataArray(tablestartline + 5).length).trim),
          "CLASS_FOR_CRRQ" ->  (dataArray(tablestartline + 6).substring(dataArray(tablestartline + 6).indexOf(":") + 1, dataArray(tablestartline + 6).length).trim),
          "CLOCK_FREQUENCY_MHZ" -> (dataArray(tablestartline + 7).substring(dataArray(tablestartline + 7).indexOf(":") + 1, dataArray(tablestartline + 7).length).trim)
        )
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(_.size > 1)
  }
  /**
    * flexi_bsc__unit_information__zusi_unitinfo
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */

  /*def flexi_bsc__unit_information__zusi_unitinfo(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__unit_information__zusi_unitinfo")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, thirdlineno, endlineno: Int = 0

    var endlinelist, startlinelist, secondlinelist, thirdlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }

      if (line.contains("INTERFACE  STATE MTU   ATTR     IP ADDRESS")) {
        secondlinelist += lineNum
      }

      if (line.trim.length == 0) {
        emptylinelist += lineNum
      }

      if (line.startsWith("BCSU")) {
        thirdlinelist += lineNum
      }

      startlinelist = secondlinelist.map(_ - 5)
      lineNum += 1
    }


    for (startline <- startlinelist) {
      val firstline = dataArray(startline - 1)
      val lastline = getClosestLineNumber(startline, endlinelist.toList.sorted)
      val requiredtableline = thirdlinelist.filter(x => x > startline && x < lastline)
      for (lineno <- requiredtableline) {
        val tableendline = getClosestLineNumber(lineno, emptylinelist.toList.sorted)
        for (i <- lineno + 1 to tableendline - 1) {
          val linedata = dataArray(i - 1)
          if (!linedata.contains("->EL") && (linedata.contains("EL") || linedata.contains("VLAN"))) {
            val tabledata = linedata
            val ipdata = tabledata.substring(Math.min(30, tabledata.length), tabledata.length).trim
            my_map += (
              "line_number"->i.toString.trim,
              "BCSU" -> (firstline.substring(Math.min(10, firstline.length), Math.min(27, firstline.length))),
              "VLAN" -> (tabledata.substring(0, Math.min(12, tabledata.length)).trim),
              "STATE" -> (tabledata.substring(Math.min(12, tabledata.length), Math.min(18, tabledata.length)).trim),
              "MTU" -> (tabledata.substring(Math.min(18, tabledata.length), Math.min(25, tabledata.length)).trim),
              "Attr" -> (tabledata.substring(Math.min(25, tabledata.length), Math.min(30, tabledata.length)).trim)
            )
            if (ipdata.length > 0) {
              my_map += (
                "IP_ADDR" -> (ipdata.split("/", -1)(0)).replace("(", "").replace(")", ""),
                "Subnet" -> (ipdata.split("/", -1)(1)).replace("(", "").replace(")", "")
              )
            }
            else {
              my_map += (
                "IP_ADDR" -> (""),
                "Subnet" -> ("")
              )
            }
          }
          list_map += my_map.toMap
        }
      }
    }
    list_map.toList.filter(_.size > 1)
  }*/

  def flexi_bsc__unit_information__zusi_unitinfo(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__unit_information__zusi_unitinfo")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, thirdlineno, endlineno: Int = 0

    var endlinelist, startlinelist, secondlinelist, thirdlinelist, emptylinelist,fourthlinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("ZUSI;")) {
        startlinelist += lineNum
      }

      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }

      if (line.contains("BSC")) {
        secondlinelist += lineNum
      }

      if (line.contains("UNIT       PHYS STATE LOCATION              INFO")){
        fourthlinelist+=lineNum
      }

      if (line.contains("WORKING STATE OF UNITS")) {
        thirdlinelist += lineNum
      }

      lineNum += 1
    }

    for (startline <- startlinelist) {
      val firstline = getClosestLineNumber(startline, secondlinelist.toList.sorted)
      val firstlinedata=dataArray(firstline-1)

      val tablestartline = getClosestLineNumber(startline, fourthlinelist.toList.sorted)
      val tableendline = getClosestLineNumber(tablestartline, endlinelist.toList.sorted)
        for (i <- tablestartline + 1 to tableendline - 1) {
          val linedata = dataArray(i-1)
            my_map += (
              "line_number"->startline.toString.trim,
              "BSC"->(firstlinedata.substring(0, Math.min(10, firstlinedata.length))).trim,
              "DTTM"->(firstlinedata.substring(Math.min(26, firstlinedata.length), Math.min(57, firstlinedata.length))),
              "UNIT" -> (linedata.substring(Math.min(0, linedata.length), Math.min(12, linedata.length))),
              "PHYS" -> (linedata.substring(Math.min(12, linedata.length), Math.min(17, linedata.length))),
              "STATE" -> (linedata.substring(Math.min(17, linedata.length), Math.min(23, linedata.length)).trim),
              "LOCATION" -> (linedata.substring(Math.min(23, linedata.length), Math.min(45, linedata.length)).trim),
              "INFO" -> (linedata.substring(Math.min(45, linedata.length),linedata.length).trim)
            )
          list_map += my_map.toMap
        }
    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    *
    * flexi_bsc__system_configuration__zwos_pr_file
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    *
    */

  def flexi_bsc__system_configuration__zwos_pr_file(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__system_configuration__zwos_pr_file")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0
    var startlinelist, secondlinelist, endlinelist = ListBuffer[Int]()
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }

     /* if (line.contains("PARAMETER CLASS:")) {
        startlinelist += lineNum
      }*/
      if (line.contains("IDENTIFIER   NAME OF PARAMETER           ACTIVATION STATUS")) {
        secondlinelist += lineNum
      }

      startlinelist=secondlinelist.map(_-2)

      lineNum += 1
    }

    for (i <- startlinelist) {
      val paramdata = dataArray(i - 1)
      val secondline = getClosestLineNumber(i, secondlinelist.toList.sorted)
      if (paramdata.toString.contains("PARAMETER CLASS:")) {
        endlineno = getNextClosestLineNumber(secondline, endlinelist.toList.sorted)
        for (k <- secondline + 2 to endlineno) {
          val data = dataArray(k - 1)
          my_map += (
            "line_number"->i.toString.trim,
            "PARAMETER_CLASS" -> (paramdata.substring(17, 22)),
            "PARAMETER_NAME" -> (paramdata.substring(22, paramdata.length - 1)),
            "IDENTIFIER" -> (data.substring(0, Math.min(13, data.length))),
            "NAME_OF_PARAMETER" ->(data.substring(Math.min(13, data.length), Math.min(38, data.length))),
            "ACTIVATION_STATUS" -> (data.substring(Math.min(38, data.length), data.length))
          )
          list_map += my_map.toMap
        }
      }
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * flexi_bsc__database_status__zdbs_dbstatecheck
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__database_status__zdbs_dbstatecheck(logfilecontent: String): List[Map[String, String]] = {
    println("flexi_bsc__database_status__zdbs_dbstatecheck")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var startlinelist, endlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    var firstlinedata, secondlinedata, thirdlinedata, fourthlinedata, tabledata = ""
    var tablefirstlineno, tableendlineno: Int = 0

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line
      if (line.contains("LOGBUF ID")) {
        endlinelist += lineNum
      }

      if (line.contains("ZDBS:")) {
        startlinelist += lineNum
      }
      lineNum += 1
    }


    for (startline <- startlinelist) {
      tablefirstlineno = startline + 14
      tableendlineno = getClosestLineNumber(tablefirstlineno, endlinelist.toList.sorted)
      for (linetoparse <- tablefirstlineno until tableendlineno) {
        tabledata = dataArray(linetoparse - 1)
        if (tabledata.trim.length != 0) {
          firstlinedata = dataArray(startline + 3)
          secondlinedata = dataArray(startline + 7)
          thirdlinedata = dataArray(startline + 9)
          fourthlinedata = dataArray(startline + 5)

          my_map += (
            "line_number"->startline.toString.trim,
            "BSC_NAME" -> (firstlinedata.substring(Math.min(9, firstlinedata.length), Math.min(23, firstlinedata.length)).trim),
            "DTTM" -> (firstlinedata.substring(Math.min(23, firstlinedata.length), firstlinedata.length).trim),

            "WO_DATABASE" -> (secondlinedata.substring(Math.min(0, secondlinedata.length), Math.min(7, secondlinedata.length)).trim),
            "WO_OCCURANCE" -> (secondlinedata.substring(Math.min(10, secondlinedata.length), Math.min(22, secondlinedata.length)).trim),
            "WO" -> (secondlinedata.substring(Math.min(22, secondlinedata.length), Math.min(42, secondlinedata.length)).trim),
            "WO_STATE" -> (secondlinedata.substring(Math.min(42, secondlinedata.length), Math.min(58, secondlinedata.length)).trim),
            "WO_SUBSTATE" -> (secondlinedata.substring(Math.min(58, secondlinedata.length), Math.min(secondlinedata.length, secondlinedata.length)).trim),

            "SP_DATABASE" -> (thirdlinedata.substring(Math.min(0, thirdlinedata.length), Math.min(7, thirdlinedata.length)).trim),
            "SP_OCCURANCE" ->(thirdlinedata.substring(Math.min(10, thirdlinedata.length), Math.min(22, thirdlinedata.length)).trim),
            "SP" ->(thirdlinedata.substring(Math.min(22, thirdlinedata.length), Math.min(42, thirdlinedata.length)).trim),
            "SP_STATE" -> (thirdlinedata.substring(Math.min(42, thirdlinedata.length), Math.min(58, thirdlinedata.length)).trim),
            "SP_SUBSTATE" ->(thirdlinedata.substring(Math.min(58, thirdlinedata.length), Math.min(thirdlinedata.length, thirdlinedata.length)).trim),

            "MEMORY_UPDATES_PREVENTED" -> (tabledata.substring(Math.min(0, tabledata.length), Math.min(11, tabledata.length)).trim),
            "DISK_UPDATES_PREV" -> (tabledata.substring(Math.min(19, tabledata.length), Math.min(37, tabledata.length)).trim),
            "TRANSACTION_OVER_FLOW" -> (tabledata.substring(Math.min(37, tabledata.length), Math.min(55, tabledata.length)).trim),
            "BACKUP_IS_ON" -> (tabledata.substring(Math.min(56, tabledata.length), Math.min(tabledata.length, tabledata.length)).trim)
          )
        }
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * ******************************************************
    * flexi_bsc__supplementary_ss7_network__zoyo
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def flexi_bsc__supplementary_ss7_network__zoyo(logfilecontent: String): List[Map[String, String]] = {
    println("Executing flexi_bsc__supplementary_ss7_network__zoyo")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, dashlineno, endlineno, thirdlineno, tableendline: Int = 0
    var endlinelist, startlinelist, secondlinelist, thirdlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNextLine) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("PARAMETER SET NAME:") && line.contains("PARAMETER SET NUMBER:")) {
        startlinelist += lineNum
      }

      if (line.trim.size == 0) {
        emptylinelist += lineNum
      }

      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }

      lineNum += 1
    }

    for (startline <- startlinelist) {
      var my_map = mutable.Map[String, String]()
      endlineno = getClosestLineNumber(startline, startlinelist.toList.sorted)
      if (endlineno == 0) tableendline = getClosestLineNumber(startline, endlinelist.toList.sorted)
      else tableendline = endlineno
      for (linenno <- startline to (tableendline - 2)) {
        my_map += ("line_number"->linenno.toString.trim)
        val linedata = dataArray(linenno - 1)
        if (linedata.contains("PARAMETER SET NAME") && linedata.contains("PARAMETER SET NUMBER")) {
          my_map += (
            "ParameterSetName" -> ( linedata.substring(linedata.indexOf(":") + 1, 39).trim),
            "SET" -> ( linedata.substring(linedata.lastIndexOf(":") + 1, linedata.length).trim)
          )
        }
        if (linedata.contains("RTO.MIN:")) {
          my_map += (
            "RTOMIN" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
        if (linedata.contains("RTO.MAX:")) {
          my_map += (
            "RTOMAX" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
        if (linedata.contains("RTO.INIT:")) {
          my_map += (
            "RTOINIT" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
        if (linedata.contains("HB.INTERVAL:")) {
          my_map += (
            "HBInterval" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
        if (linedata.contains("SACK.PERIOD:")) {
          my_map += (
            "SackPeriod" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
        if (linedata.contains("PATH.MAX.RETRANS:")) {
          my_map += (
            "PathMaxRET" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
        if (linedata.contains("ASSOCIATION.MAX.RETRANS:")) {
          my_map += (
            "ASSMAXRET" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
        if (linedata.contains("CHECKSUM:")) {
          my_map += (
            "CHECKSUM" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
        if (linedata.contains("BUNDLING:")) {
          my_map += (
            "BUNDLING" -> ( linedata.substring(linedata.indexOf(":") + 1, linedata.length).trim)
            )
        }
      }
      list_map += my_map.toMap
    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeeo
    *
    * @author Anudeep Purwar
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeeo(logfilecontent: String): List[Map[String, String]] = {
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var data_map = mutable.Map[String, String]()
    var line_number = 1
    var counter = 0
    val txt = new Scanner(logfilecontent)
    while (txt.hasNext) {
      val line = txt.nextLine()
      line match {
        case x if x.contains("ZEEO:ALL") => if(counter > 0) {list_map += data_map.toMap}; data_map = mutable.Map[String, String]("BSC"->"","DTTM"->"","NPC"->"","GMAC"->"","DMAC"->"","GMIC"->"","DMIC"->"","DISB"->"","TIM"->"","EEF"->"","EPF"->"","EOF"->"","Param"->"","HRI"->"","HRL"->"","HRU"->"","AUT"->"","ALT"->"","AML"->"","ACH"->"","IAC"->"","SAL"->"","ASG"->"","CSD"->"","CSU"->"","TGT"->"","HDL"->"","HUL"->"","CLR"->"","TTSAP"->"","PTMP"->"","SAPT"->"","CTR"->"","CTC"->"","MINHTT"->"","MAXHTT"->"","MAXHTS"->"","TCHFR"->"","SCHFR"->"","CNGT"->"","CNGS"->"","CS"->"","CSR"->"","PRDMHT"->"","PRDMHS"->"","PRDCFR"->"","PRDCNG"->"","HIFLVL"->"","HIFSHR"->"","PRDHIF"->"","PRDBNT"->"","SMBNT"->"","EMBNT"->"","GTUGT"->"","BCSUL"->"","LAPDL"->"","MSSCF"->"","MSSCS"->"","ALFRT"->"","ALHRT"->"","ALSDC"->"","DINHO"->"","DEXDR"->"","RXBAL"->"","RXANT"->"","ITCF"->"","VDLS"->"","MNDL"->"","MNUL"->"","FPHO"->"","ISS"->"","PRE"->"","SBCNF"->"","SBCNH"->"","SBCN"->"","SBCNAF"->"","SBCNAH"->"","RXTA"->"","DEC"->"","IHTA"->"","TTRC"->"","MTTR"->"","SRW"->"","PRECPL"->"","PRECPD"->"","PRECRP"->"","PRECTO"->"","PRECRD"->"","DHP"->"","DNP"->"","DLP"->"","UP1"->"","UP2"->"","UP3"->"","UP4"->"","BGSW1"->"","BGSW2"->"","BGSW3"->"","QCATR"->"","RABLL"->"","RUBLL"->"","BL02"->"","BL03"->"","BL03"->"","BL12"->"","BL13"->"","BL14"->"","BL23"->"","BL24"->"","RL02"->"","RL03"->"","RL04"->"","RL12"->"","RL13"->"","RL14"->"","RL23"->"","RL24"->"","EGIC"->"","IEPH"->"","SPL"->"","IPND"->"","CSUMP"->"","PSUMP"->"","BTON"->"","BTOFF"->"","PLTON"->"","PLTOFF"->"","CSPDP"->"","PSPDP"->"","CPLPDP"->"","MPLPDP"->"","SSME"->"","AF4WFQ"->"","AF3WFQ"->"","AF2WFQ"->"","AF1WFQ"->"","BEWFQ"->"","AAF4WF"->"","AAF3WF"->"","AAF2WF"->"","AAF1WF"->"","ABEWFQ"->"","VPBE"->"","VPAF1"->"","VPAF2"->"","VPAF3"->"","VPAF4"->"","VPEF"->"","AUCS"->"","AUPS"->"","ACP"->"","AMP"->"","CLKS"->"","SS"->"","ADSCPM"->"","ADSCPB"->""); data_map += ("line_number" -> line_number.toString); counter = counter + 1
        case x if x.contains("FlexiBSC") => val flexi_val = x.toString.split("  ")(1).trim(); data_map += ("BSC" -> flexi_val); val dttm_val = x.split("   ").last.trim; data_map += ("DTTM" -> dttm_val)
        case x if x.contains("NUMBER OF PREFERRED CELLS") => val nopc = x.split("""\....""").last.trim; data_map += ("NPC" -> nopc.toString)
        case x if x.contains("GSM MACROCELL THRESHOLD") => val gmt = x.split("""\...""").last.trim; data_map += ("GMAC" -> gmt.toString)
        case x if x.contains("GSM MICROCELL THRESHOLD") => val gmt = x.split("""\...""").last.trim; data_map += ("GMIC" -> gmt.toString)
        case x if x.contains("DCS MACROCELL THRESHOLD") => val dmt = x.split("""\...""").last.trim; data_map += ("DMAC" -> dmt.toString)
        case x if x.contains("DCS MICROCELL THRESHOLD") => val dmt = x.split("""\...""").last.trim; data_map += ("DMIC" -> dmt.toString)
        case x if x.contains("MS DISTANCE BEHAVIOUR") => val mdb = x.split("""\...""").last.trim; data_map += ("DISB" -> mdb.toString)
        case x if x.contains("BTS SITE BATTERY BACKUP FORCED HO TIMER") => val hoTimer = x.split(".... ").last.trim; data_map += ("TIM" -> hoTimer.toString)
        case x if x.contains("EMERGENCY CALL ON FACCH") => val eef = x.split("""\....""").last.trim; data_map += ("EEF" -> eef.toString)
        case x if x.contains("ANSWER TO PAGING CALL ON FACCH") => val epf = x.split("""\....""").last.trim; data_map += ("EPF" -> epf.toString)
        case x if x.contains("ORDINARY CALLS ON FACCH") => val eof = x.split("""\....""").last.trim(); data_map += ("EOF" -> eof.toString)
        case x if x.contains("RE-ESTABLISHMENT ON FACCH") => val erf = x.split("""\....""").last.trim; data_map += ("Param" -> erf.toString)
        case x if x.contains("TCH IN HANDOVER") => val hri = x.split("""\....""").last.trim; data_map += ("HRI" -> hri.toString)
        case x if x.contains("LOWER LIMIT FOR FR TCH RESOURCES") => val hrl = x.split("""\....""").last.trim; data_map += ("HRL" -> hrl.toString)
        case x if x.contains("UPPER LIMIT FOR FR TCH RESOURCES") => val hru = x.split("""\....""").last.trim; data_map += ("HRU" -> hru.toString)
        case x if x.contains("AMH UPPER LOAD THRESHOLD") => val aut = x.split("""\....""").last.trim; data_map += ("AUT" -> aut.toString)
        case x if x.contains("AMH LOWER LOAD THRESHOLD") => val alt = x.split("""\....""").last.trim; data_map += ("ALT" -> alt.toString)
        case x if x.contains("AMH MAX LOAD OF TARGET CELL") => val aml = x.split("""\....""").last.trim; data_map += ("AML" -> aml.toString)
        case x if x.contains("AMR CONFIGURATION IN HANDOVERS") => val ach = x.split("""\....""").last.trim; data_map += ("ACH" -> ach.toString)
        case x if x.contains("INITIAL AMR CHANNEL RATE") => val iac = x.split("""\....""").last.trim; data_map += ("IAC" -> iac.toString)
        case x if x.contains("SLOW AMR LA ENABLED") => val sal = x.split("""\....""").last.trim; data_map += ("SAL" -> sal.toString)
        case x if x.contains("AMR SET GRADES ENABLED") => val asg = x.split("""\....""").last.trim; data_map += ("ASG" -> asg.toString)
        case x if x.contains("FREE TSL FOR CS DOWNGRADE") => val csd = x.split("""\....""").last.trim; data_map += ("CSD" -> csd.toString)
        case x if x.contains("FREE TSL FOR CS UPGRADE") => val csu = x.split("""\....""").last.trim; data_map += ("CSU" -> csu.toString)
        case x if x.contains("TRHO GUARD TIME") => val tgt = x.split("""\....""").last.trim; data_map += ("TGT" -> tgt.toString)
        case x if x.contains("PRIORITY HO INTERFERENCE DL") => val hdl = x.split("""\....""").last.trim; data_map += ("HDL" -> hdl.toString)
        case x if x.contains("PRIORITY HO INTERFERENCE UL") => val hul = x.split("""\....""").last.trim; data_map += ("HUL" -> hul.toString)
        case x if x.contains("LOAD RATE FOR CHANNEL SEARCH") => val clr = x.split("""\....""").last.trim; data_map += ("CLR" -> clr.toString)
        case x if x.contains("TRIGGERING THRESHOLD FOR SERVICE AREA PENALTY") => val ttsap = x.split("""\..""").last.trim; data_map += ("TTSAP" -> ttsap.toString)
        case x if x.contains("PENALTY TRIGGER MEASUREMENT PERIOD") => val ptmp = x.split("""\...""").last.trim; data_map += ("PTMP" -> ptmp.toString)
        case x if x.contains("SERVICE AREA PENALTY TIME") => val sapt = x.split("""\...""").last.trim; data_map += ("SAPT" -> sapt.toString)
        case x if x.contains("CS TCH ALLOCATE RTSL0") => val ctr = x.split("""\....""").last.trim; data_map += ("CTR" -> ctr.toString)
        case x if x.contains("CS TCH ALLOCATION CALCULATION") => val ctc = x.split("""\....""").last.trim; data_map += ("CTC" -> ctc.toString)
        case x if x.contains("MINIMUM MEAN HOLDING TIME FOR TCHS") => val minhtt = x.split("""\.""").last.trim; data_map += ("MINHTT" -> minhtt.toString)
        case x if x.contains("MAXIMUM MEAN HOLDING TIME FOR TCHS") => val  maxhtt = x.split("""\.""").last.trim; data_map += ("MAXHTT" -> maxhtt.toString)
        case x if x.contains("MAXIMUM MEAN HOLDING TIME FOR SDCCHS") => val maxhts = x.split("""\.""").last.trim; data_map += ("MAXHTS" -> maxhts.toString)
        case x if x.contains("ALARM THRESHOLD FOR TCH FAILURE RATE") => val tchfr = x.split("""\..""").last.trim; data_map += ("TCHFR" -> tchfr.toString)
        case x if x.contains("ALARM THRESHOLD FOR SDCCH FAILURE RATE") => val schfr = x.split("""\..""").last.trim; data_map += ("SCHFR" -> schfr.toString)
        case x if x.contains("ALARM THRESHOLD FOR TCH CONGESTION") => val cngt = x.split("""\...""").last.trim; data_map += ("CNGT" -> cngt.toString)
        case x if x.contains("ALARM THRESHOLD FOR SDCCH CONGESTION") => val cngs = x.split("""\...""").last.trim; data_map += ("CNGS" -> cngs.toString)
        case x if x.contains("ALARM THRESHOLD FOR NUMBER OF CHANNEL SEIZURES") => val cs = x.split("""\.....""").last.trim; data_map += ("CS" -> cs.toString)
        case x if x.contains("ALARM THRESHOLD FOR NUMBER OF CH SEIZURE REQUESTS") => val csr = x.split("""\....""").last.trim; data_map += ("CSR" -> csr.toString)
        case x if x.contains("MEAS PRD FOR TCH MEAN HOLDING TIME SUPERVISION") => val prdmht = x.split("""\.""").last.trim; data_map += ("PRDMHT" -> prdmht.toString)
        case x if x.contains("MEAS PRD FOR SDCCH MEAN HOLDING TIME SUPERVISION") => val prdmhs = x.split("""\.""").last.trim; data_map += ("PRDMHS" -> prdmhs.toString)
        case x if x.contains("MEAS PRD FOR SUPERVISION OF CHANNEL FAILURE RATE") => val prdcfr = x.split("""\.""").last.trim; data_map += ("PRDCFR" -> prdcfr.toString)
        case x if x.contains("MEAS PRD FOR SUPERVISION OF CONGESTION IN BTS") => val prdcng = x.split("""\.""").last.trim; data_map += ("PRDCNG" -> prdcng.toString)
        case x if x.contains("THRESHOLD FOR HIGH TCH INTERFERENCE LEVEL") => val hiflvl = x.split("""\.""").last.trim; data_map += ("HIFLVL" -> hiflvl.toString)
        case x if x.contains("ALARM THRESHOLD FOR SHARE OF HIGH TCH INTERFERENCE") => val hifshr = x.split("""\.""").last.trim; data_map += ("HIFSHR" -> hifshr.toString)
        case x if x.contains("MEAS PRD FOR HIGH TCH INTERFERENCE SUPERVISION") => val prdhif = x.split("""\.""").last.trim; data_map += ("PRDHIF" -> prdhif.toString)
        case x if x.contains("MEAS PRD FOR SUPERVISION OF BTS WITH NO TRANSACT") => val prdbnt = x.split("""\.""").last.trim; data_map += ("PRDBNT" -> prdbnt.toString)
        case x if x.contains("STARTING MOMENT FOR SUPERVISION OF BTS") => val smbnt = x.split("""\..""").last.trim; data_map += ("SMBNT" -> smbnt.toString)
        case x if x.contains("ENDING MOMENT FOR SUPERVISION OF BTS") => val embnt = x.split("""\..""").last.trim; data_map += ("EMBNT" -> embnt.toString)
        case x if x.contains("GPRS TERRITORY UPDATE GUARD TIME") => val gtugt = x.split("""\..""").last.trim; data_map += ("GTUGT" -> gtugt.toString)
        case x if x.contains("BCSU LOAD THRESHOLD") => val bcsul = x.split("""\..""").last.trim; data_map += ("BCSUL" -> bcsul.toString)
        case x if x.contains("LAPD LOAD THRESHOLD") => val lapdl = x.split("""\..""").last.trim; data_map += ("LAPDL" -> lapdl.toString)
        case x if x.contains("UPPER LIMIT OF MS SPEED CLASS 1") => val msscf = x.split("""\..""").last.trim; data_map += ("MSSCF" -> msscf.toString)
        case x if x.contains("UPPER LIMIT OF MS SPEED CLASS 2") => val msscs = x.split("""\..""").last.trim; data_map += ("MSSCS" -> msscs.toString)
        case x if x.contains("ALARM LIMIT FOR FULL RATE TCH AVAILABILITY") => val alfrt = x.split("""\..""").last.trim; data_map += ("ALFRT" -> alfrt.toString)
        case x if x.contains("ALARM LIMIT FOR HALF RATE TCH AVAILABILITY") => val alhrt = x.split("""\..""").last.trim; data_map += ("ALHRT" -> alhrt.toString)
        case x if x.contains("ALARM LIMIT FOR SDCCH AVAILABILITY") => val alsdc = x.split("""\..""").last.trim; data_map += ("ALSDC" -> alsdc.toString)
        case x if x.contains("DISABLE INTERNAL HO") => val dinho = x.split("""\..""").last.trim; data_map += ("DINHO" -> dinho.toString)
        case x if x.contains("DISABLE EXTERNAL DR") => val dexdr = x.split("""\..""").last.trim; data_map += ("DEXDR" -> dexdr.toString)
        case x if x.contains("RX LEVEL BALANCE") => val rxbal = x.split("""\..""").last.trim; data_map += ("RXBAL" -> rxbal.toString)
        case x if x.contains("RX ANTENNA SUPERVISION PERIOD") => val rxant = x.split("""\..""").last.trim; data_map += ("RXANT" -> rxant.toString)
        case x if x.contains("NUMBER OF IGNORED TRANSCODER FAILURES") => val itcf = x.split("""\...""").last.trim; data_map += ("ITCF" -> itcf.toString)
        case x if x.contains("VARIABLE DL STEP SIZE") => val vdls = x.split("""\...""").last.trim; data_map += ("VDLS" -> vdls.toString)
        case x if x.contains("MAXIMUM NUMBER OF DL TBF") => val mndl = x.split("""\...""").last.trim; data_map += ("MNDL" -> mndl.toString)
        case x if x.contains("MAXIMUM NUMBER OF UL TBF") => val mnul = x.split("""\...""").last.trim; data_map += ("MNUL" -> mnul.toString)
        case x if x.contains("FEP IN PC HO USE") => val fpho = x.split("""\...""").last.trim; data_map += ("FPHO" -> fpho.toString)
        case x if x.contains("INTRA SEGMENT SDCCH HO GUARD") => val iss = x.split("""\....""").last.trim; data_map += ("ISS" -> iss.toString)
        case x if x.contains("PRE-EMPTION USAGE IN HANDOVER") => val pre = x.split("""\....""").last.trim; data_map += ("PRE" -> pre.toString)
        case x if x.contains("SOFT BLOCKING C/N FR") => val sbcnf = x.split("""\..""").last.trim; data_map += ("SBCNF" -> sbcnf.toString)
        case x if x.contains("SOFT BLOCKING C/N HR") => val sbcnh = x.split("""\..""").last.trim; data_map += ("SBCNH" -> sbcnh.toString)
        case x if x.contains("SOFT BLOCKING C/N 14.4") => val sbcn = x.split("""\...""").last.trim; data_map += ("SBCN" -> sbcn.toString)
        case x if x.contains("SOFT BLOCKING C/N AMR FR") => val sbcnaf = x.split("""\.""").last.trim; data_map += ("SBCNAF" -> sbcnaf.toString)
        case x if x.contains("SOFT BLOCKING C/N AMR HR") => val sbcnah = x.split("""\.""").last.trim; data_map += ("SBCNAH" -> sbcnah.toString)
        case x if x.contains("RX LEVEL BASED TCH ACCESS") & x.contains("(RXTA)") => val rxta = x.split("""\...""").last.trim; data_map += ("RXTA" -> rxta.toString)
        case x if x.contains("DELAY OF HO AND PC FOR EMERGENCY CALLS") => val dec = x.split("""\....""").last.trim; data_map += ("DEC" -> dec.toString)
        case x if x.contains("INTERNAL HO TO EXTERNAL ALLOWED") => val ihta = x.split("""\...""").last.trim; data_map += ("IHTA" -> ihta.toString)
        case x if x.contains("TCH TRANSACTION COUNT") => val ttrc = x.split("""\...""").last.trim; data_map += ("TTRC" -> ttrc.toString)
        case x if x.contains("MAX TCH TRANSACTION RATE") => val mttr = x.split("""\...""").last.trim; data_map += ("MTTR" -> mttr.toString)
        case x if x.contains("SACCH REPEAT WINDOW") => val srw = x.split("""\....""").last.trim; data_map += ("SRW" -> srw.toString)
        case x if x.contains("PRECISE PAGING LOCATION RECORD LIVING TIME") => val precpl = x.split("""\.""").last.trim; data_map += ("PRECPL" -> precpl.toString)
        case x if x.contains("PRECISE PAGING EXPANDING DELAY") => val precpd = x.split("""\.""").last.trim; data_map += ("PRECPD" -> precpd.toString)
        case x if x.contains("PRECISE PAGING ENABLED FOR REPAGINGS") => val precrp = x.split("""\.""").last.trim; data_map += ("precrp" -> precrp.toString)
        case x if x.contains("PRECISE PAGING TMSI OPTIMIZATION") => val precto = x.split("""\.""").last.trim; data_map += ("PRECTO" -> precto.toString)
        case x if x.contains("PRECISE PAGING REPAGING DELAY") => val precrd = x.split("""\.""").last.trim; data_map += ("PRECRD" -> precrd.toString)
        case x if x.contains("DL HIGH PRIORITY SSS") => val dhp = x.split("""\....""").last.trim; data_map += ("DHP" -> dhp.toString)
        case x if x.contains("DL NORMAL PRIORITY SSS") => val dnp = x.split("""\....""").last.trim; data_map += ("DNP" -> dnp.toString)
        case x if x.contains("DL LOW PRIORITY SSS") => val dlp= x.split("""\....""").last.trim; data_map += ("DLP" -> dlp.toString)
        case x if x.contains("UL PRIORITY 1 SSS") => val up1 = x.split("""\....""").last.trim; data_map += ("UP1" -> up1.toString)
        case x if x.contains("UL PRIORITY 2 SSS") => val up2 = x.split("""\....""").last.trim; data_map += ("UP2" -> up2.toString)
        case x if x.contains("UL PRIORITY 3 SSS") => val up3 = x.split("""\....""").last.trim; data_map += ("UP3" -> up3.toString)
        case x if x.contains("UL PRIORITY 4 SSS") => val up4 = x.split("""\....""").last.trim; data_map += ("UP4" -> up4.toString)
        case x if x.contains("BACKGROUND TC SCHEDULING WEIGHT FOR ARP 1") => val bgsw1 = x.split("""\..""").last.trim; data_map += ("BGSW1" -> bgsw1.toString)
        case x if x.contains("BACKGROUND TC SCHEDULING WEIGHT FOR ARP 2") => val bgsw2 = x.split("""\..""").last.trim; data_map += ("BGSW2" -> bgsw2.toString)
        case x if x.contains("BACKGROUND TC SCHEDULING WEIGHT FOR ARP 3") => val bgsw3 = x.split("""\..""").last.trim; data_map += ("BGSW3" -> bgsw3.toString)
        case x if x.contains("QC REALLOCATION ACTION TRIGGER THRESHOLD") => val qcatr = x.split("""\..""").last.trim; data_map += ("QCATR" -> qcatr.toString)
        case x if x.contains("RLC ACK BLER LIMIT") => val rabll = x.split("""\..""").last.trim; data_map += ("RABLL" -> rabll.toString)
        case x if x.contains("RLC UNACK BLER LIMIT") => val rubll = x.split("""\..""").last.trim; data_map += ("RUBLL" -> rubll.toString)
        case x if x.contains("MEAN BEP LIMIT MS MULTISLOT PWR PROF 0 WITH 2 UL TSL") => val bl02 = x.split("""\..""").last.trim; data_map += ("BL02" -> bl02.toString)
        case x if x.contains("MEAN BEP LIMIT MS MULTISLOT PWR PROF 0 WITH 3 UL TSL") => val bl03 = x.split("""\..""").last.trim; data_map += ("BL03" -> bl03.toString)
        case x if x.contains("MEAN BEP LIMIT MS MULTISLOT PWR PROF 0 WITH 4 UL TSL") => val bl04= x.split("""\..""").last.trim; data_map += ("BL04" -> bl04.toString)
        case x if x.contains("MEAN BEP LIMIT MS MULTISLOT PWR PROF 1 WITH 2 UL TSL") => val bl12 = x.split("""\..""").last.trim; data_map += ("BL12" -> bl12.toString)
        case x if x.contains("MEAN BEP LIMIT MS MULTISLOT PWR PROF 1 WITH 3 UL TSL") => val bl13 = x.split("""\..""").last.trim; data_map += ("BL13" -> bl13.toString)
        case x if x.contains("MEAN BEP LIMIT MS MULTISLOT PWR PROF 1 WITH 4 UL TSL") => val bl14 = x.split("""\..""").last.trim; data_map += ("BL14" -> bl14.toString)
        case x if x.contains("MEAN BEP LIMIT MS MULTISLOT PWR PROF 2 WITH 3 UL TSL") => val bl23 = x.split("""\..""").last.trim; data_map += ("BL23" -> bl23.toString)
        case x if x.contains("MEAN BEP LIMIT MS MULTISLOT PWR PROF 2 WITH 4 UL TSL") => val bl24 = x.split("""\..""").last.trim; data_map += ("BL24" -> bl24.toString)
        case x if x.contains("RX QUAL LIMIT MS MULTISLOT PWR PROF 0 WITH 2 UL TSL") => val rl02 = x.split("""\..""").last.trim; data_map += ("RL02" -> rl02.toString)
        case x if x.contains("RX QUAL LIMIT MS MULTISLOT PWR PROF 0 WITH 3 UL TSL") => val rl03 = x.split("""\..""").last.trim; data_map += ("RL03" -> rl03.toString)
        case x if x.contains("RX QUAL LIMIT MS MULTISLOT PWR PROF 0 WITH 4 UL TSL") => val rl04 = x.split("""\..""").last.trim; data_map += ("RL04" -> rl04.toString)
        case x if x.contains("RX QUAL LIMIT MS MULTISLOT PWR PROF 1 WITH 2 UL TSL") => val rl12 = x.split("""\..""").last.trim; data_map += ("RL12" -> rl12.toString)
        case x if x.contains("RX QUAL LIMIT MS MULTISLOT PWR PROF 1 WITH 3 UL TSL") => val rl13 = x.split("""\..""").last.trim; data_map += ("RL13" -> rl13.toString)
        case x if x.contains("RX QUAL LIMIT MS MULTISLOT PWR PROF 1 WITH 4 UL TSL") => val rl14 = x.split("""\..""").last.trim; data_map += ("RL14" -> rl14.toString)
        case x if x.contains("RX QUAL LIMIT MS MULTISLOT PWR PROF 2 WITH 3 UL TSL") => val rl23 = x.split("""\..""").last.trim; data_map += ("RL23" -> rl23.toString)
        case x if x.contains("RX QUAL LIMIT MS MULTISLOT PWR PROF 2 WITH 4 UL TSL") => val rl24 = x.split("""\..""").last.trim; data_map += ("RL24" -> rl24.toString)
        case x if x.contains("EGPRS INACTIVITY CRITERIA") => val egic = x.split("""\..""").last.trim; data_map += ("EGIC" -> egic.toString)
        case x if x.contains("EVENTS PER HOUR FOR EGPRS INACTIVITY ALARM") => val ieph = x.split("""\..""").last.trim; data_map += ("IEPH" -> ieph.toString)
        case x if x.contains("SUPERVISION PERIOD LENGTH FOR EGPRS INACTIVITY ALARM") => val spl = x.split("""\..""").last.trim; data_map += ("SPL" -> spl.toString)
        case x if x.contains("ISHO PREFERRED FOR NON-DTM MS") => val ipnd = x.split("""\..""").last.trim; data_map += ("IPND" -> ipnd.toString)
        case x if x.contains("CS UDP-MUX PORT") => val csump = x.split("""\....""").last.trim; data_map += ("CSUMP" -> csump.toString)
        case x if x.contains("PS UDP-MUX PORT") => val psump= x.split("""\....""").last.trim; data_map += ("PSUMP" -> psump.toString)
        case x if x.contains("BACKHAUL TIMER DURATION START REACTION") => val bton = x.split("""\.....""").last.trim; data_map += ("BTON" -> bton.toString)
        case x if x.contains("BACKHAUL TIMER DURATION STOP REACTION") => val btoff = x.split("""\....""").last.trim; data_map += ("BTOFF" -> btoff.toString)
        case x if x.contains("PACKET LOSS TIMER DURATION START REACTION") => val plton = x.split("""\....""").last.trim; data_map += ("PLTON" -> plton.toString)
        case x if x.contains("PACKET LOSS TIMER DURATION STOP REACTION") => val pltoff = x.split("""\...""").last.trim; data_map += ("PLTOFF" -> pltoff.toString)
        case x if x.contains("CS PACKET DROP PERIOD") => val cspdp = x.split("""\....""").last.trim; data_map += ("CSPDP" -> cspdp.toString)
        case x if x.contains("PS PACKET DROP PERIOD") => val pspdp= x.split("""\....""").last.trim; data_map += ("PSPDP" -> pspdp.toString)
        case x if x.contains("C-PLANE PACKET DROP PERIOD") => val cplpdp = x.split("""\...""").last.trim; data_map += ("CPLPDP" -> cplpdp.toString)
        case x if x.contains("M-PLANE PACKET DROP PERIOD") => val mplpdp = x.split("""\...""").last.trim; data_map += ("MPLPDP" -> mplpdp.toString)
        case x if x.contains("SSM DISABLED") => val ssme = x.split("""\.....""").last.trim; data_map += ("SSME" -> ssme.toString)
        case x if x.contains("AF4 WFQ WEIGHT") => val af4wfq= x.split("""\...""").last.trim; data_map += ("AF4WFQ" -> af4wfq.toString)
        case x if x.contains("AF3 WFQ WEIGHT") => val af3wfq = x.split("""\...""").last.trim; data_map += ("AF3WFQ" -> af3wfq.toString)
        case x if x.contains("AF2 WFQ WEIGHT") => val af2wfq = x.split("""\...""").last.trim; data_map += ("AF2WFQ" -> af2wfq.toString)
        case x if x.contains("CS UDP-MUX PORT") => val csump = x.split("""\...""").last.trim; data_map += ("CSUMP" -> csump.toString)
        case x if x.contains("AF1 WFQ WEIGHT") => val af1wfq = x.split("""\...""").last.trim; data_map += ("AF1WFQ" -> af1wfq.toString)
        case x if x.contains("BE WFQ WEIGHT") => val bewfq = x.split("""\....""").last.trim; data_map += ("BEWFQ" -> bewfq.toString)
        case x if x.contains("A-IF AF4 WFQ WEIGHT") & x.contains("(AAF4WF)") => val aaf4wf = x.split("""\..""").last.trim; data_map += ("AAF4WF" -> aaf4wf.toString)
        case x if x.contains("A-IF AF3 WFQ WEIGHT") => val aaf3wf= x.split("""\...""").last.trim; data_map += ("AAF3WF" -> aaf3wf.toString)
        case x if x.contains("A-IF AF2 WFQ WEIGHT") => val aaf2wf = x.split("""\...""").last.trim; data_map += ("AAF2WF" -> aaf2wf.toString)
        case x if x.contains("A-IF AF1 WFQ WEIGHT") => val aaf1wf = x.split("""\...""").last.trim; data_map += ("AAF1WF" -> aaf1wf.toString)
        case x if x.contains("A-IF BE WFQ WEIGHT") => val abewfq= x.split("""\...""").last.trim; data_map += ("ABEWFQ" -> abewfq.toString)
        case x if x.contains("VLAN PRIORITY BE") => val vpbe= x.split("""\.....""").last.trim; data_map += ("VPBE" -> vpbe.toString)
        case x if x.contains("VLAN PRIORITY AF1") => val vpaf1= x.split("""\....""").last.trim; data_map += ("VPAF1" -> vpaf1.toString)
        case x if x.contains("VLAN PRIORITY AF2") => val vpaf2= x.split("""\....""").last.trim; data_map += ("VPAF2" -> vpaf2.toString)
        case x if x.contains("VLAN PRIORITY AF3") => val vpaf3= x.split("""\....""").last.trim; data_map += ("VPAF3" -> vpaf3.toString)
        case x if x.contains("VLAN PRIORITY AF4") => val vpaf4= x.split("""\....""").last.trim; data_map += ("VPAF4" -> vpaf4.toString)
        case x if x.contains("VLAN PRIORITY EF") => val vpef= x.split("""\.....""").last.trim; data_map += ("VPEF" -> vpef.toString)
        case x if x.contains("ABIS U-PLANE CS TO DSCP MAPPING") => val aucs= x.split("""\.....""").last.trim; data_map += ("AUCS" -> aucs.toString)
        case x if x.contains("ABIS U-PLANE PS TO DSCP MAPPING") => val aups= x.split("""\.....""").last.trim; data_map += ("AUPS" -> aups.toString)
        case x if x.contains("ABIS C-PLANE TO DSCP MAPPING") => val acp= x.split("""\......""").last.trim; data_map += ("ACP" -> acp.toString)
        case x if x.contains("ABIS M-PLANE TO DSCP MAPPING") => val amp= x.split("""\......""").last.trim; data_map += ("AMP" -> amp.toString)
        case x if x.contains("CLOCK-SYNC TO DSCP MAPPING") => val clks= x.split("""\.....""").last.trim; data_map += ("CLKS" -> clks.toString)
        case x if x.contains("SITE SUPPORT TRAFFIC TO DSCP MAPPING") => val ss= x.split("""\.......""").last.trim; data_map += ("SS" -> ss.toString)
        case x if x.contains("A-IF CS U-PLANE DSCP PHB TC IN MGW") => val adscpm= x.split("""\...""").last.trim; data_map += ("ADSCPM" -> adscpm.toString)
        case x if x.contains("A-IF CS U-PLANE DSCP PHB TC IN BSS") => val adscpb= x.split("""\...""").last.trim; data_map += ("ADSCPB" -> adscpb.toString)
        case _ => None
      }
      line_number = line_number + 1
    }
    list_map += data_map.toMap
    list_map.toList
  }

  /**
    * ******************************************************
    * flexi_bsc__ss7_network__zobi_sccpbroadcast_defination
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def flexi_bsc__ss7_network__zobi_sccpbroadcast_defination(logfilecontent: String): List[Map[String, String]] = {
    println("Executing flexi_bsc__ss7_network__zobi_sccpbroadcast_defination")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var logEntry = mutable.Map[Int, String]()
    var secondlineno, dashlineno, endlineno, thirdlineno, tableendline: Int = 0
    var endlinelist, startlinelist, secondlinelist, thirdlinelist, emptylinelist = ListBuffer[Int]()
    var firstlinetext = ""
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNextLine) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("ZOBI;")) {
        startlinelist += lineNum
      }

      if (line.trim.size == 0) {
        emptylinelist += lineNum
      }

      if (line.contains("COMMAND EXECUTED")) {
        endlinelist += lineNum
      }

      lineNum += 1
    }
    for (i <- startlinelist) {
      // val paramdata = dataArray(i +12)
      val endline = getClosestLineNumber(i, endlinelist.toList.sorted)
      for (k <- (i +13) to endline-1) {
        val data = dataArray(k - 1)
        if (data.trim.length > 0) {
          my_map += (
            "line_number" -> k.toString.trim,
            "BROADCAST_GROUPS_ASBG" -> (data.split("/")(0).trim),
            "BROADCAST_GROUPS_CSBG" -> (data.split("/")(1).trim)
          )
        }
      }
      val listCols = List("line_number","BROADCAST_GROUPS_ASBG","BROADCAST_GROUPS_CSBG")
      listCols.foreach(c =>{
        if (!my_map.keys.toList.contains(c)) my_map += c -> ""
      })
      list_map += my_map.toMap
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * flexi_bsc__bcsu_5_pcu2_e_12_logs__ssv_pcu_esw
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__bcsu_5_pcu2_e_12_logs__ssv_pcu_esw(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("BCSU/")&&line.contains("/PCU/")&&line.contains("/OSE>ssv")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bcsu" -> line.stripPrefix("BCSU/").split("/PCU/")(0).trim)
        logEntry += ("pcu" -> line.split("/PCU/")(1).split("/OSE>ssv")(0).trim)
        for(i<-1 to 4){
          line=txt.nextLine()
          linNum+=1
        }
        logEntry += ("date" -> line.substring(31).stripSuffix("] <----").trim)
        for(i<-1 to 4){
          line=txt.nextLine()
          linNum+=1
        }
        logEntry += ("pq2_boot_image" -> line.stripPrefix("PQ2 Boot Image Software Version:").trim)
        txt.nextLine();line=txt.nextLine()
        linNum+=2
        logEntry += ("pq2_ram_image" -> line.stripPrefix("PQ2 RAM Image Software Version:").trim)
        txt.nextLine();line=txt.nextLine()
        linNum+=2
        logEntry += ("dsp_ram_image" -> line.stripPrefix("DSP RAM Image Software Version:").trim)
        txt.nextLine();line=txt.nextLine()
        linNum+=2
        logEntry += ("dsp_diagnostics_image" -> line.stripPrefix("DSP Diagnostics Image Software Version:").trim)
        txt.nextLine();line=txt.nextLine()
        linNum+=2
        logEntry += ("dsp_boot_image" -> line.stripPrefix("DSP Boot Image Software Version:").trim)


        logEntries += logEntry

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_bsc__bts_seg_and_trx_parameters__bts_dfr8k_mode
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__bts_seg_and_trx_parameters__bts_dfr8k_mode(logfilecontent: String): List[Map[String, String]] = {
   println("flexi_bsc__bts_seg_and_trx_parameters__bts_dfr8k_mode")
    val txt = new Scanner(logfilecontent)
    var linNum = 1
    var sublineNum=0
    var logEntries = ListBuffer[Map[String, String]]()
    var logEntry = Map[String, String]()

    while (txt.hasNextLine) {
      var line = txt.nextLine()
      if (line.contains("BCF-")&&line.contains("BTS-")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bcf" -> line.stripPrefix("BCF-").split("BTS-")(0).trim)
        logEntry += ("bts" -> line.split("BTS-")(1).trim)
        while (txt.hasNextLine && !line.contains("8KTRAU MODE FOR OSC AMR FR...............(DFR8K)..") && !line.contains("BCF-")&& !line.contains("BTS-")) {

            line = txt.nextLine()
            linNum += 1
          }
        if(line.contains("8KTRAU MODE FOR OSC AMR FR...............(DFR8K).."))
        logEntry += ("dfr8k" -> line.stripPrefix("8KTRAU MODE FOR OSC AMR FR...............(DFR8K)..").trim)
        else{
          logEntry += ("dfr8k" ->"")
        }
        logEntries += logEntry
      }
      linNum += 1
    }
    return logEntries.toList

  }

  /**
    * flexi_bsc__swu__mstp_configuration
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__swu__mstp_configuration(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.endsWith("spanning-tree mst configuration")) {

        line = txt.nextLine()
        linNum+=1

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("name" -> line.split("name")(1).trim)
        line = txt.nextLine()
        linNum+=1
        if(line.contains("revision")){
          logEntry += ("revision" -> line.split("revision")(1).trim)
          line = txt.nextLine()
          linNum+=1
        }else{
          logEntry += ("revision" -> "")
        }
        logEntry += ("instance" -> line.substring(2,13).trim)
        logEntry += ("vlan" -> line.splitAt(13)._2.trim)
        logEntries += logEntry

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_bsc__ip_configurations__zqwl
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip_configurations__zqwl(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.endsWith("ZQWL;")) {
        while (!line.contains("EXECUTION STARTED")) {
          line = txt.nextLine()
          linNum += 1
        }

        while (!line.contains("    --- ---- --- ------ ---- ----- ---- --------------- --- -------------------")) {
          line = txt.nextLine()
          linNum += 1
        }
        line=txt.nextLine()
        linNum+=1
        logEntry += ("line_number" -> linNum.toString)
        while(line.length!=0 && !line.contains("COMMAND EXECUTED")){
          logEntry += ("cha_num" -> line.substring(3,7).trim)
          logEntry += ("bcf_num" -> line.substring(7,12).trim)
          logEntry += ("trx_num" -> line.substring(12,16).trim)
          logEntry += ("busbaudrate" -> line.substring(16,23).trim)
          logEntry += ("type" -> line.substring(23,28).trim)
          logEntry += ("index" -> line.substring(28,34).trim)
          logEntry += ("q1_addr" -> line.substring(34,39).trim)
          logEntry += ("equipment" -> line.substring(39,55).trim)
          logEntry += ("equgen" -> line.substring(55,59).trim)
          logEntry += ("state" -> line.splitAt(59)._2.trim)
          logEntries += logEntry
          line=txt.nextLine()
          linNum+=1
        }

      }
      linNum += 1
    }
    return logEntries.toList
  }

  /**
    * flexi_bsc__system_configuration__zwti_pi_piulocation
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__system_configuration__zwti_pi_piulocation(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.endsWith("ZWTI:PI;")) {
        while (!line.contains("PIU TYPE INDEX         LOC            UNIT           SEN              ITI")) {
          line = txt.nextLine()
          linNum += 1
        }
        txt.nextLine()
        line=txt.nextLine()
        linNum+=2
        logEntry += ("line_number" -> linNum.toString)
        while(line.length!=0 && !line.contains("COMMAND EXECUTED")){
          logEntry += ("piu_type" -> line.substring(0,9).trim)
          logEntry += ("piu_index" -> line.substring(9,15).trim)
          logEntry += ("location" -> line.substring(15,35).trim)
          logEntry += ("unit" -> line.splitAt(35)._2.trim)
          logEntries += logEntry
          line=txt.nextLine()
          linNum+=1
        }

      }
      linNum += 1
    }
    return logEntries.toList
  }

  /**
    * flexi_bsc__switch_info_txt__zwyi
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__switch_info_txt__zwyi(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZEWL;")) {

        while (!line.contains("ID     UNIT             IF       IP ADDRESS      SUPERV           HW ID CODE")) {
          line = txt.nextLine()
          linNum += 1
        }
        line = txt.nextLine()
        linNum += 1

        if (line.contains("                        NAME                     UNIT             IN HEX")) {
          line = txt.nextLine()
          linNum += 1
          if (line.contains("------ ---------------- -------- --------------- ---------------- ----------")) {
            line = txt.nextLine()
            linNum += 1
            logEntry += ("line_number" -> linNum.toString)
            while (!line.contains("TOTAL OF")) {
              logEntry += ("id" -> line.substring(0, 7).trim)
              logEntry += ("unit" -> line.substring(7, 24).trim)
              logEntry += ("if_name" -> line.substring(24, 33).trim)
              logEntry += ("ip" -> line.substring(33, 49).trim)
              logEntry += ("superv_unit" -> line.substring(49, 66).trim)
              logEntry += ("hw_id" -> line.splitAt(66)._2.trim)
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
    * flexi_bsc__switch_info_txt__sg_test_table_pattern
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__switch_info_txt__sg_test_table_pattern(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val retxt = logfilecontent.split("\\n").toList
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("LOADING PROGRAM VERSION")) {

        while (!line.startsWith("FlexiBSC") && txt.hasNext()) {
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
          linNum += 1
        }
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bsc" -> line.split("\\s+")(1).trim)
        logEntry += ("date_time" -> (line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))
        while (!line.startsWith("WORKING STATE OF UNITS") && txt.hasNext()) {
          line = txt.nextLine()
          if(!txt.hasNext()){
            return logEntries.toList
          }
          linNum += 1
        }
        if (txt.hasNext()) {
          txt.nextLine();
          line = txt.nextLine()
          linNum += 2

          while (!line.contains("TOTAL OF")) {
            if (!line.isEmpty) {
              logEntry += ("unit" -> line.substring(1, 12).trim)
              logEntry += ("phy_state" -> line.substring(12, 24).trim)
              logEntry += ("location" -> line.substring(24, 45).trim)
              logEntry += ("info" -> line.splitAt(45)._2.trim)
              logEntries += logEntry
            }
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
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo_sran_mutecall
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo_sran_mutecall(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("BASE CONTROL FUNCTION BCF") && line.endsWith("DATA")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bcf" -> line.split("BASE CONTROL FUNCTION BCF-")(1).stripSuffix("DATA").trim)
        line = txt.nextLine;
        linNum += 1
        logEntry += ("site_type" -> line.stripPrefix("SITE TYPE ............................").trim)
        txt.nextLine;
        line = txt.nextLine;
        linNum += 2
        if (line.startsWith("SBTS ID ..............................")) {
          logEntry += ("sbts_id" -> line.stripPrefix("SBTS ID ..............................").trim)

          while (txt.hasNext() && !line.contains("PACKET ABIS CONGESTION CONTROL ....................(PACC)...")) {
            line = txt.nextLine()
            linNum += 1
          }
          //if(line.startsWith("PACKET ABIS CONGESTION CONTROL ....................(PACC)...")) {
          logEntry += ("pacc" -> line.stripPrefix("PACKET ABIS CONGESTION CONTROL ....................(PACC)...").trim)
          //}
          while (txt.hasNext() && !line.contains("PACKET DELAY VARIATION.............................(PDV)....")) {
            line = txt.nextLine()
            linNum += 1
          }
          logEntry += ("pdv" -> line.stripPrefix("PACKET DELAY VARIATION.............................(PDV)....").trim)
          logEntries += logEntry
        }


      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_zefo_mutecall
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_zefo_mutecall(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("BASE CONTROL FUNCTION BCF") && line.endsWith("DATA")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bcf" -> line.split("BASE CONTROL FUNCTION BCF-")(1).stripSuffix("DATA").trim)
        line = txt.nextLine;
        linNum += 1
        logEntry += ("site_type" -> line.stripPrefix("SITE TYPE ............................").trim)
        line = txt.nextLine;
        linNum += 1
        if (line.startsWith("BTS SITE SUBTYPE .....................")) {
          logEntry += ("bts_site_subtype" -> line.stripPrefix("BTS SITE SUBTYPE .....................").trim)
          line = txt.nextLine;
          linNum += 1
          if (line.startsWith("SBTS ID ..............................")) {
            logEntry += ("sbts_id" -> line.stripPrefix("SBTS ID ..............................").trim)
            line = txt.nextLine;
            linNum += 1
            if (line.startsWith("ADMINISTRATIVE STATE .................")) {
              logEntry += ("administrative_state" -> line.stripPrefix("ADMINISTRATIVE STATE .................").trim)
              line = txt.nextLine;
              linNum += 1
              if (line.startsWith("OPERATIONAL STATE ....................")) {
                logEntry += ("op_state" -> line.stripPrefix("OPERATIONAL STATE ....................").trim)

                for (i <- 1 to 28) {
                  line = txt.nextLine()
                }
                linNum += 28
                if (line.contains("PACKET ABIS CONGESTION CONTROL ....................(PACC)...")) {
                  logEntry += ("pacc" -> line.stripPrefix("PACKET ABIS CONGESTION CONTROL ....................(PACC)...").trim)
                  for (i <- 1 to 12) {
                    line = txt.nextLine()
                  }
                  linNum += 12

                  if (line.contains("PACKET DELAY VARIATION.............................(PDV)....")) {
                    logEntry += ("pacc" -> line.stripPrefix("PACKET DELAY VARIATION.............................(PDV)....").trim)
                    logEntries += logEntry
                  }
                }
              }

            }
          }
        }
      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeei_bcsu_usage
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeei_bcsu_usage(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("UNIT        TRXS   TELECOM LINKS  O&M LINKS   TCHS")) {
        txt.nextLine;
        line = txt.nextLine()
        linNum += 2
        logEntry += ("line_number" -> linNum.toString)
        while (!(line.startsWith("========") || line.startsWith("TOTAL"))) {

          logEntry += ("unit" -> line.substring(0, 12).trim)
          logEntry += ("trxs" -> line.substring(12, 23).trim)
          logEntry += ("lapd_telecom_link" -> line.substring(23, 37).trim)
          logEntry += ("lapd_om" -> line.substring(37, 44).trim)
          logEntry += ("real_tech" -> line.splitAt(44)._2.trim)
          logEntries += logEntry
          line = txt.nextLine()
          linNum += 1
        }

      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__etxx_configuration__zdwq_abis_d_channel
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__etxx_configuration__zdwq_abis_d_channel(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("DATA OF ABIS D-CHANNEL")) {

        for (i <- 1 to 5) {
          line = txt.nextLine()
        }
        linNum += 5
        logEntry += ("line_number" -> linNum.toString)

        while (line.length != 0) {
          var value = line.split("\\s+")
          logEntry += ("name" -> value(0).trim)
          logEntry += ("num" -> value(1).trim)
          logEntry += ("int_id" -> value(2).trim)
          logEntry += ("sapi" -> value(3).trim)
          logEntry += ("tei" -> value(4).trim)
          logEntry += ("association_name" -> value(5).trim)
          logEntry += ("stream_number" -> value(6).trim)
          logEntry += ("host_unit" -> value(7).trim)
          logEntry += ("state" -> value(8).trim)

          logEntries += logEntry
          line = txt.nextLine();
          linNum += 1

        }
      }


      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__bts_seg_and_trx_parameters__bts_dfca_mode
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__bts_seg_and_trx_parameters__bts_dfca_mode(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("BCF-") && line.contains("  BTS-")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bcf" -> line.split("BTS-")(0).stripPrefix("BCF-").trim)
        logEntry += ("bts" -> line.split("BTS-")(1).split("\\s+")(0).trim)
        while (txt.hasNextLine && !line.contains("DFCA PARAMETERS:")) {
          line = txt.nextLine();
          linNum += 1
        }
        if(txt.hasNextLine) {
          line = txt.nextLine;
          linNum += 1
          if (line.contains("DFCA MODE ............................(DMOD)..")) {
            logEntry += ("dfca" -> line.split("DFCA MODE ............................\\(DMOD\\)..")(1).trim)
            logEntries += logEntry
          }
        }

      }


      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__locked_files__ziwx_lfiles
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__locked_files__ziwx_lfiles(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("FILE NAME       FILE   FILE   FILE      DATA                   PAGE")) {

        for (i <- 1 to 4) {
          line = txt.nextLine()
        }
        linNum += 4
        logEntry += ("line_number" -> linNum.toString)
        while (txt.hasNextLine && !line.startsWith("FILE NAME") && line.length != 0) {
          if (line.length != 0) {
            logEntry += ("file_name" -> line.substring(0, 17).trim)
            logEntry += ("file_version" -> line.substring(17, 21).trim)
            logEntry += ("file_no" -> line.substring(21, 27).trim)
            logEntry += ("file_length" -> line.substring(27, 37).trim)
            logEntry += ("data_length" -> line.substring(37, 47).trim)
            logEntry += ("creator" -> line.substring(47, 51).trim)
            logEntry += ("attr" -> line.splitAt(51)._2.trim)
            logEntries += logEntry
          }
          line = txt.nextLine();
          linNum += 1
        }

      }


      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__ip_configurations_txt__fb_ipc_mc_etme_eep
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip_configurations_txt__fb_ipc_mc_etme_eep(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ETME") && line.split("\\s+").length == 1) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("etme" -> line.trim)
        while (txt.hasNextLine && !line.contains("VLAN23     UP    1500  LI      ")) {
          line = txt.nextLine();
          linNum += 1
        }
        if (line.contains("VLAN23     UP    1500  LI      ")) {
          logEntry += ("ip_part1" -> line.split("VLAN23     UP    1500  LI      \\(")(1).split('.')(0).trim)
          logEntry += ("ip_part2" -> line.split("VLAN23     UP    1500  LI      \\(")(1).split('.')(1).trim)
          logEntry += ("ip_part3" -> line.split("VLAN23     UP    1500  LI      \\(")(1).split('.')(2).trim)
          logEntry += ("ip_part4" -> line.split("VLAN23     UP    1500  LI      \\(")(1).split('.')(3).split('/')(0).trim)
          logEntry += ("subnet" -> line.split("VLAN23     UP    1500  LI      \\(")(1).split('.')(3).split('/')(0).trim.stripSuffix(")").trim)
          logEntries += logEntry
        }
      }


      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__supplementary_ss7_network__zocj
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__supplementary_ss7_network__zocj(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZOCJ;")) {

        while (!line.contains("FlexiBSC ") && txt.hasNextLine) {
          line = txt.nextLine();
          linNum += 1
        }
        if(line.contains("FlexiBSC ")){
          logEntry += ("line_number" -> linNum.toString)
          logEntry += ("bsc" -> line.split("\\s+")(1).trim)
          logEntry += ("date_time" -> (line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))

          while (txt.hasNextLine && !(line.contains("SET NUMBER: ") && line.contains(" SET NAME: "))) {
            line = txt.nextLine();
            linNum += 1
          }
          while (line.length != 0) {
            logEntry += ("set_number" -> line.split("SET NAME:")(0).stripPrefix("SET NUMBER: ").trim)
            logEntry += ("set_name" -> line.split("SET NAME:")(1).trim)
           // println(line)
            while (txt.hasNextLine && !line.contains("---  ----                              -----          ------")) {
              line = txt.nextLine();
              linNum += 1
            }

            line = txt.nextLine();
            linNum += 1
            while (line.length != 0) {
              logEntry += ("no" -> line.substring(0, 4).trim)
              logEntry += ("name" -> line.substring(4, 40).trim)
              logEntry += ("value" -> line.substring(40, 52).trim)
              logEntry += ("unit" -> line.splitAt(52)._2.trim)
              line = txt.nextLine()
              linNum += 1
              logEntries += logEntry
            }
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
    * flexi_bsc__clock_and_lapd_status__zdsb_lapd_state
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__clock_and_lapd_status__zdsb_lapd_state(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZDSB;")) {

        while (txt.hasNextLine && !line.contains("-----  --- ---- ---   -- ----------- ---  ----------------  ---- ------- ---")) {
          line = txt.nextLine();
          linNum += 1
        }
        if (txt.hasNextLine){
          line = txt.nextLine()
          linNum += 1
          logEntry += ("line_number" -> linNum.toString)
          while (line.length != 0) {
            logEntry += ("name" -> line.substring(0, 6).trim)
            logEntry += ("number" -> line.substring(6, 11).trim)
            logEntry += ("sapi" -> line.substring(11, 16).trim)
            logEntry += ("tei" -> line.substring(16, 22).trim)
            logEntry += ("bit_rate" -> line.substring(22, 24).trim)
            logEntry += ("external_pcm_tsl_sub_tsl" -> line.substring(24, 41).trim)
            logEntry += ("unit" -> line.substring(41, 49).trim)
            logEntry += ("term" -> line.substring(49, 54).trim)
            logEntry += ("term_function" -> line.substring(54, 60).trim)
            logEntry += ("log_term" -> line.substring(60, 64).trim)
            logEntry += ("internal_pcm_tsl" -> line.substring(64, 73).trim)
            logEntry += ("parameter_set" -> line.splitAt(73)._2.trim)

            logEntries += logEntry
            line = txt.nextLine();
            linNum += 1
          }
        }
      }


      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__measurement_status__ztpi_meas_state
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__measurement_status__ztpi_meas_state(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.startsWith("FlexiBSC")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bsc" -> line.split("\\s+")(1).trim)
        logEntry += ("date_time" -> (line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))

        txt.nextLine();
        line = txt.nextLine();
        linNum += 2

        if (line.contains("TYPE:                   ")) {
          logEntry += ("type" -> line.split("TYPE:                   ")(1).trim)
          line = txt.nextLine();
          linNum += 1
          if (line.contains("LAST MODIFIED:          ")) {
            logEntry += ("last_modified_date" -> line.split("LAST MODIFIED:          ")(1).split("\\s+")(0).trim)
            logEntry += ("last_modified_time" -> line.split("LAST MODIFIED:          ")(1).split("\\s+")(1).trim)
            line = txt.nextLine();
            linNum += 1
            logEntry += ("admin_state" -> line.split("ADMIN. STATE:           ")(1).trim)
            line = txt.nextLine();
            linNum += 1
            logEntry += ("oper_state" -> line.split("OPER. STATE:            ")(1).trim)

            while (txt.hasNextLine && !line.contains("MON   ")) {
              line = txt.nextLine();
              linNum += 1
            }
            if (line.contains("MON   ")) {
              logEntry += ("me_interval_mon_time" -> line.split("MON   ")(1).trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("me_interval_tue_time" -> line.split("TUE   ")(1).trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("me_interval_wed_time" -> line.split("WED   ")(1).trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("me_interval_thu_time" -> line.split("THU   ")(1).trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("me_interval_fri_time" -> line.split("FRI   ")(1).trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("me_interval_sat_time" -> line.split("SAT   ")(1).trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("me_interval_sun_time" -> line.split("SUN   ")(1).trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("start_date" -> line.split("START DATE:             ")(1).trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("stop_date" -> line.split("STOP DATE:              ").lift(1).getOrElse("").trim)
              line = txt.nextLine();
              linNum += 1
              logEntry += ("output_interval" -> line.split("OUTPUT INTERVAL:        ")(1).trim)
              logEntries += logEntry
            }
          }
        }
      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_bsc__preprocessor_sw__zdpx_gsw_preproinput
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__preprocessor_sw__zdpx_gsw_preproinput(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZDPX:")) {
        for (i <- 1 to 4) {
          line = txt.nextLine();

        }
        linNum += 4
        if (line.startsWith("FlexiBSC")){
          logEntry += ("line_number" -> linNum.toString)
          logEntry += ("bsc" -> line.split("\\s+")(1).trim)
          logEntry += ("date_time" -> (line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))
          for (i <- 1 to 4) {
            line = txt.nextLine();

          }
          linNum += 4
          while(!line.contains("COMMAND EXECUTED")){
            logEntry += ("disk_image" -> line.split("  DISK IMAGE:  ")(1).trim)
            txt.nextLine;line = txt.nextLine();linNum+=2
            logEntry += ("gsw" -> line.trim)
            txt.nextLine;line = txt.nextLine();linNum+=2
            logEntry += ("sw256" -> line.trim)
            txt.nextLine;line = txt.nextLine();linNum+=2
            logEntry += ("primary_image" -> line.split("  PRIMARY IMAGE: ")(1).trim)
            line = txt.nextLine();linNum+=1
            logEntry += ("backup_image" -> line.split("  BACKUP IMAGE:  ")(1).trim)
            txt.nextLine;txt.nextLine;line = txt.nextLine();linNum+=3
            logEntries += logEntry
          }
        }
      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__supplementary_ss7_network__zodi
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__supplementary_ss7_network__zodi(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZODI;")) {

        while(txt.hasNextLine && !line.startsWith("FlexiBSC ")){
          line = txt.nextLine;linNum+=1
        }

        if (line.startsWith("FlexiBSC")){
          logEntry += ("line_number" -> linNum.toString)
          logEntry += ("bsc" -> line.split("\\s+")(1).trim)
          logEntry += ("date_time" -> (line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))

          while(!line.startsWith("-----------------------  -----------------------------  ---------------")){
            line = txt.nextLine;linNum+=1
          }
          line = txt.nextLine;linNum+=1
          while(txt.hasNextLine && line.length!=0){
            logEntry += ("oosg" -> line.substring(0,24).trim)
            logEntry += ("odsg" -> line.substring(24,56).trim)
            logEntry += ("treatment" -> line.splitAt(56)._2.trim)
            logEntries += logEntry
            txt.nextLine;line= txt.nextLine();linNum+=2

          }
        }
      }


      linNum += 1
    }
    logEntries.toList
  }
  /**
    * flexi_bsc__ip_configurations__zqht
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip_configurations__zqht(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZQHT:") && line.endsWith(";")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("unit" -> line.split("ZQHT:")(1).stripSuffix(";").trim)

        while (txt.hasNextLine && !line.contains("packets tx:") && !line.contains("packets rx:")) {
          line = txt.nextLine;
          linNum += 1
        }

        if (line.contains("packets tx: ") && line.contains("packets rx:")) {
          logEntry += ("tx_packets" -> line.split("packets rx:")(0).split("packets tx:")(1).trim)
          logEntry += ("rx_packets" -> line.split("packets rx:")(1).trim)
          line = txt.nextLine;
          linNum += 1
          if (line.contains("broadcasts tx:") && line.contains("broadcasts rx:")) {
            logEntry += ("broadcasts_tx" -> line.split("broadcasts rx:")(0).stripPrefix("  broadcasts tx:").trim)
            logEntry += ("broadcasts_rx" -> line.split("broadcasts rx:")(1).trim)
            line = txt.nextLine;
            linNum += 1
            if (line.contains("VLAN tx:") && line.contains("VLAN rx:")) {
              logEntry += ("vlan_tx" -> line.split("VLAN rx:")(0).stripPrefix("  VLAN tx:                  ").trim)
              logEntry += ("vlan_rx" -> line.split("VLAN rx:")(1).trim)
              line = txt.nextLine;
              linNum += 1
              if (line.contains("Errors:")) {
                logEntry += ("errors" -> line.split("Errors:")(1).trim)
                line = txt.nextLine;
                linNum += 1
                if (line.contains("tx carrier sense lost:")) {
                  logEntry += ("tx_carrier_sense_lost" -> line.split("tx carrier sense lost:")(1).trim)
                  logEntries += logEntry
                }
              }
            }
          }
        }
      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__ip_configurations_txt__zqkb_statroute
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_bsc__ip_configurations_txt__zqkb_statroute(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.endsWith("ZQKB;")) {

        while(txt.hasNextLine && !line.startsWith("INTERROGATED STATIC ROUTES")){
          line = txt.nextLine;linNum+=1
        }

        while(txt.hasNextLine && !line.startsWith("---------------------- -------------------------------------------- ----- -----")){
          line = txt.nextLine;linNum+=1
        }
        line = txt.nextLine;linNum+=1
        logEntry += ("line_number" -> linNum.toString)
        while(line.length!=0){
          logEntry += ("unit" -> line.substring(0,22).trim)
          logEntry += ("ip1" -> line.splitAt(22)._2.split('.')(0).trim)
          logEntry += ("ip2" -> line.splitAt(22)._2.split('.')(1).trim)
          logEntry += ("ip3" -> line.splitAt(22)._2.split('.')(2).trim)
          logEntry += ("ip4" -> line.splitAt(22)._2.split('.')(3).split("\\s+")(0).split('/')(0).trim)
          line = txt.nextLine;linNum+=1
          if(line.contains("DEFAULT ")){
            logEntry += ("df" -> line.split("DEFAULT")(1).trim)
            logEntry += ("df_ip1" -> "")
            logEntry += ("df_ip2" -> "")
            logEntry += ("df_ip3" -> "")
            logEntry += ("df_ip4" -> "")

          }else {
            logEntry += ("df" -> "")
            logEntry += ("df_ip1" -> line.split(">")(1).split('.')(0).trim)
            logEntry += ("df_ip2" -> line.split(">")(1).split('.')(1).trim)
            logEntry += ("df_ip3" -> line.split(">")(1).split('.')(2).trim)
            logEntry += ("df_ip4" -> line.split(">")(1).split('.')(3).split('/')(0).trim)
          }
          logEntries += logEntry
          line = txt.nextLine;linNum+=1
        }
      }


      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__database_status__zdbd_omu
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__database_status__zdbd_omu(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZDBD:OMU:;")) {

        for(i<-1 to 4){
          line = txt.nextLine()
        }
        linNum+4

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("bsc_name" -> line.split("\\s+")(1).trim)
        logEntry += ("date_time" -> (line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))
        for(i<-1 to 6){
          line = txt.nextLine()
        }
        linNum+6
        while(txt.hasNextLine && !line.contains("COMMAND EXECUTED")){

          if(line.length!=0){
            var value = line.split("\\s+")
            logEntry += ("database" -> value(0).trim)
            logEntry += ("occurance" -> value(1).trim)
            logEntry += ("dumping_or_loading" -> value(2).trim)
            logEntry += ("consist_disk1" -> value(3).trim)
            logEntry += ("consist_disk2" -> value(4).trim)
            logEntry += ("allowed_operations" -> value(5).trim)
            logEntry += ("log_wri_count" -> value(6).trim)
            logEntry += ("wri_err_count" -> value(7).trim)
            logEntry += ("log_empty_count" -> value(8).trim)
            logEntry += ("empty_err_count" -> value(9).trim)
            logEntry += ("free_disk_log_buff" -> value(10).trim)

            logEntries += logEntry

          }
          line = txt.nextLine()
          linNum+=1
        }
      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewo_output
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewo_output(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZEWO:")) {

        txt.nextLine();line=txt.nextLine();linNum+=2
        if(line.contains("LOADING PROGRAM VERSION 21.1-0")){
          txt.nextLine();line=txt.nextLine();linNum+=2
          if(line.contains("EXECUTION STARTED")){

            for(i<- 1 to 3){
              line = txt.nextLine()
            }
            linNum+=3
          }
          if(line.startsWith("FlexiBSC")){
            logEntry += ("line_number" -> linNum.toString)
            logEntry += ("bsc_name" -> line.split("\\s+")(1).trim)
            logEntry += ("date_time" -> (line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))

            for(i<- 1 to 5){
              line = txt.nextLine()
            }
            linNum+=5

            while(!line.startsWith("COMMAND EXECUTED")){
              if(line.length!=0&&line.length > 72) {
                logEntry += ("bcf_number" -> line.substring(0, 18).trim)
                logEntry += ("status" -> line.substring(18, 24).trim)
                logEntry += ("build_id" -> line.substring(24, 42).trim)
                logEntry += ("version" -> line.substring(42, 53).trim)
                logEntry += ("subdir" -> line.substring(53, 64).trim)
                logEntry += ("state" -> line.substring(64, 72).trim)
                logEntry += ("sw_master" -> line.splitAt(72)._2.trim)
                logEntries += logEntry
              }
              line = txt.nextLine()
              linNum+=1
            }
          }
        }
      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeei_bcsu
    * @author Anudeep
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeei_bcsu(logfilecontent: String): List[Map[String, String]] = {
    var flag = 0
    var list_map = mutable.ListBuffer[mutable.Map[String, String]]()
    var data_map = mutable.Map[String, String]()
    var line_number = 1
    val txt = new Scanner(logfilecontent)
    val table_col_list = List("UNIT","TRXS","D_CH TELECOM LINKS","D_CH OM LINKS","REAL TCHS")
    while (txt.hasNext) {
      val line = txt.nextLine();
      line match {
        case x if x.contains("ZEEI::BCSU") =>  flag =1;data_map+=("line_number"-> line_number.toString)
        case x if x.contains("FlexiBSC") & flag ==1 =>data_map+=("BSC Name"->"","DTTM"->"");val bscName=x.split("   ").head.split(" ").last.trim;data_map+=("BSC Name" -> bscName); val DTTM = x.split("   ").last.trim; data_map+=("DTTM" -> DTTM)
        case x if x.contains("BCSU-") & flag ==1 =>val row_values = x.split(" ").filter(y => y!="").toList; var tbl_val_map = (table_col_list zip row_values).toMap; var tbl_map:mutable.Map[String, String] = data_map++tbl_val_map; list_map+=tbl_map; tbl_val_map.empty
        case x if x.contains("TOTAL") & flag == 1 => val total_val = x.split(" ").filter(x => x!="");list_map.foreach(_+=("TOTAL TRX"-> total_val(1),"TOTAL REAL TCH"->total_val(2)))
        case x if x.contains("HARDWARE SUPPORTED MAXIMUM TRX CAPACITY :")  & flag ==1=> val hsmtc = x.split(":").last.trim; list_map.foreach(_+=("HARDWARE SUPPORTED MAXIMUM TRX CAPACITY"->hsmtc))
        case x if x.contains("HW AND SW SUPPORTED MAXIMUM TRX CAPACITY:")  & flag ==1=> val hsmtc = x.split(":").last.trim; list_map.foreach(_+=("HW AND SW SUPPORTED MAXIMUM TRX CAPACITY"->hsmtc))
        case x if x.contains("COMMAND EXECUTED")  & flag ==1=> flag=0; data_map.clear()
        case _ => None
      }
      line_number =line_number + 1
    }
    list_map.map(_.toMap).toList
  }

  /**
    * flexi_bsc__bsc_alarm_history_txt__zahp_alarmwithsupplentary
    * @author Anudeep
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__bsc_alarm_history_txt__zahp_alarmwithsupplentary(logfilecontent: String): List[Map[String, String]] = {
    var flag = 0
    var list_map = ListBuffer[Map[String, String]]()
    var data_map = mutable.Map[String, String]()
    var line_number = 1
    val txt = new Scanner(logfilecontent)
    while (txt.hasNext) {
      val line = txt.nextLine();
      line match {
        case x if x.contains("<HIST>") => flag =1; val tblVal =x.replace("<HIST>","").split("    ").map(_.trim).filter(a => a!="");data_map+=("line_number"->line_number.toString,"BSC"-> tblVal(0),"UNIT"-> tblVal(1),"DTTM"->tblVal(3));
        case x if !x.isEmpty & flag ==1 => flag = 2; val tblVal = x.split("   ").map(_.trim).filter(a => a!="").last; data_map+=("issuer"-> tblVal)
        case x if !x.isEmpty & flag ==2 => flag = 3; val tblVal =   x.split(" ").map(_.trim).filter(a => a!=""); data_map+=("Notice_id"->tblVal(0),"Alarm_id"->tblVal(1),"Alarm_description"->x.replace(tblVal(0),"").replace(tblVal(1),"").trim)
        case x if !x.isEmpty & flag ==3 => val tblVal = x.trim;data_map+=("Supp_info"->tblVal);list_map += data_map.toMap;data_map.clear(); flag=0;
        case _ => None
      }
      line_number = line_number + 1
    }
    list_map.toList
  }

  /**
    * flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo
    * @author Anudeep
    * @param logfilecontent
    * @return
    */
  def flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo(logfilecontent: String): List[Map[String, String]] = {
    var flag = 0
    var list_map = ListBuffer[Map[String, String]]()
    var data_map = mutable.Map[String, String]()
    var tbl_map = mutable.Map[String, String]()
    var seg_map = mutable.Map[String, String]()
    var all_val_map = mutable.Map[String, String]()
    var line_number = 1
    var bts_state = 1
    val txt = new Scanner(logfilecontent)
    while (txt.hasNext) {
      val line = txt.nextLine();
      line match {
        case x if x.contains("ZEFO:") => flag=1;data_map+=("line_number"->"","BSC" -> "", "DTTM" -> ""); tbl_map+=( "BCF" -> "", "Type_of_Site" -> "", "Ad_State" -> "", "OPERATIONAL_state" -> "", "RXDL" -> "", "BBU" -> "", "NTIM" -> "", "BTIM" -> "", "MASTER_BCF" -> "", "Clock_Source" -> "", "SENA" -> "", "SM" -> "", "T200F" -> "", "T200S" -> "", "TOPST" -> "", "TRS2" -> "", "PLU" -> "", "PLR" -> "", "RFSS" -> "", "ETPGID" -> "", "EBID" -> "", "VLANID" -> "", "ULTS" -> "", "ULCIR" -> "", "ULCBS" -> "", "PACC" -> "", "BU1" -> "", "BU2" -> "", "PL1" -> "", "PL2" -> "", "DLCIR" -> "", "MEMWT" -> "", "MBMWT" -> "", "MMPS" -> "", "ETMEID" -> "", "BCF UP TO DATE" -> "", "PDV" -> "", "CU PANE IP" -> "", "M PLANE IP" -> "", "CSMUXP" -> "", "PSMUXP" -> "");seg_map+=("segment" -> "");data_map+=("line_number" -> line_number.toString)
        case x if x.contains("FlexiBSC") & flag==1=> val data= x.replace("FlexiBSC","").split("                   "); data_map+=("BSC"->data(0).trim,"DTTM"-> data.last.trim.toString)
        case x if x.contains("BASE CONTROL FUNCTION") =>  val bcf = x.replace("BASE CONTROL FUNCTION","").split(" ")(1).trim; tbl_map+=("BCF" -> bcf)
        case x if x.contains("SITE TYPE") & flag==1 => val type_of_site = x.split("""\.......""").last.trim;tbl_map+=("Type_of_Site"-> type_of_site)
        case x if x.contains("ADMINISTRATIVE STATE") & flag==1 => val ad_state =   x.split("""\......""").last.trim;tbl_map+=("Ad_State" -> ad_state)
        case x if x.contains("OPERATIONAL STATE") & flag==1 => val operational_state = x.split("""\..""").last.trim;tbl_map+=("OPERATIONAL_state" ->operational_state)
        case x if x.contains("RX DIFFERENCE LIMIT") & flag==1 => val rxdl = x.split("""\..""").last.trim;tbl_map+=("RXDL"->rxdl)
        case x if x.contains("BTS BATTERY BACKUP PROCEDURE") & flag==1 => val bbu = x.split("""\..""").last.trim;tbl_map+=("BBU"->bbu)
        case x if x.contains("TRX SHUTDOWN TIMER") & flag==1 => val nitm=  x.split("""\..""").last.trim;tbl_map+=("NTIM" -> nitm)
        case x if x.contains("BTIM") & flag==1 => val btim=x.replace("."," ").replace("BCCH TRX SHUTDOWN TIMER","").replace("(BTIM)","").trim;tbl_map+=("BTIM"-> btim)
        case x if x.contains("MASTER CLOCK BCF") & flag==1 => val master_bcf=x.split("""\.""").last.trim;tbl_map+=("MASTER_BCF"->master_bcf)
        case x if x.contains("CLOCK SOURCE") & flag==1 => val clock_source=x.split("""\..""").last.trim;tbl_map+=("Clock_Source"->clock_source)
        case x if x.contains("SYNCHRONIZATION ENABLED") & flag==1 => val sena=x.split("""\..""").last.trim;tbl_map+=("SENA"->sena)
        case x if x.contains("SYNCHRONIZATION MODE") & flag==1 => val sm=  x.split("""\..""").last.trim;tbl_map+=("SM"->sm)
        case x if x.contains("FACCH LAPDM T200") & flag==1=> val t200f=x.split("""\.""").last.trim;tbl_map+=("T200F"->t200f)
        case x if x.contains("SDCCH LAPDM T200") & flag==1=> val t200s=x.split("""\.""").last.trim;tbl_map+=("T200S"->t200s)
        case x if x.contains("BSS TOP SYNC TIMING LIMIT") & flag==1 => val topst=  x.split("""\.""").last.trim;tbl_map+=("TOPST"->topst)
        case x if x.contains("ADDITIONAL 2 E1/T1 IF") & flag==1=>val trs2 = x.split("""\..""").last.trim;tbl_map+=("TRS2"->trs2)
        case x if x.contains("20W POWER LICENCE USAGE") & flag==1=>val trs2 = x.split("""\..""").last.trim;tbl_map+=("PLU"->trs2)
        case x if x.contains("20W POWER LICENCE REQUESTED") & flag==1=>val plr = x.split("""\..""").last.trim;tbl_map+=("PLR"->plr)
        case x if x.contains("RF SHARING STATE") & flag==1=>val rfss = x.split("""\..""").last.trim;tbl_map+=("RFSS"->rfss)
        case x if x.contains("ETP GROUP ID") & flag==1=>val etpgid = x.split("""\.""").last.trim;tbl_map+=("ETPGID"->etpgid)
        case x if x.contains("ETP BCF ID ") & flag==1=>val ebid = x.split("""\..""").last.trim;tbl_map+=("EBID"->ebid
          )
        case x if x.contains("VLAN ID") & flag==1=>val vlanid = x.split("""\.""").last.trim;tbl_map+=("VLANID"->vlanid)
        case x if x.contains("UPLINK TRAFFIC SHAPING") & flag==1=>val ults = x.split("""\..""").last.trim;tbl_map+=("ULTS"->ults)
        case x if x.contains("UL COMMITTED INFORMATION RATE") & flag==1=>val ulcir = x.split("""\..""").last.trim;tbl_map+=("ULCIR"->ulcir)
        case x if x.contains("UL COMMITTED BURST SIZE") & flag==1=>val ulcir = x.split("""\..""").last.trim;tbl_map+=("ULCBS"->ulcir)
        case x if x.contains("PACKET ABIS CONGESTION CONTROL") & flag==1=>val pacc = x.split("""\..""").last.trim;tbl_map+=("PACC"->pacc)
        case x if x.contains("BU1 ABIS THROUGHPUT THRESHOLD ") & flag==1=>val bu1 = x.split("""\..""").last.trim;tbl_map+=("BU1"->bu1)
        case x if x.contains("BU2 ABIS THROUGHPUT THRESHOLD") & flag==1=>val bu2 = x.split("""\..""").last.trim;tbl_map+=("BU2"->bu2)
        case x if x.contains("PACKET LOSS IN ETHERNET ON DL ABIS PL1 THRESHOLD") & flag==1=>val pl1 = x.split("""\..""").last.trim;tbl_map+=("PL1"->pl1)
        case x if x.contains("PACKET LOSS IN ETHERNET ON DL ABIS PL2 THRESHOLD") & flag==1=>val pl2 = x.split("""\..""").last.trim;tbl_map+=("PL2"->pl2)
        case x if x.contains("DOWNLINK COMMITTED INFORMATION RATE") & flag==1=>val dlcir = x.split("""\..""").last.trim;tbl_map+=("DLCIR"->dlcir)
        case x if x.contains("MAXIMUM ETP MULTIPLEXING WAIT TIME") & flag==1=>val memwt = x.split("""\..""").last.trim;tbl_map+=("MEMWT"->memwt)
        case x if x.contains("MAXIMUM BTS MULTIPLEXING WAIT TIME") & flag==1=>val mbmwt = x.split("""\..""").last.trim;tbl_map+=("MBMWT"->mbmwt)
        case x if x.contains("MAXIMUM MULTIPLEXING PACKET SIZE") & flag==1=>val mmps = x.split("""\..""").last.trim;tbl_map+=("MMPS"->mmps)
        case x if x.contains("ETME HARDWARE ID") & flag==1=>val etmeid = x.split("""\..""").last.trim;tbl_map+=("ETMEID"->etmeid)
        case x if x.contains("BCF UP TO DATE") & flag==1=>val bcf_up_to_date = x.split("""\..""").last.trim;tbl_map+=("BCF UP TO DATE"->bcf_up_to_date)
        case x if x.contains("PACKET DELAY VARIATION") & flag==1=>val pdv = x.split("""\..""").last.trim;tbl_map+=("PDV"->pdv)
        case x if x.contains("BTS C/U-PLANE IP ADDRESS AND SUBNET MASK LENGTH") & flag==1=>val trs2 = x.replace(""".""","").replace("BTS C/U-PLANE IP ADDRESS AND SUBNET MASK LENGTH","").trim;tbl_map+=("CU PANE IP"->trs2)
        case x if x.contains("BTS M-PLANE IP ADDRESS AND SUBNET MASK LENGTH") & flag==1=>val trs2 =  x.replace(""".""","").replace("BTS M-PLANE IP ADDRESS AND SUBNET MASK LENGTH","").trim;tbl_map+=("M PLANE IP"->trs2)
        case x if x.contains("USED CS UDP MUX PORT") & flag==1=>val csmuxp = x.split("""\..""").last.trim;tbl_map+=("CSMUXP"->csmuxp)
        case x if x.contains("USED PS UDP MUX PORT") & flag==1=>val psmuxp = x.split("""\..""").last.trim;tbl_map+=("PSMUXP"->psmuxp)
        case x if x.contains("SEG-") & flag==1=>val seg= x.split(" ").last.trim; seg_map+=("segment"->seg);bts_state=1;
        case x if x.contains("BTS-") & flag==1=>val trs2 = x.split("""\..""").last.trim;seg_map+=("BTS_Operational_State"->trs2);if(bts_state<4){all_val_map=tbl_map++seg_map++data_map;list_map+=all_val_map.toMap;all_val_map.clear();};bts_state=bts_state+1;if(bts_state>3){bts_state=1;}
        case x if x.contains("COMMAND EXECUTED")& flag==1=>bts_state=1;flag=0;data_map.clear()
        case _ => None
      }
      line_number = line_number+1
    }
    list_map.toList
  }

  def flexi_bsc__ip_configurations__fb_ipc_internal_ip(logfilecontent: String): List[Map[String, String]] = {
    var flag = 0
    var list_map = ListBuffer[Map[String, String]]()
    var data_map = mutable.Map[String, String]()
    var tbl_map = mutable.Map[String, String]()
    var all_val_map = mutable.Map[String, String]()
    var line_number = 1
    var bts_state = 1
    val txt = new Scanner(logfilecontent)
    while (txt.hasNext) {
      val line = txt.nextLine();
      line match {
        case x if x.contains("BCSU") & (x.trim.split("   ").length < 2) & x.trim.length < 7 =>flag=1; val bcs=x.trim; data_map +=("line_number"->line_number.toString,"BCSU"->bcs);tbl_map+=("VLAN"->"","STATE"->"","MTU"->"","Attr"->"","IP ADDR"->"")
        case x if !x.contains("->") & (x.contains("EL") || x.contains("VLAN"))  & flag==1 => val tbl_val = x.trim.split("  ").filter(_!="");if(tbl_val.length==5){tbl_map+=("VLAN"->tbl_val(0),"STATE"->tbl_val(1),"MTU"->tbl_val(2),"Attr"->tbl_val(3),"IP ADDR"->tbl_val(4));all_val_map=data_map++tbl_map;list_map+=all_val_map.toMap;all_val_map.clear()}
        case x if x.contains("->EL0") & flag==1 => None
        case x if x.contains("BCSU") & (x.trim.split("   ").length > 1) => flag=0
        case x if x.trim.length==0=> flag=0;
        case _ => None
      }
      line_number = line_number+1
    }
    list_map.toList
  }

  def flexi_bsc__zeei__zeei_et(logfilecontent: String): List[Map[String, String]] = {
    var flag = 0
    var list_map = ListBuffer[Map[String, String]]()
    var data_map = mutable.Map[String, String]()
    var tbl_map = mutable.Map[String, String]()
    var all_val_map = mutable.Map[String, String]()
    var line_number = 1
    var line_number_current_curser = 1
    val txt = new Scanner(logfilecontent)
    while (txt.hasNext) {
      val line = txt.nextLine();
      if (line.contains("BCF-")) {
        flag=1
        data_map+=("line_number"->line_number.toString,"BCF"->"","BTS_TYPE"->"","State"->"","Op_State"->"","BCSU"->"","BCF_DCH_NAME"->"","LINK_STATE"->"","LAC"->"","CI"->"","BTS_ID"->"","bts_state"->"","bts_op_state"->"","Param1"->"","Param2"->"","Param3"->"","Param4"->"")
        data_map+=("BCF"->line.substring(0,8),"BTS_TYPE"->line.substring(9,19).trim,"State"->line.substring(23,25).trim,"Op_State"->line.substring(25,28).trim,"BCSU"->line.substring(62,64).trim,"BCF_DCH_NAME"->line.substring(64,70).trim,"LINK_STATE"->line.substring(70).trim)
      }
      if (line.contains("BTS-") & flag==1){
        val data = line.trim.split(" ").filter(_!="")
        data_map+=("LAC"->data(0),"CI"->data(1),"BTS_ID"->data(2),"bts_state"->data(3),"bts_op_state"->data(4),"Param1"->data(5),"Param2"->data(6))
        line_number_current_curser=line_number+1
      }
      if (line_number_current_curser==line_number){
        val data = line.trim.split(" ").filter(_!="")
        data_map+=("Param3"->data.head,"Param4"->data.last)
      }
      if (line.contains("TRX-")){
        tbl_map+=("Network"->"","TRX_id"->"","ADMINSTATE"->"","OPSTATE"->"","FREQ"->"","FRT"->"","ETPCM"->"","DETAILS"->"")
          tbl_map+=("Network"->line.substring(0,12).trim)
          tbl_map+=("TRX_id"->line.substring(13,21).trim)
          tbl_map+=("ADMINSTATE"->line.substring(22,24).trim)
          tbl_map+=("OPSTATE"->line.substring(25,29).trim)
          tbl_map+=("FREQ"->line.substring(30,38).trim)
          tbl_map+=("FRT"-> line.substring(38,41).trim)
          tbl_map+=("ETPCM"->line.substring(43,45).trim)
          tbl_map+=("DETAILS"->line.substring(47).trim)
        all_val_map=data_map++tbl_map
        list_map+=all_val_map.toMap
        tbl_map.clear()
        all_val_map.clear()
      }
      if (line.length==0){
        flag=0
      }

     line_number = line_number+1
    }
    list_map.toList
  }

/**
    * ******************************************************
    * flexi_bsc__bsc_alarm_history_txt__alarm_sup
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ****************************************************
    */
  def flexi_bsc__bsc_alarm_history_txt__alarm_sup(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var log_start_line = 0
    var list_map = mutable.ListBuffer[Map[String, String]]()
    val log_starting_text = "ALARM"
    val log_ending_text = "END OF ALARM"
    var line = ""
    var log_started = false
    var skip_nextline = false
    while (txt.hasNext) {
      if(skip_nextline) skip_nextline = false
      else {
        line = txt.nextLine()
      }
      //      logEntry += lineNum -> line
      if (!log_started && line.contains(log_starting_text)) {
        log_started = true
        log_start_line = lineNum + 3
      }
      if (line.contains(log_ending_text)) {
        log_start_line = -1
      }
      if (lineNum == log_start_line && !line.isEmpty) {
        var inner_index = 0
        var my_map = mutable.Map[String, String]()
        my_map += "line_number" -> lineNum.toString
        while (inner_index < 4) {
          inner_index match {
            case 0 =>
              if (line.contains("HIST")) {
                my_map += ("BSC" -> line.substring(11, 33).replaceAll("""(?m)\s+$""", ""),
                  "Unit" -> line.substring(34, 45).replaceAll("""(?m)\s+$""", ""),
                  "Alarm_Type" -> line.substring(46, 55).replaceAll("""(?m)\s+$""", ""),
                  "DTTM" -> line.substring(56, line.length).replaceAll("""(?m)\s+$""", ""))
              }
              else {
                my_map += ("BSC" -> line.substring(10, 33).replaceAll("""(?m)\s+$""", ""),
                  "Unit" -> line.substring(34, 45).replaceAll("""(?m)\s+$""", ""),
                  "Alarm_Type" -> line.substring(46, 55).replaceAll("""(?m)\s+$""", ""),
                  "DTTM" -> line.substring(56, line.length).replaceAll("""(?m)\s+$""", ""))
              }
              line = txt.nextLine()
              lineNum += 1
            case 1 =>
              my_map += ("Severity" -> line.substring(0, 3).replaceAll("""(?m)\s+$""", ""),
                "Not_type" -> line.substring(11, 21).replaceAll("""(?m)\s+$""", ""),
                "Param1" -> line.substring(22, 33).replaceAll("""(?m)\s+$""", "")
              //  "Issuer" -> line.substring(34, line.length).replaceAll("""(?m)\s+$""", "")
              )
              line = txt.nextLine()
              lineNum += 1
            case 2 =>
              my_map += ("Trans_id" -> line.substring(4, 10).replaceAll("""(?m)\s+$""", ""),
                "Alarm_id" -> line.substring(11, 15).replaceAll("""(?m)\s+$""", ""),
                "Alarm_text" -> line.substring(16, line.length).replaceAll("""(?m)\s+$""", ""))
              line = txt.nextLine()
              lineNum += 1
            case 3 =>
              if (line == ""){
                var index_loop = 1
                while (index_loop < 10) {
                  my_map += (s"Supp$index_loop" -> "")
                  index_loop += 1
                }
                lineNum = lineNum - 1
                skip_nextline = true
              }
              else {
                val complete_log_line = line.substring(4, line.length)
                val log_list = complete_log_line.split(" ")
                var index_loop = 1
                log_list.foreach(value => {
                  my_map += (s"Supp$index_loop" -> value)
                  index_loop += 1
                })
                while (index_loop < 10) {
                  my_map += (s"Supp$index_loop" -> "")
                  index_loop += 1
                }
              }
          }
          inner_index += 1
        }
        list_map += my_map.toMap
        log_start_line = lineNum + 2
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexi_bsc__bsc_alarm_history__data_science
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__bsc_alarm_history__data_science(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if(line.startsWith("    <HIST>")){
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("orignal_log_file_name" -> "BSC_Alarm_history.txt")
        logEntry += ("parsed_log_file_name" -> "BSC_Alarm_history_parsed.csv")
        logEntry += ("bsc_identifier" -> line.trim.split("\\s+").lift(1).getOrElse("").trim)
        logEntry += ("bsc_unit" -> line.trim.split("\\s+").lift(2).getOrElse("").trim)
        logEntry += ("alarm_type" -> line.trim.split("\\s+").lift(3).getOrElse("").trim)
        logEntry += ("timestamp" -> (line.trim.split("\\s+").lift(4).getOrElse("").trim + " " + line.trim.split("\\s+").lift(5).getOrElse("").trim))
        line = txt.nextLine()
        linNum+=1
        val severity  = line.trim.split("\\s+")(0).trim
        val event_category = line.trim.split("\\s+")(1).trim
        logEntry += ("severity" -> severity)
        logEntry += ("event_category" -> event_category)
        line = txt.nextLine()
        linNum+=1
        var value = line.trim.split("\\s+")
        logEntry += ("event_id" -> value(0).trim.replaceAll("[()]",""))
        logEntry += ("alarm_id" -> value(1).trim)
        var msg = ""
        for(i<- 2 to value.length-1){
          msg = msg + " " +value(i)
        }
        logEntry += ("alarm_message" -> msg.trim)
        line = txt.nextLine()
        linNum+=1
        logEntry += ("event_criticality" -> (severity+"_" + event_category))
        logEntry += ("event" -> (severity+"_" + event_category + "_" + value(1)))
        logEntries+=logEntry


      }

      linNum+=1

    }
    logEntries.toList
  }

  /**
    * flexi_bsc__ip_configurations__zqri_ip_configuration
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_bsc__ip_configurations__zqri_ip_configuration(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val skip = 4
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZQRI;")) {

        while (txt.hasNextLine && !line.contains("FlexiBSC")) {
          line = txt.nextLine()
          linNum += 1
        }
        logEntry += ("line_number" -> linNum.toString)
        if(line.split("\\s+").length >= 3){
          logEntry += ("BSC" -> line.split("\\s+")(1).trim)
          logEntry += ("DTTM" -> (line.split("\\s+")(2).trim + " " + line.split("\\s+")(3).trim))
        }


        while (txt.hasNextLine && !line.contains("------------ ----- ----- --------")) {
          line = txt.nextLine()
          linNum += 1
        }
        if (txt.hasNextLine) {
          while (txt.hasNextLine && !(line.contains("COMMAND EXECUTED"))){

            while (txt.hasNextLine && !(line.trim.split("\\s+").length == 2 && !line.startsWith(" "))) {
              line = txt.nextLine()
              linNum += 1
            }
            if (txt.hasNextLine) {

              logEntry += ("Unit" -> line.trim.split("\\s+")(0))
              logEntry += ("UNIT_PIU" -> line.trim.split("\\s+")(1))

              while (txt.hasNextLine && line.trim.length!=0) {

                if (line.startsWith("   ->IL")) {
                  logEntry += ("Interface" -> line.substring(1, 12).trim.replace("->", ""))
                  logEntry += ("Interface1" -> line.substring(12, 25).trim)
                  logEntry += ("Attribute" -> line.substring(25, 36).trim)
                  logEntry += ("IP_ADD" -> line.splitAt(36)._2.trim)
                  logEntry += ("VLAN_ID" -> "")
                  logEntry += ("STATE" -> "")
                  logEntry += ("MTU" -> "")
                  logEntries += logEntry
                } else if (line.endsWith("0/0")) {

                  logEntry += ("Interface1" -> line.trim.split("\\s+")(0).trim)
                  logEntry += ("Interface" -> "")
                  logEntry += ("Attribute" -> "")
                  logEntry += ("IP_ADD" -> "")
                  logEntry += ("VLAN_ID" -> line.trim.split("\\s+")(1).trim)
                  logEntry += ("STATE" -> "")
                  logEntry += ("MTU" -> "")
                  logEntries += logEntry

                } else if (line.trim.split("\\s+").length == 5) {
                  logEntry += ("Interface1" -> "")
                  logEntry += ("Interface" -> line.trim.split("\\s+")(0).trim)
                  logEntry += ("STATE" -> line.trim.split("\\s+")(1).trim)
                  logEntry += ("MTU" -> line.trim.split("\\s+")(2).trim)
                  logEntry += ("Attribute" -> line.trim.split("\\s+")(3).trim)
                  logEntry += ("IP_ADD" -> line.trim.split("\\s+")(4).trim)
                  logEntry += ("VLAN_ID" -> "")
                  logEntries += logEntry
                }
                line = txt.nextLine()
                linNum += 1
              }
            }
          }

        }
      }
      linNum += 1
    }

    logEntries.toList


  }

}