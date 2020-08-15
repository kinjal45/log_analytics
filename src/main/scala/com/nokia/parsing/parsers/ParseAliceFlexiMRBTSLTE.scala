package com.nokia.parsing.parsers

import java.util.Scanner

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParseAliceFlexiMRBTSLTE {

  /**
    * flexi_mr_bts_lte__rawalarmhistory__mrbts_rawalarmhistory
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def flexi_mr_bts_lte__rawalarmhistory__mrbts_rawalarmhistory(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var log_start_lineNum = 0
    var param = ""
    var param_line_num = 0
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.contains("faults")) {
        log_started = true
        log_start_lineNum = lineNum + 4
        param_line_num = lineNum
        param = line.split(" ").filter(x => x != "")(0)
      }

      if (lineNum == log_start_lineNum) {
        if (line == "") {
          log_start_lineNum = 0
          log_started = false
        }
        else {
          var my_map = mutable.Map[String, String]()
          my_map += ("line_number" -> param_line_num.toString,
            "Param" -> param,
            "DTTM" -> line.split("\\t")(0).replaceAll("""(?m)\s+$""", ""),
            "FaultId" -> line.split("\\t")(1).replaceAll("""[()]""", ""),
            "Fault_Description" -> line.split("\\t")(2).replaceAll("""(?m)\s+$""", ""),
            "FaultSrc" -> line.split("\\t")(3).split("\\s+").lift(0).getOrElse("").replaceAll("""(?m)\s+$""", ""),
            "SerialNumber" -> line.split("\\t")(3).split("\\s+").lift(1).getOrElse("").replaceAll("""[()]""", ""))
          list_map += my_map.toMap
          log_start_lineNum += 1
        }
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * flexi_mr_bts_lte__scf_xml__fetch_lnrel
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */


  def flexi_mr_bts_lte__scf_xml__fetch_lnrel(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("\"NOKLTE:LNREL\"")) {
        logEntry += "line_number" -> linNum.toString
        logEntry += ("LNREL" -> line.split("distName=\"")(1).split("\"")(0).trim)
        line = txt.nextLine()
        linNum += 1

        while (!line.contains("</managedObject>")) {
          if (line.contains("<p name=\"") && line.contains("</p>")) {
            logEntry += ("PARAM" -> line.split("<p name=\"")(1).split(">")(0).stripSuffix("\"").trim)
            logEntry += ("VALUE" -> line.split("<p name=\"")(1).split(">")(1).split("<")(0).trim)
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
    * flexi_mr_bts_lte__trs__parse_ethlk
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_mr_bts_lte__trs__parse_ethlk(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("<managedObject class=\"NOKLTE:ETHLK\" distName=\"")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("ethlk" -> ( line.split("<managedObject class=\"NOKLTE:ETHLK\" distName=\"")(1).split("\"")(0).trim))
        line = txt.nextLine()
        linNum += 1

        while (!line.contains("</managedObject>")) {
          if (line.contains("<p name=\"") && line.contains("</p>")) {
            logEntry += ("param" -> ( line.split("<p name=\"")(1).split(">")(0).stripSuffix("\"").trim))
            logEntry += ("value" -> ( line.split("<p name=\"")(1).split(">")(1).split("<")(0).trim))
            logEntries += logEntry
          }
          line = txt.nextLine()
          linNum += 1
        }

      }

      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_mr_bts_lte__messages_2__read_messages_2
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_mr_bts_lte__messages_2__read_messages_2(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (!line.isEmpty && line.contains("\\t")) {
        //println("if")
        var split = line.split("\\t")
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("date" -> ( split(0).split("\\s+")(0).trim))
        logEntry += ("na" -> ( split(0).split("\\s+")(1).trim))
        logEntry += ("error_type" -> ( split(0).split("\\s+")(2).trim))
        logEntry += ("message" -> ( split(1).trim))
        logEntries += logEntry

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_mr_bts_lte__runtime_btsom_log__1011_runtime
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_mr_bts_lte__runtime_btsom_log__1011_runtime(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("FCT-1011")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("runtime_btsom" -> ( "FCT-1011" + line.split("FCT-1011")(1).trim))
        logEntries += logEntry


      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_mr_bts_lte__011_rawalarmhistory__011_rawalarmhistory_alldata
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_mr_bts_lte__011_rawalarmhistory__011_rawalarmhistory_alldata(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("T") && linNum == 1) {

        for (_ <- 0 to 2; if txt.hasNextLine) {

          while (txt.hasNextLine && !(line.endsWith("faults") || line.endsWith("Faults"))) {
            line = txt.nextLine()
            linNum += 1
          }

          if ((line.endsWith("faults") || line.endsWith("Faults"))) {

            logEntry += ("line_number" -> linNum.toString)
            logEntry += ("fault_status" -> line.stripSuffix("faults").trim)

            txt.nextLine;
            line = txt.nextLine();

            if (line.contains("TimeStamp") && line.contains("FaultId") && line.contains("FaultSource") &&
              line.contains("NumOfFaults (current/last 10s, min, h, day, all)")) {

              for (i <- 1 to 2; if txt.hasNextLine) {
                line = txt.nextLine()
              }
              linNum += 4


              while (line.length != 0) {
                var value = line.split("\\t")
                logEntry += ("date_time" -> (value(0).trim))
                logEntry += ("fault_id" -> (value(1).stripSuffix(")").stripPrefix("(").trim))
                logEntry += ("fault_name" -> (value(2).trim))
                logEntry += ("source" -> (value(3).trim))
                var fault_rised = ""
                for (i <- 4 to value.length - 1) {
                  fault_rised = fault_rised + value(i)
                }
                logEntry += ("fault_rised" -> (fault_rised.trim))
                logEntries += logEntry
                line = txt.nextLine()
                linNum += 1
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
    * flexi_mr_bts_lte__cupl__parse_scf_xml
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_mr_bts_lte__cupl__parse_scf_xml(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("<p name=\"") && line.contains("</p>")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("parameter" -> ( line.split("<p name=\"")(1).split("\">")(0).trim))
        logEntry += ("value" -> ( line.split("\">")(1).split("</p>").lift(0).getOrElse("").trim))
        logEntries += logEntry
      }
      linNum += 1
    }

    logEntries.toList


  }

  /**
    * flexi_mr_bts_lte__123d_rawalarmhistory__123d_rawalarmhistory
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_mr_bts_lte__123d_rawalarmhistory__123d_rawalarmhistory(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.endsWith("Faults")) {
        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("fault_status" -> (line.stripSuffix("faults").trim))

        while (txt.hasNextLine && !(line.startsWith("TimeStamp") && line.contains("FaultId") && line.contains("FaultSource") && line.contains("Unit")
          && line.contains("(") && line.contains(")"))) {
          line = txt.nextLine()
          linNum += 1
        }
        line = txt.nextLine()
        linNum+=1


        while (line.length != 0 && txt.hasNextLine) {
          if (line.contains("(") && line.contains(")")) {
            logEntry += ("date_time" -> (line.split("\\s+")(0).trim))
            logEntry += ("fault_id" -> (line.split('(')(1).split(')')(0).trim))
            logEntry += ("fault_desc" -> (line.split('(')(1).split(')')(1).split("\\s+")(0).trim))
            logEntry += ("fault_source" -> (line.split('(')(1).split(')')(1).split("\\s+")(1).trim))
            logEntry += ("unit" -> (line.split('(')(2).split(')')(0).trim))
            logEntry += ("rest_data" -> (line.split('(')(2).split(')')(1).trim))
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
    * flexi_mr_bts_lte__1011_blackbox__parse_blackbox
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_mr_bts_lte__1011_blackbox__parse_blackbox(logfilecontent: String): List[Map[String, String]] = {

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
          val date_time = split(0).trim.replaceAll("[<>]", "").split("T")
          logEntry += ("date" -> ( date_time.lift(0).getOrElse("")))
          logEntry += ("time" -> ( date_time.lift(1).getOrElse("").split("\\.").lift(0).getOrElse("")))
          var event = ""
          for (i <- 1 to split.length - 1) {
            event = event + " " + split(i)
          }
          logEntry += ("event" -> ( event.trim))

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
    * flexi_mr_bts_lte__runtime_default__1011_runtime_default
    * @author Kaan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_mr_bts_lte__runtime_default__1011_runtime_default(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      val line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("FCT-1011-0-")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("runtime_default" -> ("FCT-1011-0-" + line.split("FCT-1011-0-")(1).trim))
        logEntries += logEntry

      }
      linNum += 1
    }

    return logEntries.toList
  }

  /**
    * flexi_mr_bts_lte__ram_fault_history__fault_2
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_mr_bts_lte__ram_fault_history__fault_2(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if(line.endsWith("faults:")){
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("type" -> line.stripSuffix("faults:").trim)

        for(i <-1 to 4){
          line = txt.nextLine()
          linNum+=1
        }

        while(!line.isEmpty){
          var value = line.split("\\t")
          logEntry += ("date" -> value(0).trim)
          logEntry += ("fault" -> value(1).split('(')(0).trim)
          logEntry += ("fault_id" -> value(1).split('(')(1).stripSuffix(")").trim)
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
    * flexi_mr_bts_lte__rawalarmhistory__alarm_1
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_mr_bts_lte__rawalarmhistory__alarm_1(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("T")&&line.contains("Z")&&line.contains(":")&& line.contains("-")) {

        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("date_time" -> line.trim)

        for(i <-1 to 2; if txt.hasNextLine){
          line = txt.nextLine()
          linNum+=1
        }
        if(line.endsWith("faults")){
          logEntry += ("param" -> line.stripSuffix("faults").trim)

          for(i <-1 to 4; if txt.hasNextLine){
            line = txt.nextLine()
            linNum+=1
          }

          while(!line.isEmpty){
            var value = line.split("\\t")
            logEntry += ("date_time1" -> value(0).trim)
            logEntry += ("fault_id" -> value(1).trim)
            logEntry += ("fault_description" -> value(2).trim)
            logEntry += ("fault_src" -> value(3).split('(')(0).trim)
            logEntry += ("serial_number" -> value(3).split('(').lift(1).getOrElse("").stripSuffix(")"))
            logEntries += logEntry
            line=txt.nextLine()
            linNum+=1
          }
        }

      }
      linNum += 1
    }

    return logEntries.toList
  }




}