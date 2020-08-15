package com.nokia.parsing.parsers

import java.util.Scanner

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParseAliceFlexiMRWCDMABTS {

  /**
    * flexi_mr_wcdma_bts__pm__any_module_pm
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def flexi_mr_wcdma_bts__pm__any_module_pm(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      var split = line.split("\\s+")
      if(split.size>4){
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("hex_code" -> ( split(0).trim))
        logEntry += ("module_id" -> ( split(1).trim))
        logEntry += ("time" -> ( split(2).replaceAll("[<>]", "") trim))
        logEntry += ("int_value" -> ( split(3).trim))
        logEntry += ("severity" -> ( split(4).splitAt(split(4).indexOf('/'))._1.trim))
        logEntry += ("source" -> ( split(4).splitAt(split(4).indexOf('/'))._2.trim))
        var msg = ""
        for (i <- 5 to split.length - 1) {
          msg = msg + " " + split(i)
        }
        logEntry += ("message" -> ( msg.trim))
        logEntries += logEntry
      }
      linNum += 1
    }
    logEntries.toList
  }
  
  /**
    * flexi_mr_wcdma_bts__1011_rawalarmhistory__x011_rawalarmhistory
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_mr_wcdma_bts__1011_rawalarmhistory__x011_rawalarmhistory(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.endsWith("faults")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("fault_status" -> ( line.stripSuffix("faults").trim))
        for (i <- 1 to 4) {
          line = txt.nextLine()
        }
        linNum += 1

        while (line.length != 0) {

          var value = line.split("\\t")
          logEntry += ("date_time" -> ( value(0).trim))
          logEntry += ("fault_id" -> ( value(1).stripSuffix(")").stripPrefix("(") trim))
          logEntry += ("fault_name" -> ( value(2).trim))
          logEntry += ("fault_source" -> ( value(3).trim))
          logEntry += ("subunit" -> ( value(4).split('(')(0).trim))
          logEntry += ("subunit2" -> ( value(4).split('(')(1).stripSuffix(")").trim))
          logEntry += ("detecting_unit" -> ( value(5).trim))
          logEntry += ("sicad1" -> ( value(7).trim))
          logEntry += ("sicad2" -> ( value(8).trim))
          logEntry += ("sicad3" -> ( value(9).trim))
          logEntry += ("sicad4" -> ( value(10).trim))
          logEntry += ("current" -> ( value(11).trim))
          logEntry += ("min" -> ( value(12).trim))
          logEntry += ("h" -> ( value(13).trim))
          logEntry += ("day" -> ( value(14).trim))
          logEntry += ("all" -> ( value(15).trim))
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
    * flexi_mr_wcdma_bts__alarms_xml__alarms_xml
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_mr_wcdma_bts__alarms_xml__alarms_xml(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("<Alarms>") &&(line.contains("<AlarmObservation>")&& !line.isEmpty)) {
        txt.nextLine()
        line = txt.nextLine()
        linNum+=2
        if(line.contains("<AlarmInformation>")){
          line = txt.nextLine()
          linNum+=1
          if(line.contains("alarmNumber>")&&line.contains("</_alarmNumber")){
            logEntry += ("line_number" -> linNum.toString)
            logEntry += ("alarm_number" -> line.split("alarmNumber>")(1).stripSuffix("</_").trim)
            line = txt.nextLine()
            linNum+=1
            logEntry += ("alarm_activity" -> line.split("alarmActivity>")(1).stripSuffix("</_").trim)
            line = txt.nextLine()
            linNum+=1
            logEntry += ("severity" -> line.split("severity>")(1).stripSuffix("</_").trim)
            txt.nextLine()
            line = txt.nextLine()
            linNum+=2
            logEntry += ("alarm_detail" -> line.split("alarmDetail>")(1).stripSuffix("</_").trim)
            line = txt.nextLine()
            linNum+=1
            logEntry += ("alarm_detail_nbr" -> line.split("alarmDetailNbr>")(1).stripSuffix("</_").trim)
            txt.nextLine()
            line = txt.nextLine()
            linNum+=2
            if(line.contains("featureCode>")&& line.contains("</featureCode")){
              logEntry+=("feature_code" -> line.split("featureCode>")(1).stripSuffix("</_").trim)
              var (value,lin) = flexi_mr_wcdma_bts__alarms_xml__alarms_xml_parsing(line,txt,linNum)
              logEntry++=value
              linNum = lin

            }else if(line.contains("alarmAdditionalInfo")){
              logEntry+=("feature_code" -> "")
              var (value,lin) = flexi_mr_wcdma_bts__alarms_xml__alarms_xml_parsing(line,txt,linNum)
              logEntry++=value
              linNum = lin

            }

            logEntries += logEntry

          }

        }

      }
      linNum += 1
    }

    logEntries.toList
  }

  def flexi_mr_wcdma_bts__alarms_xml__alarms_xml_parsing(lin:String, txt : Scanner,lineNum:Int) ={

    var line = lin
    var linNum = lineNum
    var logEntry = mutable.Map[String, String]()
    txt.nextLine();line = txt.nextLine()
    linNum+=2
    logEntry+=("event_type" -> line.split("eventType>")(1).stripSuffix("</_").trim)
    line = txt.nextLine()
    linNum+=1
    logEntry+=("observation_time" -> line.split("observationTime>")(1).stripSuffix("</_").trim)
    for(i<-1 to 5){
      line = txt.nextLine()
    }
    linNum+=5
    logEntry+=("cell_obs_time" -> line.split("cellularObjectType>")(1).stripSuffix("</_").trim)
    line = txt.nextLine()
    linNum+=1
    logEntry+=("cell_obs_index" -> line.split("cellularObjectIndex>")(1).stripSuffix("</_").trim)
    for(i<-1 to 4){
      line=txt.nextLine()
    }
    linNum+=4
    logEntry+=("id" -> line.split("id>")(1).stripSuffix("</_").trim)
    line = txt.nextLine()
    linNum+=1
    logEntry+=("type_plug_unit" -> line.split("typeOfPlugInUnit>")(1).stripSuffix("</_").trim)
    line = txt.nextLine()
    linNum+=1
    logEntry+=("unit_nbr" -> line.split("unitNumber>")(1).stripSuffix("</_").trim)
    line = txt.nextLine()
    linNum+=1
    logEntry+=("subunit_nbr" -> line.split("subUnitNumber>")(1).stripSuffix("</_").trim)
    (logEntry,linNum)


  }

  /**
    * flexi_mr_wcdma_bts__1011_rawalarmhistory_txt__active_alarms
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */
  def flexi_mr_wcdma_bts__1011_rawalarmhistory_txt__active_alarms(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("Active faults")) {
        for (i <- 1 to 4) {
          line = txt.nextLine()
        }
        linNum += 1
        logEntry += ("line_number" -> linNum.toString)
        while (line.length != 0) {

          var value = line.split("\\t")
          logEntry += ("date_time" -> ( value(0).trim))
          logEntry += ("fault_id" -> (value(1).stripSuffix(")").stripPrefix("(") trim))
          logEntry += ("fault_name" -> (value(2).trim))
          logEntry += ("fault_source" -> (value(3).trim))
          logEntry += ("subunit" -> (value(4).split('(')(0).trim))
          logEntry += ("subunit2" -> (value(4).split('(')(1).stripSuffix(")").trim))
          logEntry += ("detecting_unit" -> ( value(5).trim))
          logEntry += ("sicad1" -> ( value(7).trim))
          logEntry += ("sicad2" -> (value(8).trim))
          logEntry += ("sicad3" -> ( value(9).trim))
          logEntry += ("sicad4" -> (value(10).trim))
          logEntry += ("current" -> (value(11).trim))
          logEntry += ("min" -> (value(12).trim))
          logEntry += ("h" -> (value(13).trim))
          logEntry += ("day" -> (value(14).trim))
          logEntry += ("all" -> (value(15).trim))
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
    * flexi_mr_wcdma_bts__amr_and_hspa_call_ru40_traceId1_moc__html_parsing
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def flexi_mr_wcdma_bts__amr_and_hspa_call_ru40_traceId1_moc__html_parsing(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("Order")&&line.contains("Time")&&line.contains("UE")&&line.contains("NodeB")&&
        line.contains("RNC")&&line.contains("CN/DRNC")) {

        line = txt.nextLine()
        linNum+=1
        logEntry += ("line_number" -> linNum.toString)
        while(!line.startsWith("</pre>")) {
          logEntry += ("order" -> line.split("<TT>")(1).split("</FONT><FONT")(0).trim.split("\\s+")(0).trim)
          logEntry += ("time" -> line.split("<TT>")(1).split("</FONT><FONT")(0).trim.split("\\s+")(1).trim)
          logEntry += ("details" -> (line.split("</FONT><a href=\"javascript:func_")(1).split("</a><a>")(0).split(">")(2).trim))
          logEntries += logEntry
          line = txt.nextLine()
          linNum+=1
        }

      }
      linNum += 1
    }

    return logEntries.toList
  }



}
