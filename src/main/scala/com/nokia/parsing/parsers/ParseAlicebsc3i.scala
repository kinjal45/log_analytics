package com.nokia.parsing.parsers

import java.util.Scanner

import scala.collection.mutable.ListBuffer

object ParseAlicebsc3i {

  /**
    * bsc3i__switch_info__zw6g_topology
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def bsc3i__switch_info__zw6g_topology(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)

    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("T1 -") && line.contains(" (CURRENT IS T") && line.contains(", NETWORK ELEMENT ")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("topology" -> line.split(" \\(CURRENT IS T")(1).split(", NETWORK ELEMENT ")(0).trim)
        logEntry += ("bsc_type" -> line.split(" \\(CURRENT IS T")(1).split(", NETWORK ELEMENT ")(1).stripSuffix(")").trim)
        logEntries += logEntry
      }


      linNum += 1
    }
    logEntries.toList
  }
  def bsc3i__bsc_alarm__zahp_zaho_bsc_alarms(logfilecontent: String): List[Map[String, String]] = {
    println("Exec bsc3i__bsc_alarm__zahp_zaho_bsc_alarms")
    val txt = new Scanner(logfilecontent)
    var linNum = 1
    var logCompleted,parentlogstarted, logstarted,firstloopstarted,secondloopstarted ,thirdloopstarted,fourthloopstarted= false
    var line = ""
    var logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ZAHP")) {
        parentlogstarted = true
      }
      else if(parentlogstarted && line.contains("ALARM")){
        logstarted = true
      }
      else if (line.contains("END OF ALARM ")) {
        logCompleted = true
        parentlogstarted=false
        logstarted=false
      }

      var firstlineNo,secondLineNo,thirdLineNo,fourthLineNo=0
      while (logstarted  && !logCompleted) {
        val actuallineno = linNum+1
        line = txt.nextLine()
        if (!line.contains("END OF ALARM ") && !line.isEmpty) {
          if (line.contains("<HIST>"))
            firstlineNo = actuallineno
          if (line.trim.length > 0 && firstloopstarted && secondLineNo == 0 && actuallineno > firstlineNo && firstlineNo != 0)
            secondLineNo = actuallineno
          if (line.trim.length > 0 && secondloopstarted && thirdLineNo == 0 && actuallineno > secondLineNo && secondLineNo != 0)
            thirdLineNo = actuallineno
          if (line.trim.length > 0 && thirdloopstarted && fourthLineNo == 0 && actuallineno > thirdLineNo && thirdLineNo != 0)
            fourthLineNo = actuallineno

          if (actuallineno == firstlineNo) {
            secondLineNo = 0
            thirdLineNo = 0
            fourthLineNo = 0
            firstloopstarted = true
            val firstlinedata=line.split("\\s+")
            /*   logEntry += ("line_number" -> (linNum+1).toString)
               logEntry += ("BSC" -> line.substring(11, 33))
               logEntry += ("Unit" -> line.substring(34, 45))
               logEntry += ("Alarm_Type" -> line.substring(46, 55))
               logEntry += ("DTTM" -> line.substring(56, line.length))
   */
            logEntry += ("line_number" -> (linNum+1).toString)
            logEntry += ("BSC" -> firstlinedata(0).trim)
            logEntry += ("Unit" -> firstlinedata(1).trim)
            logEntry += ("Alarm_Type" -> firstlinedata(2).trim)
            logEntry += ("DTTM" -> firstlinedata.slice(2,firstlinedata.length).mkString(" "))
          }
          else if (actuallineno == secondLineNo) {
            secondloopstarted = true
            /*    logEntry += ("Severity" -> line.substring(0, 3).replaceAll("""(?m)\s+$""", ""))
 logEntry += ("Not_type" -> line.substring(11, 21).replaceAll("""(?m)\s+$""", ""))
 logEntry += ("Param1" -> line.substring(22, 33).replaceAll("""(?m)\s+$""", ""))
 logEntry += ("Issuer" -> line.substring(34, line.length).replaceAll("""(?m)\s+$""", ""))
*/
            val secondlinedata=line.split("\\s+")
            logEntry += ("Severity" -> secondlinedata(0).trim)
            logEntry += ("Not_type" -> secondlinedata(1).trim)
            logEntry += ("Param1" -> secondlinedata(2).trim)
            logEntry += ("Issuer" -> secondlinedata.last.trim)
          }
          else if (actuallineno == thirdLineNo) {
            thirdloopstarted = true
            /*    logEntry += ("Trans_id" -> line.substring(4, 10).replaceAll("""(?m)\s+$""", ""))
            logEntry += ("Alarm_id" -> line.substring(11, 15).replaceAll("""(?m)\s+$""", ""))
            logEntry += ("Alarm_text" -> line.substring(16, line.length).replaceAll("""(?m)\s+$""", ""))
*/
            val thirdlinedata=line.split("\\s+")
            logEntry += ("Trans_id" -> thirdlinedata.filter(x=>x.contains("(")).lift(0).getOrElse(""))
            logEntry += ("Alarm_id" -> thirdlinedata.filterNot(x=>x.contains("(")).lift(1).getOrElse(""))
            logEntry += ("Alarm_text" -> thirdlinedata.filterNot(x=>x.contains("(")).slice(2,thirdlinedata.length).mkString(" "))

          }
          else if (actuallineno == fourthLineNo) {
            fourthloopstarted = true
            val complete_log_line = line.trim
            val log_list = complete_log_line.split(" ").filter(_.trim.size>0)
            var index_loop = 1
            for(i<- index_loop to Math.min(log_list.length-1,9)){
              logEntry += (s"Supp$i" -> log_list(i))
              index_loop += 1
            }
            while (index_loop < 10) {
              logEntry += (s"Supp$index_loop" -> "")
              index_loop += 1
            }
          }
          else if (line.contains("COMMAND EXECUTED")) {
            logCompleted = true
            parentlogstarted = false
            logstarted = false
          }
          linNum += 1
        }
        logEntries += logEntry

      }
      linNum += 1
    }
    logEntries.toList.distinct.map(x=>x.filter(x=>x._1!="")).filter(x=>x.size==21)
  }
}
