package com.nokia.parsing.parsers

import java.util.Scanner

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks._

object ParseAliceFixedNetwork {

  /**
    * fixed_network__final_11050a20_TSF__tsf_parsing_sample
    *
    * @author Anudeep Purwar
    * @param logfilecontent
    * @return
    */
  def fixed_network__final_11050a20_TSF__tsf_parsing_sample(logfilecontent: String): List[Map[String, String]] = {
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var data_map = mutable.Map[String, String]()
    var line_number = 1
    var counter = 0;
    val txt = new Scanner(logfilecontent)
    while (txt.hasNext) {
      val line = txt.nextLine();
      line match {
        case x if x.contains("seq-number") => data_map = mutable.Map[String,String]("seq_number" -> "", "error task" -> "", "error type" -> "", "error class" -> "", "user name" -> "", "build name" -> "", "file name" -> "", "line number" -> "", "error info" -> "","line_number" -> ""); val seqNum = x.split(":").last.trim; data_map += ("seq_number" -> seqNum);data_map += ("line_number" -> line_number.toString)
        case x if x.contains("error task") => val errorTask = x.split(":").last.trim; data_map += ("error task" -> errorTask)
        case x if x.contains("error type") => val errorType = x.split(":").last.trim; data_map += ("error type" -> errorType)
        case x if x.contains("error class") => val errorClass = x.split(":").last.trim; data_map += ("error class" -> errorClass)
        case x if x.contains("user name") => val userName = x.split(":").last.trim; data_map += ("user name" -> userName)
        case x if x.contains("build name") => val buildName = x.split(":").last.trim; data_map += ("build name" -> buildName)
        case x if x.contains("file name") => val fileName =   x.split(":").last.trim; data_map += ("file name" -> fileName)
        case x if x.contains("line number") => val lineNumber = x.split(":").last.trim; data_map += ("line number" -> lineNumber)
        case x if x.contains("error info") => val errorInfo = x.split(":").last.trim; data_map += ("error info" -> errorInfo); list_map+=data_map.toMap;
        case _ => None
      }
      line_number = line_number + 1
    }
    list_map.toList
  }
  /**
    * @author Sumit Gupta
    * @param logfilecontent
    * @return
    */
  def fixed_network__alexx__error_record_alexx(logfilecontent: String): List[Map[String, String]] = {
    val txt = new Scanner(logfilecontent)
    val logEntries = new ListBuffer[Map[String, String]]()
    val tokens= List("report received", "remote time","time","seq-number","error task","error type","error class","user name","build name","file name","line number","error info","call stack")
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      ParseAliceFixedNetwork.lineNumber+=1
      var logEntry = Map[String, String]()
      if (line.contains("************* ERROR RECORD *************") ) {
        if(txt.hasNextLine){
          logEntry += ("line_number"  -> ParseAliceFixedNetwork.lineNumber.toString)
          var nextLine= txt.nextLine()
          ParseAliceFixedNetwork.lineNumber+=1
          for( token <- tokens) {
            if(token.equalsIgnoreCase("error info")) {
              var errorInfo= readUntilEnd(nextLine,token,"call stack",txt)
              if(nextLine.contains(token))
                logEntry += (replaceSpecialChars(token)  -> errorInfo)
              else
                logEntry += (replaceSpecialChars(token)  -> "")
            }else if(token.equalsIgnoreCase("call stack")) {
              var callStack = readUntilEnd(nextLine,token,"****************** END *****************",txt)
              if(nextLine.contains(token))
                logEntry += (replaceSpecialChars(token)  -> callStack)
              else
                logEntry += (replaceSpecialChars(token) -> "")
            }else {
              if(nextLine.contains(token)) {
                logEntry += (replaceSpecialChars(token) -> splitterCommon(nextLine, token))
                nextLine = txt.nextLine()
                ParseAliceFixedNetwork.lineNumber+=1
              }else{
                logEntry += (replaceSpecialChars(token)  -> "")
              }
            }
          }
          logEntries += logEntry
        }
      }
    }
    logEntries.toList
  }


  def splitterCommon(line : String, splitStr: String): String = {
    val value = line.split(splitStr).last.split(" : ")
    var finalValue = value.last
    return finalValue.substring(0,finalValue.length-1).trim
  }

  def readUntilEnd(nextLine : String, token: String, nextToken: String, txt: Scanner ): String ={
    var value = splitterCommon(nextLine, token)
    breakable
    {
      while (txt.hasNextLine) {
        var checkNextLine = txt.nextLine()
        ParseAliceFixedNetwork.lineNumber+=1
        if (!checkNextLine.toLowerCase.contains(nextToken.toLowerCase())) {
          val splittedValues = checkNextLine.split("RECV,\"")
          val finalval= splittedValues.last.substring(0,splittedValues.last.length-1)
          value += finalval
        } else {
          break
        }
      }
    }
    return value
  }

  def replaceSpecialChars(token:String) : String = {
    if(token.equalsIgnoreCase("line number"))
      "line_num"
    else
      token.replaceAll("\\W","_")
  }

  object ParseAliceFixedNetwork{
    var lineNumber=0;
  }

}
