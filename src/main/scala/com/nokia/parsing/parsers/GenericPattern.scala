package com.nokia.parsing.parsers

import java.util.Scanner

import scala.collection.mutable.ListBuffer


object GenericPattern {

  import Utils.Util._

  def main(args: Array[String]): Unit = {
    val logFileContent = scala.io.Source.fromFile("MML_history.txt").mkString
    println(parser_zqri_generic(logFileContent))
  }

  import Utils.Util._

  def parser_zqri_generic(logfilecontent: String): List[Map[String, String]] = {
    val result = getNonEmptyLines(logfilecontent, "ZQRI;", "COMMAND EXECUTED")
    println(result)
    val logEntries = new ListBuffer[Map[String, String]]()
    val lines = result.split("\n")
    var logEntry = Map[String, String]()
    for (i <- 0 to lines.length - 1) {
      var j = i;
      if (lines(j).contains(getAliceLineNumber())) {
        val lineNum = lines(j).split(getAliceLineNumber())(1).trim
        do {
          var line = lines(j)
          if(line.contains("--------") ){
            do{
              j+=1
              line=lines(j)
              logEntry += ("line_number" -> lineNum)
              if(!line.startsWith(" ")){
                logEntry += ("UNIT" -> getValue(line.split("\\s+"),0))
                do{
                  j+=1
                  line=lines(j)
                  if(line.startsWith(" ")){

                    if(!line.contains("->") && !line.contains("VLAN")  && line.trim.split("\\s+").length>=3) {

                      val split = line.trim.split("\\s+")
                      logEntry += ("VLAN" -> getValue(split, 0))
                      logEntry += ("STATE" -> getValue(split, 1))
                      logEntry += ("MTU" -> getValue(split, 2))
                      logEntry += ("Attr" -> getValue(split, 3))
                      logEntry += ("IP_ADDR" -> getValue(split, 4))
                      logEntry += ("VLAN_UNIT" -> "")
                      logEntry += ("VLAN_MTU" -> "")
                      logEntry += ("VLAN_ATT" -> "")
                      logEntries += logEntry
                    }else{
                      if(line.contains("VLAN")){
                        var split1 = line.trim.split("\\s+")
                        logEntry += ("VLAN" -> getValue(split1, 0))
                        logEntry += ("STATE" -> getValue(split1, 1))
                        logEntry += ("MTU" -> getValue(split1, 2))
                        logEntry += ("Attr" -> getValue(split1, 3))
                        logEntry += ("IP_ADDR" -> getValue(split1, 4))
                        do{
                          j+=1
                          line=lines(j)
                          val split = line.trim.split("\\s+")
                          logEntry += ("VLAN_UNIT" -> getValue(split, 0).replaceAll("->",""))
                          logEntry += ("VLAN_MTU" -> getValue(split, 1))
                          logEntry += ("VLAN_ATT" -> getValue(split, 2))
                        }while (j < lines.length - 1 &&  !line.contains("->") && !line.contains(getAliceLineNumber()))
                        j-=1
                        logEntries += logEntry
                      }
                    }
                  }
                }while (j < lines.length - 1 &&  line.startsWith(" "))
                j-=1
              }
            }while (j < lines.length - 1 &&  !lines(j).contains(getAliceLineNumber()))
            j=j-1
          }
          j=j+1
        } while (j < lines.length - 1 && !lines(j).contains(getAliceLineNumber()))

      }

    }
    logEntries.filter(_.nonEmpty).toList
  }

  def parser_zqri(logfilecontent: String): List[Map[String, String]] = {
    val result = getNonEmptyLines(logfilecontent, "ZQRI;", "COMMAND EXECUTED")
    val logEntries = new ListBuffer[Map[String, String]]()
    //println(result)

    val lines = result.split("\n")
    var logEntry = Map[String, String]()
    for (i <- 0 to lines.length - 1) {
      var j = i;
      if (lines(j).contains(getAliceLineNumber())) {
        val lineNum = lines(j).split(getAliceLineNumber())(1).trim
        do {
          var line = lines(j)
          if(line.contains("BCSU") && (line.trim.split("\\s+").length<=2)){
            val splittedData = line.trim.split("\\s+")
            logEntry += ("line number" -> lineNum)
            logEntry += ("BCSU" -> getValue(splittedData, 0))
            do{
              line=lines(j+1)
              if(!line.contains("->") && line.contains("EL")  && line.trim.split("\\s+").length==5) {
                val split = line.trim.split("\\s+")
                logEntry += ("VLAN" -> getValue(split, 0))
                logEntry += ("STATE" -> getValue(split, 1))
                logEntry += ("MTU" -> getValue(split, 2))
                logEntry += ("Attr" -> getValue(split, 3))
                logEntry += ("IP_ADDR" -> getValue(split, 4))
                logEntries += logEntry
              }
              if(!line.contains("->") && line.contains("VLAN")  && line.trim.split("\\s+").length== 5){
                if(lines(j+2).contains("->EL")) {
                  val split = line.trim.split("\\s+")
                  logEntry += ("VLAN" -> getValue(split, 0))
                  logEntry += ("STATE" -> getValue(split, 1))
                  logEntry += ("MTU" -> getValue(split, 2))
                  logEntry += ("Attr" -> getValue(split, 3))
                  logEntry += ("IP_ADDR" -> getValue(split, 4))
                  logEntries += logEntry
                }
              }
              j=j+1
            }while (j < lines.length - 1 &&  !line.contains("BCSU"))
            j=j-1
            //logEntries += logEntry
          }
          j=j+1
        } while (j < lines.length - 1 && !lines(j).contains(getAliceLineNumber()))

      }

    }
    logEntries.filter(_.nonEmpty).toList
  }




}
