package com.nokia.parsing.parsers

import java.util.Scanner

import Utils.Util.{getClosestLineNumber, getNextClosestLineNumber}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object ParserAlicemcRNC {

  /**
    * mcrnc__rnchw__embedded_sw_status
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */
  def mcrnc__rnchw__embedded_sw_status(logfilecontent: String): List[Map[String, String]] = {
    println("mcrnc__rnchw__embedded_sw_status")
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

      if (line.equals("|FIRMWARE            |ACTIVE/MAIN BANK         |ROLLBACK/BACKUP BANK     |PENDING BANK             |")) {
        secondlinelist += lineNum
      }
      startlinelist = secondlinelist.map(_ - 2)
      if (line.contains("-------")) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (linenno <- startlinelist) {
      secondlineno = getClosestLineNumber(linenno, secondlinelist.toList.sorted)
      endlineno = getNextClosestLineNumber(secondlineno, endlinelist.toList.sorted)
      val firstlinedata = dataArray(linenno - 1)
      for (tableline <- (secondlineno + 2) to (endlineno - 2)) {
        val tablelinedata = dataArray(tableline - 1).split("\\|", -1).filter(_.nonEmpty)
        my_map += (
          "line_number" -> linenno.toString.trim,
          "CHASSIS" -> (firstlinedata).trim,
          "FIRMWARE" -> (tablelinedata(0)).trim,
          "ACTIVE_MAINBANK" -> (tablelinedata(1)).trim,
          "ROLLBACK_BACKUPBANK" -> (tablelinedata(2)).trim,
          "PENDINGBANK" -> (tablelinedata(3)).trim
        )
        list_map += my_map.toMap

      }
    }
    list_map.toList.filter(x => x.size > 1)
  }


  /**
    * mcrnc__rncrnw__radio_network_iuparams
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */
  def mcrnc__rncrnw__radio_network_iuparams(logfilecontent: String): List[Map[String, String]] = {
    println("mcrnc__rncrnw__radio_network_iuparams")

    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0

    var endlinelist = ListBuffer[Int]()
    var startlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("Parameters")) {
        startlinelist += lineNum
      }
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (linenno <- startlinelist) {
      endlineno = getClosestLineNumber(linenno, endlinelist.toList.sorted)
      val linedata = dataArray(linenno - 1)
      my_map += (
        "line_number" -> linenno.toString,
        "INTERFACE" -> dataArray(linenno - 1).substring(0, dataArray(linenno - 1).indexOf("Parameters")).trim,
        "IUId" -> dataArray(linenno + 1).substring(18, dataArray(linenno + 1).length).trim,
        "CNDomainVersion" -> dataArray(linenno + 2).substring(29, dataArray(linenno + 2).length).trim,
        "SignPointCode" -> dataArray(linenno + 3).substring(29, dataArray(linenno + 3).length).trim,
        "IuState" -> dataArray(linenno + 4).substring(29, dataArray(linenno + 4).length).trim,
        "IuLinkState" -> dataArray(linenno + 5).substring(29, dataArray(linenno + 5).length).trim,
        "IPBasedRouteId" -> dataArray(linenno + 6).substring(29, dataArray(linenno + 6).length).trim,
        "IPQMid" -> dataArray(linenno + 7).substring(29, dataArray(linenno + 7).length).trim,
        "DestIPAddress" -> dataArray(linenno + 8).substring(29, dataArray(linenno + 8).length).trim,
        "IPNetmask" -> dataArray(linenno + 9).substring(29, dataArray(linenno + 9).length).trim
      )
      list_map += my_map.toMap
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * mcrnc__rncsignaling__rncsignaling_sccp_destination_point_code
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def mcrnc__rncsignaling__rncsignaling_sccp_destination_point_code(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.equals(" Point Code ")) {
        log_started = true
        my_map.clear()
      }
      if (log_started) {
        if (line.contains("Point code Name")) {
          my_map += ("Point_code_Name" -> line.substring(37, line.length),
            "line_number" -> lineNum.toString)
        }
        if (line.contains("Point code Id")) {
          my_map += ("Point_code_Id" -> line.substring(37, line.length))
        }
        if (line.contains("Remote AS Name")) {
          my_map += ("Remote_AS_Name" -> line.substring(37, line.length))
        }
        if (line.contains("Point code")) {
          my_map += ("Point_code" -> line.substring(37, line.length))
        }
        if (line.contains("SAP Profile Name")) {
          my_map += ("SAP_Profile_Name" -> line.substring(37, line.length))
        }
        if (line.contains("PC Type")) {
          my_map += ("PC_Type" -> line.substring(37, line.length))
        }
        if (line.contains("Status")) {
          my_map += ("Status" -> line.substring(37, line.length))
        }
        if (line.contains("Include PC in called party address")) {
          my_map += ("Include_PC_in_called_party_address" -> line.substring(37, line.length))
        }
        if (line.contains("SST on DPC Accessible")) {
          my_map += ("SST_on_DPC_Accessible" -> line.substring(37, line.length))
        }
        if (line.contains("SCCP Timer Profile Name")) {
          my_map += ("SCCP_Timer_Profile_Name" -> line.substring(37, line.length))
          list_map += my_map.toMap
          log_started = false
        }
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * mcrnc__rncsignaling__rncsignaling_ss7_association
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

 /* def mcrnc__rncsignaling__rncsignaling_ss7_association(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    var another_dest_field_present = false
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.equals(" M3UA Association")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {
        if (line.contains("association id")) {
          my_map += ("association_id" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""),
            "line_number" -> lineNum.toString)
        }
        if (line.contains("primary-local-ip-addr")) {
          my_map += ("primary_local_ip_addr" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("secondary-local-ip-addr")) {
          my_map += ("secondary_local_ip_addr" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("local-client-port")) {
          my_map += ("local_client_port" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("local-as-name")) {
          my_map += ("local_as_name" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("vrf-name")) {
          my_map += ("vrf_name" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("node")) {
          my_map += ("node" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("remote-as-name")) {
          my_map += ("remote_as_name" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("primary-remote-ip-add")) {
          my_map += ("primary_remote_ip_add" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("secondary-remote-ip-addr")) {
          my_map += ("secondary_remote_ip_addr" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("remote-port")) {
          my_map += ("remote_port" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("exchange-mode")) {
          my_map += ("exchange_mode" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("sctp-profile")) {
          my_map += ("sctp_profile" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("communication-type")) {
          my_map += ("communication_type" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("admin-state")) {
          my_map += ("admin_state" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("role")) {
          my_map += ("role" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("status")) {
          my_map += ("status" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("active-path")) {
          my_map += ("active_path" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (another_dest_field_present) {
          my_map += ("destination_reachable2" -> line.substring(42, line.length).replaceAll("""(?m)\s+$""", ""))
          another_dest_field_present = false
        }
        if (line.contains("destination-reachable")) {
          if (line.endsWith(",")) another_dest_field_present = true
          my_map += ("destination_reachable" -> line.substring(42, line.length).stripSuffix(",").replaceAll("""(?m)\s+$""", ""))
        }
        if (lineNum != (start_log + 1) && line.contains("-----------")) {
          log_started = false
          val cols_list = List("association_id", "primary_local_ip_addr", "secondary_local_ip_addr",
            "local_client_port", "local_as_name", "vrf_name", "node", "remote_as_name", "primary_remote_ip_add", "secondary_remote_ip_addr",
            "remote_port", "exchange_mode", "sctp_profile", "communication_type", "admin_state", "role", "status", "active_path", "destination_reachable", "destination_reachable2")
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
    list_map.toList
  }
*/
def mcrnc__rncsignaling__rncsignaling_ss7_association(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    var another_dest_field_present = false
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.equals(" M3UA Association")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {
        if (line.contains("association id")) {
          my_map += ("association_id" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""),
            "line_number" -> lineNum.toString)
        }
        if (line.contains("primary-local-ip-addr")) {
          my_map += ("primary_local_ip_addr" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("secondary-local-ip-addr")) {
          my_map += ("secondary_local_ip_addr" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("local-client-port")) {
          my_map += ("local_client_port" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("local-as-name")) {
          my_map += ("local_as_name" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("vrf-name")) {
          my_map += ("vrf_name" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("node")) {
          my_map += ("node" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("remote-as-name")) {
          my_map += ("remote_as_name" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("primary-remote-ip-add")) {
          my_map += ("primary_remote_ip_add" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("secondary-remote-ip-addr")) {
          my_map += ("secondary_remote_ip_addr" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("remote-port")) {
          my_map += ("remote_port" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("exchange-mode")) {
          my_map += ("exchange_mode" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("sctp-profile")) {
          my_map += ("sctp_profile" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("communication-type")) {
          my_map += ("communication_type" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("admin-state")) {
          my_map += ("admin_state" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("role")) {
          my_map += ("role" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("status")) {
          my_map += ("status" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (line.contains("active-path")) {
          my_map += ("active_path" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (another_dest_field_present) {
          my_map += ("destination_reachable2" -> line.substring(line.indexOf(":")+1, line.length).replaceAll("""(?m)\s+$""", ""))
          another_dest_field_present = false
        }
        if (line.contains("destination-reachable")) {
          if (line.endsWith(",")) another_dest_field_present = true
          my_map += ("destination_reachable" -> line.substring(line.indexOf(":")+1, line.length).stripSuffix(",").replaceAll("""(?m)\s+$""", ""))
        }
        if (lineNum != (start_log + 1) && line.contains("-----------")) {
          log_started = false
          val cols_list = List("association_id", "primary_local_ip_addr", "secondary_local_ip_addr",
            "local_client_port", "local_as_name", "vrf_name", "node", "remote_as_name", "primary_remote_ip_add", "secondary_remote_ip_addr",
            "remote_port", "exchange_mode", "sctp_profile", "communication_type", "admin_state", "role", "status", "active_path", "destination_reachable", "destination_reachable2")
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
    list_map.toList
  }

  /**
    * mcrnc__rncsignaling__rncsignalingsccpsubsystemremote
    *
    * @author Kushal Mahajan
    * @param logfilecontent
    * @return
    */

  def mcrnc__rncsignaling__rncsignalingsccpsubsystemremote(logfilecontent: String): List[Map[String, String]] = {
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.equals("REMOTE SUBSYSTEMS")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      if (log_started) {
        if (line.contains("SCCP SubSystem Identifier")) {
          my_map += ("SCCP_SubSystem_Identifier" -> line.substring(33, line.length).replaceAll("""(?m)\s+$""", ""),
            "line_number" -> lineNum.toString)
        }
        else if (line.contains("SCCP SubSystem                 :")) {
          my_map += ("SCCP_SubSystem" -> line.substring(33, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("SCCP SubSystem Number")) {
          my_map += ("SCCP_SubSystem_Number" -> line.substring(33, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("SAP Profile Name")) {
          my_map += ("SAP_Profile_Name" -> line.substring(33, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("Dest Point code Name")) {
          my_map += ("Dest_Point_code_Name" -> line.substring(33, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("Concerned SubSystem")) {
          my_map += ("Concerned_SubSystem" -> line.substring(33, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        else if (line.contains("Status")) {
          my_map += ("Status" -> line.substring(33, line.length).replaceAll("""(?m)\s+$""", ""))
        }
        if (lineNum != (start_log + 1) && line.contains("-----------")) {
          log_started = false
          list_map += my_map.toMap
        }
      }
      lineNum += 1
    }
    list_map.toList
  }

  /**
    * mcrnc__rnchw__embedded_sw_version(
    *
    * @author Kinjal Singh
    * @param logfilecontent
    * @return
    */
  /*def mcrnc__rnchw__embedded_sw_version(logfilecontent: String): List[Map[String, String]] = {
    print("mcrnc__rnchw__embedded_sw_version")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var list_map = mutable.ListBuffer[Map[String, String]]()
    var log_started = false
    var start_log = 0
    var my_map = mutable.Map[String, String]()
    while (txt.hasNext()) {
      val line = txt.nextLine()
      if (!log_started && line.startsWith("Alarm ID                 :")) {
        log_started = true
        start_log = lineNum
        my_map.clear()
      }
      println("log_started>>"+log_started)
      if (log_started) {
        if (line.contains("Alarm ID")) {
          my_map += ("ALARM_ID" -> line.split(":")(1).trim)
        }
        if (line.contains("Specific problem")) {
          my_map += ("SPECIFIC_PROBLEM" -> line.split(":")(1).trim)
        }
        if (line.contains("Managed object")) {
          my_map += ("MANAGED_OBJECT" -> line.split(":")(1).trim)
        }
        if (line.contains("Severity")) {
          my_map += ("SEVERITY" -> line.split(":")(1).trim)
        }
        if (line.contains("Cleared")) {
          my_map += ("CLEARED" -> line.split(":")(1).trim)
        }
        if (line.contains("Clearing")) {
          my_map += ("CLEARING" -> line.split(":")(1).trim)
        }
        if (line.contains("Acknowledged")) {
          my_map += ("ACKNOWLEDGED" -> line.split(":")(1).trim)
        }
        if (line.contains("Ack. user ID")) {
          my_map += ("ACK_USER_ID" -> line.split(":")(1).trim)
        }
        if (line.contains("Ack. time")) {
          my_map += ("ACK_TIME" -> line.split(":")(1).trim)
        }
        if (line.contains("Alarm time")) {
          my_map += ("ALARM_TIME" -> line.split(":")(1).trim)
        }
        if (line.contains("Event type")) {
          my_map += ("EVENT_TYPE" -> line.split(":")(1).trim)
        }
        if (line.contains("Application")) {
          my_map += ("APPLICATION" -> line.split(":")(1).trim)
        }
        if (line.contains("Identif appl. addl. info")) {
          my_map += ("IDENTIF_APPL_ADDL_INFO" -> line.split(":")(1).trim)
        }
        if (line.contains("Appl. addl. info")) {
          var str = line.split("\\s+:")(1).trim
          var second_line = txt.nextLine
          var thrid_line = txt.nextLine
          if (second_line.contains("=") | thrid_line.contains("=")) {
            str = str.concat(second_line.trim).concat(thrid_line.trim)
            lineNum += 2

          }
          my_map += ("APPL_ADDL_INFO" -> str)
        }
        else if (line.contains("Appl. addl. info") && !txt.nextLine.contains("Notification")) {
          my_map += ("APPL_ADDL_INFO" -> line.split(":")(1).trim.concat(txt.nextLine.trim))

        }

        if (lineNum != (start_log + 1) && line.contains("----------------------------------------------")) {
          my_map += "line_number" -> start_log.toString
          log_started = false

          val cols_list = List("ALARM_ID", "SPECIFIC_PROBLEM", "MANAGED_OBJECT", "SEVERITY", "CLEARED", "CLEARING",
            "ACKNOWLEDGED", "ACK_USER_ID", "ACK_TIME", "ALARM_TIME", "EVENT_TYPE", "APPLICATION", "IDENTIF_APPL_ADDL_INFO", "APPL_ADDL_INFO")
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

    list_map.toList
  }
*/
  /**
    * mcrnc__rnchw__embedded_sw_version(
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */
  def mcrnc__rnchw__embedded_sw_version(logfilecontent: String): List[Map[String, String]]={

    println("mcrnc__rnchw__embedded_sw_status")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var secondlineno, thirdlineno,endlineno: Int = 0

    var startlinelist,secondlinelist,thirdlinelist,endlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var my_map = mutable.Map[String, String]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.equals("|Firmware Name  |Installed Version                                                   |Baseline Version    |")) {
        startlinelist += lineNum
      }

      if (line.equals("|               |Active/Main           |Backup/Rollback       |Pending               |                    |")) {
        secondlinelist += lineNum
      }

      if (line.equals("|               |Bank                  |Bank                  |Bank                  |                    |")) {
        thirdlinelist += lineNum
      }
      if (line.contains("-------")) {
        endlinelist += lineNum
      }
      lineNum += 1
    }
    for (linenno <- startlinelist) {
      secondlineno = getClosestLineNumber(linenno, secondlinelist.toList.sorted)
      thirdlineno = getClosestLineNumber(linenno, thirdlinelist.toList.sorted)
      endlineno = getNextClosestLineNumber(thirdlineno, endlinelist.toList.sorted)
      val firstlinedata = dataArray(linenno - 1)
      for (tableline <- (thirdlineno + 2) to (endlineno - 1)) {
        val tablelinedata = dataArray(tableline - 1).split("\\|", -1).filter(_.nonEmpty)
        my_map += (
          "line_number" -> linenno.toString.trim,
          "FIRMWARE" -> (tablelinedata(0)).trim,
          "ACTIVE_MAINBANK" -> (tablelinedata(1)).trim,
          "ROLLBACK_BACKUPBANK" -> (tablelinedata(2)).trim,
          "PENDINGBANK" -> (tablelinedata(3)).trim,
          "BASELINE_BANK" -> (tablelinedata(4)).trim
        )
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * mcrnc__rnchw__rnchw_smartctl_hdd_grown_defect
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def mcrnc__rnchw__rnchw_smartctl_hdd_grown_defect(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("ssh") && line.contains("smartctl --all")) {

        logEntry += ("line_number" -> linNum.toString)


        logEntry += ("box" -> (line.split("ssh")(1).split("\\s+")(1).trim))
        linNum += 1
        do {
          line = txt.nextLine()
          if (line.contains("SMART Health Status:")) {
            logEntry += ("smart_health_status" -> (line.split(":")(1).trim))
          } else if (line.contains("Elements in grown defect list:")) {
            logEntry += ("elements_in_grown_defect_list" -> (line.split(":")(1).trim))
          }

          linNum += 1
        } while (!line.contains("Elements in grown defect list:"))
        logEntries += logEntry
        linNum -= 1

      }
      linNum += 1
    }

    logEntries.toList


  }

  /**
    * mcrnc__sw_manage_install__sw_manage_install
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  /*def mcrnc__sw_manage_install__sw_manage_install(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("set sw-manage install delivery ")) {

        while (!line.startsWith("ERROR Execution of SS_IPMgmt_convert.sh: ")) {
          line = txt.nextLine()
          linNum += 1
        }
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("error_conversion_script_execution" -> (line.stripPrefix("ERROR Execution of SS_IPMgmt_convert.sh: ").trim))

        logEntries += logEntry

      }
      linNum += 1
    }
    logEntries.toList
  }
*/
def mcrnc__sw_manage_install__sw_manage_install(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("set sw-manage install delivery ")) {

          if(line.startsWith("ERROR Execution of SS_IPMgmt_convert.sh: ")) {
            logEntry += ("line_number" -> linNum.toString)
            logEntry += ("error_conversion_script_execution" -> (line.stripPrefix("ERROR Execution of SS_IPMgmt_convert.sh: ").trim))
            logEntries += logEntry
          }
          else  {
            line = txt.nextLine()
            linNum += 1
          }
      }
      linNum += 1
    }
    logEntries.toList
  }


  /**
    * mcrnc__fsip_switch_port__fsip_switch_port
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def mcrnc__fsip_switch_port__fsip_switch_port(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("show config fsClusterId")) {

        for (i <- 1 to 10) {
          line = txt.nextLine()
        }
        linNum += 10
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("fsip_switch_port_admin_state" -> (line.stripPrefix("fsipSwitchPortAdminState: ").trim))

        logEntries += logEntry

      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * mcrnc__startuplog___dram_info
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def mcrnc__startuplog___dram_info(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("U-Boot")) {

        for (i <- 1 to 11) {
          line = txt.nextLine()
        }
        linNum += 11
        if (line.startsWith("DRAM: ")) {
          logEntry += ("line_number" -> linNum.toString)
          logEntry += ("status" -> (line.stripPrefix("DRAM: ").trim))
          line = txt.nextLine()
          linNum += 1
         // println(line, linNum)
          logEntry += ("ddr_value" -> (line.stripPrefix("    DDR: ").trim))
          if (txt.hasNext()) {
            line = txt.nextLine()
            linNum += 1
            if (line.startsWith("FLASH: ")) {
              logEntry += ("flash_value" -> (line.stripPrefix("FLASH: ").trim))
            } else logEntry += ("flash_value" -> (""))
          } else logEntry += ("flash_value" -> (""))
          logEntries += logEntry

        }
      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * mcrnc__syslog__master_syslog_unit_text_data
    *
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */


  def mcrnc__syslog__master_syslog_unit_text_data(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()

      var logEntry = Map[String, String]()
      if (line.contains("err ") && line.contains(": RU:") && line.contains("|PNAME:") && line.contains("|LPID:") && line.contains("|PID:") && line.contains("|TEXT:") &&
        line.contains("|DATA:")) {

        logEntry += ("line_number" -> linNum.toString)

        logEntry += ("date_time" -> (line.split("er")(0).trim))
        logEntry += ("severity" -> ("err" + line.split(": RU:")(0).split("er")(1).split("\\s+")(0).trim))
        logEntry += ("unit" -> (line.split(": RU:")(0).split("err")(1).split("\\s+")(1).trim))
        logEntry += ("info" -> (line.split(": RU:")(0).split("err")(1).split("\\s+")(2).trim))
        logEntry += ("ru" -> (line.split(": RU:")(1).split("\\|PNAME:")(0).trim))
        logEntry += ("pname" -> (line.split("\\|PNAME:")(1).split("\\|LPID:")(0).trim))
        logEntry += ("lpid" -> (line.split("\\|LPID:")(1).split("\\|PID:")(0).trim))
        logEntry += ("pid" -> (line.split("\\|PID:")(1).split("\\|TEXT:")(0).split('|')(0).trim))
        logEntry += ("text" -> (line.split("\\|TEXT:")(1).split("\\|DATA:")(0).trim))
        logEntry += ("data" -> (line.split("\\|DATA:").lift(1).getOrElse("")))

        logEntries += logEntry

      } else if (line.contains(" wa") && line.contains(": RU:") && line.contains("|PNAME:") && line.contains("|LPID:") && line.contains("|PID:") && line.contains("|TEXT:") &&
        line.contains("|DATA:")) {
        logEntry += ("line_number" -> linNum.toString)
        logEntry += ("date_time" -> (line.split("wa")(0).trim))
        logEntry += ("severity" -> ("wa" + line.split(": RU:")(0).split("wa")(1).split("\\s+")(0).trim))
        logEntry += ("unit" -> (line.split(": RU:")(0).split("wa")(1).split("\\s+")(1).trim))
        logEntry += ("info" -> (line.split(": RU:")(0).split("wa")(1).split("\\s+")(2).trim))
        logEntry += ("ru" -> (line.split(": RU:")(1).split("\\|PNAME:")(0).trim))
        logEntry += ("pname" -> (line.split("\\|PNAME:")(1).split("\\|LPID:")(0).trim))
        logEntry += ("lpid" -> (line.split("\\|LPID:")(1).split("\\|PID:")(0).trim))
        logEntry += ("pid" -> (line.split("\\|PID:")(1).split("\\|TEXT:")(0).split('|')(0).trim))
        logEntry += ("text" -> (line.split("\\|TEXT:")(1).split("\\|DATA:")(0).trim))
        logEntry += ("data" -> (line.split("\\|DATA:").lift(1).getOrElse("")))

        logEntries += logEntry


      }
      linNum += 1
    }
    logEntries.toList
  }

  /**
    * mcrnc__rnchw__lmp_hw_status
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */
  def mcrnc__rnchw__lmp_hw_status(logfilecontent: String): List[Map[String, String]] = {
    println("mcrnc__rnchw__lmp_hw_status")
    var lineNum = 1
    val txt = new Scanner(logfilecontent)
    var endlineno: Int = 0

    var endlinelist = ListBuffer[Int]()
    var startlinelist = ListBuffer[Int]()

    var list_map = mutable.ListBuffer[Map[String, String]]()
    var dataArray = mutable.ArrayBuffer[String]()

    while (txt.hasNext) {
      val line = txt.nextLine()
      dataArray += line

      if (line.contains("RNCHW START")) {
        startlinelist += lineNum
      }
      if (line.trim.length == 0) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (linenno <- startlinelist) {
      endlineno = getClosestLineNumber(linenno + 4, endlinelist.toList.sorted)
      val firstlinedata = dataArray(linenno - 1)
      for (tableline <- (linenno + 4) to (endlineno - 2)) {
        var my_map = mutable.Map[String, String]()
        val tablelinedata = dataArray(tableline - 1)
        if (tablelinedata.substring(0, 3).equals("LMP")) {
          my_map += (
            "line_number" -> (linenno.toString.trim),
            "RNCid" -> (firstlinedata.substring(5, firstlinedata.indexOf("RNCHW"))).trim,
            "LMP_ID" -> (tablelinedata.split(":", -1)(0)).trim,
            "NODE_STATUS" -> (tablelinedata.split(":", -1)(1)).trim
          )
        }
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(x => x.size > 1)
  }

  /**
    * mcrnc__rnchw__rnchwsfpstatus
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    */
  def mcrnc__rnchw__rnchwsfpstatus(logfilecontent: String): List[Map[String, String]] = {
    println("mcrnc__rnchw__rnchwsfpstatus")
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

      if (line.contains("RNC backplane configuration")) {
        startlinelist += lineNum
      }
      if (line.contains("***")) {
        secondlinelist += lineNum
      }

      if (line.trim.length == 0) {
        endlinelist += lineNum
      }
      lineNum += 1
    }

    for (linenno <- startlinelist) {
      endlineno = getClosestLineNumber(linenno + 4, endlinelist.toList.sorted)
      for (tableline <- (linenno + 1) to (endlineno - 2)) {
        val tablelinedata = dataArray(tableline - 1)
        my_map += ("line_number" -> linenno.toString.trim)
        if (tablelinedata.contains("***")) {
          my_map += "LMPParam" -> tablelinedata.substring(5, tablelinedata.lastIndexOf("***")).trim
        }
        else {
          val tablelinedata = dataArray(tableline - 1).split("\\s", -1).filter(_.nonEmpty)
          my_map += (
            "SFP_NAME" -> (tablelinedata(0)).trim,
            "ENAB_DISAB" -> (tablelinedata(1)).trim,
            "LEFTPARAM" -> (dataArray(tableline-1).substring(27,49)).trim,
            "RIGHT1" -> (dataArray(tableline-1).substring(49,64)).trim,
            "RIGHT2" -> (dataArray(tableline-1).substring(64,71)).trim,
            "RIGHT3" -> (dataArray(tableline-1).splitAt(71)._2).trim
          )
        }
        list_map += my_map.toMap
      }
    }
    list_map.toList.filter(x => x.size > 2)
  }

  /**
    * ******************************************************
    * mcrnc__rncalarm__mcrnc_active_alarm
    *
    * @author Rakesh Adhikari
    * @param logfilecontent
    * @return
    * ***************************************************
    */

  def mcrnc__rncalarm__mcrnc_active_alarm(logfilecontent: String): List[Map[String, String]] = {
    println("mcrnc__rncalarm__mcrnc_active_alarm")
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

      if (line.contains("show alarm active")) {
        startlinelist += lineNum
      }

      if (line.startsWith("Alarm ID")) {
        secondlinelist += lineNum
      }

      if (line.contains("----------------")) {
        dashlinelist += lineNum
      }
      lineNum += 1
      alllinecount = lineNum
    }
    for (startline <- startlinelist) {
      var my_map = mutable.Map[String, String]()
      var alarmidlineno, SpecificProblemlineno, Managedobjectlineno, severitylineno, clearedlineno, clearinglineno, acklineno, ackidlineno, acktimelineno, alarmtimelineno,
      eventlineno, applicationlineno, identifyappllineno, appladdllineno = 0

      tableendline = getClosestLineNumber(startline, startlinelist.toList.sorted)
      if (tableendline != 0)
        tablelist = secondlinelist.filter(x => (x > startline) && (x < tableendline))
      else
        tablelist = secondlinelist.filter(x => (x > startline) && (x < alllinecount))


      if (dataArray(startline + 2).contains("Alarm ID")) {
        for (secondlineno <- tablelist) {
          endlineno = getClosestLineNumber(secondlineno, dashlinelist.toList.sorted) - 2
          // println("secondlineno to endlineno>>"+secondlineno +">>>"+ endlineno)
          for (i <- secondlineno to endlineno) {
            if (dataArray(i - 1).startsWith("Alarm ID")) alarmidlineno = i
            if (dataArray(i - 1).startsWith("Specific problem")) SpecificProblemlineno = i
            if (dataArray(i - 1).startsWith("Managed object")) Managedobjectlineno = i
            if (dataArray(i - 1).startsWith("Severity")) severitylineno = i
            if (dataArray(i - 1).startsWith("Cleared")) clearedlineno = i
            if (dataArray(i - 1).startsWith("Clearing")) clearinglineno = i
            if (dataArray(i - 1).startsWith("Acknowledged")) acklineno = i
            if (dataArray(i - 1).startsWith("Ack. user ID")) ackidlineno = i
            if (dataArray(i - 1).startsWith("Ack. time")) acktimelineno = i
            if (dataArray(i - 1).startsWith("Alarm time")) alarmtimelineno = i
            if (dataArray(i - 1).startsWith("Event type")) eventlineno = i
            if (dataArray(i - 1).startsWith("Application")) applicationlineno = i
            if (dataArray(i - 1).startsWith("Identif appl. addl. info")) identifyappllineno = i
            if (dataArray(i - 1).startsWith("Appl. addl. info")) appladdllineno = i
          }
          var alarmline, specificline, managedline, sevline, clearedline, clearingline, ackline, ackuserline,
          acktimeline, alarmtimeline, eventline, appline, indentifline, applline = ""

          my_map += ("line_number" -> startline.toString.trim)

          for (i <- alarmidlineno to SpecificProblemlineno - 1) {
            alarmline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Alarm_ID" -> (alarmline.trim)

          for (i <- SpecificProblemlineno to Managedobjectlineno - 1) {
            specificline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Specific_Problem" -> (specificline.trim)

          for (i <- Managedobjectlineno to severitylineno - 1) {
            managedline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Managed_Object" -> (managedline.trim)

          for (i <- severitylineno to clearedlineno - 1) {
            sevline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Severity" -> (sevline.trim)

          for (i <- clearedlineno to clearinglineno - 1) {
            clearedline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Cleared" -> (clearedline.trim)

          for (i <- clearinglineno to acklineno - 1) {
            clearingline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Clearing" -> (clearingline.trim)

          for (i <- acklineno to ackidlineno - 1) {
            ackline += dataArray(i - 1).substring(27, dataArray(i - 1).length).trim
          }
          my_map += "Acknowledged" -> (ackline.trim)

          for (i <- ackidlineno to acktimelineno - 1) {
            ackuserline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Ack_user_ID" -> (ackuserline.trim)

          for (i <- acktimelineno to alarmtimelineno - 1) {
            acktimeline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Ack_time" -> (acktimeline.trim)

          for (i <- alarmtimelineno to eventlineno - 1) {
            alarmtimeline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Alarm_time" -> (alarmtimeline.trim)

          for (i <- eventlineno to applicationlineno - 1) {
            eventline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Event_type" -> (eventline.trim)

          for (i <- applicationlineno to identifyappllineno - 1) {
            appline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Application" -> (appline.trim)

          for (i <- identifyappllineno to appladdllineno - 1) {
            indentifline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Identif_appl_addl_info" -> (indentifline.trim)

          for (i <- appladdllineno to endlineno) {
            applline += dataArray(i - 1).substring(27, dataArray(i - 1).length).replace(":", "").trim
          }
          my_map += "Appl_addl_info" -> (applline.trim)

          list_map += my_map.toMap
        }
      }
    }
    list_map.toList.filter(_.size > 1)
  }

  /**
    * mcrnc__rnc_backplane_txt__rnc_backplane
    * @author Karan Manchanda
    * @param logfilecontent
    * @return
    */

  def mcrnc__rnc_backplane_txt__rnc_backplane(logfilecontent: String): List[Map[String, String]] = {

    val txt = new Scanner(logfilecontent)
    var linNum = 1
    val logEntries = new ListBuffer[Map[String, String]]()
    while (txt.hasNextLine) {
      var line = txt.nextLine()
      var logEntry = Map[String, String]()
      if (line.contains("RNC backplane configuration")) {

        line = txt.nextLine()
        linNum+=1

        while(!line.isEmpty){
          var flag = false
          logEntry += ("line_number" -> linNum.toString)
          if(line.endsWith("***")){
            logEntry += ("lmp_param" -> line.replaceAll("\\*", ""))
            line=txt.nextLine()
            linNum+=1
            while(!line.endsWith("***")&&line.startsWith("SFP")) {
              logEntry += ("sfp" -> line.split("\\s+")(0).trim)
              logEntry += ("enab_disab" -> line.split("\\s+")(1).trim)
              if(line.toLowerCase.contains("up")){
                var value = line.split("Up")(0).trim.split("\\s+")
                var left_param = ""
                for(i<- 2 to value.length-1){
                  left_param += " " + value(i)
                }
                logEntry += ("left_param" -> left_param.trim)
                logEntry += ("state" -> "Up")
                value = line.split("Up")(1).trim.split("\\s+")
                logEntry += ("right1" -> value(0).trim)
                logEntry += ("right2" -> value(1).trim)
                logEntry += ("right3" -> value(2).trim)
              }else{
                var value = line.split("Down")(0).trim.split("\\s+")
                var left_param = ""
                for(i<- 2 to value.length-1){
                  left_param += " " + value(i)
                }
                logEntry += ("left_param" -> left_param.trim)
                logEntry += ("state" -> "Down")
                value = line.split("Down")(1).trim.split("\\s+")
                logEntry += ("right1" -> value(0).trim)
                logEntry += ("right2" -> value(1).trim)
                logEntry += ("right3" -> value(2).trim)


              }
              logEntries += logEntry
              line = txt.nextLine()
              linNum+=1
              flag = true
            }
          }
          if(flag){
            line
          }else{
            line = txt.nextLine()
            linNum+=1
          }

        }

      }
      linNum += 1
    }

    return logEntries.toList
  }

}