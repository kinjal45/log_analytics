package com.nokia.parsing

import java.net.SocketTimeoutException

import LogParser.LogBundleConfigFetcher.LogBundle
import Utils.ELKUtil.postDataToELK
import Utils.ProxyToES
import Utils.Util.{postParserJobOutput, sendScalaMail}
import com.amazonaws.services.kinesis.model.InvalidArgumentException
import com.nokia.compiler.ExternalScalaFileHelperForParser
import com.nokia.parsing.parsers.ChooseParser
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.rest.EsHadoopNoNodesLeftException
import org.joda.time.DateTime
import org.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, TimeoutException}
import scala.tools.reflect.ToolBoxError

object ProcessLogs {
  def parseLog(fileName_parserName: String, logFileContent: String, dateToProcess: String, logId: String, startTime: String, input_parser_output_map: Map[String, ListBuffer[Int]], parserCodeMap: Map[String, String],
               input_map: Map[String, List[String]], input_output_map: Map[String, List[Int]],
               spark: SparkSession, conf: Broadcast[JSONObject],
               proxyToES: ProxyToES, logBundle: LogBundle) = {
    val logElement: String = fileName_parserName.split(">>>>")(0).trim
    var parserFunctionReturn = List[Map[String, String]]()
    var log_key = fileName_parserName.trim
    var parserStatus = mutable.Map[String, String]()
    var lineOfAnamoly = 0
    val parserFileKey = log_key.trim
    var timeout=0
    import scala.collection.JavaConverters._
    //var customproductcheckforTimeout=conf.value.getJSONArray("customProduct").asScala.toList
    val customproductidcheckforTimeout=conf.value.getJSONArray("customProductId").asScala.toList
   /* if(customproductcheckforTimeout.contains(logBundle.product_name.trim)){*/
    if(customproductidcheckforTimeout.contains(logBundle.prod_id)){
        timeout=conf.value.getString("customexecutionTimeOut").toInt
      }
    else{
       timeout=conf.value.getString("defaultexecutionTimeOut").toInt
    }
    try {
      lazy val f = Future {
        if (parserCodeMap(log_key).nonEmpty) {
          println("Parser " + logElement+" execution started at "+DateTime.now().toString())
          println("Processor count>>>"+Runtime.getRuntime.availableProcessors())
          //parserFunctionReturn = new ExternalScalaFileHelper().processExternalFile(parserCodeMap(fileName_parserName), new String(java.util.Base64.getEncoder.encode(logFileContent.getBytes(java.nio.charset.StandardCharsets.UTF_8.name))))
          parserFunctionReturn = new ExternalScalaFileHelperForParser().processExternalFileForParser(parserCodeMap(fileName_parserName), logFileContent)
        } else {
          println("Parser " + logElement+" execution started from jar at "+DateTime.now().toString())
          println("Processor count here>>>"+Runtime.getRuntime.availableProcessors())
          parserFunctionReturn = ChooseParser.chooseParser(logElement, logFileContent)
        }
      }
      Await.result(f, timeout minute)
      println("Parser " + logElement+" execution completed at "+DateTime.now().toString())
      parserFunctionReturn = parserFunctionReturn.map(x => x.map { case (k, v) => k.replaceAll("\\W", "_") -> v.toLowerCase })

      if (parserFunctionReturn.size != 0) {
        lineOfAnamoly = parserFunctionReturn.distinct.size
        parserStatus += parserFileKey -> "T"
      }
      else {
        log_key = log_key + ">>>>Pattern records not present in the log file"
        parserStatus += parserFileKey -> "O"
      }
    }
    catch {
      case ex: TimeoutException => {
        ex.printStackTrace()
        println("Timeout Error in Parser "+ex.getMessage)
        log_key = logElement + s">>>>Parser not working - Timeout after ${timeout} minutes"
        parserStatus += parserFileKey -> "F"
        sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("DETeam"),
          s"ALERT:::Parser Timeout ${DateTime.now()} ",
          s"Parser $log_key >>>> $parserFileKey  not working - Timeout after ${timeout} minutes  for bundle id ${logBundle.id} uploaded by ${logBundle.username} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")}",conf.value.getString("region"))
      }
      case e1: InvalidArgumentException => {
        e1.printStackTrace()
        println("Exception while decryption of code from json")
        log_key = log_key + ">>>>Exception while decryption of code from json"
        parserStatus += parserFileKey -> "F"
        sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("SETeam"),
          s"ALERT:::Exception while decryption of code from json ${DateTime.now()} ",
          s"$log_key for bundle id ${logBundle.id} uploaded by ${logBundle.username} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")}",conf.value.getString("region"),
          conf.value.getString("customccSender"))
      }
      case e3: Exception => {
        println("Exception while parsing Log files")
        println(e3.getMessage)
        println("***********************************")
        log_key = log_key + ">>>>Log entries not present as per the pattern requirement"
        parserStatus += parserFileKey -> "F"
      }
      case e4: ToolBoxError => {
        println("Exception while Compilation")
        println(e4.getMessage)
        println("***********************************")
        log_key = log_key + ">>>>Error while compiling the custom parser"
        parserStatus += parserFileKey -> "F"
      }
    }

    afterParsingLogs(logBundle, log_key, input_map, input_output_map, proxyToES: ProxyToES, spark, conf, parserFunctionReturn.distinct,
      input_parser_output_map, parserCodeMap, lineOfAnamoly,logElement)
    parserStatus.toMap
  }

  def afterParsingLogs(logBundle: LogBundle, fileKey: String, input_map: Map[String, List[String]], input_output_map: Map[String, List[Int]],
                       proxyToES: ProxyToES,
                       spark: SparkSession, conf: Broadcast[JSONObject], listofMap: List[Map[String, String]],
                       input_parser_output_id_map: Map[String, ListBuffer[Int]],
                       parserCodeMap: Map[String, String], lines_of_anamoly: Int,logElement:String): Unit = {

    val fileKeyList = fileKey.split(">>>>")
    val parser = fileKeyList(0)
    val f_name = fileKeyList(1)
    val output_file_id = input_output_map(f_name)(input_map(f_name).indexOf(parser))
    var mapToReturn = mutable.Map[String, DataFrame]()
    var responseMap = mutable.Map[Int, Int]()
    val executionStartTime = DateTime.now.toString()
    val dateToProcess = DateTime.now.toLocalDate.toString()
    try {

      var input_map_keys = ListBuffer(input_map.keys.toSeq: _*)
      input_map_keys -= f_name
      var actualOutputPathId = input_output_map(f_name)(input_map(f_name).indexOf(parser))
      if (fileKeyList.length == 3) {
        fileKeyList(2) match {
          case "Pattern records not present in the log file" => {
            postParserJobOutput(logBundle.id.toString, output_file_id.toInt, executionStartTime, DateTime.now().toString, "2", fileKeyList(2), fileKeyList(2),
              "0", conf, spark, proxyToES)
          }
          case _ => {
            postParserJobOutput(logBundle.id.toString, output_file_id.toInt, executionStartTime, DateTime.now().toString, "3", fileKeyList(2), fileKeyList(2),
              "0", conf, spark, proxyToES)
          }
        }
      }
      else {
        val cols = listofMap.take(1).flatMap(_.keys)
        var newListOfMap,newListOfMapTemp = List[Map[String, Any]]()
        var rev_input_output_map = Map[Int, String]()
        input_output_map.foreach { m =>
          m._2.foreach { opfileid =>
            rev_input_output_map += opfileid -> m._1.split("/").last.trim
          }
        }
        var rev_input_parser_output_id_map = Map[Int, String]()
        input_parser_output_id_map.foreach { m =>
          m._2.foreach { opfileid =>
            val keyToGet = m._1 + ">>>>" + logBundle.path + "/" + rev_input_output_map(opfileid)
            if (parserCodeMap.keys.toList.contains(keyToGet)) {
              rev_input_parser_output_id_map += opfileid -> m._1
            }
          }
        }
        responseMap += (actualOutputPathId -> lines_of_anamoly)

        if (!cols.contains("line_number")) {
         newListOfMapTemp = listofMap.map(x => x + (("zip_id" -> logBundle.id.toString), ("output_file_id" -> output_file_id.toString),
            ("gui_rules_parser_name" -> rev_input_parser_output_id_map(output_file_id)),
            ("parser_name" -> rev_input_parser_output_id_map(output_file_id).split("__").last.mkString),
            ("input_file_id" -> rev_input_output_map(output_file_id)),
            ("PROCESS_DATE" -> dateToProcess),
            ("username" -> logBundle.username),
            ("line_number" -> 0),
            ("row_index" -> 0)))
        }
        else {
         newListOfMapTemp = listofMap.map(x => x + (("zip_id" -> logBundle.id.toString), ("output_file_id" -> output_file_id.toString),
            ("gui_rules_parser_name" -> rev_input_parser_output_id_map(output_file_id)),
            ("parser_name" -> rev_input_parser_output_id_map(output_file_id).split("__").last.mkString),
            ("input_file_id" -> rev_input_output_map(output_file_id)),
            ("PROCESS_DATE" -> dateToProcess),
            ("username" -> logBundle.username),
            ("row_index" -> x.get("line_number").get.toInt)
          ))
        }
        import scala.collection.JavaConverters._
        val belllabsParser=conf.value.getJSONArray("bellLabsParser").asScala.toList
        val flexi_mr_bts_lte_Parser=conf.value.getJSONArray("customFlexiMRBTSLTEParser").asScala.toList
        println("flexi_mr_bts_lte_Parser>>>"+flexi_mr_bts_lte_Parser)

        if(belllabsParser.contains(logElement)) {
          belllabsParser.foreach { parser =>
            if (logElement.equals(parser)) {
              newListOfMap = newListOfMapTemp.map(x => x + (("customer_name" -> logBundle.username), ("product_name" -> logBundle.product_name)))
            }
            else {
              newListOfMap = newListOfMapTemp
            }
          }
        }
       else if(flexi_mr_bts_lte_Parser.contains(logElement)) {
          flexi_mr_bts_lte_Parser.foreach { parser =>
            if (logElement.equals(parser)) {
              var fileidMap = mutable.Map[String, ListBuffer[String]]()
              var filteredfileidMap = mutable.Map[String, ListBuffer[String]]()
              logBundle.file_details.foreach { vals =>
                if (fileidMap.keys.toList.contains(vals.input_file_id.trim)) {
                  vals.input_file_id -> fileidMap(vals.file_path.trim)
                }
                else {
                  fileidMap += vals.input_file_id -> ListBuffer(vals.file_path)
                }
              }
              println("logElement>>" + logElement)
              if (logElement.contains("startup") || logElement.contains("runtime") || logElement.contains("pm")) {
                println("startup,runtime,pm Parser")
                newListOfMap = newListOfMapTemp.map(x => x + (("snapshot" -> logBundle.path.split("/")(6)), ("zip_id" -> logBundle.id), ("file_Path" -> fileidMap(rev_input_output_map(output_file_id)).head)))
              }
              else {
                println("other flexi_mr_bts Parser")
                newListOfMap = newListOfMapTemp.map(x => x + (("File_name" -> logBundle.path.split("/")(6)),("file_Path" -> fileidMap(rev_input_output_map(output_file_id)).head)))
              }
            }
          }
        }
          else {
            newListOfMap = newListOfMapTemp
          }
        if (responseMap.keys.toList.contains(output_file_id)) {
          val anomalyList = responseMap(output_file_id)
          if (anomalyList == 0) {
            postParserJobOutput(logBundle.id.toString, output_file_id.toInt, executionStartTime, DateTime.now().toString, "2", "Pattern records not present in the log file", "Pattern records not present in the log file",
              anomalyList.toString, conf, spark, proxyToES)
          } else {
            println("*********************")
            postDataToELK(newListOfMap, conf.value.getString("elkParserIndex"), conf, logBundle.id)
            postParserJobOutput(logBundle.id.toString, output_file_id.toInt, executionStartTime, DateTime.now().toString, "2", "", "",
              anomalyList.toString, conf, spark, proxyToES)

          }
        }
      }
    } catch {
      case e1@(_: EsHadoopIllegalArgumentException | _: EsHadoopNoNodesLeftException | _: SocketTimeoutException) =>
        e1.printStackTrace()
        postParserJobOutput(logBundle.id.toString, output_file_id.toInt, executionStartTime, DateTime.now().toString, "3", "Error Writing to Elastic",
          "Error Writing to Elastic", "0", conf, spark, proxyToES)
        sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("DETeam"), s"ALERT:::Error Writing to Elastic ${DateTime.now()} ", s"Error Writing to Elastic for bundle id ${logBundle.id} uploaded by ${logBundle.username} in${conf.value.getString("dataCenter")}  k8s_${conf.value.getString("region")}",conf.value.getString("region"))
        mapToReturn.toMap
      case e2: org.apache.spark.SparkException =>
        e2.printStackTrace()
        postParserJobOutput(logBundle.id.toString, output_file_id.toInt, executionStartTime, DateTime.now().toString, "3", "Invalid data format",
          "Invalid data format", "0", conf, spark, proxyToES)
        sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("DETeam"), s"ALERT:::Invalid data format ${DateTime.now()} ", s"Invalid data format for bundle id ${logBundle.id} uploaded by ${logBundle.username} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")}",conf.value.getString("region"))
        mapToReturn.toMap
      case e2@(_: java.net.UnknownHostException | _: com.amazonaws.services.s3.model.AmazonS3Exception) =>
        println("Please check s3 credentials or check appropriate permission of s3 buckets")
        val dispmessage = "Please check s3 credentials or check appropriate permission of s3 buckets"
        e2.printStackTrace()
        println("***********************************")
        postParserJobOutput(logBundle.id.toString, output_file_id.toInt, executionStartTime, DateTime.now().toString, "3", dispmessage,
          dispmessage, lines_of_anamoly.toString, conf, spark, proxyToES)
        sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"),
          conf.value.getString("SETeam"),
          s"ALERT:::Please check s3 credentials or check appropriate permission of s3 buckets ${DateTime.now()} ",
          s"Please check s3 credentials or check appropriate permission of s3 buckets for bundle id ${logBundle.id} uploaded by ${logBundle.username} in${conf.value.getString("dataCenter")} ${conf.value.getString("dataCenter")}  k8s_${conf.value.getString("region")}",conf.value.getString("region"),
          conf.value.getString("customccSender")
        )
      case e: Exception =>
        println("Error in Processing Logs")
        e.printStackTrace()
        println("***********************************")
        println(e.getMessage)
        e.getStackTrace.foreach(println)
        postParserJobOutput(logBundle.id.toString, output_file_id.toInt, executionStartTime, DateTime.now().toString, "3", "Error in Processing Logs",
          "Error in Processing Logs", lines_of_anamoly.toString, conf, spark, proxyToES)
    }
  }
}

