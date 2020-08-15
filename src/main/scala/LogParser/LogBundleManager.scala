package LogParser

import LogParser.LogBundleConfigFetcher.{InputArgs, LogBundle}
import Utils.ELKUtil._
import Utils.ProxyToES
import Utils.Util._
import com.nokia.parsing.LogParsing
import com.nokia.rules.{ProcessRules, RulesExecution}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.{DateTime, Hours, Minutes, Seconds}
import org.json.JSONObject

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scalaj.http.{Http, HttpOptions}

object LogBundleManager {

  def analyze(logBundle: LogBundle, spark: SparkSession, starttime: DateTime, conf: Broadcast[JSONObject], proxyToES: ProxyToES):
  (Int, DateTime, Int, String, String, String, ListBuffer[DataFrame]) = {

    val initializeDataStructureForSparkJobResult: (DateTime, Map[String, List[String]], ListBuffer[Map[String, String]], Int, Map[String, ListBuffer[String]], Map[String, List[Int]], mutable.Map[String, ListBuffer[Int]]) = LogBundleManager.initializeDataStructureForSparkJob(spark,logBundle, conf, proxyToES)
    val parserstarttime: DateTime = initializeDataStructureForSparkJobResult._1
    val input_map: Map[String, List[String]] = initializeDataStructureForSparkJobResult._2
    val parserCodeMapList: ListBuffer[Map[String, String]] = initializeDataStructureForSparkJobResult._3
    val totalParserCount: Int = initializeDataStructureForSparkJobResult._4
    val parser_input_filemap: Map[String, ListBuffer[String]] = initializeDataStructureForSparkJobResult._5
    val input_output_map: Map[String, List[Int]] = initializeDataStructureForSparkJobResult._6
    val input_parser_output_id_map: mutable.Map[String, ListBuffer[Int]] = initializeDataStructureForSparkJobResult._7


    val parsedOutput = LogParsing.parseLogFiles(logBundle, input_map, input_output_map, input_parser_output_id_map.toMap, spark, conf,
      totalParserCount, parserCodeMapList.flatten.toMap[String, String], proxyToES)
    println("*******PARSING COMPLETED*********")


    val rulestarttime = DateTime.now()
    val logparsingexectime = calculateRunTime(parserstarttime, rulestarttime)
    println("Log Parsing  execution took time of>>" + logparsingexectime)

    try {
      postjobresponse(logBundle.id, "parser_timing", parserstarttime.toString, rulestarttime.toString, conf, proxyToES)
    }
    catch{
      case e:Exception=>e.printStackTrace()
    }

    // val mltype = "rule_timing"
    var totaljobexecutiontime, ruleexecutionTime = ""
    var ruleendTime: DateTime = null
    var rulescount = 0
    var ML_executed = false
    val mltype = "rule_timing"
    var listRuleOutput = ListBuffer[DataFrame]()

    def submit_ml_api(zip_id: String, product_name: String, conf: Broadcast[JSONObject], starttime: DateTime, endtime: DateTime) = {
      println("Starting with the ML execution")

      println("Starting with the Similarity function execution")

      val rawFileIdWithCount = getRawFileIdAndCount(conf, logBundle, spark)

      println("rawFileIdWithCount>>>" + rawFileIdWithCount)
      /*val jsonMLStartString = s"""{"zip_id": "${logBundle.id}" , "msg": "ML Job Execution Started @ ${rulestarttime.toString}"}"""
      proxyToES.postJson(jsonMLStartString, conf, "general_status")
     */
      println(
        s"""{"elkRuleIndex": "${conf.value.getString("elkRuleIndex")}",
           |"elkParserIndex": "${conf.value.getString("elkParserIndex")}",
           |"elkRawFileIndex": "${conf.value.getString("elkRawFileIndex")}",
           |  "zipID": "${logBundle.id.toString}",
           |  "elkIP": "${conf.value.getString("elkIp")}",
           |  "elkPort": "${conf.value.getString("elkPort")}",
           |  "elkUserName": "${conf.value.getString("elkUserName")}",
           |  "elkPassword": "${conf.value.getString("elkPassword")}",
           |  "reqRawFileID":"${rawFileIdWithCount._2}",
           |  "totalRawFileCount":"${rawFileIdWithCount._1}",
           |  "uIIp":"${conf.value.getString("uIIp")}",
           |  "productId":"${logBundle.prod_id}"}""".stripMargin)

      val response = Http(s"http://${conf.value.getString("rmIp")}:5000/MLSubmit")
        .postData(
          s"""{"elkRuleIndex": "${conf.value.getString("elkRuleIndex")}",
             |"elkParserIndex": "${conf.value.getString("elkParserIndex")}",
             |"elkRawFileIndex": "${conf.value.getString("elkRawFileIndex")}",
             |  "zipID": "${logBundle.id.toString}",
             |  "elkIP": "${conf.value.getString("elkIp")}",
             |  "elkPort": "${conf.value.getString("elkPort")}",
             |  "elkUserName": "${conf.value.getString("elkUserName")}",
             |  "elkPassword": "${conf.value.getString("elkPassword")}",
             |  "reqRawFileID":"${rawFileIdWithCount._2}",
             |  "totalRawFileCount":"${rawFileIdWithCount._1}",
             |  "uIIp":"${conf.value.getString("uIIp")}",
             |  "productId":"${logBundle.prod_id}"}""".stripMargin)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .options(HttpOptions.connTimeout(5000000)).options(HttpOptions.readTimeout(5000000)).asString
      println(response)
    }
    def raw_file_archival(zip_id: String,conf: Broadcast[JSONObject],spark:SparkSession)={
      val bundlename=logBundle.path.split("/")(4)
      val rawFileCount=spark.sparkContext.textFile(logBundle.path+"/*")
      println(s"Calling Raw file archival for $zip_id for bundle $bundlename")
      println(s"""{
          |"elkRawFileIndex": "${conf.value.getString("elkRawFileIndex")}",
          |"zip_id": "${logBundle.id.toString}",
          |"elkIP": "${conf.value.getString("elkIp")}",
          |"elkPort": "${conf.value.getString("elkPort")}",
          |"elkUserName": "${conf.value.getString("elkUserName")}",
          |"elkPassword": "${conf.value.getString("elkPassword")}",
          |"rawFileCount":"${rawFileCount.count()}",
          |"minioS3Endpoint": "${conf.value.getString("minioS3Endpoint")}",
          |"minioS3SecretKey": "${logBundle.k8_s3_secret}",
          |"minioS3AccessKey": "${logBundle.k8_s3_key}",
          |"minioS3BucketName": "${conf.value.getString("minioS3BucketName")}",
          |"minioObjectPath": "${logBundle.path.split("//")(1)}",
          |"globalS3Endpoint": "${logBundle.global_s3_url}",
          |"globalS3AccessKey": "${logBundle.global_s3_key}",
          |"globalS3SecretKey": "${logBundle.global_s3_secret}",
          |"globalS3BucketName": "${conf.value.getString("globalS3BucketName")}"
          |}""".stripMargin)
      val response = Http(s"http://${conf.value.getString("rmIp")}:5000/archive")
        .postData(
          s"""{
             |"elkRawFileIndex": "${conf.value.getString("elkRawFileIndex")}",
             |"zip_id": "${logBundle.id.toString}",
             |"elkIP": "${conf.value.getString("elkIp")}",
             |"elkPort": "${conf.value.getString("elkPort")}",
             |"elkUserName": "${conf.value.getString("elkUserName")}",
             |"elkPassword": "${conf.value.getString("elkPassword")}",
             |"rawFileCount":"${rawFileCount.count()}",
             |"minioS3Endpoint": "${conf.value.getString("minioS3Endpoint")}",
             |"minioS3SecretKey": "${logBundle.k8_s3_secret}",
             |"minioS3AccessKey": "${logBundle.k8_s3_key}",
             |"minioS3BucketName": "${conf.value.getString("minioS3BucketName")}",
             |"minioObjectPath": "${logBundle.path.split("//")(1)}",
             |"globalS3Endpoint": "${logBundle.global_s3_url}",
             |"globalS3AccessKey": "${logBundle.global_s3_key}",
             |"globalS3SecretKey": "${logBundle.global_s3_secret}",
             |"globalS3BucketName": "${conf.value.getString("globalS3BucketName")}"
             |}""".stripMargin)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .options(HttpOptions.connTimeout(5000000)).options(HttpOptions.readTimeout(5000000)).asString
      println(response)
    }
    try {

      // val query = s"""?q=file_path:*Computer_log* AND zip_id:${logBundle.id}"""
      import scala.collection.JavaConverters._
      val productIDForML=conf.value.getJSONArray("parserDependentMLProductIDs").asScala.toList
      /*if ((logBundle.product_name == "Flexi-BSC")) {*/
      if (productIDForML.contains(logBundle.prod_id)) {
        submit_ml_api(logBundle.id.toString, logBundle.product_name.toString, conf, parserstarttime, rulestarttime)
        println("Computer logs are present, ML execution is processed")
        ML_executed = true
      }


      println("*******STARTING RULES*********")
      try {
        postjobresponse(logBundle.id, "rule_timing", rulestarttime.toString, "", conf, proxyToES)
      }
      catch{
        case e:Exception=>e.printStackTrace()
      }
      var mapTempViews = mutable.Map[String, ListBuffer[Int]]()
      logBundle.rules.foreach(rule => {
        rule.pattern_name.foreach(pattern => {
          if (!mapTempViews.keys.toList.contains(pattern.trim)) mapTempViews += pattern.trim -> mutable.ListBuffer[Int](rule.output_file_id)
          else mapTempViews(pattern.trim) += rule.output_file_id
        })
      })

      var rule_opid_map = Map[String, Int]()
      logBundle.rules.foreach { valu =>
        val rulename = valu.rule_name
        val opid = valu.output_file_id
        rule_opid_map += rulename.trim -> opid
      }


      val linesOfAnamoly = RulesExecution.registerTempViews(logBundle.id, rulestarttime, mapTempViews, parsedOutput, spark, input_parser_output_id_map.toMap,
        conf, input_map, proxyToES, logBundle)

      println("Starting Rules Execution after Creating Temp Views")
      logBundle.rules.filter(x => !linesOfAnamoly._2.contains(x.output_file_id)).par.map(rule => {
        listRuleOutput += ProcessRules.executeRule(rule, spark, logBundle.id.toString, input_parser_output_id_map.toMap, linesOfAnamoly._1, conf,
          logBundle, parser_input_filemap, rule_opid_map, proxyToES)
        rule
      })
      ruleendTime = DateTime.now()

      ruleexecutionTime = calculateRunTime(rulestarttime, ruleendTime)

      println("*******RULES COMPLETED*********")
      println("Rules execution took time of>>" + ruleexecutionTime)
      println("Log Parsing  execution took time of>>" + logparsingexectime)

      try {
        postjobresponse(logBundle.id, "rule_timing", rulestarttime.toString, ruleendTime.toString, conf, proxyToES)
      }
      catch{
        case e:Exception=>e.printStackTrace()
      }
      try {
        //if ((conf.value.getJSONArray("productsApplicableForML").toList contains (logBundle.product_name)) & !ML_executed) {
        if ((conf.value.getJSONArray("productsIdsApplicableForML").toList contains (logBundle.prod_id)) & !ML_executed) {
          submit_ml_api(logBundle.id.toString, logBundle.product_name.toString, conf, parserstarttime, ruleendTime)
        }

      }
      catch {
        case ex: Exception => ex.printStackTrace()
      }

      /* val mlendtime = DateTime.now()
     // mlexectime = calculateRunTime(ruleendTime, mlendtime)
      println("ML execution took time of>>" + mlexectime)*/

      /*val jsonMLEndString = s"""{"zip_id": "${logBundle.id}" , "msg": "ML Job Execution Completed @ ${mlendtime.toString}"}"""
    proxyToES.postJson(jsonMLEndString, conf, "general_status")*/

      totaljobexecutiontime = (Hours.hoursBetween(parserstarttime, ruleendTime).getHours).toString + " Hour " + (Minutes.minutesBetween(parserstarttime, ruleendTime).getMinutes % 60).toString + " Minutes " +
        (Seconds.secondsBetween(parserstarttime, ruleendTime).getSeconds % 60).toString + " Seconds"

      //raw_file_archival(logBundle.id,conf,spark)

    }
    catch {
      case ex: Exception => ex.printStackTrace()
    }
    (rulescount, ruleendTime, totalParserCount, logparsingexectime, ruleexecutionTime, totaljobexecutiontime, listRuleOutput)

  }


  def initializeDataStructureForSparkJob(spark:SparkSession=null,logBundle: LogBundle, conf: Broadcast[JSONObject], proxyToES: ProxyToES): (DateTime, Map[String, List[String]], ListBuffer[Map[String, String]], Int, Map[String, ListBuffer[String]], Map[String, List[Int]], mutable.Map[String, ListBuffer[Int]]) = {
    val parserstarttime = DateTime.now()
    var input_map = Map[String, List[String]]()
    var parserCodeMapList = ListBuffer[Map[String, String]]()
    var parser_input_filemap = Map[String, ListBuffer[String]]()
    var input_output_map = Map[String, List[Int]]()
    var totalParserCount = 0
    var input_parser_output_id_map = mutable.Map[String, ListBuffer[Int]]()

    if(!logBundle.parsing.isEmpty){
      postjobresponse(logBundle.id, "parser_timing", parserstarttime.toString, "", conf, proxyToES)
      logBundle.parsing.map(parser => {
        input_map += (logBundle.path.trim + "/" + parser.input_file_id.trim -> parser.pattern_code_map.map(_.keys.head))
        totalParserCount += parser.pattern_code_map.length
        parser.pattern_code_map.foreach(x => {
          parserCodeMapList += x.map { case (key, value) => key.trim + ">>>>" + logBundle.path.trim + "/" + parser.input_file_id.trim -> value }
        })
        parser
      })

      input_map.foreach {
        data =>
          val parserlist = data._2
          parserlist.foreach { parser =>
            if (parser_input_filemap.keys.toList.contains(parser)) {
              parser_input_filemap(parser) += data._1.split("/").last.trim
            }
            else {
              parser_input_filemap += parser -> ListBuffer(data._1.split("/").last.trim)
            }
          }
      }

      logBundle.parsing.map(parser => {
        input_output_map += (logBundle.path.trim + "/" + parser.input_file_id.trim -> parser.output_file_id)
        parser
      })
      input_map.keys.foreach { keys =>
        val parserlist = input_map(keys)
        val opidlist = input_output_map(keys)
        for (i <- parserlist.indices) {
          if (input_parser_output_id_map.keys.toList.contains(parserlist(i))) {
            input_parser_output_id_map(parserlist(i)) += opidlist(i)
          }
          else {
            input_parser_output_id_map += parserlist(i).trim -> ListBuffer(opidlist(i))
          }
        }
      }
    }
    else{
      sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"),
        conf.value.getString("SETeam"),
        s"ALERT:::Parser information empty..Please check",
        s"Parser information empty for bundle id ${logBundle.id} uploaded by user ${logBundle.username} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")}  region.",conf.value.getString("region"),
        conf.value.getString("customccSender")
      )
    }
    (parserstarttime, input_map, parserCodeMapList, totalParserCount, parser_input_filemap, input_output_map, input_parser_output_id_map)
  }

  def writeSparkJobSummary(spark: SparkSession, logBundle: LogBundle, rulescount: Int, ruleendTime: DateTime, totalParserCount: Int,
                               logparsingexectime: String, ruleexecutionTime: String, totaljobexecutiontime: String,
                               starttime: DateTime, conf: Broadcast[JSONObject],
                               inputJson: InputArgs, proxyToES: ProxyToES): Unit = {
    import spark.implicits._
    var ruleCountCounter = rulescount
    import java.net._
    val userName = logBundle.username
    val localhost: InetAddress = InetAddress.getLocalHost
    val localIpAddress: String = localhost.getHostName
    println(s"localIpAddress = $localIpAddress")
    //val filename = "spark-execution-summary_" + Date.valueOf(DateTime.now.toLocalDate.toString) + ".csv"
    logBundle.rules.foreach { _ => ruleCountCounter += 1 }
    val curentbundledf = List((conf.value.getString("uIIp"),
      localIpAddress,
      logBundle.id,
      logBundle.product_name,
      userName,
      logBundle.path,
      starttime.toString,
      ruleendTime.toString(),
      totalParserCount,
      logparsingexectime,
      ruleCountCounter,
      ruleexecutionTime,
      //mlexectime,
      totaljobexecutiontime
    )).toDF(
      "ALICE_URL",
      "CLUSTER_ID",
      "BUNDLE_ID",
      "PRODUCT_NAME",
      "USER_NAME",
      "BUNDLE_PATH",
      "SPARK_EXECUTION_START_TIME",
      "SPARK_EXECUTION_END_TIME",
      "PARSER_COUNT",
      "PARSER_EXECUTION_TIME",
      "RULES_COUNT",
      "RULE_EXECUTION_TIME",
      //"ML_EXECUTION_TIME",
      "TOTAL_JOB_EXECUTION_TIME"
    )
    curentbundledf.show(false)
    writeToES(curentbundledf, conf.value.getString("elkJobSummaryIndex"), spark, conf)
    println(s"Spark Execution Summary Report generated......Done in ELK spark_execution_summary Index of machine ${conf.value.getString("elkIp")}")
  }

}
