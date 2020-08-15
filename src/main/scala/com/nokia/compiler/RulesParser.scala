package com.nokia.compiler

import LogParser.LogBundleConfigFetcher.InputArgs
import Utils.ProxyToES
import Utils.ELKUtil._
import Utils.Util.{DuplicateColumnException, calculateRunTime, convertRowToJSON}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}
import org.elasticsearch.spark._
import org.joda.time.DateTime
import org.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.native.Json

import scala.collection.JavaConverters._
import scala.collection.mutable

object RulesParser {

  def executeRules(inputJson: InputArgs, conf: JSONObject, proxyToES: ProxyToES): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.es.index.auto.create", "true")
      .set("spark.es.nodes", conf.getString("elkIp"))
      .set("spark.es.port", conf.getString("elkPort"))
      .set("spark.es.net.http.auth.user", conf.getString("elkUserName"))
      .set("spark.es.net.http.auth.pass", conf.getString("elkPassword"))
      .set("spark.es.nodes.wan.only", "true")
      .set("spark.es.nodes.wan.only", "true")
      .set("spark.es.nodes.client.only", "false")

    val spark: SparkSession =
      SparkSession.builder().config(sparkConf).appName("AliceProcessingTwentyDotTwo").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val conf1=spark.sparkContext.broadcast(conf)
    val app_id = spark.sparkContext.getConf.get("spark.app.id")
    var userName,productName=""

    try {
      val get_driver_id = spark.sparkContext.getConf.get("hive.metastore.warehouse.dir","1")
      var driver_id=""
      if (get_driver_id!=1){
        driver_id=get_driver_id.split("/").filter(_.contains("driver-")).head
      }
      else
        driver_id="1"

      println("driver_id>>"+driver_id)
      println("app_id>>"+app_id)

      import java.net._
      val localhost: InetAddress = InetAddress.getLocalHost
      val localIpAddress: String = localhost.toString.split("/")(1)

      println(s"localIpAddress = $localIpAddress")

      val driver_app_list = List((localIpAddress,driver_id,app_id,"Alice",inputJson.sid,userName,productName,"0",DateTime.now().toLocalDate.toString()))
      import spark.implicits._
      val driver_app_df = driver_app_list.toDF("Cluster_ID","Driver_ID", "App_ID","Job_Type","zip_id","User_Name","Product_Name","Product_Id","Execution_Date")
      driver_app_df.show(false)
      writeToES(driver_app_df, conf.getString("elkRmIndex"), spark, conf1)
    }
    catch{
      case ex:Exception=> {
        println(s"Exception while writing job to ${conf.getString("elkRmIndex")} index")
        ex.printStackTrace()
        val localIpAddress=""
        val driver_id = 0
        val driver_app_list = List((localIpAddress,driver_id,app_id,"Alice",inputJson.sid,userName,productName,"0",DateTime.now().toLocalDate.toString()))
        import spark.implicits._
        val driver_app_df = driver_app_list.toDF("Cluster_ID","Driver_ID", "App_ID","Job_Type","zip_id","User_Name","Product_Name","Product_Id","Execution_Date")
        driver_app_df.show(false)
        writeToES(driver_app_df, conf.getString("elkRmIndex"), spark, conf1)
      }
    }
    var postResult = ""
    val starttime = DateTime.now()
    try {

      inputJson.customRulesData.foreach(customRule => {
        val zip_id = customRule.zip_id
        val pattern_name = customRule.pattern_name
        println(s"**********Rules Started at $starttime**********")
        pattern_name.map { pname =>
          println(s"**********Reading data from ES for parser $pname**********")
          val esData = spark.sparkContext.esJsonRDD(conf1.value.getString("elkParserIndex"),
            s"""{"query":{"bool": {"must":
               | [{"match": {"zip_id": "$zip_id"}},
               | {"match": {"gui_rules_parser_name": "$pname"}}]}}}"""
              .stripMargin).map(a => a._2)
          println(s"**********Data read completed from ES for parser $pname**********")
          println(s"**********Creating Dataframe for parser $pname**********")
          val df = spark.read.json(esData)
          df.drop("zip_id", "parser_name", "gui_rules_parser_name", "input_file_id", "output_file_id").createOrReplaceTempView(pname.split("__").last.trim)
          println(s"**********Dataframe registered for parser $pname**********")
        }
      })

      val accumulatedPatternMap=getAssociatedPattern(spark,inputJson,conf1)

      accumulatedPatternMap.keys.par.map(parser=>{
        val view_name=parser.split("__").last
        val associatedDF=accumulatedPatternMap(parser)
        associatedDF.show(10,false)
        println(s"Creating view $view_name")
        associatedDF.createOrReplaceTempView(view_name)
        spark.sqlContext.cacheTable(view_name)
      })



      val query = new String(java.util.Base64.getDecoder.decode(inputJson.query))
      println(s"**********Running query : $query**********")
      val queryOutput = spark.sql(query).limit(50)
      println(s"**********Query Ran successfully**********")
      val queryOutputCols = queryOutput.columns.toList
      if(queryOutputCols.distinct.size != queryOutputCols.size){
        throw new DuplicateColumnException(s"Duplicate columns found ${queryOutputCols.distinct.mkString(",")}")
      }
      val acc = spark.sparkContext.collectionAccumulator[Map[String, Any]]("JsonCollector")
      val jsonString = queryOutput.foreach(a => acc.add(convertRowToJSON(a)))
      println(s"**********Converting Dataframe to JSON**********")
      postResult = generateJSON(inputJson.sid, "valid", "Success", acc.value.asScala.toList)
      println(s"**********Converting Dataframe to JSON completed**********")


    }
    catch {
      case ex: DuplicateColumnException => postResult = generateJSON(inputJson.sid,"Invalid",ex.toString.split("\\n")(0).split("Exception:")(1).trim, List[Map[String, Any]]())
        ex.printStackTrace()
      case ex: ParseException => postResult = generateJSON(inputJson.sid, "Invalid", ex.toString.split("\\n")(1), List[Map[String, Any]]())
        ex.printStackTrace()
      case ex: AnalysisException => postResult = generateJSON(inputJson.sid, "Invalid", ex.toString.split("\\n")(0).split("Exception:")(1), List[Map[String, Any]]())
        ex.printStackTrace()
      case ex: Throwable => postResult = generateJSON(inputJson.sid, "Invalid", ex.toString, List[Map[String, Any]]())
        ex.printStackTrace()

    }
    finally {
      println(postResult)
      proxyToES.postJson(postResult, conf1, "rules/res_rules")
      println(s"**********Data sent to UI**********")
      val endTime = DateTime.now()
      println(s"**********GUI Rules Execution Ended at $endTime**********")
      val total_time=calculateRunTime(starttime, endTime)
      println(s"*************GUI Rules Execution Time ${calculateRunTime(starttime, endTime)}*************")
      import spark.implicits._
      import java.net._
      val userName=""
      val localhost: InetAddress = InetAddress.getLocalHost
      val localIpAddress: String = localhost.getHostName
      println(s"localIpAddress = $localIpAddress")

      val curentbundledf = List(
        (conf.getString("uIIp"),
          localIpAddress,
          inputJson.sid,
          "",
          userName,
          "",
          starttime.toString,
          endTime.toString,
          "",
          "",
          "",
          "",
          //"",
          total_time
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
      writeToES(curentbundledf, conf.getString("elkJobSummaryIndex"), spark, conf1)
      spark.stop()
    }
  }

  private def generateJSON(sid: String, status: String, msg: String, results: List[Map[String, Any]]): String = {
    val p = RulesOutput(sid, status, msg, results)
    Json(DefaultFormats).write(p)
  }

  case class RulesOutput(sid: String, status: String, msg: String, results: List[Map[String, Any]])
  def getAssociatedPattern(spark:SparkSession,inputJson:InputArgs ,conf:Broadcast[JSONObject]):Map[String,DataFrame]={
    var associatedParseMap=mutable.Map[String,DataFrame]()
    val associatedBundles=inputJson.associated_log_bundles
    import org.elasticsearch.spark._
    println("Fetching associated_log_bundles for "+associatedBundles)
    associatedBundles.foreach(vals=>{
      val zip_id=vals.id
      val patternlist=vals.pattern_name
      patternlist.map{patterns=>
        println(s"Fetching for $zip_id and $patterns")
        val getELKData= spark.sparkContext.esJsonRDD(conf.value.getString("elkParserIndex"),
          s"""{"query":{"bool": {"must":
             | [{"match": {"zip_id": "$zip_id"}},
             | {"match": {"gui_rules_parser_name": "$patterns"}}]}}}"""
            .stripMargin).map(a => a._2)
        val colstodelete=List("output_file_id","parser_name","gui_rules_parser_name","pattern_name")
        val elkDF=spark.read.json(getELKData)
        associatedParseMap+=patterns->elkDF.drop(colstodelete:_*)
      }
    })
    associatedParseMap.toMap
  }

}
