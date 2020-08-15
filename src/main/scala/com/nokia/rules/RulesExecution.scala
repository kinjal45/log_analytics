package com.nokia.rules

import LogParser.LogBundleConfigFetcher.LogBundle
import Utils.ProxyToES
import Utils.ELKUtil._
import Utils.Util._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._
import org.joda.time.DateTime
import org.json.JSONObject

import scala.collection.immutable.TreeMap
import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object RulesExecution {

  def registerTempViews(logID: String, executionStartTime: DateTime, mapTempViews: mutable.Map[String, ListBuffer[Int]],
                        parsedOutput: Map[String, String], spark: SparkSession,
                        input_parser_output_id_map: Map[String, ListBuffer[Int]], conf: Broadcast[JSONObject],
                        input_map: Map[String, List[String]], proxyToES: ProxyToES, logBundle: LogBundle): (TreeMap[String, Map[String, String]], List[Int]) = {
    val lines_of_anamoly = TreeMap[String, Map[String, String]]()
    var listFailedRules = ListBuffer[Int]()
    val accumulatedPatternMap = getAssociatedPattern(spark, logBundle, conf)

    accumulatedPatternMap.keys.par.map(parser => {
      val view_name = parser.split("__").last
      val associatedDF = accumulatedPatternMap(parser)
      associatedDF.show(10, false)
      println(s"Creating view $view_name")
      associatedDF.createOrReplaceTempView(view_name)
      spark.sqlContext.cacheTable(view_name)
    })
    println("##############################")
    println(parsedOutput)
    println("##############################")
    mapTempViews.keys.par.map(parser => {
      try {
        val dfMapKeyList = parsedOutput.keys.filter(_.contains(parser)).toList
        var ifDataPresent= false
        if (dfMapKeyList.nonEmpty) {
          val parserFileList = parsedOutput.keys.toList.filter(_.split(">>>>")(0).equals(parser))
          parserFileList.foreach { parserFileName =>
            if (parsedOutput(parserFileName).contains("T")) {
              ifDataPresent = true
            }
          }
          if(ifDataPresent){
            val sourceDF = fetchDatafromELK(parser, conf, logID, spark)
            val viewname = parser.split("__").last
            val logfile = dfMapKeyList.head.split(">>>>")(1)
            println("Registering temp view for "+viewname)
            sourceDF.withColumn("LineOfAnamoly", concat_ws(">>", col("line_number"), lit(logfile)))
              .createOrReplaceTempView(viewname)
            spark.sqlContext.cacheTable(viewname)
          }else {
              val disp_msg = "No anomalies/failure in parser>>" + parser+ " either because the required file not present or the pattern not found in the uploaded file"
              mapTempViews(parser).foreach(ruleOutput => {
                if (!listFailedRules.contains(ruleOutput)) {
                  listFailedRules += ruleOutput
                }
                postRuleJobOutput(logID.toString, ruleOutput, executionStartTime.toString(), DateTime.now().toString, "2", disp_msg,
                  disp_msg, "0", conf, spark, proxyToES)
              })
            }
        }
        else {
          val disp_msg = "No anomalies/failure in parser>>" + parser+ " either because the required file not present or the pattern not found in the uploaded file"
          mapTempViews(parser).foreach(ruleOutput => {
            if (!listFailedRules.contains(ruleOutput)) {
              listFailedRules += ruleOutput
            }
            postRuleJobOutput(logID.toString, ruleOutput, executionStartTime.toString(), DateTime.now().toString, "2", disp_msg,
              disp_msg, "0", conf, spark, proxyToES)
          })
        }
      }
      catch {
        case e: Exception =>
          println("Exception in registering temp view")
          e.printStackTrace()
          println(e.getMessage)
          e.getStackTrace.foreach(println)
          val disp_msg = "Failed to execute rule due to failure in parser>>" + parser
          mapTempViews(parser).foreach(ruleOutput => {
            if (!listFailedRules.contains(ruleOutput)) {
              listFailedRules += ruleOutput
              postRuleJobOutput(logID.toString, ruleOutput, executionStartTime.toString(), DateTime.now().toString, "3", disp_msg,
                "Failed to execute rule due to failure in parser>>" + parser,
                "", conf, spark, proxyToES)
               }
          })
      }
    })
    (lines_of_anamoly, listFailedRules.toList)
  }

  def getAssociatedPattern(spark: SparkSession, logBundle: LogBundle, conf: Broadcast[JSONObject]): Map[String, DataFrame] = {
    var associatedParseMap = mutable.Map[String, DataFrame]()
    val associatedBundles = logBundle.associated_log_bundles
    println("Fetching associated_log_bundles for " + associatedBundles)
    associatedBundles.foreach(vals => {
      val zip_id = vals.id
      val patternlist = vals.pattern_name
      patternlist.map { patterns =>
        println(s"Fetching for $zip_id and $patterns")
        val getELKData = spark.sparkContext.esJsonRDD(conf.value.getString("elkParserIndex"),
          s"""{"query":{"bool": {"must":
             | [{"match": {"zip_id": "$zip_id"}},
             | {"match": {"gui_rules_parser_name": "$patterns"}}]}}}"""
            .stripMargin).map(a => a._2)
        val colstodelete = List("output_file_id", "parser_name", "gui_rules_parser_name", "pattern_name")
        val elkDF = spark.read.json(getELKData)
        associatedParseMap += patterns -> elkDF.drop(colstodelete: _*)
      }
    })
    associatedParseMap.toMap
  }


}
