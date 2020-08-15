package com.nokia.rules

import LogParser.LogBundleConfigFetcher.{LogBundle, Rules}
import Utils.ELKUtil._
import Utils.ProxyToES
import Utils.Util.{postRuleJobOutput, sendScalaMail}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.joda.time.DateTime
import org.json.JSONObject
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.immutable.TreeMap
import scala.collection.mutable.ListBuffer
import scala.concurrent.{Await, Future, TimeoutException}

object ProcessRules {

  def executeRule(rule: Rules, spark: SparkSession, logId: String,
                  parser_output_map: Map[String, ListBuffer[Int]], lines_of_anamoly: TreeMap[String, Map[String, String]],
                  conf: Broadcast[JSONObject], logBundle: LogBundle,
                  parser_input_filemap: Map[String, ListBuffer[String]], rule_opid_map: Map[String, Int], proxyToES: ProxyToES): DataFrame = {
    val executionStartTime = DateTime.now.toString()
    val query_decoded = new String(java.util.Base64.getDecoder.decode(rule.query))
    println("rule name>>>" + rule.rule_name)
    var dfForES = spark.emptyDataFrame
    var selectedsparksqlDF, joinedDF, cleareddfforES = spark.emptyDataFrame
    var output_id_parsermap = Map[Int, List[String]]()
    logBundle.rules.map { rules =>
      output_id_parsermap += rules.output_file_id -> rules.pattern_name.map(x => x.split("__").last.trim)
    }
    import scala.collection.JavaConverters._
    var timeout=0

    val customproductidcheckforTimeout=conf.value.getJSONArray("customProductId").asScala.toList
    /* if(customproductcheckforTimeout.contains(logBundle.product_name.trim)){*/
    if(customproductidcheckforTimeout.contains(logBundle.prod_id)){
      timeout=conf.value.getString("customexecutionTimeOut").toInt
    }
    else{
      timeout=conf.value.getString("defaultexecutionTimeOut").toInt
    }
    try {
      println("query_decoded>>" + query_decoded.toLowerCase)
      var sparksqlDFcount = 0L
      var sparksqlDF:Dataset[Row]=null
      var formattedquery=""

      val splitdecodedqueryArray=query_decoded.toLowerCase.split(" ").map(_.split(",")).flatten
      val line_number_stats=splitdecodedqueryArray.filter(_.contains("line_number"))
      println("line_number_stats>>"+line_number_stats.toList)
      // val input_file_id_stats=splitdecodedqueryArray.filter(_.contains("input_file_id"))

      if (line_number_stats.length > 1) {
        val lineno_cols = line_number_stats.filter(_.contains(".line_number"))
        val lineno_cols_dash = query_decoded.toLowerCase.contains("line_number_")
        println("lineno_cols_dash>>"+lineno_cols_dash)
        if (lineno_cols.length > 0) {
          if(lineno_cols_dash) {
            println("1")
            formattedquery = (s"select ${lineno_cols.head} as line_number,${lineno_cols.head.split("\\.").head}.input_file_id as input_file_id,").concat(query_decoded.toLowerCase().stripPrefix("select")).toLowerCase()
            //formattedquery = (s"select ${lineno_cols.head.split("\\.").head}.input_file_id as input_file_id,").concat(query_decoded.toLowerCase().stripPrefix("select")).toLowerCase()
          }
          else {
            println("2")
            formattedquery = query_decoded.toLowerCase()
          }
        }
        else{
          println("3")
          formattedquery = query_decoded.toLowerCase()
        }
      }

      if (line_number_stats.length == 0) {
        if(query_decoded.toLowerCase().split(" ").filter(_.contains("*")).size>0) {
          println("4")

          if(query_decoded.toLowerCase().contains(".*")||query_decoded.toLowerCase().contains(" *")){
            println("4A")
            formattedquery = query_decoded.toLowerCase()
          }

          else {
            println("4B")
            if(query_decoded.toLowerCase.contains("group by")){
              val subformattedquery = ("select input_file_id,line_number, ").concat(query_decoded.toLowerCase().stripPrefix("select ")).toLowerCase()
              formattedquery=subformattedquery.replace("group by " ,"group by input_file_id,line_number,")
            }
            else
              formattedquery = ("select input_file_id,line_number, ").concat(query_decoded.toLowerCase().stripPrefix("select ")).toLowerCase()
          }//("select input_file_id,line_number, ").concat(query_decoded.toLowerCase().stripPrefix("select * ")).toLowerCase()
        }
        else
        {
          println("5")
          if(query_decoded.toLowerCase.contains("group by")){
            val subformattedquery = ("select input_file_id,line_number, ").concat(query_decoded.toLowerCase().stripPrefix("select ")).toLowerCase()
            formattedquery=subformattedquery.replace("group by " ,"group by input_file_id,line_number,")
          }
          else if(query_decoded.toLowerCase.contains("join")){
            val query=query_decoded.toLowerCase.trim
            val tablename=query.substring(query.indexOf("select")+7,query.indexOf(".")).trim
            formattedquery=(s"select ${tablename}.input_file_id,${tablename}.line_number, ").concat(query_decoded.toLowerCase().stripPrefix("select ")).toLowerCase()
          }
          else{
            formattedquery = ("select input_file_id,line_number, ").concat(query_decoded.toLowerCase().stripPrefix("select ")).toLowerCase()
          }
        }
      }

      if (line_number_stats.length == 1) {
        println("6")
        formattedquery=("select input_file_id,").concat(query_decoded.toLowerCase().stripPrefix("select")).toLowerCase()
      }
      println("formattedquery>>>"+formattedquery)
      import scala.concurrent.duration._
      lazy val f = Future {
        println(s"Rule ${rule.rule_name} execution started at ${DateTime.now()}")
        sparksqlDF= spark.sqlContext.sql(formattedquery).dropDuplicates().persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        sparksqlDFcount = sparksqlDF.count()
      }
      Await.result(f, timeout minute)
      println(s"Rule ${rule.rule_name} execution completed at ${DateTime.now()}")
      println("count of rules>>" + sparksqlDFcount)
      if (sparksqlDFcount > 0) {
        var inputid_linenumber_df = spark.emptyDataFrame
        val patternlist = rule.pattern_name

        /*patternlist.foreach { pattern =>
          val patternname = pattern.split("__").last.trim
          inputid_linenumber_df = spark.sqlContext.sql(s"select line_number,input_file_id from $patternname").dropDuplicates()
        }

        val line_numbers_cols = sparksqlDF.columns.filter(_.contains("line_number"))

        if (line_numbers_cols.length > 1) {
          selectedsparksqlDF = inputid_linenumber_df.join(
            (sparksqlDF.withColumn("line_number_list", lit(concat_ws(",", line_numbers_cols.map(c => col(c)): _*)))
              .withColumn("line_number_array", split(col("line_number_list"), ",").cast("array<Integer>"))
              .withColumn("line_number", explode(col("line_number_array")))
              .drop("line_number_array").drop("line_number_list")).select("line_number"), Seq("line_number"))

          if (sparksqlDF.columns.contains("input_file_id") && selectedsparksqlDF.columns.contains("input_file_id")) {
            joinedDF = sparksqlDF.withColumn("matched_column", row_number().over(Window.orderBy(lit(1))))
              .join(selectedsparksqlDF.withColumn("matched_column", row_number().over(Window.orderBy(lit(1)))), Seq("input_file_id", "matched_column"))
              .drop("matched_column")

          }
          else {
            joinedDF = sparksqlDF.withColumn("matched_column", row_number().over(Window.orderBy(lit(1)))).
              join(selectedsparksqlDF.withColumn("matched_column", row_number().over(Window.orderBy(lit(1)))), Seq("matched_column"))
              .drop("matched_column")
          }
        }

        if (line_numbers_cols.length == 0) {
          selectedsparksqlDF = inputid_linenumber_df
            .withColumn("matchedcol", row_number().over(Window.orderBy(lit(1))))
          if (sparksqlDF.columns.contains("input_file_id") && selectedsparksqlDF.columns.contains("input_file_id"))
            joinedDF = sparksqlDF.withColumn("matchedcol", row_number().over(Window.orderBy(lit(1)))).join(selectedsparksqlDF, Seq("matchedcol", "input_file_id")).drop("matchedcol")
          else
            joinedDF = sparksqlDF.withColumn("matchedcol", row_number().over(Window.orderBy(lit(1)))).join(selectedsparksqlDF, Seq("matchedcol")).drop("matchedcol")
        }

        if (line_numbers_cols.length == 1) {
          selectedsparksqlDF = inputid_linenumber_df
          if (sparksqlDF.columns.contains("input_file_id") && selectedsparksqlDF.columns.contains("input_file_id"))
            joinedDF = sparksqlDF.join(selectedsparksqlDF, Seq("line_number", "input_file_id"))
          else
            joinedDF = sparksqlDF.join(selectedsparksqlDF, Seq("line_number"))
        }
*/
        cleareddfforES = sparksqlDF.dropDuplicates()

        dfForES = cleareddfforES.drop("LineOfAnamoly").withColumn("zip_id", lit(logId.toString))
          .withColumn("output_file_id", lit(rule.output_file_id.toString))
          .withColumn("rule_applied", lit(rule.rule_name))
        //.repartition(getExecutorCoresCount(spark))
      }
      if (sparksqlDFcount == 0) {
        postRuleJobOutput(logId, rule_opid_map(rule.rule_name.trim), executionStartTime, DateTime.now().toString, "2", "Anomalies not found", "Anomalies not found",
          "0", conf, spark, proxyToES)
      }
      else {
        writeToES(dfForES, conf.value.getString("elkRuleIndex"), spark, conf)
        postRuleJobOutput(logId, rule_opid_map(rule.rule_name.trim), executionStartTime, DateTime.now().toString, "2", "", "",
          sparksqlDFcount.toString, conf, spark, proxyToES)
      }
    }
    catch {
      case ex: TimeoutException => {
        ex.printStackTrace()
        val disp_msg = s"Rule not working - - Timeout after ${timeout} minutes"
        println("Timeout Error in Parser " + ex.getMessage)
        postRuleJobOutput(logId, rule_opid_map(rule.rule_name.trim), executionStartTime, DateTime.now().toString, "3", disp_msg, disp_msg,
          "", conf, spark, proxyToES)
       sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("mailIdtoList"), s"$disp_msg ${DateTime.now()} ", s"ALERT:::Rule Timeout after ${timeout} minutes for bundle id ${logId} uploaded by ${logBundle.username} for rule ${rule.rule_name} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")} region", conf.value.getString("region"))
      }
      case e: org.apache.spark.SparkException =>{
        e.printStackTrace()
        if (e.getStackTrace.contains("illegal_argument_exception")) {
          postRuleJobOutput(logBundle.id.toString, rule_opid_map(rule.rule_name.trim), executionStartTime, DateTime.now().toString, "3", "Invalid data format",
            "Invalid data format", "0", conf, spark, proxyToES)
          sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("mailIdtoList"), s"ALERT:::Invalid data format dated ${DateTime.now()} ", s"Invalid data format exception for bundle id ${logId} uploaded by ${logBundle.username} for rule ${rule.rule_name} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")} region", conf.value.getString("region"))
        }
        else if (e.getStackTrace.contains("TimeoutException")) {
          e.printStackTrace()
          val disp_msg = s"Rule not working - - Timeout after ${timeout} minutes"
          println("Timeout Error in Parser " + e.getMessage)
          postRuleJobOutput(logId, rule_opid_map(rule.rule_name.trim), executionStartTime, DateTime.now().toString, "3", disp_msg, disp_msg,
            "", conf, spark, proxyToES)
        }
        else  {
          val disp_msg = "Error while Rule Parsing"
          e.printStackTrace()
          postRuleJobOutput(logId, rule_opid_map(rule.rule_name.trim), executionStartTime, DateTime.now().toString, "3", disp_msg, disp_msg,
            "", conf, spark, proxyToES)
          sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("DETeam"), s"ALERT:::$disp_msg ${DateTime.now()} ", s"$disp_msg for bundle id ${logId} uploaded by ${logBundle.username} for rule ${rule.rule_name} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")} region", conf.value.getString("region"))
        }
      }
      case e1: AnalysisException =>
        var disp_msg = ""
        println(e1.getMessage())
        if (e1.getMessage().contains("Table or view not found")) {
          val failedParserName = e1.getMessage().split(":")(1).split(";")(0).trim
          disp_msg = s"File not present for parser: $failedParserName in uploaded bundle"
        } else {
          var msg = e1.getMessage()
          if (msg.length > 50)
            msg = msg.substring(0, 50)
          disp_msg = "Incorrect SQL query..Please check syntax of SQL query " + msg.replaceAll("\\W", " ")
          sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("DETeam"), s"ALERT:::$disp_msg ${DateTime.now()} ", s"Incorrect SQL query for bundle id ${logId} uploaded by ${logBundle.username} for rule ${rule.rule_name} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")} region",conf.value.getString("region"))
        }
        e1.printStackTrace()
        println("*********************")
        postRuleJobOutput(logId, rule_opid_map(rule.rule_name.trim), executionStartTime, DateTime.now().toString, "3", disp_msg, disp_msg,
          "", conf, spark, proxyToES)
      case e2: Exception =>
        println("Exception in Rule Parsing " + rule.rule_name)
        val disp_msg = "Error while Rule Parsing"
        println(e2.getMessage)
        e2.getStackTrace.foreach(println)
        e2.printStackTrace()
        println("*********************")
        postRuleJobOutput(logId, rule_opid_map(rule.rule_name.trim), executionStartTime, DateTime.now().toString, "3", disp_msg, disp_msg,
          "", conf, spark, proxyToES)
        sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"), conf.value.getString("DETeam"), s"ALERT:::$disp_msg ${DateTime.now()} ", s"$disp_msg for bundle id ${logId} uploaded by ${logBundle.username} for rule ${rule.rule_name} in${conf.value.getString("dataCenter")}  k8s_${conf.value.getString("region")} region",conf.value.getString("region"))
    }
    dfForES
  }
}

