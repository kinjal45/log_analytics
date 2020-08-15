package com.nokia.parsing

import LogParser.LogBundleConfigFetcher.LogBundle
import ProcessLogs.parseLog
import Utils.ProxyToES
import Utils.Util._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.json.JSONObject
import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Try

object LogParsing {


  def parseLogFiles(logBundle: LogBundle, input_map: Map[String, List[String]], input_output_map: Map[String, List[Int]],
                    input_parser_output_id_map: Map[String, ListBuffer[Int]], spark: SparkSession,
                    conf: Broadcast[JSONObject], totalParserCount: Int,
                    parserCodeMap: Map[String, String], proxyToES: ProxyToES): Map[String,String] = {
    val executionStartTime = DateTime.now.toString()
    val dateToProcess = DateTime.now.toLocalDate.toString()
    val parserStatus = Map[String, ListBuffer[String]]()
    val sc = spark.sparkContext
    sc.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    sc.hadoopConfiguration.set("fs.s3a.endpoint", logBundle.k8_s3_url)
    sc.hadoopConfiguration.set("fs.s3a.access.key", logBundle.k8_s3_key)
    sc.hadoopConfiguration.set("fs.s3a.secret.key", logBundle.k8_s3_secret)
    sc.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled","false")
    sc.hadoopConfiguration.set("fs.s3a.path.style.access","true")
    //sc.getConf.set("spark.cores.max", totalParserCount.toString)

    try {
      println(sc.getConf.getAll.mkString(";"))
      val processor_count=Runtime.getRuntime.availableProcessors()
      println("Processor count>>>"+processor_count)
      var fileNameContentsRDD: RDD[(String, String)] = spark.sparkContext.emptyRDD

      fileNameContentsRDD = spark.sparkContext.wholeTextFiles(logBundle.path.trim, minPartitions = input_map.size)//.repartition(processor_count.toInt)
      println("fileNameContentsRDD size>>"+fileNameContentsRDD.partitions.size)

      val explodedRdd = fileNameContentsRDD.map(m => explodeRddParserwise(input_map, m._1, m._2)).filter(x => x.nonEmpty).flatMap(y => y)
      println("explodedRdd size>>"+explodedRdd.partitions.size)



   /*   val getParserStatus = explodedRdd.map(tupleOfFileAndContent => {
        val parserStatus= Try {
          parseLog(tupleOfFileAndContent._1, tupleOfFileAndContent._2,
            dateToProcess, logBundle.id.toString, executionStartTime, input_parser_output_id_map, parserCodeMap,
            input_map, input_output_map, spark, conf, proxyToES, logBundle)
        }.toOption
        parserStatus match {
          case Some(t) => t
          case None =>  {
            val fileKeyList = tupleOfFileAndContent._1.split(">>>>")
            val parser = fileKeyList(0)
            val f_name = fileKeyList(1)
            val output_file_id = input_output_map(f_name)(input_map(f_name).indexOf(parser))
            postParserJobOutput(logBundle.id.toString, output_file_id, executionStartTime, DateTime.now().toString, "3", "Exception in LogParsing","Exception in LogParsing", "0", conf, spark, proxyToES)
            Map[String,String]  (parser -> "F")
          }
        }
      }
      )*/

      val getParserStatus = explodedRdd.mapPartitions(itr=>itr.map { tupleOfFileAndContent => {
        val parserStatus = Try {
          parseLog(tupleOfFileAndContent._1, tupleOfFileAndContent._2,
            dateToProcess, logBundle.id.toString, executionStartTime, input_parser_output_id_map, parserCodeMap,
            input_map, input_output_map, spark, conf, proxyToES, logBundle)
        }.toOption
        parserStatus match {
          case Some(t) => t
          case None => {
            val fileKeyList = tupleOfFileAndContent._1.split(">>>>")
            val parser = fileKeyList(0)
            val f_name = fileKeyList(1)
            val output_file_id = input_output_map(f_name)(input_map(f_name).indexOf(parser))
            postParserJobOutput(logBundle.id.toString, output_file_id, executionStartTime, DateTime.now().toString, "3", "Exception in LogParsing", "Exception in LogParsing", "0", conf, spark, proxyToES)
            Map[String, String](parser -> "F")
          }
        }
      }
      }
      )
      val accumMap = sc.collectionAccumulator[Map[String, String]]("File Keys")
      var parserStatusMap = mutable.Map[String, String]()
      getParserStatus.foreach(m => accumMap.add(m))

      import scala.collection.JavaConverters._

      val parserStatus = accumMap.value.asScala.toList
      println(parserStatus)
      parserStatus.foreach { map =>
        map.foreach { mapVals =>
          parserStatusMap += mapVals
        }
      }
      parserStatusMap.toMap
    }
    catch {
      case e: Exception => {
        e.printStackTrace()
        input_parser_output_id_map.values.toList.distinct.foreach(outPathidlist => {
          outPathidlist.foreach { outPathid =>
            var dispmsg= "Exception in LogParsing"
            if(e.getMessage.toLowerCase.contains("path does not exist")){
              dispmsg= "s3 path does not exist"
              postParserJobOutput(logBundle.id.toString, outPathid, executionStartTime, DateTime.now().toString, "3", dispmsg,dispmsg, "0", conf, spark, proxyToES)
              sendScalaMail(conf.value.getString("smtpHost"), conf.value.getString("mailSender"),
                conf.value.getString("SETeam"),
                s"ALERT:::$dispmsg ${DateTime.now()} ",
                s"$dispmsg for bundle id ${logBundle.id} uploaded by ${logBundle.username} in ${conf.value.getString("dataCenter")} k8s_${conf.value.getString("region")}",conf.value.getString("region"),
                conf.value.getString("customccSender"))
            }
          }
        })
      }
        throw new Exception
    }
  }

  def explodeRddParserwise(input_map: Map[String, List[String]], fileName: String, fileContent: String): List[(String, String)] = {
    var my_list = new ListBuffer[(String, String)]()
    if (input_map.keys.toList.contains(fileName)) {
      input_map(fileName).foreach(x => {
        my_list += (x + ">>>>" + fileName -> fileContent)
      })
    }
    my_list.toList
  }


}
