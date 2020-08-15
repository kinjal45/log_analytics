package com.nokia.compiler

import java.util.Base64

import Utils.{ELKUtil, ProxyToES}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.spark._
import org.joda.time.DateTime
import org.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods.parse
import Utils.Util._
import scala.collection.mutable
import scala.collection.mutable.ListBuffer


case class AnomalyString(zip_id: String, input_file_id: Int, jobType: String, data: List[Data])

case class Data(row_index: Int, comment: String, anomaly_flag: String)
object updateAnomaly {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    var conf: JSONObject = null
    var updateString: AnomalyString = null
    try {
      try {
        implicit val formats: DefaultFormats.type = DefaultFormats
        println("args(0)>>>" + args(0))
        println("args(1)>>>" + args(1))
        conf = new JSONObject(new String(java.util.Base64.getDecoder.decode(args(1))))
        updateString = parse(new String(Base64.getDecoder.decode(args(0)))).extract[AnomalyString]
        println("conf>>" + conf)
        println("updateString>>" + updateString)
      }
      catch {
        case ex: Exception => throw new Exception("Please Check Proper Configurations in Input Conf file")
      }
      val spark: SparkSession =
        SparkSession.builder().appName("AnomalyUpdate")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
          .config("spark.es.index.auto.create", "true")
          .config("spark.es.nodes", conf.getString("elkIp"))
          .config("spark.es.port", conf.getString("elkPort"))
          .config("spark.es.net.http.auth.user", conf.getString("elkUserName"))
          .config("spark.es.net.http.auth.pass", conf.getString("elkPassword"))
          .config("spark.es.nodes.wan.only", "true")
          .config("spark.es.nodes.client.only", "false")
          .getOrCreate()
      spark.sparkContext.setLogLevel("ERROR")
      val zip_id = updateString.zip_id
      var createdList = mutable.ListBuffer[(String, Int, Int, String, String)]()
      val row_index_list = mutable.ListBuffer[Int]()
      if (updateString.data.size > 0) {
        val starttime=DateTime.now()
        println(s"Anomaly updation started @ ${starttime.toString}")
        updateString.data.foreach { entry =>
        row_index_list += entry.row_index
        createdList = createdList :+ ((zip_id, updateString.input_file_id, entry.row_index, entry.comment, entry.anomaly_flag.toString))
      }

        val conf1=spark.sparkContext.broadcast(conf)
        val dfForES = updateAnomalyandComment(conf, zip_id, updateString, createdList, row_index_list, spark)
        dfForES.persist(org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK)
        println("dfForES")
        dfForES.show(false)
        val deleteStatus = deleteOldEntries(conf, zip_id, updateString, createdList, row_index_list, spark)
        if (deleteStatus) {
          println("finale write")
          dfForES.show(false)
          //dfForES.write.format("org.elasticsearch.spark.sql").mode("append").save(conf.getString("elkRawFileIndex"))
          ELKUtil.writeToES(dfForES,conf.getString("elkRawFileIndex"),spark,conf1)
          val jsonObj=new ProxyToES()
          jsonObj.postJson(s"""{"zip_id": "$zip_id" , "msg": "Anomaly updated @ ${DateTime.now()}"}""", conf1, "general_status")
          val endtime=DateTime.now()
          println(s"Anomaly updation completed @${endtime}")
          val timeTaken=calculateRunTime(starttime,endtime)
          println(s"Time taken for Anomaly updation is ${timeTaken}")
        }
      }

      }
      catch {
        case ex:Exception => println("Exception while updating Anomaly")
          ex.printStackTrace()
      }
    }


  def updateAnomalyandComment(conf: JSONObject, zip_id: String, updateString: AnomalyString, createdList: ListBuffer[(String, Int, Int, String, String)], row_index_list: ListBuffer[Int], spark: SparkSession): DataFrame = {
    try {
      var finalDF=spark.emptyDataFrame
      println(s"**********Reading data from ES for zip id $zip_id for file id ${updateString.input_file_id} for row index ${row_index_list.mkString(",")} **********")
      val esData = spark.sparkContext.esJsonRDD(conf.getString("elkRawFileIndex"),
        s"""
           |{
           |  "query": {
           |    "bool": {
           |      "must": [
           |        {
           |          "bool": {
           |            "must": [
           |              {
           |                "match": {
           |                  "zip_id": "${zip_id}"
           |                }
           |              }
           |            ]
           |          }
           |        },
           |        {
           |          "bool": {
           |            "should": [
           |              {"terms" : { "row_index": [${row_index_list.mkString(",")}]}}
           |            ]
           |          }
           |        },
           |        {
           |          "bool": {
           |            "must": [
           |              {
           |                "match": {
           |                  "input_file_id": ${updateString.input_file_id}
           |                }
           |             }
           |            ]
           |          }
           |        }
           |      ]
           |    }
           |  }
           |}
           |

          """.stripMargin).map(a => a._2)
      val esDataDf = spark.read.json(esData)
      println("***********esDataDf**************")
      esDataDf.show(false)
      if(esDataDf.columns.toList.size>0){
      val esDataCols = esDataDf.columns
      import org.apache.spark.sql.functions.col
      val nonupdateCols = esDataCols.filterNot(_.equals("comment")).filterNot(_.equals("anomaly_flag")).map(cols => col(cols))
      val dfwithnoupdates = esDataDf.select(nonupdateCols: _ *)
      val updateList = createdList.toList
      import spark.implicits._
      val updateDF = updateList.toDF("zip_id", "input_file_id", "row_index", "comment", "anomaly_flag")
      println("*************updateDF************")

      finalDF = dfwithnoupdates.join(updateDF, Seq("zip_id", "input_file_id", "row_index"))
      println("*************finalDF*************")
      finalDF.show(false)
       }
      finalDF
    }
    catch {
      case e: Exception => throw new Exception("Issue in updating")
    }
  }

  def deleteOldEntries(conf: JSONObject, zip_id: String, updateString: AnomalyString,
                       createdList: ListBuffer[(String, Int, Int, String, String)], row_index_list: ListBuffer[Int], spark: SparkSession) = {
    try{
    val delupdateAnomaliesquery =
      s"""{
         |  "query": {
         |    "bool": {
         |      "must": [
         |        {
         |          "bool": {
         |            "must": [
         |              {
         |                "match": {
         |                  "zip_id": "${zip_id}"
         |                }
         |              }
         |            ]
         |          }
         |        },
         |        {
         |          "bool": {
         |            "should": [
         |              {"terms" : { "row_index": [${row_index_list.mkString(",")}]}}
         |            ]
         |          }
         |        },
         |        {
         |          "bool": {
         |            "must": [
         |              {
         |                "match": {
         |                  "input_file_id": ${updateString.input_file_id}
         |                }
         |             }
         |            ]
         |          }
         |        }
         |      ]
         |    }
         |  }
         |}
                                        """.stripMargin

    import scala.sys.process._
    val deletecmd = List("curl", "-X", "POST", s"${conf.getString("elkIp")}:${conf.getString("elkPort")}/${conf.getString("elkRawFileIndex")}/_delete_by_query?wait_for_completion=false&slices=auto", "-H",
      "Content-Type: application/json", s"-d $delupdateAnomaliesquery"
    )
    val delqueryres = new JSONObject(deletecmd.!!)
    print(delqueryres)
    val checkstatusquery =s"""${delqueryres.getString("task")}""".stripMargin
    println(checkstatusquery)
    import scala.concurrent.duration._
    val checktime = 30.seconds.fromNow
    var completed = false
    var i=0
    while (checktime.hasTimeLeft() && !completed) {
      println("checktime>>"+checktime.timeLeft)
      println("turn no>>"+(i+1))
      import scala.sys.process._
      val statuschkcmd = List("curl", "-X", "GET", s"${conf.getString("elkIp")}:${conf.getString("elkPort")}/_tasks/$checkstatusquery")
      println(statuschkcmd)
      val statuschk = statuschkcmd.!!
      val statuscheckjsonstring = new JSONObject(statuschk)
      println("******statuscheckjsonstring***")
      println(statuscheckjsonstring)
      completed = statuscheckjsonstring.getBoolean("completed")
      i+=1
    }
      if(completed){
        true
      }
      else {
        false
      }
    }
    catch{
      case e:Exception=>throw new Exception("Issue in deleting")
    }
  }
}