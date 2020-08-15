package Utils

import java.time.format.DateTimeFormatter
import java.time.{Clock, LocalDateTime}
import java.util.{Date, NoSuchElementException, Properties, Scanner}

import LogParser.LogBundleConfigFetcher.LogBundle
import javax.mail.{Address, Message, MessagingException, Session, Transport}
import javax.mail.internet.{InternetAddress, MimeMessage}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.joda.time.{DateTime, Seconds}
import org.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object Util {

  /**
    * Calculate the time between startDate and endDate and return it either in seconds or as a formatted string
    *
    * @param startDate - start time
    * @param endDate   - end time
    * @param formatted - return time difference in seconds (false | default) or as a formatted string (format HH:MM:SS)
    * @return - time difference
    */
  def calculateRunTime(startDate: DateTime, endDate: DateTime, formatted: Boolean = false) = {
    val sec = Seconds.secondsBetween(startDate, endDate).toString().replaceAll("[^0-9.]", "").toInt

    if (formatted) {
      val s = sec.toInt % 60
      val m = (sec.toInt / 60) % 60
      val h = sec.toInt / 60 / 60
      f"$h:$m%02d:$s%02d" // return formatted as HH:MM:SS
    }
    else
      s"$sec s"
  }

  def writeoutputfile(outputDF: DataFrame, outputfilePath: String, outputFileType: String, partitionColumn: Any) = {
    println("Writing output in path>>>" + outputfilePath)
    outputFileType match {
      case "csv" => outputDF.coalesce(1).write.mode("append").format("csv").option("header", true).option("quoteAll", true).save(outputfilePath)
      case "parquet" => outputDF.coalesce(1).write.mode("append").format("parquet").option("header", true).save(outputfilePath)
    }
  }


  def getClosestLineNumber(line: Int, list: List[Int]): Int = {
    try {
      list.filter(_ > line).head
    }
    catch {
      case e1: NoSuchElementException => 0
    }
  }

  def getNextClosestLineNumber(line: Int, list: List[Int]): Int = {
    try {
      list.filter(_ > line).tail.head
    }
    catch {
      case e1: NoSuchElementException => 0
    }

  }


  def getSecondNextClosestLineNumber(line: Int, list: List[Int]): Int = {
    try {
      list.filter(_ > line).tail.tail.head
    }
    catch {
      case e1: NoSuchElementException => 0
    }
  }

  def postParserJobOutput(jobId: String = "", output_file_id: Int, startTime: String = "", endTime: String = "", status: String, shortErrorMessage: String = "",
                          longErrorMessage: String = "", number_of_anamolies: String
                          , conf: Broadcast[JSONObject], spark: SparkSession, proxyToES: ProxyToES) = {

    import org.json4s._
    case class jsonStruct(zip_id: String, output_file_id: Int, status: Int,
                          start_time: String, end_time: String,
                          success: success, failure: failure)
    case class success(no_of_lines: Int)
    case class failure(ui_display_msg: String, ui_detailed_msg: String)

    val succobj = success(number_of_anamolies.toInt)
    val failobj = failure(shortErrorMessage, longErrorMessage)
    val jsonobj = jsonStruct(jobId, output_file_id.toInt, status.toInt, startTime, endTime, succobj, failobj)
    implicit val format = DefaultFormats
    val jsonString = Json(DefaultFormats).write(jsonobj)
    println("*************parserjsonstring*****************")
    println(jsonString)
    try {
      proxyToES.postJson(jsonString, conf, "logic/parser_status")
    }
    catch {
      case e1: Exception => e1.printStackTrace()
    }
  }

  def postRuleJobOutput(jobId: String, output_file_id: Int, startTime: String, endTime: String, status: String, shortErrorMessage: String = "",
                        longErrorMessage: String = "", number_of_anamolies: String = "",
                        conf: Broadcast[JSONObject], spark: SparkSession, proxyToES: ProxyToES) = {
    import org.json4s._
    case class jsonStruct(zip_id: String, output_file_id: Int, status: Int,
                          start_time: String, end_time: String,
                          success: success, failure: failure)
    case class success(no_of_lines: String)

    case class failure(ui_display_msg: String, ui_detailed_msg: String)

    val succobj = success(number_of_anamolies)

    val failobj = failure(shortErrorMessage, longErrorMessage)
    val jsonobj = jsonStruct(jobId, output_file_id.toInt, status.toInt, startTime, endTime, succobj, failobj)

    implicit val format = DefaultFormats
    val jsonString = Json(DefaultFormats).write(jsonobj)
    println("*************rulejsonstring*****************")
    println(jsonString)
    try {
      proxyToES.postJson(jsonString, conf, "logic/rule_status")
    }
    catch {
      case e1: Exception => e1.printStackTrace()
    }

  }

  def getlinesno(rowList: List[String]): ListBuffer[Int] = {
    val linesofAnamoly = mutable.ListBuffer[Int]()
    for (i <- 0 to rowList.length - 1) {
      val coldata = rowList(i)
      if (coldata.indexOf(":") > 0) {
        linesofAnamoly += coldata.substring(0, coldata.indexOf(":")).toInt
      }
    }
    linesofAnamoly
  }

  def moveAndRenameS3File(srcPath: String, destFilePath: String, spcon: SparkContext) = {
    val src = new Path(srcPath)
    val fs = src.getFileSystem(spcon.hadoopConfiguration)
    val file = fs.globStatus(new Path(src + "/part*"))
    for (urlStatus <- file) {
      val dest = new Path(destFilePath)
      println("Parser destination path>>" + dest)
      fs.rename(urlStatus.getPath, dest)
    }
  }

  def postjobresponse(zip_id: String, `type`: String, start_time: String, end_time: String, conf: Broadcast[JSONObject], proxyToES: ProxyToES) = {

    case class jsonforJob(zip_id: String, `type`: String, start_time: String, end_time: String)

    val jsonobj = jsonforJob(zip_id, `type`, start_time, end_time)

    implicit val format = DefaultFormats
    val jsonString = Json(DefaultFormats).write(jsonobj)
    println("*************jobstatusstring*****************")
    println(jsonString)
    proxyToES.postJson(jsonString, conf, s"logic/${`type`}")
  }


  def convertRowToJSON(row: Row): Map[String, Any] = {
    val m = row.getValuesMap(row.schema.fieldNames)
    println(m)
    scala.util.parsing.json.JSONObject(m).obj
  }


  def getID(bundleID: String) = {
    val current_ts = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(LocalDateTime.now(Clock.systemUTC()))
    val driverid = s"DRIVER-$current_ts-$bundleID"
    val appid = s"APP-$current_ts-$bundleID"
    println("driverid>>" + driverid)
    (appid, driverid)
  }

  def getNonEmptyLines(logFileContent: String, start: String, end: String): String = {
    val txt = new Scanner(logFileContent)
    val sb = new StringBuilder()
    var lineNum = 1

    while (txt.hasNextLine) {
      var line = txt.nextLine()
      if (checkIfContains(start, line)) {
        sb.append(line + "\t" + getAliceLineNumber() + lineNum + "\n")
        line = txt.nextLine()
        lineNum += 1
        while (!checkIfContains(end, line) && txt.hasNextLine) {
          if (line.trim.isEmpty) {
            line = txt.nextLine()
            lineNum += 1
          } else {
            sb.append(line + "\n")
            if (txt.hasNextLine) {
              line = txt.nextLine()
              lineNum += 1
            }
          }
        }
      }
      lineNum += 1
    }
    return sb.toString()
  }

  def checkIfContains(stringToCheck: String, line: String): Boolean = {
    if (stringToCheck.contains("&&")) {
      var result = stringToCheck.split("&&").iterator.dropWhile(x => line.contains(x))
      if (result.isEmpty) true
      else false
    } else if (stringToCheck.contains("||")) {
      var result = stringToCheck.split("||").iterator.dropWhile(x => line.contains(x))
      if (result.length < stringToCheck.split("||").length) true
      else false
    } else {
      line.contains(stringToCheck)
    }
  }

  def getAliceLineNumber(): String = {
    "##AliceLineNumber##"
  }

  def getValue(data: Array[String], index: Integer): String = {
    data.lift(index).getOrElse("")
  }

  def getSubString(str: String, start: Int, end: Int): String = {
    var finalStr = "";
    try {
      finalStr = str.substring(start, end)
      finalStr.trim
    }
    catch {
      case e1: Exception => finalStr
    }
  }

  def getSplitAt(str: String, index: Int): (String, String) = {
    var tuple = ("", "")
    try {
      tuple = str.splitAt(index)
      tuple
    }
    catch {
      case e1: Exception => tuple
    }
  }

  /* def getExecutorCoresCount(spark: SparkSession) = {

     // val executorcores=spark.sparkContext.getConf.get("spark.executor.cores").toInt
     // println(spark.sparkContext.getConf.getAll.mkString("\n"))
     // val executorcores=java.lang.Runtime.getRuntime.availableProcessors()
     val executorcores = spark.sparkContext.getConf.getInt("spark.executor.cores", 1)
     val executorinstances = Math.max(1, spark.sparkContext.getExecutorStorageStatus.length - 1)
     val partitionMultiplier = 1
     println("executorcores>>" + executorcores)
     println("executorinstances>>" + executorinstances)
     println("Returning multiplied value>>" + (executorinstances * executorcores))
     Math.floor(partitionMultiplier * executorinstances * executorcores).toInt
   }*/

  def getRawFileIdAndCount(conf: Broadcast[JSONObject], logBundle: LogBundle, spark: SparkSession) = {
    val rawFileKeyword = conf.value.getJSONArray("rawFileKeywordForML")
    var idcountList = mutable.ListBuffer[String]()
    var rawFileList = mutable.ListBuffer[String]()

    var toReturn: (String, String) = ("NA", "NA")
    if (rawFileKeyword.length > 0) {
      var fileidMap = mutable.Map[String, ListBuffer[String]]()

      var filteredfileidMap = mutable.Map[String, ListBuffer[String]]()
      logBundle.file_details.foreach { vals =>
        if (fileidMap.keys.toList.contains(vals.file_path.trim)) {
          fileidMap(vals.file_path.trim) += vals.input_file_id
        }
        else {
          fileidMap += vals.file_path -> ListBuffer(vals.input_file_id)
        }
      }
      println("fileidMap>>>" + fileidMap)

      for (i <- 0 to rawFileKeyword.length - 1) {
        var productName = rawFileKeyword.getJSONObject(i).getString("product")
        var productId = rawFileKeyword.getJSONObject(i).getString("product_id")
        //if (productName.equals(logBundle.product_name)) {
        println("Product ID>>"+productId)
        if(logBundle.prod_id.equals(productId)){
          println("Raw File Check Required")
          val filekeywordforML = rawFileKeyword.getJSONObject(i).getString("fileKeyword")
          val filekeywordList = filekeywordforML.split(",")
          filekeywordList.foreach { vals =>
            fileidMap.keys.toList.foreach { filepath =>
              if (filepath.split("/").last.contains(vals))
                rawFileList += filepath
            }

          }
          //println("rawFileList>>>" + rawFileList)
          rawFileList.foreach { vals =>
            filteredfileidMap += "s3a:/" + vals -> fileidMap(vals)
          }

          //println("filteredfileidMap>>>" + filteredfileidMap)
          if (filteredfileidMap.size > 0) {
            filteredfileidMap.values.foreach { vals =>
              vals.foreach { ipfileid =>
                //println("ipfileid>>" + ipfileid)
                spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", logBundle.k8_s3_url)
                spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", logBundle.k8_s3_key)
                spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", logBundle.k8_s3_secret)
                spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled","false")
                spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access","true")
                val count = spark.sparkContext.textFile(logBundle.path + "/" + ipfileid).count
                idcountList += ipfileid + ">>>>" + count
              }
            }
            println("idcountList>>" + idcountList)
            var allFileCount = 0L

            var allFileiDs = ""
            idcountList.foreach { vals =>
              val cnt = Integer.valueOf(vals.split(">>>>")(1))
              val fileids = vals.split(">>>>")(0)
              allFileCount += cnt
              allFileiDs += fileids + ","
            }
            toReturn = (allFileCount.toString, allFileiDs.substring(0, allFileiDs.lastIndexOf(",")))
            println("toReturn>>>" + toReturn)
          }
        }
      }

    }
    toReturn
  }


  def sendScalaMail (smtpHost:String,mailSender:String,mailIdtoList:String,strMailSubj:String,strMailBody:String,region:String,mailIdccList:String="",mailIdbccList:String="")= {
    println("Region>>>>"+region)
    val filter_region_for_mailservice=List("dev","uat","staging")
    if (!filter_region_for_mailservice.contains(region.toLowerCase)) {
     if ((mailIdtoList == null) && (mailIdtoList.equals(""))) {
        println("Receipient List cannot be empty")
      }
      else {
        val prop: Properties = new Properties()
        prop.put("mail.smtp.host", smtpHost)
        prop.put("mail.debug", "false")
        var session: Session = Session.getInstance(prop)
        var toPersonList: Array[String] = mailIdtoList.split(";", -1)
        //var toPersonList = mailIdtoList.map(_.split(";", -1)).flatten(x=>x)
        var toMailListSB: String = ""
        var toPersonName: String = ""
        var toMailId: String = ""
        var index: Int = 0


        try {
          var msg: MimeMessage = new MimeMessage(session)
          msg.setFrom(new InternetAddress(mailSender))
          if (mailIdtoList.length > 1) {
            val recipientList = mailIdtoList.split(";", -1)
            val recipientAddress = new Array[Address](recipientList.length)
            var tocounter = 0
            for (recipient <- recipientList) {
              recipientAddress(tocounter) = new InternetAddress(recipient.trim)
              tocounter += 1
            }
            msg.setRecipients(Message.RecipientType.TO, recipientAddress)
          }
          if (mailIdccList.length > 1) {
            val recipientccList = mailIdccList.split(";", -1)
            val recipientccAddress = new Array[Address](recipientccList.length)
            var cccounter = 0
            for (ccrecipient <- recipientccList) {
              recipientccAddress(cccounter) = new InternetAddress(ccrecipient.trim)
              cccounter += 1
            }
            msg.setRecipients(Message.RecipientType.CC, recipientccAddress)
          }
          if (mailIdbccList.length > 1) {
            val recipientbccList = mailIdbccList.split(";", -1)
            val recipientbccAddress = new Array[Address](recipientbccList.length)
            var bcccounter = 0
            for (bccrecipient <- recipientbccList) {
              recipientbccAddress(bcccounter) = new InternetAddress(bccrecipient.trim)
              bcccounter += 1
            }
            msg.setRecipients(Message.RecipientType.BCC, recipientbccAddress)
          }

          msg.setHeader("Content-Type", "text/html")
          msg.setSubject(strMailSubj)
          msg.setSentDate(new Date())
          msg.setContent(strMailBody, "text/html")
          Transport.send(msg)
        }
        catch {
          case me: MessagingException => {
            me.printStackTrace()
          }
        }
      }
    }
  }

  final case class RetryException(private val message: String = "",
                                  private val cause: Throwable = None.orNull)
    extends Exception(message, cause)

  def retry[T](n: Int,x: Int)(fn: => T): T = {
    try {
      fn
    }
    catch {
      case e: RetryException if n > 1 => println(x - (n-1) + " Try"); retry(n - 1,x)(fn)

    }
  }

  case class DuplicateColumnException(private val message: String = "",
                                      private val cause: Throwable = None.orNull)
    extends Exception(message, cause)


}


