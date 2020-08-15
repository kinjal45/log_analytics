package LogParser

import Utils.ELKUtil._
import Utils.ProxyToES
import com.nokia.compiler.{ExternalParser, RulesParser}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.json.JSONObject
import org.json4s.native.JsonMethods._
import org.json4s.{DefaultFormats, _}

import scalaj.http.{Http, HttpOptions}


object LogBundleConfigFetcher {
  implicit val formats: DefaultFormats.type = DefaultFormats

  case class LogBundle(id: String,prod_id:String,username:String,product_name: String, path: String,
                       //s3_key: String, s3_secret: String, s3_url: String,
                       k8_s3_key: String, k8_s3_secret: String, k8_s3_url: String,
                       global_s3_key: String, global_s3_secret: String, global_s3_url: String,
                       parsing: List[Parsing],
                       rules: List[Rules],associated_log_bundles:List[AssociatedBundles],file_details:List[filedetails])

  case class filedetails(input_file_id:String,file_path:String)

  case class TokenClass(data: Data, msg: String, status: String, error: String)

  case class Data(token: String, uname: String, roles: Array[String])

  case class Parsing(input_file_id: String, output_file_id: List[Int], pattern_code_map: List[Map[String, String]])

  case class Rules(pattern_name: List[String], output_file_id: Int, rule_name: String, query: String)

  case class CustomRule(zip_id: String, pattern_name: List[String])

  case class InputArgs(sid: String, id: String, file_data: String, code_data: String, customRulesData: List[CustomRule], query: String, region: String, jobType: String,associated_log_bundles:List[AssociatedBundles])

  case class AssociatedBundles(id:String,pattern_name:List[String])


  def main(args: Array[String]): Unit = {
    require(args.length == 2, "Required JSON as input parameter for processing")
    val proxyToES = new ProxyToES
    println("Encoded Argument Recieved>>>" + args(0))
    val decoded_args = new String(java.util.Base64.getDecoder.decode(args(0)))
    println("Decoded Argument>>>" + decoded_args)
    val inputArgs = parse(decoded_args).extract[InputArgs]
    val conf = new JSONObject(new String(java.util.Base64.getDecoder.decode(args(1))))
    println("conf>>"+conf)
    inputArgs.id.trim match {
      // Admin UI
      case "" =>
        inputArgs.file_data match {
          case "" =>
            //Rules Execution
            RulesParser.executeRules(inputArgs, conf, proxyToES)

          case _ =>
            //Parser execution
            ExternalParser.process(inputArgs, conf, proxyToES)

        }
      //Regular Execution
      case _ =>
        process(inputArgs, conf, proxyToES)
    }
  }

  def process(inputJson: InputArgs, conf1: JSONObject, proxyToES: ProxyToES): Unit = {
    val starttime = DateTime.now()
    var config:LogBundleConfigFetcher.LogBundle=null
    println("Parsing started at>>>" + starttime)
    import scala.concurrent.duration._
    val checktime = 9.minutes.fromNow
    var completed = false
    var i=0
    val sleepMap=Map(1->60000,2->180000,3->300000)

    while(!completed && checktime.hasTimeLeft() && i<5) {
      try {
        config = restCalls(inputJson.id, conf1.getString("uIIp"))
        i+=1
        completed=true
      }
      catch {
        case e: Exception => e.printStackTrace()
        i+=1
        val jsonObj = new ProxyToES()
        jsonObj.postInitialJson(s"""{"zip_id": "${inputJson.id}" , "msg": "Failed to get Job Definition @ ${DateTime.now()}"}""", conf1, "general_status")
        Thread.sleep(sleepMap(i))
      }
    }
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    val spark: SparkSession =
      SparkSession.builder().appName("AliceProcessingTwentyDotTwo")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.es.index.auto.create", "true")
        .config("spark.es.nodes", conf1.getString("elkIp"))
        .config("spark.es.port", conf1.getString("elkPort"))
        .config("spark.es.net.http.auth.user", conf1.getString("elkUserName"))
        .config("spark.es.net.http.auth.pass", conf1.getString("elkPassword"))
        .config("spark.es.nodes.wan.only", "true")
        .config("spark.es.nodes.client.only", "false")
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val conf=spark.sparkContext.broadcast(conf1)

    val app_id = spark.sparkContext.getConf.get("spark.app.id")
    var userName=config.username
    var productId=config.prod_id
    var productName=config.product_name
    try {
      val get_driver_id = spark.sparkContext.getConf.get("hive.metastore.warehouse.dir","1")
      var driver_id=""
      if (get_driver_id!=1){
        driver_id=get_driver_id.split("/").filter(_.contains("driver-")).lift(0).getOrElse("")
      }
      else
        driver_id="1"

      println("driver_id>>"+driver_id)
      println("app_id>>"+app_id)

      import java.net._
      val localhost: InetAddress = InetAddress.getLocalHost
      val localIpAddress: String = localhost.toString.split("/")(1)

      println(s"localIpAddress = $localIpAddress")
      println(s"Writing to ${conf1.getString("elkRmIndex")} index")
      val driver_app_list = List((localIpAddress,driver_id,app_id,"Alice",inputJson.id,userName,productName,productId,DateTime.now().toLocalDate.toString()))
      import spark.implicits._
      val driver_app_df = driver_app_list.toDF("Cluster_ID","Driver_ID", "App_ID","Job_Type","zip_id","User_Name","Product_Name","Product_Id","Execution_Date")
      driver_app_df.show(false)
      writeToES(driver_app_df, conf1.getString("elkRmIndex"), spark, conf)
    }
    catch{
      case ex:Exception=> {
        println(s"Exception while writing to ${conf1.getString("elkRmIndex")} index")
        ex.printStackTrace()
        val localIpAddress=""
        val driver_id = 0
        val driver_app_list = List((localIpAddress,driver_id,app_id,"Alice",inputJson.id,userName,productName,productId,DateTime.now().toLocalDate.toString()))
        import spark.implicits._
        val driver_app_df = driver_app_list.toDF("Cluster_ID","Driver_ID", "App_ID","Job_Type","zip_id","User_Name","Product_Name","Product_Id","Execution_Date")
        driver_app_df.show(false)
        writeToES(driver_app_df, conf1.getString("elkRmIndex"), spark, conf)
      }
    }

    val (rulescount,ruleendTime,totalParserCount_fromAnalyze,logparsingexectime,ruleexecutionTime,totaljobexecutiontime,_) =
      LogBundleManager.analyze(config, spark, starttime, conf, proxyToES)
    LogBundleManager.writeSparkJobSummary(spark,config,rulescount,ruleendTime,totalParserCount_fromAnalyze,logparsingexectime,ruleexecutionTime,totaljobexecutiontime, starttime,conf, inputJson,proxyToES)
    spark.stop()
  }

  //@throws(classOf[Exception])
  def restCalls(bundle_id: String, ip: String): LogBundle = {
    val tokenConfig = Http(s"http://$ip/api/ctoken").postData("""{"uname":"spark","pass":"s!@i6$p!0"}""")
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .options(HttpOptions.connTimeout(50000))
      .options(HttpOptions.readTimeout(50000))
      .options(HttpOptions.followRedirects(true)).asString
    println("tokenConfig>>" + tokenConfig)

    val configToken = parse(tokenConfig.body.toString).extract[TokenClass]
    val tokenId = configToken.data.token

    println("Log Bundle>>>" + bundle_id)
    val inpJson = Http(s"http://$ip/api/logic/bundle_info").postData(s"""{"id":"$bundle_id"}""")
      .header("Content-Type", "application/json")
      .header("Charset", "UTF-8")
      .header("token", tokenId)
      .options(HttpOptions.connTimeout(5000000))
      .options(HttpOptions.readTimeout(5000000))
      .options(HttpOptions.followRedirects(true)).asString
    println(inpJson)
    val config = parse(inpJson.body.toString).extract[LogBundle]
    config

  }
}