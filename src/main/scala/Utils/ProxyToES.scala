package Utils

import LogParser.LogBundleConfigFetcher.TokenClass
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.json.JSONObject
import org.json4s.DefaultFormats
import Utils.Util._
import org.json4s.native.JsonMethods.parse

import scalaj.http.{Http, HttpOptions, HttpResponse}

class ProxyToES extends java.io.Serializable {

  def getResponseFromWS( api:String, data: String, tokenId: String): HttpResponse[String] ={
    retry(3,3){
      val response = Http(api).postData(data)
        .header("Content-Type", "application/json")
        .header("Charset", "UTF-8")
        .header("token", tokenId)
        .options(HttpOptions.connTimeout(500000))
        .options(HttpOptions.readTimeout(500000))
        .options(HttpOptions.followRedirects(true)).asString
      if (!response.isSuccess) {throw new RetryException("Please Retry")}
      response
    }
  }

  @throws(classOf[Exception])
  def postJson(jsonStr: String, conf: Broadcast[JSONObject], api: String): Unit = {
    val tokenConfig = getResponseFromWS(s"http://${conf.value.getString("uIIp")}/api/ctoken", """{"uname":"spark","pass":"s!@i6$p!0"}""", "")
    implicit val formats: DefaultFormats.type = DefaultFormats
    val configToken = parse(tokenConfig.body.toString).extract[TokenClass]
    val tokenId = configToken.data.token
    if (api.equals("general_status")) {
      val inpJson = getResponseFromWS(s"http://${conf.value.getString("uIIp")}/api/logic/$api", jsonStr, tokenId )
      println(inpJson)
    }
    else {
      val inpJson = getResponseFromWS(s"http://${conf.value.getString("uIIp")}/api/$api", jsonStr, tokenId )
      println(inpJson)
    }
  }

  def postInitialJson(jsonStr: String, conf: JSONObject, api: String): Unit = {
    val tokenConfig = getResponseFromWS(s"http://${conf.getString("uIIp")}/api/ctoken", """{"uname":"spark","pass":"s!@i6$p!0"}""", "")
    implicit val formats: DefaultFormats.type = DefaultFormats
    val configToken = parse(tokenConfig.body.toString).extract[TokenClass]
    val tokenId = configToken.data.token
    if (api.equals("general_status")) {
      val inpJson = getResponseFromWS(s"http://${conf.getString("uIIp")}/api/logic/$api", jsonStr, tokenId )
      println(inpJson)
    }
    else {
      val inpJson = getResponseFromWS(s"http://${conf.getString("uIIp")}/api/$api", jsonStr, tokenId )
      println(inpJson)
    }
  }

  def postCustomParserJson(jsonStr: String, conf: JSONObject, api: String, spark: SparkSession = null): Unit = {
    val tokenConfig = getResponseFromWS(s"http://${conf.getString("uIIp")}/api/ctoken", """{"uname":"spark","pass":"s!@i6$p!0"}""", "")
    implicit val formats: DefaultFormats.type = DefaultFormats
    val configToken = parse(tokenConfig.body.toString).extract[TokenClass]
    val tokenId = configToken.data.token
    if (api.equals("general_status")) {
      val inpJson = getResponseFromWS(s"http://${conf.getString("uIIp")}/api/logic/$api", jsonStr, tokenId )
      println(inpJson)
    }
    else {
      val inpJson = getResponseFromWS(s"http://${conf.getString("uIIp")}/api/$api", jsonStr, tokenId )
      println(inpJson)
    }
  }

/*  def retry[T](n: Int)(fn: => T): T = {
    try {
      fn
    } catch {
      case e if n > 1 =>
        retry(n - 1)(fn)
    }
  }*/
}