package com.nokia.compiler

import LogParser.LogBundleConfigFetcher.InputArgs
import Utils.ProxyToES
import org.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.native.Json

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, _}

object ExternalParser {

  case class Output(sid: String, status: String, msg: String, results: List[Map[String, String]])

  implicit val formats: DefaultFormats.type = DefaultFormats

  def process(inputJson: InputArgs, conf: JSONObject, proxyToES: ProxyToES): Unit = {
    try {
      val exectimeout=conf.getString("defaultexecutionTimeOut").toInt
      lazy val f = Future {
        parseScala(inputJson, conf, proxyToES); true
      }
      Await.result(f, exectimeout minute)
    } catch {
      case ex: TimeoutException => {
        var resultJson = ""
        resultJson = generateJSON(inputJson.sid, "Invalid", "There is a Timeout issue please check your code - " + ex.getMessage, List[Map[String, String]]())
        println(resultJson)
        proxyToES.postCustomParserJson(resultJson, conf,"parser/res_parser")
      }
    }
  }

  def parseScala(inputJson: InputArgs, conf: JSONObject, proxyToES: ProxyToES): Unit = {
    val code = inputJson.code_data
    val file = inputJson.file_data
    var resultJson = ""
    try {
      val listOfMap = new ExternalScalaFileHelper().processExternalFile(code, file)
      resultJson = generateJSON(inputJson.sid, "Valid", "Success", listOfMap)
    } catch {
      case ex :Throwable=> resultJson = generateJSON(inputJson.sid, "Invalid", ex.toString.replaceAll("\\n", "-").replaceAll("\\r", "-").replaceAll("scala.tools.reflect.ToolBoxError: reflective ", ""), List[Map[String, String]]())
    }
    println(resultJson)
    proxyToES.postCustomParserJson(resultJson, conf, "parser/res_parser")
  }

  private def generateJSON(sid: String, status: String, msg: String, results: List[Map[String, String]]): String = {
    val p = Output(sid, status, msg, results)
    val json = Json(DefaultFormats).write(p)
    json
  }


}
