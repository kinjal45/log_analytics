package Utils

import java.net.SocketTimeoutException

import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.client.config.RequestConfig
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.client.RestClientBuilder.HttpClientConfigCallback
import org.elasticsearch.client._
import org.elasticsearch.hadoop.EsHadoopIllegalArgumentException
import org.elasticsearch.hadoop.rest.EsHadoopNoNodesLeftException
import org.joda.time.DateTime
import org.json.JSONObject
import Utils.Util._

import scala.collection.JavaConverters._

object ELKUtil {

  def postDataToELK(newListOfMap: List[Map[String, Any]], indexName: String, conf: Broadcast[JSONObject], zipId: String): Unit = {
    var client: RestHighLevelClient = null
    try {
      val credentialsProvider = new BasicCredentialsProvider
      credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(conf.value.getString("elkUserName"), conf.value.getString("elkPassword")))
      val builder = RestClient.builder(new HttpHost(conf.value.getString("elkIp"), Integer.valueOf(conf.value.getString("elkPort"))))
        .setRequestConfigCallback(new RestClientBuilder.RequestConfigCallback() {
          //set timeout
          override def customizeRequestConfig(requestConfigBuilder: RequestConfig.Builder): RequestConfig.Builder = requestConfigBuilder
            .setConnectTimeout(Integer.valueOf(conf.value.getString("elkWriteTimeOut")))
            .setSocketTimeout(Integer.valueOf(conf.value.getString("elkWriteTimeOut")))
            .setConnectionRequestTimeout(Integer.valueOf(conf.value.getString("elkConnectionTimeOut")))
        }).setHttpClientConfigCallback(new HttpClientConfigCallback() {
        override def customizeHttpClient(httpClientBuilder: HttpAsyncClientBuilder): HttpAsyncClientBuilder = {
          httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
        }
      })
      client = new RestHighLevelClient(builder)
/*      val requestBuilder = RequestOptions.DEFAULT.toBuilder
      requestBuilder.setHttpAsyncResponseConsumerFactory(
        new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(1024 * 1024 * 1024))*/

      //var request = new BulkRequest()
      //request.setRefreshPolicy("wait_for")
      //var sizeOfRequest = 1L
      val elasticClient = new ElasticsearchDao(client,conf)
      println(DateTime.now())
      retry(2,2) {
        try {
          newListOfMap.foreach { vals =>
            elasticClient.save(vals.asJava.asInstanceOf[java.util.Map[String, AnyRef]], indexName)

            //request.add(new IndexRequest(indexName).source(newMap))
          }
        }catch{
          case ex: SocketTimeoutException =>
            throw new RetryException("ELK is down...Please retry")
          case ex: EsHadoopNoNodesLeftException =>
            throw new RetryException("ELK is busy...Please retry")
          case ex: Exception =>
            throw new RetryException("ELK is down...Please retry")
        }
      }
      elasticClient.flush()
      println(DateTime.now())
      //client.bulk(request, requestBuilder.build)
    }
    catch {
      case e1@(_: EsHadoopIllegalArgumentException | _: EsHadoopNoNodesLeftException) =>
        e1.printStackTrace()
        throw new EsHadoopIllegalArgumentException
      case e2: SocketTimeoutException =>
        e2.printStackTrace()
        throw new SocketTimeoutException
      case e2: Exception =>
        e2.printStackTrace()
        throw new SocketTimeoutException
    } finally {
      if (client != null)
        client.close()
    }
  }


  def fetchDatafromELK(parser: String, conf: Broadcast[JSONObject], zip_id: String, spark: SparkSession) = {
    import org.elasticsearch.spark._
    println("Fetching data for the multiple parsers")
    val esData = spark.sparkContext.esJsonRDD(conf.value.getString("elkParserIndex"),
      s"""{"query":{"bool": {"must":
         | [{"match": {"zip_id": "$zip_id"}},
         | {"match": {"gui_rules_parser_name": "$parser"}}]}}}"""
        .stripMargin).map(a => a._2)
    println(s"**********Data read completed from ES for parser $parser**********")
    println(s"**********Creating Dataframe for parser $parser**********")
    val df = spark.read.json(esData)
    df
  }


  def writeToES(dfForES: DataFrame, indexName: String, spark: SparkSession, conf: Broadcast[JSONObject]) = {
    println("indexName>>>" + indexName)
    println("Printing ES loop")
    println("df part>>>" + dfForES.rdd.getNumPartitions)
    import org.apache.spark.sql.functions.col
    val doc_id_cols = Array("zip_id", "pattern_name", "row_index")
    if (indexName == conf.value.getString("elkParserIndex")) {
      println("Parser Index")
      dfForES.drop("pattern_name")
        .withColumn("row_index", col("line_number").cast(IntegerType))
        .drop("line_number")
        .coalesce(5)
        .write.format("org.elasticsearch.spark.sql")
        .mode("append")
        .save(conf.value.getString("elkParserIndex"))
    }
    if (indexName == conf.value.getString("elkRuleIndex")) {
      println("Rules Index")
      dfForES.drop("pattern_name_list")
        .withColumn("row_index", col("line_number").cast(IntegerType))
        .drop("line_number")
        .coalesce(5)
        .write.format("org.elasticsearch.spark.sql")
        .mode("append")
        .save(conf.value.getString("elkRuleIndex"))
    }
    else {
      println("Writing to >>" + indexName)
      dfForES.write.format("org.elasticsearch.spark.sql").mode("append").save(indexName)
    }
  }

  def postJsontoELK(indexName: String, jsonString: String, spark: SparkSession) = {
    val jsonDf = spark.sqlContext.read.json(spark.sparkContext.parallelize(Seq(jsonString)))
    jsonDf.write.format("org.elasticsearch.spark.sql")
      .mode("append")
      .save(indexName)
  }

}
