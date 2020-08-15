/*
import LogParser.LogBundleConfigFetcher.LogBundle
import LogParser.LogBundleManager
import Utils.ProxyToES
import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.json.JSONObject
import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import org.mockito.ArgumentMatchers
import org.mockito.Mockito._
import org.scalatest.FunSuite
import org.scalatest.mock.MockitoSugar

class LogBundleConfigFetcherTest extends FunSuite with SharedSparkContext with MockitoSugar{

  implicit val formats: DefaultFormats.type = DefaultFormats

  test("test initializing spark context") {
    val spark_conf = sc.getConf
    val spark: SparkSession =
      SparkSession.builder().config(spark_conf).getOrCreate()
//    val aliceConfig = """{
//                        |  "uIIp": "10.129.3.7",
//                        |  "elkIp": "10.129.3.82",
//                        |  "elkParserIndex": "spark_parser",
//                        |  "elkRuleIndex": "spark_rules",
//                        |  "elkParserStatusIndex":"parser_status",
//                        |  "elkRuleStatusIndex":"rules_status"
//                        |}"""
    val conf = new JSONObject("""{  "uIIp": "10.129.3.7",  "elkIp": "10.129.3.82",  "elkParserIndex": "spark_parser_dev",  "elkRuleIndex": "spark_rules_dev",  "elkParserStatusIndex":"parser_status",  "elkRuleStatusIndex":"rules_status", "productsApplicableForML":["Fixed_Network", "Flexi-BSC"], "elkRawFileIndex": "spark_raw_dev"}""")
    val starttime = DateTime.now()
    val proxyToES = mock[ProxyToES]
    doNothing().when(proxyToES).writeToES(ArgumentMatchers.any[sql.DataFrame],ArgumentMatchers.anyString(),ArgumentMatchers.any[SparkSession],ArgumentMatchers.any[JSONObject])
    doNothing().when(proxyToES).postJson(ArgumentMatchers.anyString(), ArgumentMatchers.any[JSONObject], ArgumentMatchers.anyString(),ArgumentMatchers.any[SparkSession])
    val config = parse(getJSON).extract[LogBundle]
    val (_, _, _, _, _, _,listRuleOutput)  = LogBundleManager.analyze(config, spark, starttime, conf, proxyToES)
    assert(listRuleOutput.head.count() === 367)
  }

  def getJSON = {
    var path = System.getProperty("user.dir")
    s"""{
  "s3_key": "46849BVQEHLVWZY01NXT",
  "s3_secret": "JgMQZAds8iH4jcAcT84ilGHWqRPeT1dOtQSAyEwl",
  "s3_url": "bh-dc-s3-dhn-15.eecloud.nsn-net.net",
  "id": "2dcdae89-fc5b-4218-a009-6f6e0f3e832b",
  "product_name": "Flexi_Bsc",
  "path": "file:${path}/src/test/test_input",
  "parsing": [
    {
      "input_file_id": 77626,
      "output_file_id": [
        52038,
        52040,
        52042,
        52044,
        52055,
        52069,
        52092
      ],
      "pattern_code_map": [
        {
          "flexi_bsc__system_configuration__zwoi_pr_file": ""
        },
        {
          "flexi_bsc__system_configuration__zwti_c": ""
        },
        {
          "flexi_bsc__system_configuration__zwos_pr_file": ""
        },
        {
          "flexi_bsc__system_configuration__zwti_u_configuration": ""
        },
        {
          "flexi_bsc__system_configuration__zwti_p_configuration": ""
        },
        {
          "flexi_bsc__system_configuration__zwti_pi_piulocation": ""
        }
      ]
    }
  ],
  "rules": [
    {
      "pattern_name": [
        "flexi_bsc__system_configuration__zwoi_pr_file"
      ],
      "output_file_id": 18585,
      "rule_name": "mute_calls_all_bts_configuration",
      "query": "U0VMRUNUICogZnJvbSB6d29pX3ByX2ZpbGU="
    }
  ],
  "associated_log_bundles": [],
  "file_details": [
    {
      "input_file_id": 14632,
      "file_path": "/alice-uat-bangalore/v2/Alexx_Sample1_2019_12_02_06_41_02/files/Alexx_Sample1/10.72.105.65_ALEXX_octo_20190227_152221.log"
    }
  ]
}""".stripMargin
  }
}
*/
