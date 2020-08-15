name := "AliceParser"
version := "20.02.05-k8s"
scalaVersion := "2.11.11"
publishMavenStyle := true
isSnapshot := false

publishTo := {
  val artifactory = "http://artifactory-espoo1.ext.net.nokia.com/artifactory/"
  Some("Artifactory Realm" at artifactory +"ava-maven-snapshots-local")
}

credentials += Credentials("Artifactory Realm",
  "ava-maven-snapshots-local",
  "alice",
  "******")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.3",
  "org.apache.spark" %% "spark-sql" % "2.4.3",
  "org.json4s" %% "json4s-native" % "3.5.3",
  "org.json4s" %% "json4s-jackson" %" 3.5.3",
  "com.amazonaws" % "aws-java-sdk" % "1.11.534",
  "com.amazonaws" % "aws-java-sdk-core" % "1.11.534",
  "com.amazonaws" % "aws-java-sdk-dynamodb" % "1.11.534",
  "com.amazonaws" % "aws-java-sdk-kms" % "1.11.534",
  "com.amazonaws" % "aws-java-sdk-s3" % "1.11.534",
  "org.apache.hadoop" % "hadoop-aws" % "3.1.2",
  "commons-httpclient" % "commons-httpclient" % "3.1",
  //"org.apache.httpcomponents" % "httpclient" % "4.5.3",
  "joda-time" %"joda-time" %"2.9.9",
  "org.apache.commons" % "commons-email" % "1.5",
  //"org.scalaj" %% "scalaj-http" %"2.3.0",
  "com.databricks" %% "spark-xml" % "0.4.1",
  "org.scalaj" %% "scalaj-http" %"2.3.0",
  //"joda-time" % "joda-time" % "2.8.1",
  "org.apache.commons" %"commons-io"% "1.3.2",
  //"org.apache.kafka" %% "kafka" % "0.10.2.2",
  "org.elasticsearch" %% "elasticsearch-spark-20" % "7.3.2",
  "org.mockito" % "mockito-core" % "2.10.0" % Test,
  "com.holdenkarau" %% "spark-testing-base" % "2.1.1_0.7.4" % Test,
  "org.mockito" % "mockito-all" % "1.10.19" % Test,
  "org.json" % "json" % "20180813",
  "org.elasticsearch.client" % "elasticsearch-rest-high-level-client" % "7.3.2",
  "org.scala-lang" % "scala-compiler" % "2.11.11"
)