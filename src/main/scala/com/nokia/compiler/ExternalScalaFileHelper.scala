package com.nokia.compiler

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

trait ExternalProcessing {
  def parser(code:String): List[Map[String,String]]
}


case class LoadExternalScalaFile(code: String)  {
  val toolbox = currentMirror.mkToolBox()
  val fileContents = new String(java.util.Base64.getDecoder.decode(code))

  val tree = toolbox.parse(
    s"""import com.nokia.compiler._; new ExternalProcessing {
       |import java.util.Scanner
       |import scala.io.Source
       |import scala.collection.mutable.ListBuffer
       |$fileContents
       |}
       |""".stripMargin)

  val compiledCode = toolbox.compile(tree)

  def getFileReference  = compiledCode().asInstanceOf[ExternalProcessing]
}

class ExternalScalaFileHelper {

  def processExternalFile(code: String, file: String): List[Map[String,String]] = {

    val externalFile = LoadExternalScalaFile(code)
    val externalProcessing = externalFile.getFileReference
    externalProcessing.parser(new String(java.util.Base64.getDecoder.decode(file)))
  }
}
