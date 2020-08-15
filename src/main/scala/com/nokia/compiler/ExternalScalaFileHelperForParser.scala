package com.nokia.compiler

import scala.reflect.runtime.currentMirror
import scala.tools.reflect.ToolBox

trait ExternalProcessingForParser {
  def parser(code:String): List[Map[String,String]]
}


case class LoadExternalScalaFileFroParser(code: String)  {
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

class ExternalScalaFileHelperForParser {

  def processExternalFileForParser(code: String, file: String): List[Map[String,String]] = {

    val externalFile = LoadExternalScalaFileFroParser(code)
    val externalProcessing = externalFile.getFileReference
    externalProcessing.parser(file)
  }
}
