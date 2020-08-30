package com.zyc.zdh.datasources.http

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap

class HttpOptions(
                   @transient val parameters: CaseInsensitiveMap[String])
  extends Serializable {

  import HttpOptions._

  def this(parameters: Map[String, String]) = this(CaseInsensitiveMap(parameters))

  require(
    parameters.get(HTTP_URL).isDefined,
    s"Option '$HTTP_URL' is required. " +
      s"Option '$HTTP_URL' is not null.")

  def getSchema(): String = {
    parameters.getOrElse(SCHEMA, "value").toString
  }

  def getHttp_Url: String = {
    if (!parameters.get(HTTP_URL).get.endsWith("/") && !parameters.get(PATHS).get.startsWith("/"))
      parameters.get(HTTP_URL).get + "/" + parameters.get(PATHS).get
    else
      parameters.get(HTTP_URL).get + parameters.get(PATHS).get
  }

  def getMethod(): String ={
    parameters.getOrElse(METHOD,"get").toString.toLowerCase
  }

  def getTimeOut():Long={
    parameters.getOrElse(TIME_OUT,"5000").toLong
  }

  def getFileType(): String ={
    parameters.getOrElse(FILETYPE,"json").toString.toLowerCase
  }
  def getSep(): String ={
    parameters.getOrElse(SEP,",").toString
  }

}

object HttpOptions {

  val HTTP_URL = "url"
  val SCHEMA = "schema"
  val PATHS = "paths"
  val METHOD="method"
  val TIME_OUT="time_out"
  val FILETYPE="fileType"
  val SEP="sep"



}
