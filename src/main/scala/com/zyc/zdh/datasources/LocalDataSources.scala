package com.zyc.zdh.datasources

import com.zyc.base.util.JsonSchemaBuilder
import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object LocalDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols:Array[String],outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:输入源为[外部上传],开始匹配对应参数")

      var url = inputOptions.getOrElse("url", "")
      var port = "22"
      if (url.contains(":")) {
        port = url.split(":")(1)
        url = url.split(":")(0)
      }
      val sep = inputOptions.getOrElse("sep", ",")

      val username = inputOptions.getOrElse("user", "")
      val password = inputOptions.getOrElse("password", "")

      val paths = inputOptions.getOrElse("paths", "").toString
      if (paths.trim.equals("")) {
        throw new Exception("[zdh],外部上传数据源读取:paths为空")
      }

      val fileType = inputOptions.getOrElse("fileType", "csv").toString.toLowerCase

      val encoding = inputOptions.getOrElse("encoding", "")

      val schema = JsonSchemaBuilder.getJsonSchema(inputCols.mkString(","))
      var df = spark.emptyDataFrame
      if (!url.equals("")) {
        //nginx 资源
        logger.error("[数据采集]:[外部上传]:[READ]:读取ftp资源")
        logger.error("[数据采集]:[外部上传]:[READ]:读取ftp资源,目前只支持utf-8 编码")
        df = spark.read.
          format("com.zyc.zdh.datasources.sftp.SftpSource").
          schema(schema).
          options(inputOptions).
          option("inputCols",inputCols.mkString(",")).
          option("host", url).
          option("port", port).
          option("username", username).
          option("password", password).
          option("fileType", fileType).
          option("delimiter", sep).
          load(paths)
      } else {
        //本地资源
        logger.error("[数据采集]:[外部上传]:[READ]:读取本地资源")
        if (!fileType.equals("orc") && !fileType.equals("parquet")) {
          df = spark.read.format(fileType).schema(schema).options(inputOptions).load("file:///" + paths)
        } else {
          df = spark.read.format(fileType).options(inputOptions).load("file:///" + paths).select(inputCols.map(col(_)): _*)
        }
      }

      filter(spark,df,inputCondition,duplicateCols)
    } catch {
      case ex: Exception => {
        logger.error("[数据采集]:[外部上传]:[READ]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:[外部上传]:[SELECT]")
      logger.debug("[数据采集]:[外部上传]:[SELECT]:" + select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[外部上传]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    } catch {
      case ex: Exception => {
        logger.error("[数据采集]:[外部上传]:[SELECT]:[ERROR]:" + ex.getMessage.replace("\"","'"), "error")
        throw ex
      }
    }
  }


}
