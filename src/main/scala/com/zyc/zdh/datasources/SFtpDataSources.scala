package com.zyc.zdh.datasources

import com.zyc.base.util.JsonSchemaBuilder
import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object SFtpDataSources extends ZdhDataSources {
  val logger = LoggerFactory.getLogger(this.getClass)


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:[SFTP]:匹配文件格式")
      var url=inputOptions.getOrElse("url","")
      var port="22"
      if(url.contains(":")){
        port=url.split(":")(1)
        url=url.split(":")(0)
      }
      if(url.trim.equals("")){
        throw new Exception("[zdh],ftp数据源读取:url为空")
      }

      val paths=inputOptions.getOrElse("paths","")

      if(paths.trim.equals("")){
        throw new Exception("[zdh],ftp数据源读取:paths为空")
      }
      val sep=inputOptions.getOrElse("sep",",")

      val username=inputOptions.getOrElse("user","")
      val password=inputOptions.getOrElse("password","")

      val fileType=inputOptions.getOrElse("fileType", "csv").toString.toLowerCase

      val schema=JsonSchemaBuilder.getJsonSchema(inputCols.mkString(","))
      logger.info("[数据采集]:[SFTP]:[READ]:paths:"+url+":"+port+paths)
      val df = spark.read.
        format("com.zyc.zdh.datasources.sftp.SftpSource").
        schema(schema).
        options(inputOptions).
        option("inputCols",inputCols.mkString(",")).
        option("host", url).
        option("port",port).
        option("username", username).
        option("password",password).
        option("fileType", fileType).
        option("delimiter", sep).
        load(paths)

      filter(spark,df,inputCondition,duplicateCols)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[SFTP]:[READ]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:[SFTP]:[SELECT]")
      logger.debug("[数据采集]:[SFTP]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[SFTP]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[SFTP]:[SELECT]:[ERROR]"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    try {
      logger.info("[数据采集]:[SFTP]:[WRITE]:[options]:"+options.mkString(","))
      var url=options.getOrElse("url","")
      var port="22"
      if(url.contains(":")){
        port=url.split(":")(1)
        url=url.split(":")(0)
      }

      val paths=options.getOrElse("paths","")
      val sep=options.getOrElse("sep",",")

      val username=options.getOrElse("user","")
      val password=options.getOrElse("password","")

      val filtType=options.getOrElse("fileType", "csv").toString.toLowerCase

      //合并小文件操作
      var df_tmp = merge(spark,df,options)

      df_tmp.write.
        format("com.springml.spark.sftp").
        options(options).
        option("host", url).
        option("port",port).
        option("username", username).
        option("password",password).
        option("fileType", filtType).
        option("delimiter", sep).
        save(paths)

      df_tmp

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[SFTP]:[WRITE]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }


}
