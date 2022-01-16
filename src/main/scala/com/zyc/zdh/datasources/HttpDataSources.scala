package com.zyc.zdh.datasources

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object HttpDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols:Array[String],outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
   try{
     logger.info("[数据采集]:输入源为[HTTP],开始匹配对应参数")
     val url: String = inputOptions.getOrElse("url", "").toString

     if(url.trim.equals("")){
       throw new Exception("[zdh],http数据源读取:url为空")
     }

     val paths = inputOptions.getOrElse("paths", "").toString
     if (paths.trim.equals("")) {
       throw new Exception("[zdh],http数据源读取:paths为空")
     }
     var df=spark.read.format("com.zyc.zdh.datasources.http.datasources.HttpRelationProvider")
       .options(inputOptions)
       .option("url",url)
       .option("schema",inputCols.mkString(","))
       .option("paths",paths)
       .load()

     filter(spark,df,inputCondition,duplicateCols)
   }catch {
     case ex:Exception=>{
       logger.error("[数据采集]:[HTTP]:[READ]:[ERROR]:"+ex.getMessage.replace("\"","'"))
       throw ex
     }
   }


  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:[HTTP]:[SELECT]")
      logger.debug("[数据采集]:[HTTP]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[HTTP]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[HTTP]:[SELECT]:[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
        throw ex
      }
    }
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    logger.info("[数据采集]:[HTTP]:[WRITE]:")
    throw new Exception("[数据采集]:[HTTP]:[WRITE]:[ERROR]:不支持写入solr数据源")
  }


}
