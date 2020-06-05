package com.zyc.zdh.datasources

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 只支持集群,不支持单机
  */
object SolrDataSources extends ZdhDataSources {

  val logger=LoggerFactory.getLogger(this.getClass)

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {

    logger.info("[数据采集]:[SOLR]:[READ]:其他参数:," + inputOptions.mkString(",") + " [FILTER]:" + inputCondition)

    import spark.implicits._

    val zkUrl = inputOptions.getOrElse("url", "")

    val collection = inputOptions.getOrElse("paths", "")

    //参考 https://github.com/lucidworks/spark-solr
    val df = spark.read.format("solr")
      .options(inputOptions)
      .option("zkhost", zkUrl)
      .option("collection", collection)
      .load

    filter(spark,df,inputCondition,duplicateCols)

  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:[SOLR]:[SELECT]")
      logger.debug("[数据采集]:[SOLR]:[SELECT]:"+select.mkString(","))
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[SOLR]:[SELECT]:[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
        throw ex
      }
    }
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    logger.info("[数据采集]:[SOLR]:[WRITE]:")
    throw new Exception("[数据采集]:[SOLR]:[WRITE]:[ERROR]:不支持写入solr数据源")
  }
}
