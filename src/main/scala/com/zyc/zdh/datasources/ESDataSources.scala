package com.zyc.zdh.datasources

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object ESDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols:Array[String],outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame ={
    try {
      logger.info("[数据采集]:输入源为[ES],开始匹配对应参数")


      val path=inputOptions.getOrElse("paths","").toString
      if(path.trim.equals("")){
        throw new Exception("[zdh],es数据源读取:paths为空")
      }

      val url: String = inputOptions.getOrElse("url", "").toString

      if(url.trim.equals("") || !url.trim.contains(":")){
        throw new Exception("[zdh],es数据源读取:url为空或者不是ip:port 格式")
      }

      val nodes: String =  inputOptions.getOrElse("es.nodes", url.split(":")(0)).toString
      if(nodes.trim.equals("")){
        throw new Exception("[zdh],es数据源读取:nodes为空")
      }

      val port: String = inputOptions.getOrElse("es.port", url.split(":")(1)).toString
      if(port.trim.equals("")){
        throw new Exception("[zdh],es数据源读取:port为空")
      }


      logger.info("[数据采集]:[ES]:[READ]:路径:" + path + "," + inputOptions.mkString(",") + " [FILTER]:" + inputCondition)

      //      cfg.put("es.nodes", "192.168.56.12");
      //      cfg.put("es.port", "9200");

      //获取jdbc 配置
      val df=spark.read.format("org.elasticsearch.spark.sql").options(inputOptions.+("es.nodes"->nodes).+("es.port"->port)).load(path)

      filter(spark,df,inputCondition,duplicateCols)

    } catch {
      case ex: Exception => {
        logger.error("[数据采集]:[ES]:[READ]:路径:" + inputOptions.getOrElse("paths","").toString+ "[ERROR]:" + ex.getMessage)
        throw ex
      }
    }
  }

  /**
    *
    * @param spark
    * @param df
    * @param select
    * @param dispatch_task_id
    * @return
    */
  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id:String): DataFrame = {
    try{
      logger.info("[数据采集]:[ES]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[ES]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[ES]:[SELECT]:[ERROR]:"+ex.getMessage)
        throw ex
      }
    }

  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {

    writeDS(spark,df,options.getOrElse("paths",""),options)
  }

  /**
    *  写入ES
    * @param spark
    * @param df
    * @param path
    * @param options
    * @param dispatch_task_id
    */
  def writeDS(spark: SparkSession, df: DataFrame, path: String, options: Map[String, String])(implicit dispatch_task_id: String): Unit = {
    try {
      logger.info("[数据采集]:[ES]:[WRITE]:路径:" + path + "," + options.mkString(","))
      val url: String = options.getOrElse("url", "").toString

      if(url.trim.equals("") || !url.trim.contains(":")){
        throw new Exception("[zdh],es数据源写入:url为空或者不是ip:port 格式")
      }

      val nodes: String =  options.getOrElse("es.nodes", url.split(":")(0)).toString
      if(nodes.trim.equals("")){
        throw new Exception("[zdh],es数据源写入:nodes为空")
      }

      val port: String = options.getOrElse("es.port", url.split(":")(1)).toString
      if(port.trim.equals("")){
        throw new Exception("[zdh],es数据源写入:port为空")
      }

      val opt=options.+(("es.nodes"->nodes),("es.port"->port))
      if(options.getOrElse("mode","").equals("")){
        df.write.format("org.elasticsearch.spark.sql").options(opt).save(path)
      }else{
        df.write.format("org.elasticsearch.spark.sql").options(opt).mode(options.getOrElse("mode","")).save(path)
      }

    } catch {
      case ex: Exception => {
        logger.error("[数据采集]:[ES]:[WRITE]:路径:" +path + "," + "[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

}
