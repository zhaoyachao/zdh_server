package com.zyc.zdh.datasources

import com.redislabs.provider.redis._
import com.zyc.base.util.JsonSchemaBuilder
import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object RedisDataSources extends ZdhDataSources {

  val logger=LoggerFactory.getLogger(this.getClass)

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:输入源为[REDIS]")
      logger.info("[数据采集]:[REDIS]:[READ]:[paths]:"+inputOptions.getOrElse("paths", "")+"[其他参数]:"+inputOptions.mkString(",")+"[FILTER]:"+inputCondition)
      import spark.implicits._

      //"redis://:yld@127.0.0.1:6379"
      val url = inputOptions.getOrElse("url", "")
      if(url.trim.equals("")){
        throw new Exception("[zdh],redis数据源读取:url为空")
      }

      val password: String = inputOptions.getOrElse("password", "").toString
      //可以是表达式 也可以是具体的key
      val paths = inputOptions.getOrElse("paths", "")
      if(paths.trim.equals("")){
        throw new Exception("[zdh],redis数据源读取:paths为空")
      }
      var host = url
      var port = "6379"
      if (url.contains(":")) {
        host = url.split(":")(0)
        port = url.split(":")(1)
      }


      val schema = JsonSchemaBuilder.getJsonSchema(inputCols.mkString(","))

      //string,list,hash,set,table
      val dataType = inputOptions.getOrElse("data_type", "string").toLowerCase

      val redisConfig = new RedisConfig(new RedisEndpoint(host,port.toInt,password))

      val df = dataType match {
        case "string" => spark.sparkContext.fromRedisKV(paths)(redisConfig).toDF(inputCols: _*)
        case "hash" => {
          spark.read
            .format("org.apache.spark.sql.redis")
            .option("host", host)
            .option("keys.pattern", paths)
            .option("port", port)
            .option("auth", password)
            .options(inputOptions)
            .schema(schema)
            .load()
        }
        case "list" => spark.sparkContext.fromRedisList(paths)(redisConfig).toDF(inputCols: _*)
        case "set" => spark.sparkContext.fromRedisSet(paths)(redisConfig).toDF(inputCols: _*)
        case "table"=>{
          spark.read
            .format("org.apache.spark.sql.redis")
            .option("host", host)
            .option("table", paths)
            .option("port", port)
            .option("auth", password)
            .options(inputOptions)
            .schema(schema)
            .load()
        }
      }

      filter(spark,df,inputCondition,duplicateCols)
    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.error("[数据采集]:[REDIS]:[READ]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }


  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:[REDIS]:[SELECT]")
    logger.debug("[数据采集]:[REDIS]:[SELECT]:"+select.mkString(","))
    if(select==null || select.isEmpty){
      logger.debug("[数据采集]:[REDIS]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
      return df
    }
    df.select(select: _*)
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    try{
      logger.info("[数据采集]:[REDIS]:[WRITE]:[options]:"+options.mkString(","))
      import spark.implicits._

      //"redis://:yld@127.0.0.1:6379"
      val url = options.getOrElse("url", "")
      if(url.trim.equals("")){
        throw new Exception("[zdh],redis数据源读取:url为空")
      }

      val password: String = options.getOrElse("password", "").toString
      //可以是表达式 也可以是具体的key
      val paths = options.getOrElse("paths", "")
      if(paths.trim.equals("")){
        throw new Exception("[zdh],redis数据源读取:paths为空")
      }
      var host = url
      var port = "6379"
      if (url.contains(":")) {
        host = url.split(":")(0)
        port = url.split(":")(1)
      }


      //string,list,hash,set,table
      val dataType = options.getOrElse("data_type", "string").toLowerCase

      val saveModel=options.getOrElse("model","append")

      val redisConfig = new RedisConfig(new RedisEndpoint(host,port.toInt,password))

      val redisRDD = dataType match {
        case "string" => spark.sparkContext.toRedisKV(df.map(row=>(row.getString(0),row.getString(1))).rdd)
        //      case "hash" => {
        //        df.map(row=>(row.getString(0),row.getString(1)))
        //
        //      }
        //      case "list" => spark.sparkContext.toRedisList(paths)(redisConfig).toDF(inputCols: _*)
        //      case "set" => spark.sparkContext.fromRedisSet(paths)(redisConfig).toDF(inputCols: _*)
        case "table"=>{
          df.write.format("org.apache.spark.sql.redis")
            .mode(saveModel)
            .option("host", host)
            .option("table", paths)
            .option("port", port)
            .option("auth", password)
            .options(options)
            .save()
        }
      }
    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.error("[数据采集]:[REDIS]:[WRITE]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }


  }

}
