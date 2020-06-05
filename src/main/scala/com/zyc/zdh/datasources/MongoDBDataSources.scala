package com.zyc.zdh.datasources

import java.io.StringReader
import java.util

import com.mongodb.{MongoClient, MongoClientURI}
import com.zyc.zdh.ZdhDataSources
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.delete.Delete
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object MongoDBDataSources extends ZdhDataSources {

  val logger=LoggerFactory.getLogger(this.getClass)

  override def getSchema(spark: SparkSession, options: Map[String, String])(implicit dispatch_task_id: String): Array[StructField] = super.getSchema(spark, options)


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols:Array[String],outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {

    try{

      logger.info("[数据采集]:输入源为[MONGODB],开始匹配对应参数")
      val url: String = inputOptions.getOrElse("url", "").toString
      if(url.trim.equals("")){
        throw new Exception("[zdh],MONGODB数据源读取:url为空")
      }
      var map=inputOptions.+("spark.mongodb.input.uri"->url)
      val paths=inputOptions.getOrElse("paths","").toString
      if(paths.trim.equals("")){
        throw new Exception("[zdh],MONGODB数据源读取:paths为空")
      }
      map=map.+("spark.mongodb.input.collection"->paths)


      val df=spark.read.format("mongo").options(map).load()
      filter(spark,df,inputCondition,duplicateCols)

    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[MONGODB]:[SELECT]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:[MONGODB]:[SELECT]")
      logger.debug("[数据采集]:[MONGODB]:[SELECT]:"+select.mkString(","))
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[MONGODB]:[SELECT]:[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
        throw ex
      }
    }
  }


  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {

    try{
      logger.info("[数据采集]:[MONGODB]:[WRITE]:表名:"+options.getOrElse("paths","")+","+options.mkString(","))
      val url=options.getOrElse("url","")
      if(url.trim.equals("")){
        throw new Exception("[zdh],MONGODB数据源输出:url为空")
      }
      var map=options.+("spark.mongodb.output.uri"->url)
      val paths=options.getOrElse("paths","").toString
      if(paths.trim.equals("")){
        throw new Exception("[zdh],MONGODB数据源输出:paths为空")
      }
      map=map.+("spark.mongodb.output.collection"->paths)

      if(!sql.equals("")){
        deleteJDBC(url,paths,sql)
      }

      var format="mongo"
      df.write.format(format).mode(map.getOrElse("model","append")).options(map).save()
    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.error("[数据采集]:[MONGODB]:[WRITE]:[ERROR]:表名:"+options.getOrElse("paths","")+","+"[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

  def deleteJDBC(url:String,collection:String,sql:String)(implicit dispatch_task_id: String): Unit ={
    try{

      logger.info("[数据采集]:[MONGODB]:[CLEAR]:url"+url+",collection:"+collection)

      val connectString=new MongoClientURI(url)

      val mongoClient=new MongoClient(connectString)

      import com.mongodb.client.model.Filters._
      val dataBase=mongoClient.getDatabase(connectString.getDatabase)

      val parser=new CCJSqlParserManager();
      val reader=new StringReader(sql);
      val list=new util.ArrayList[String]();
      val stmt=parser.parse(new StringReader(sql));

      if(stmt.isInstanceOf[Delete]){
        val whereExpr=stmt.asInstanceOf[Delete].getWhere
        whereExpr.toString match {

          case ex if (ex.contains(">=")) =>dataBase.getCollection(collection).deleteMany(gte(ex.split(">=")(0),ex.split(">=")(1)))
          case ex if (ex.contains("<=")) =>dataBase.getCollection(collection).deleteMany(lte(ex.split("<=")(0),ex.split("<=")(1)))
          case ex if (ex.contains(">")) =>dataBase.getCollection(collection).deleteMany(gt(ex.split(">")(0),ex.split(">")(1)))
          case ex if (ex.contains("<")) =>dataBase.getCollection(collection).deleteMany(lt(ex.split("<")(0),ex.split("<")(1)))
          case ex if (ex.contains("=")) =>dataBase.getCollection(collection).deleteMany(com.mongodb.client.model.Filters.eq(ex.split("=")(0),ex.split("=")(1)))
        }

      }
    }catch {
      case ex:Exception=>{
        logger.info("[数据采集]:[MONGODB]:[CLEAR]:[ERROR]:url:"+url+",collection:"+collection+ex.getMessage.replace("\"","'"))
        ex.printStackTrace()
      }
    }

  }

}
