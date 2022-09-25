package com.zyc.zdh.datasources

import java.util.Properties

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 使用此数据源连接所有的jdbc数据,包括hive,mysql,oracle 等
  */
object GreenplumDataSources extends ZdhDataSources{

  val logger=LoggerFactory.getLogger(this.getClass)

  /**
    * 获取数据源schema
    *
    * @param spark
    * @param options
    * @return
    */
  override def getSchema(spark: SparkSession, options: Map[String,String])(implicit dispatch_task_id:String): Array[StructField] = {
    logger.info("[数据采集]:[Greenplum]:[SCHEMA]:"+options.mkString(","))
    var dbtable: String = options.getOrElse("dbtable", "").toString
    val paths = options.getOrElse("paths", "").toString
    if (paths.trim.equals("")) {
      throw new Exception("[zdh],Greenplum数据源读取:paths为空")
    }
    var dbschema=""
    if(paths.contains(".")){
      dbtable=paths.split(".")(1)
      dbschema=paths.split(".")(0)
    }else{
      dbtable=paths
    }
    var tmpOptions=options.+("dbtable"->dbtable,"dbschema"->dbschema)
    spark.read.format("greenplum").options(tmpOptions).load().schema.fields
  }


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    var tmpOptions=inputOptions
    try{
      logger.info("[数据采集]:输入源为[Greenplum],开始匹配对应参数")
      val url: String = inputOptions.getOrElse("url", "").toString
      if(url.trim.equals("")){
        throw new Exception("[zdh],Greenplum数据源读取:url为空")
      }
      var dbtable: String = inputOptions.getOrElse("dbtable", "").toString
      val paths = inputOptions.getOrElse("paths", "").toString
      if (paths.trim.equals("")) {
        throw new Exception("[zdh],Greenplum数据源读取:paths为空")
      }
      var dbschema=""
      if(paths.contains(".")){
        dbtable=paths.split("\\.")(1)
        dbschema=paths.split("\\.")(0)
      }else{
        dbtable=paths
      }

      val user: String = inputOptions.getOrElse("user", "").toString
      if(user.trim.equals("")){
        logger.info("[zdh],Greenplum数据源读取:user为空")
     //   throw new Exception("[zdh],jdbc数据源读取:user为空")
      }
      val password: String = inputOptions.getOrElse("password", "").toString
      if(password.trim.equals("")){
        logger.info("[zdh],Greenplum数据源读取:password为空")
      //  throw new Exception("[zdh],jdbc数据源读取:password为空")
      }
//      val driver: String = inputOptions.getOrElse("driver", "").toString
//      if(driver.trim.equals("")){
//        throw new Exception("[zdh],Greenplum数据源读取:driver为空")
//      }

      tmpOptions=inputOptions.+("dbtable"->dbtable,"dbschema"->dbschema)

      logger.info("[数据采集]:[Greenplum]:[READ]:表名:"+tmpOptions.getOrElse("dbtable","")+","+tmpOptions.mkString(",")+" [FILTER]:"+inputCondition)
      //获取jdbc 配置
      //https://github.com/kongyew/greenplum-spark-connector/blob/master/usecase1/README.MD
      var format="greenplum"
      var df:DataFrame=spark.read.format(format).options(tmpOptions).load()

      filter(spark,df,inputCondition,duplicateCols)

    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[Greenplum]:[READ]:表名:"+tmpOptions.getOrElse("paths","")+"[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
        throw ex
      }
    }

  }

  /**
    * 读取数据源之后的字段映射
    * @param spark
    * @param df
    * @param select
    * @return
    */
  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id:String): DataFrame = {
    try{
      logger.info("[数据采集]:[Greenplum]:[SELECT]")
      logger.debug("[数据采集]:[Greenplum]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[Greenplum]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[Greenplum]:[SELECT]:[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
        throw ex
      }
    }

  }


  override def writeDS(spark: SparkSession,df:DataFrame,options: Map[String,String], sql: String)(implicit dispatch_task_id:String): Unit = {
    try{
      logger.info("[数据采集]:[JDBC]:[WRITE]:表名:"+options.getOrElse("paths","")+","+options.mkString(","))

      val paths = options.getOrElse("paths", "").toString
      if (paths.trim.equals("")) {
        throw new Exception("[zdh],Greenplum数据源读取:paths为空")
      }
      var dbtable: String = paths
      var dbschema=""
      if(paths.contains(".")){
        dbtable=paths.split(".")(1)
        dbschema=paths.split(".")(0)
      }
      val url=options.getOrElse("url","")
      if(!sql.equals("")){
        deleteJDBC(spark,url,options,sql)
      }

      var  tmpOptions=options.+("dbtable"->dbtable,"dbschema"->dbschema)
      var format="greenplum"
      //合并小文件操作
      var df_tmp = merge(spark,df,options)
      df_tmp.write.format(format).mode(SaveMode.Append).options(tmpOptions).save()

    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.info("[数据采集]:[JDBC]:[WRITE]:表名:"+options.getOrElse("dbtable","")+","+"[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }


  /**
    * 写入jdbc 之前 清空特定数据
    *
    * @param spark
    * @param url
    * @param options
    * @param sql
    */
  def deleteJDBC(spark: SparkSession, url: String, options:  Map[String,String], sql: String)(implicit dispatch_task_id:String): Unit = {
    logger.info("[数据采集]:[Greenplum]:[CLEAR]:url:"+url+","+options.mkString(",")+",sql:"+sql)
    import scala.collection.JavaConverters._
    val properties=new Properties()
    properties.putAll(options.asJava)
    var driver = properties.getProperty("driver", "")
    if (driver.equals("")) {
      driver = getDriver(url)
    }
    Class.forName(driver)
    var cn: java.sql.Connection = null
    var ps: java.sql.PreparedStatement = null
    try {
      cn = java.sql.DriverManager.getConnection(url, properties)
      ps = cn.prepareStatement(sql)
      ps.execute()
      ps.close()
      cn.close()
    }
    catch {
      case ex: Exception => {
        ps.close()
        cn.close()
        if(ex.getMessage.replace("\"","'").contains("doesn't exist") || ex.getMessage.replace("\"","'").contains("Unknown table")){
          logger.warn("[数据采集]:[Greenplum]:[CLEAR]:[WARN]:"+ex.getMessage.replace("\"","'"))
        }else{
          throw ex
        }
      }
    }
  }


  def getDriver(url: String): String = {

    url match {
      case u if u.toLowerCase.contains("jdbc:mysql") => "com.mysql.jdbc.Driver"
      case _ => ""
    }


  }

}
