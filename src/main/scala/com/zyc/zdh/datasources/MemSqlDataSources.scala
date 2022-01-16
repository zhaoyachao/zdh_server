package com.zyc.zdh.datasources

import java.util.Properties

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 使用此数据源连接所有的memsql数据,包括hive,mysql,oracle 等
  */
object MemSqlDataSources extends ZdhDataSources{

  val logger=LoggerFactory.getLogger(this.getClass)

  /**
    * 获取数据源schema
    *
    * @param spark
    * @param options
    * @return
    */
  override def getSchema(spark: SparkSession, options: Map[String,String])(implicit dispatch_task_id:String): Array[StructField] = {
    logger.info("[数据采集]:[MEMSQL]:[SCHEMA]:"+options.mkString(","))
    spark.read.format("memsql").options(options).load().schema.fields
  }


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:输入源为[MEMSQL],开始匹配对应参数")
      val url: String = inputOptions.getOrElse("url", "").toString
      if(url.trim.equals("")){
        throw new Exception("[zdh],memsql数据源读取:url为空")
      }
      val dbtable: String = inputOptions.getOrElse("dbtable", "").toString
      if(dbtable.trim.equals("")){
        throw new Exception("[zdh],memsql数据源读取:dbtable为空")
      }
      val user: String = inputOptions.getOrElse("user", "").toString
      if(user.trim.equals("")){
        logger.info("[zdh],memsql数据源读取:user为空")
     //   throw new Exception("[zdh],memsql数据源读取:user为空")
      }
      val password: String = inputOptions.getOrElse("password", "").toString
      if(password.trim.equals("")){
        logger.info("[zdh],memsql数据源读取:password为空")
      //  throw new Exception("[zdh],memsql数据源读取:password为空")
      }
      val driver: String = inputOptions.getOrElse("driver", "").toString
      if(driver.trim.equals("")){
        throw new Exception("[zdh],memsql数据源读取:driver为空")
      }
      val paths = inputOptions.getOrElse("paths", "").toString
      if(!paths.contains(".")){
        throw new Exception("[zdh],memsql数据源读取:表名必须是database.table")
      }
      logger.info("[数据采集]:[MEMSQL]:[READ]:表名:"+paths+","+inputOptions.mkString(",")+" [FILTER]:"+inputCondition)
      //获取memsql 配置
      var format="memsql"
      if(inputOptions.getOrElse("url","").toLowerCase.contains("memsql:hive2:")){
        format="org.apache.spark.sql.execution.datasources.hive.HiveRelationProvider"
        logger.info("[数据采集]:[MEMSQL]:[READ]:表名:"+inputOptions.getOrElse("dbtable","")+",使用自定义hive-memsql数据源")
      }
      if(inputOptions.getOrElse("url","").toLowerCase.contains("memsql:clickhouse:")){
        format="org.apache.spark.sql.execution.datasources.clickhouse.ClickHouseRelationProvider"
        logger.info("[数据采集]:[MEMSQL]:[READ]:表名:"+inputOptions.getOrElse("dbtable","")+",使用自定义clickhouse-memsql数据源")
      }

      var df:DataFrame=spark.read
        .format(format)
        .option("ddlEndpoint", url)
        .option("user", user)
        .option("password",password)
        .options(inputOptions)
        .load(paths)

      filter(spark,df,inputCondition,duplicateCols)

    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[MEMSQL]:[READ]:表名:"+inputOptions.getOrElse("paths","")+"[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
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
      logger.info("[数据采集]:[MEMSQL]:[SELECT]")
      logger.debug("[数据采集]:[MEMSQL]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[MEMSQL]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[MEMSQL]:[SELECT]:[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
        throw ex
      }
    }

  }


  override def writeDS(spark: SparkSession,df:DataFrame,options: Map[String,String], sql: String)(implicit dispatch_task_id:String): Unit = {
    try{
      logger.info("[数据采集]:[MEMSQL]:[WRITE]:表名:"+options.getOrElse("paths","")+","+options.mkString(","))
      val url=options.getOrElse("url","")
      if(!sql.equals("")){
        deletememsql(spark,url,options,sql)
      }

      val paths = options.getOrElse("paths", "").toString
      if(!paths.contains(".")){
        throw new Exception("[zdh],memsql数据源写入:表名必须是database.table")
      }

      val model = options.getOrElse("model", "").toString.toLowerCase match {
        case "overwrite" => SaveMode.Overwrite
        case "append" => SaveMode.Append
        case "errorifexists" => SaveMode.ErrorIfExists
        case "ignore" => SaveMode.Ignore
        case _ => SaveMode.Append
      }

      //合并小文件操作
      var df_tmp = merge(spark,df,options)

      var format="memsql"
      df_tmp.write.format(format)
        .mode(model)
        .option("ddlEndpoint", url)
        .options(options).save(paths)

    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.info("[数据采集]:[MEMSQL]:[WRITE]:表名:"+options.getOrElse("paths","")+","+"[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }


  /**
    * 写入memsql 之前 清空特定数据
    *
    * @param spark
    * @param url
    * @param options
    * @param sql
    */
  def deletememsql(spark: SparkSession, url: String, options:  Map[String,String], sql: String)(implicit dispatch_task_id:String): Unit = {
    logger.info("[数据采集]:[MEMSQL]:[CLEAR]:url:"+url+","+options.mkString(",")+",sql:"+sql)
    import scala.collection.JavaConverters._
    val properties=new Properties()
    properties.putAll(options.asJava)
    var driver = properties.getProperty("driver", "org.mariadb.jdbc.Driver")
    Class.forName(driver)
    var cn: java.sql.Connection = null
    var ps: java.sql.PreparedStatement = null
    try {
      cn = java.sql.DriverManager.getConnection("jdbc:mariadb://"+url, properties)
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
          logger.warn("[数据采集]:[MEMSQL]:[CLEAR]:[WARN]:"+ex.getMessage.replace("\"","'"))
        }else{
          throw ex
        }
      }
    }
  }


  def getDriver(url: String): String = {

    url match {
      case u if u.toLowerCase.contains("jdbc:mariadb") => "org.mariadb.jdbc.Driver"
      case _ => ""
    }


  }

}
