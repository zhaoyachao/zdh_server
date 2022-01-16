package com.zyc.zdh.datasources

import java.util.Properties

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 使用此数据源连接所有的jdbc数据,包括hive,mysql,oracle 等
  */
object TidbDataSources extends ZdhDataSources{

  val source="TIDB"
  val logger=LoggerFactory.getLogger(this.getClass)

  /**
    * 获取数据源schema
    *
    * @param spark
    * @param options
    * @return
    */
  override def getSchema(spark: SparkSession, options: Map[String,String])(implicit dispatch_task_id:String): Array[StructField] = {
    logger.info(s"[数据采集]:[${source}]:[SCHEMA]:"+options.mkString(","))
    spark.read.format("jdbc").options(options).load().schema.fields
  }


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:输入源为[TIDB],开始匹配对应参数")

      // 检查spark.tispark.pd.addresses,spark.sql.extensions
      if(!spark.conf.getAll.contains("spark.tispark.pd.addresses") || !spark.conf.get("spark.sql.extensions") .equalsIgnoreCase("org.apache.spark.sql.TiExtensions")){
        throw new Exception("[zdh],TIDB数据源读取:请设置spark.tispark.pd.addresses,并且set spark.sql.extensions=org.apache.spark.sql.TiExtensions,也可使用jdbc方式读取tidb")
      }

      val tableName=inputOptions.getOrElse("paths","").toString
      if(tableName.trim.equals("")|| !tableName.contains(".")){
        throw new Exception("[数据采集]:[TIDB]:[READ]:paths参数为空,必须是database.tablename 格式")
      }

      logger.info("[数据采集]:[TIDB]:[READ]:表名:"+tableName+","+inputOptions.mkString(",")+" [FILTER]:"+inputCondition)


      val sql=s"select ${inputCols.mkString(",")} from ${tableName}";

      logger.info(sql)
      var df:DataFrame=spark.sql(sql)

      filter(spark,df,inputCondition,duplicateCols)

    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[TIDB]:[READ]:表名:"+inputOptions.getOrElse("dbtable","")+"[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
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
      logger.info("[数据采集]:[TIDB]:[SELECT]")
      logger.debug("[数据采集]:[TIDB]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[TIDB]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[TIDB]:[SELECT]:[ERROR]:"+ex.getMessage.replace("\"","'"),"error")
        throw ex
      }
    }

  }


  override def writeDS(spark: SparkSession,df:DataFrame,options: Map[String,String], sql: String)(implicit dispatch_task_id:String): Unit = {
    try{
      logger.info("[数据采集]:[TIDB]:[WRITE]:表名:"+options.getOrElse("dbtable","")+","+options.mkString(","))
      var options_tmp=options
      val dbtable: String = options.getOrElse("paths", "").toString
      if(dbtable.trim.equals("")|| !dbtable.contains(".")){
        throw new Exception("[数据采集]:[TIDB]:[WRITE]:paths参数为空,必须是database.tablename 格式")
      }

      val url=options.getOrElse("url","")
      if(url.trim.equals("")){
        throw new Exception("[数据采集]:[TIDB]:[WRITE]:url参数为空")
      }
      var addr="127.0.0.1"
      var port="4000"
      if(url.contains(":")){
        addr=url.split(":")(0)
        port=url.split(":")(1)
      }else{
        addr=url
      }

      options_tmp=options_tmp.+("tidb.user"->options.getOrElse("user","root"))
      options_tmp=options_tmp.+("tidb.password"->options.getOrElse("password",""))
      options_tmp=options_tmp.+("table"->dbtable.split("\\.")(1))
      options_tmp=options_tmp.+("tidb.addr"->addr)
      options_tmp=options_tmp.+("tidb.port"->port)
      options_tmp=options_tmp.+("database"->dbtable.split("\\.")(0))

      if(!sql.equals("")){
        deleteTIDB(spark,url,options,sql)
      }

      var format="tidb"
//      df.write.
//        format("tidb").
//        option("tidb.user", "root").
//        option("tidb.password", "").
//        option("database", "tpch_test").
//        option("table", "target_table_orders").
//        mode("append").
//        save()
      //合并小文件操作
      var df_tmp = merge(spark,df,options)
      df_tmp.write.format(format).mode(SaveMode.Append).options(options_tmp).save()

    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.info("[数据采集]:[TIDB]:[WRITE]:表名:"+options.getOrElse("dbtable","")+","+"[ERROR]:"+ex.getMessage.replace("\"","'"))
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
  def deleteTIDB(spark: SparkSession, url: String, options:  Map[String,String], sql: String)(implicit dispatch_task_id:String): Unit = {
    val new_url="jdbc:mysql://"+url
    logger.info("[数据采集]:[TIDB]:[CLEAR]:url:"+new_url+","+options.mkString(",")+",sql:"+sql)
    import scala.collection.JavaConverters._
    val properties=new Properties()
    properties.putAll(options.asJava)
    var driver = properties.getProperty("driver", "com.mysql.cj.jdbc.Driver")
    if (driver.equals("")) {
      driver = getDriver(new_url)
    }
    Class.forName(driver)
    var cn: java.sql.Connection = null
    var ps: java.sql.PreparedStatement = null
    try {
      cn = java.sql.DriverManager.getConnection(new_url, properties)
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
          logger.warn("[数据采集]:[TIDB]:[CLEAR]:[WARN]:"+ex.getMessage.replace("\"","'"))
        }else{
          throw ex
        }
      }
    }
  }


  def getDriver(url: String): String = {

    url match {
      case u if u.toLowerCase.contains("jdbc:mysql") => "com.mysql.cj.jdbc.Driver"
      case _ => ""
    }


  }

}
