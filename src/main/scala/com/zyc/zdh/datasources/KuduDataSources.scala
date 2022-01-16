package com.zyc.zdh.datasources

import com.zyc.zdh.ZdhDataSources
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.slf4j.LoggerFactory

object KuduDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols:Array[String],outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {

    try{
      logger.info("[数据采集]:[KUDU]:匹配文件格式")
      val url: String = inputOptions.getOrElse("url", "").toString
      if (url.trim.equals("")) {
        throw new Exception("[zdh],kudu数据源读取:url为空")
      }
      val paths = inputOptions.getOrElse("paths", "").toString

      if (paths.trim.equals("")) {
        throw new Exception("[zdh],kudu数据源读取:paths为空")
      }

      logger.info("[数据采集]:[KUDU]:[READ]:[TABLE]:"+paths+",[options]:"+inputOptions.mkString(","))

      val kuduOptions: Map[String, String] = Map("kudu.table" -> paths, "kudu.master" -> url)

      import org.apache.kudu.spark.kudu._
      val df = spark.read.options(kuduOptions).format("kudu").load()

      filter(spark,df,inputCondition,duplicateCols)

    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.error("[数据采集]:[KUDU]:[READ]:[TABLE]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:[KUDU]:[SELECT]")
      logger.debug("[数据采集]:[KUDU]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[KUDU]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[KUDU]:[SELECT]:[ERROR]"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }


  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {

    try {
      import spark.implicits._
      logger.info("[数据采集]:[KUDU]:[WRITE]:[options]:" + options.mkString(",")+",[FILTER]:"+sql+",[END]")
      val url: String = options.getOrElse("url", "").toString
      if (url.trim.equals("")) {
        throw new Exception("[zdh],kudu数据源写入:url为空")
      }
      val paths = options.getOrElse("paths", "").toString
      if (paths.trim.equals("")) {
        throw new Exception("[zdh],kudu数据源写入:paths为空")
      }

      if(!sql.trim.equals("")){
        throw new Exception("[zdh],kudu数据源写入:暂不支持删除历史数据")
      }

      val primaryKey = options.getOrElse("primary_key", "zdh_auto_md5")
        .split(",",-1).toSeq
      val replicas = options.getOrElse("replicas", "1").toInt

      val kuduOptions: Map[String, String] = Map("kudu.table" -> paths, "kudu.master" -> url)

      val kuduContext = new KuduContext(url, spark.sparkContext)
      import org.apache.kudu.spark.kudu._

      //合并小文件操作
      var df_tmp = merge(spark,df,options)

      var df_result=df_tmp
      var schema = df_result.schema
      if(primaryKey(0).equals("zdh_auto_md5")){
        df_result=df_tmp.withColumn("zdh_auto_md5",md5(concat(rand(),current_timestamp())))
        schema=df_result.schema
      }else{
        schema=StructType(df_result.schema.fields.map(f=>if(primaryKey.contains(f.name)){
          StructField(f.name,f.dataType,false,f.metadata)
        }else{
          f
        }))
      }


      if (!kuduContext.tableExists(paths)) {
        logger.info("[数据采集]:[KUDU]:[WRITE]:写入表不存在,将自动创建表")
        val kuduTableOptions = new CreateTableOptions()
        import scala.collection.JavaConverters._
        kuduTableOptions.setRangePartitionColumns(primaryKey.asJava).setNumReplicas(replicas);
        kuduContext.createTable(paths, schema, primaryKey, kuduTableOptions)
        logger.info("[数据采集]:[KUDU]:[WRITE]:完成自动创建表")
      }



      kuduContext.insertRows(df_result, paths)

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[KUDU]:[WRITE]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

}
