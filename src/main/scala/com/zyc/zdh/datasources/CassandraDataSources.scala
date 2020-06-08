package com.zyc.zdh.datasources

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object CassandraDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {

    try {
      logger.info("[数据采集]:输入源为[CASSANDRA],开始匹配对应参数")
      val url: String = inputOptions.getOrElse("url", "").toString
      if (url.trim.equals("")) {
        throw new Exception("[zdh],cassandra数据源读取:url为空")
      }

      //keyspace.table
      val table: String = inputOptions.getOrElse("paths", "").toString
      if (table.trim.equals("") || !table.contains(".")) {
        throw new Exception("[zdh],cassandra数据源读取:paths为空,或者没有指定keyspace")
      }

      spark.conf.set("spark.cassandra.connection.host", url)
      if (url.contains(":")) {
        spark.conf.set("spark.cassandra.connection.host", url.split(":")(0))
        spark.conf.set("spark.cassandra.connection.port", url.split(":")(1))
      }

      var df = spark
        .read
        .format("org.apache.spark.sql.cassandra")
        .options(inputOptions)
        .option("table", table.split("\\.")(1))
        .option("keyspace", table.split("\\.")(0))
        .load()

      filter(spark,df,inputCondition,duplicateCols)

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[CASSANDRA]:[WRITE]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:[CASSANDRA]:[SELECT]")
      logger.debug("[数据采集]:[CASSANDRA]:[SELECT]:" + select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[CASSANDRA]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    } catch {
      case ex: Exception => {
        logger.error("[数据采集]:[CASSANDRA]:[SELECT]:[ERROR]" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    try {
      logger.info("[数据采集]:[CASSANDRA]:[WRITE]:[options]:" + options.mkString(","))

      //默认是append
      val model = options.getOrElse("model", "").toString.toLowerCase match {
        case "overwrite" => SaveMode.Overwrite
        case "append" => SaveMode.Append
        case "errorifexists" => SaveMode.ErrorIfExists
        case "ignore" => SaveMode.Ignore
        case _ => SaveMode.Append
      }

      val url: String = options.getOrElse("url", "").toString
      if (url.trim.equals("")) {
        throw new Exception("[zdh],cassandra数据源读取:url为空")
      }

      spark.conf.set("spark.cassandra.connection.host", url)
      if (url.contains(":")) {
        spark.conf.set("spark.cassandra.connection.host", url.split(":")(0))
        spark.conf.set("spark.cassandra.connection.port", url.split(":")(1))
      }

      val table: String = options.getOrElse("paths", "").toString
      if (table.trim.equals("") || !table.contains(".")) {
        throw new Exception("[zdh],cassandra数据源读取:paths为空,或者没有指定keyspace")
      }

      var df_tmp=df
      //合并小文件操作
      if(!options.getOrElse("merge","-1").equals("-1")){
        df_tmp=df.repartition(options.getOrElse("merge","200").toInt)
      }

      try {
        df_tmp.write
          .format("org.apache.spark.sql.cassandra")
          .mode(model)
          .options(options)
          .option("table", table.split("\\.")(1))
          .option("keyspace", table.split("\\.")(0))
          .save()
      } catch {
        case ex: Exception => {
          import com.datastax.spark.connector._
          if (ex.getMessage.replace("\"","'").contains("any similarly named keyspace and table pairs")) {
          logger.info("[数据采集]:[CASSANDRA]:[WRITE]:[WARN]:表或者键空间不存在,将进行自动创建")
            df_tmp.createCassandraTable(table.split("\\.")(0),table.split("\\.")(1))
            df_tmp.write
              .format("org.apache.spark.sql.cassandra")
              .mode(model)
              .options(options)
              .option("table", table.split("\\.")(1))
              .option("keyspace", table.split("\\.")(0))
              .save()
          }
        }
      }

    } catch {
      case ex: Exception => {

        ex.printStackTrace()
        logger.error("[数据采集]:[CASSANDRA]:[WRITE]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

}
