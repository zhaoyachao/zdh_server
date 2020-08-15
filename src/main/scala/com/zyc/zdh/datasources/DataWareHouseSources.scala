package com.zyc.zdh.datasources

import java.util.Properties

import com.zyc.base.util.JsonUtil
import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory
case class desc_table_class(col_name:String,data_type:String,comment:String)

/**
  * 本地数据仓库分析
  */
object DataWareHouseSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
    * 获取数据源schema
    *
    * @param spark
    * @param options
    * @return
    */
  override def getSchema(spark: SparkSession, options: Map[String, String])(implicit dispatch_task_id: String): Array[StructField] = {
    logger.info("[数据采集]:[DATAWAREHOUSE]:[SCHEMA]:" + options.mkString(","))
    null
  }


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols: Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:输入源为[DATAWAREHOUSE],开始匹配对应参数")

      logger.info("[数据采集]:[DATAWAREHOUSE]:[READ]:" + inputOptions.mkString(","))
      logger.info("[数据采集]:[DATAWAREHOUSE]:执行语句:"+sql)
      if (!sql.trim.equals("")) {
        val exe_sql_ary = sql.split(";\r\n|;\n")
        var result: DataFrame = null
        exe_sql_ary.foreach(sql_t => {
          if (!sql_t.trim.equals(""))
            result = spark.sql(sql_t)
        })

        result
      } else {
        logger.info("[数据采集]:[DATAWAREHOUSE]:执行语句:为空")
        null
      }
    } catch {
      case ex: Exception => {
        logger.error("[数据采集]:[DATAWAREHOUSE]:[READ]:" + "[ERROR]:" + ex.getMessage.replace("\"","'"), "error")
        throw ex
      }
    }
  }

  /**
    * 读取数据源之后的字段映射
    *
    * @param spark
    * @param df
    * @param select
    * @return
    */
  override def process(spark: SparkSession, df: DataFrame, select: Array[Column], zdh_etl_date: String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:[DATAWAREHOUSE]:[SELECT]")
      logger.debug("[数据采集]:[DATAWAREHOUSE]:[SELECT]:" + select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[DATAWAREHOUSE]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select:_*)
    } catch {
      case ex: Exception => {
        logger.error("[数据采集]:[DATAWAREHOUSE]:[SELECT]:[ERROR]:" + ex.getMessage.replace("\"","'"), "error")
        throw ex
      }
    }

  }

  def show_databases(spark: SparkSession): String = {
    import spark.implicits._
    val databaseNames = spark.sql("show databases").select("databaseName").as[String].collect()

    JsonUtil.toJson(databaseNames)
  }

  def show_tables(spark: SparkSession, databaseName: String): String = {
    import spark.implicits._
    spark.sql("use " + databaseName)
    val tableNames = spark.sql("show tables").select("tableName").as[String].collect()

    JsonUtil.toJson(tableNames)
  }

  def desc_table(spark: SparkSession, table: String): String = {
    import spark.implicits._

    import org.apache.spark.sql.functions._
    val tableCols = spark.sql("desc "+table).as[desc_table_class].collect()

    JsonUtil.toJson(tableCols)
  }
}
