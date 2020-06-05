package com.zyc.zdh

import java.util
import java.util.Properties

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

trait ZdhDataSources {

  val loggerz=LoggerFactory.getLogger(this.getClass)
  /**
    * 获取schema
    *
    * @param spark
    * @return
    */
  def getSchema(spark: SparkSession, options: Map[String, String])(implicit dispatch_task_id: String): Array[StructField] = ???

  /**
    *
    * @param spark
    * @param dispatchOption
    * @param inPut
    * @param inputOptions
    * @param inputCondition
    * @param inputCols
    * @param duplicateCols 去重字段
    * @param outPut
    * @param outputOptionions
    * @param outputCols
    * @param sql
    * @param dispatch_task_id
    * @return
    */
  def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String], inputCondition: String,
            inputCols: Array[String],duplicateCols:Array[String],
            outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = ???


  def filter(spark:SparkSession,df:DataFrame,inputCondition: String,duplicateCols:Array[String]): DataFrame ={
    var df_tmp=df
    if(inputCondition!=null && !inputCondition.trim.equals("")){
      df_tmp=df.filter(inputCondition)
    }
    if(duplicateCols!=null && duplicateCols.size>0){
      return df_tmp.dropDuplicates(duplicateCols)
    }
    return df_tmp

  }
  /**
    * 处理逻辑
    *
    * @param spark
    * @param df
    * @param select
    * @param dispatch_task_id
    * @return
    */
  def process(spark: SparkSession, df: DataFrame, select: Array[Column], zdh_etl_date: String)(implicit dispatch_task_id: String): DataFrame = ???


  /**
    * 写入数据总入口
    *
    * @param spark
    * @param df
    * @param options
    * @param sql
    * @param dispatch_task_id
    */
  def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = ???


  def dataQuality(spark: SparkSession, df: DataFrame, error_rate: String, primary_columns: String, column_size: Int, rows_range: String, column_is_null: Seq[Column], column_length: Seq[Column],column_regex: Seq[Column]): Map[String, String] = {

    val report = new util.HashMap[String, String]()
    report.put("result", "通过")
    import scala.collection.JavaConverters._
    try {
      import spark.implicits._
      val total_count = df.count()
      var error_count = 0L;
      //表级质量检测
      //判断主键是否重复
      if (!primary_columns.trim.equals("")) {
        val cols = primary_columns.split(",").map(col(_))
        val new_count = df.select(concat_ws("_", cols: _*) as "primary_key")
          .map(f => (f.getAs[String]("primary_key"), 1)).rdd.reduceByKey(_ + _)
        val primary_keys = new_count.filter(f => f._2 > 1)
        val primary_count = primary_keys.count()
        if (primary_count > 0) {
          error_count = error_count + primary_count
          loggerz.info("存在主键重复")
          report.put("primary_columns", "存在主键重复")
          report.put("result", "不通过")
        } else {
          loggerz.info("主键检测通过")
          report.put("primary_columns", "主键检测通过")
        }
      }

      //判断
      if (column_size > 0) {
        if (df.columns.size != column_size) {
          report.put("column_size", "解析字段个数不对")
          loggerz.info("解析字段个数不对")
          report.put("result", "不通过")
        } else {
          report.put("column_size", "字段个数检测通过")
          loggerz.info("字段个数检测通过")
        }
      }

      //判断行数
      if (!rows_range.equals("")) {
        var start_count = 0
        var end_count = 0
        if(rows_range.contains("-")){
          start_count = rows_range.split("-")(0).toInt
          end_count = rows_range.split("-")(1).toInt
        }else{
          start_count=rows_range.toInt
          end_count=start_count
        }

        if (total_count < start_count || total_count > end_count) {
          loggerz.info("数据行数异常")
          report.put("rows_range", "数据行数异常")
          report.put("result", "不通过")
        } else {
          report.put("rows_range", "数据行数检测通过")
          loggerz.info("数据行数检测通过")
        }
      }

      //字段级检测
      //是否为空检测,
      //col("").isNull
      if (column_is_null.size > 0) {
        val filter_columns = column_is_null.tail.foldLeft(column_is_null.head)((x, y) => {
          x or y
        })

        val null_count = df.filter(filter_columns).count()
        if (null_count > 0) {
          error_count = error_count + null_count
          report.put("column_is_null", "存在字段为空,但是此字段不允许为空")
          report.put("result", "不通过")
          loggerz.info("存在字段为空,但是此字段不允许为空")
        } else {
          report.put("column_is_null", "字段是否为空,检测通过")
          loggerz.info("字段是否为空,检测通过")
        }
      }


      //
      //length(col(""))==长度
      if (column_length.size > 0) {
        val filter_column_length = column_length.tail.foldLeft(column_length.head)((x, y) => {
          x or y
        })

        val length_count = df.filter(filter_column_length).count()
        if (length_count > 0) {
          error_count = error_count + length_count
          report.put("column_length", "存在字段长度不满足")
          report.put("result", "不通过")
          loggerz.info("存在字段长度不满足")
        } else {
          loggerz.info("字段长度检测通过")
          report.put("column_length", "字段长度检测通过")
        }
      }

      if (column_regex.size > 0) {

        val c_r=column_regex.map(f=>f==="false")
        val filter_column_regex = c_r.tail.foldLeft(c_r.head)((x, y) => {
          x or y
        })
        val regex_count = df.filter(filter_column_regex).count()
        if (regex_count > 0) {
          error_count = error_count + regex_count
          report.put("column_regex", "正则判断不通过")
          report.put("result", "不通过")
          loggerz.info("存在正则不满足")
        } else {
          loggerz.info("正则判断检测通过")
          report.put("column_regex", "正则判断检测通过")
        }
      }

      loggerz.info("error_count:" + error_count)




      var error_num = total_count * error_rate.toDouble
      if (error_num < 1) {
        loggerz.info("容错的条数至少是1条")
        error_num = 1
      }
      if (error_count <= error_num && error_count!=0) {
        loggerz.info("在指定容错率范围内")
        report.put("result", "容错率内")
      }
      report.asScala.toMap[String, String]
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }


  }
}
