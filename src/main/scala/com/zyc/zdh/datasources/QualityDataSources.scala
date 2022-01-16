package com.zyc.zdh.datasources

import java.io.{BufferedWriter, OutputStreamWriter}
import java.net.URI

import com.zyc.common.MariadbCommon
import com.zyc.zdh.ZdhDataSources
import oracle.net.aso.e
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory

object QualityDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  val SQL_FUNCTION = "SQL函数校验"
  val REGEX_FUNCTION = "正则规则校验"
  val ZDH_FUNCTION = "ZDH内置校验"

  override def dataQuality(spark: SparkSession, df: DataFrame, error_rate: String, primary_columns: String, column_size: Int, rows_range: String, column_is_null: Seq[Column], column_length: Seq[Column], column_regex: Seq[Column]): Map[String, String] = {
    super.dataQuality(spark, df, error_rate, primary_columns, column_size, rows_range, column_is_null, column_length, column_regex)


  }

  /**
    * 正则规则校验
    * @param spark
    * @param df
    * @param quality_rule_config
    */
  def dataRegexQuality(spark: SparkSession, df: DataFrame,quality_rule_config:Array[Map[String,Any]])(implicit dispatch_task_id: String): Seq[(String,String,String,Long)] ={
    try{
      logger.info(s"[数据质量检测]:[${REGEX_FUNCTION}]:开始")
      //规则code,规则名称映射关系
      var rule_code_name_map = new java.util.HashMap[String,String]()
      var column_regex: Seq[(String,String)] = Seq.empty[(String,String)]
      //解析正则规则
      quality_rule_config.filter(p=>p.getOrElse("rule_type","").toString.equalsIgnoreCase("2")).foreach(p=>{
        var rule_expr = p.getOrElse("rule_expr","").toString.replace("\\","\\\\")
        var rule_code = p.getOrElse("rule_code","").toString
        rule_code_name_map.put(rule_code, p.getOrElse("rule_name","").toString)
        //格式(ColumnExpr, ColumnName)
        val regex_column_tuple:Seq[(String,String)] = p.getOrElse("quality_columns","").toString.split(",")
          .zipWithIndex
          .map(colExpr=>(s"sum(if(${colExpr._1} rlike '$rule_expr',0,1)) as ${rule_code}__${colExpr._1}__${colExpr._2}", s"${rule_code}__${colExpr._1}__${colExpr._2}"))
        column_regex = column_regex.++:(regex_column_tuple)
      })
      if(column_regex.isEmpty){
        logger.info(s"[数据质量检测]:[${REGEX_FUNCTION}]:未找到正则规则,结束")
        return Seq.empty[(String,String,String,Long)]
      }
      logger.info(s"[数据质量检测]:[${REGEX_FUNCTION}]:${column_regex.map(_._1).mkString(",")}")
      val result = df.selectExpr(column_regex.map(_._1):_*).take(1)(0)

      val result_columns = column_regex.map(_._2)


      //Seq[(columnName,rule_code, rule_name,count)]
      var regex_error: Seq[(String,String,String,Long)] = Seq.empty[(String,String,String,Long)]
      //根据名称校验指标是否满足
      result_columns.foreach(colName=>{
        val count = result.getAs[Long](colName)
        //大于0表示,不满足正则
        if(count>0){
          regex_error = regex_error.+:(colName.split("__")(1),colName.split("__")(0),rule_code_name_map.get(colName.split("__")(0)),count)
        }
      })
      if(!regex_error.isEmpty){
        regex_error.foreach(f=>{
          logger.info(s"指标:${f._1} ,在${f._3}规则下,有${f._4}条数据不满足")
        })
        //MariadbCommon.insertQuality(task_logs_id, dispatch_task_id, etl_task_id, etl_date, report, owner)
      }
      logger.info(s"[数据质量检测]:[${REGEX_FUNCTION}]:完成")
      return regex_error
    }catch {
      case e:Exception=>{
        logger.error(e.getMessage)
        throw e
      }
    }



  }

  def dataSqlQuality(spark: SparkSession, df: DataFrame,quality_rule_config:Array[Map[String,Any]])(implicit dispatch_task_id: String): Seq[(String,String,String,Long)] ={
    try{
      logger.info(s"[数据质量检测]:[${SQL_FUNCTION}]:开始")
      //规则code,规则名称映射关系
      var rule_code_name_map = new java.util.HashMap[String,String]()
      var column_regex: Seq[(String,String)] = Seq.empty[(String,String)]
      //解析SQL规则
      quality_rule_config.filter(p=>p.getOrElse("rule_type","").toString.equalsIgnoreCase("1")).foreach(p=>{
        var rule_expr = p.getOrElse("rule_expr","").toString
        var rule_code = p.getOrElse("rule_code","").toString
        rule_code_name_map.put(rule_code, p.getOrElse("rule_name","").toString)
        //格式(ColumnExpr, ColumnName)
        val regex_column_tuple:Seq[(String,String)] = p.getOrElse("quality_columns","").toString.split(",")
          .zipWithIndex
          .map(p=>{
            (rule_expr.replace("$column", p._1),p._1,p._2)
          })
          .map(colExpr=>(s"sum(if(${colExpr._1},0,1)) as ${rule_code}__${colExpr._2}__${colExpr._3}", s"${rule_code}__${colExpr._2}__${colExpr._3}"))
        column_regex = column_regex.++:(regex_column_tuple)
      })
      if(column_regex.isEmpty){
        logger.info(s"[数据质量检测]:[${SQL_FUNCTION}]:未找到SQL规则,结束")
        return Seq.empty[(String,String,String,Long)]
      }
      logger.info(s"[数据质量检测]:[${SQL_FUNCTION}]:${column_regex.map(_._1).mkString(",")}")
      val result = df.selectExpr(column_regex.map(_._1):_*).take(1)(0)

      val result_columns = column_regex.map(_._2)


      //Seq[(columnName,rule_code, rule_name,count)]
      var regex_error: Seq[(String,String,String,Long)] = Seq.empty[(String,String,String,Long)]
      //根据名称校验指标是否满足
      result_columns.foreach(colName=>{
        val count = result.getAs[Long](colName)
        //大于0表示,不满足正则
        if(count>0){
          regex_error = regex_error.+:(colName.split("__")(1),colName.split("__")(0),rule_code_name_map.get(colName.split("__")(0)),count)
        }
      })
      if(!regex_error.isEmpty){
        regex_error.foreach(f=>{
          logger.info(s"指标:${f._1} ,在${f._3}规则下,有${f._4}条数据不满足")
        })
        //MariadbCommon.insertQuality(task_logs_id, dispatch_task_id, etl_task_id, etl_date, report, owner)
      }
      logger.info(s"[数据质量检测]:[${SQL_FUNCTION}]:完成")
      return regex_error
    }catch {
      case e:Exception=>{
        logger.error(e.getMessage)
        throw e
      }
    }



  }

  def dataZdhQuality(spark: SparkSession, df: DataFrame,quality_rule_config:Array[Map[String,Any]])(implicit dispatch_task_id: String): Seq[(String,String,String,Long)] ={
    try{

      import spark.implicits._
      logger.info(s"[数据质量检测]:[${ZDH_FUNCTION}]:开始")
      logger.info(s"[数据质量检测]:[${ZDH_FUNCTION}]:目前支持以下几种,1 primark_key_check: 主键是否重复校验")
      //规则code,规则名称映射关系
      var rule_code_name_map = new java.util.HashMap[String,String]()
      var column_regex: Seq[(String,String)] = Seq.empty[(String,String)]
      //解析正则规则
      quality_rule_config.filter(p=>p.getOrElse("rule_type","").toString.equalsIgnoreCase("3")).zipWithIndex.foreach(p=> {
        var rule_expr = p._1.getOrElse("rule_expr", "").toString
        if (!rule_expr.equalsIgnoreCase("primary_key_check")) {
          throw new Exception(s"${ZDH_FUNCTION},目前不支持${rule_expr}表达式")
        }
        var rule_code = p._1.getOrElse("rule_code", "").toString
        rule_code_name_map.put(rule_code, p._1.getOrElse("rule_name", "").toString)
        //格式(ColumnExpr, ColumnName)
        val newColName = p._1.getOrElse("quality_columns", "").toString.split(",").mkString("_")
        val concat_ws_name = p._1.getOrElse("quality_columns", "").toString
        val regex_column: String = s"concat_ws('_', ${concat_ws_name}) as primary_key__${newColName}__${p._2}"
        column_regex = column_regex.+:((regex_column, s"primary_key__${p._1.getOrElse("quality_columns", "").toString.split(",").mkString("_")}__${p._2}"))
      })

      if(column_regex.isEmpty){
        logger.info(s"[数据质量检测]:[${ZDH_FUNCTION}]:未找到内置规则,结束")
        return Seq.empty[(String,String,String,Long)]
      }
      logger.info(s"[数据质量检测]:[${ZDH_FUNCTION}]:${column_regex.map(_._1).mkString(",")}")

      //Seq[(columnName,rule_code, rule_name,count)]
      var regex_error: Seq[(String,String,String,Long)] = Seq.empty[(String,String,String,Long)]

      for(i <- 0 until column_regex.size){
        val result = df.selectExpr(column_regex(i)._1)
          .map(f => (f.getAs[String](column_regex(i)._2), 1)).rdd.reduceByKey(_ + _)
        val primary_count = result.filter(f => f._2 > 1).map(_._2).sum().toLong

        if(primary_count>0){
          var colName = column_regex(i)._2
          regex_error = regex_error.+:(colName.split("__")(1),colName.split("__")(0),rule_code_name_map.get(colName.split("__")(0)),primary_count)
        }
      }

      if(!regex_error.isEmpty){
        regex_error.foreach(f=>{
          logger.info(s"指标:${f._1} ,在${f._3}规则下,有${f._4}条数据不满足")
        })
      }
      logger.info(s"[数据质量检测]:[${ZDH_FUNCTION}]:完成")
      return regex_error
    }catch {
      case e:Exception=>{
        logger.error(e.getMessage)
        throw e
      }
    }



  }
}