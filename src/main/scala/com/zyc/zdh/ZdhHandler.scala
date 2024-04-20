package com.zyc.zdh

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.zyc.base.util.JsonUtil
import com.zyc.common.SparkBuilder

object ZdhHandler {

  private val threadpool = new ThreadPoolExecutor(
    1, // core pool size
    100, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable]()
  )


  def moreEtl(param: Map[String, Any]): Unit = {

    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_EtlInfo = param.getOrElse("dsi_EtlInfo", List.empty[Map[String, Map[String, Any]]]).asInstanceOf[List[Map[String, Map[String, Any]]]]

    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl任务信息
    val etlMoreTaskInfo = param.getOrElse("etlMoreTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlMoreTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val outPutCols = null

    //清空语句
    val clear = etlMoreTaskInfo.getOrElse("data_sources_clear_output", "").toString

    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandlerMore(spark, task_logs_id, dispatchOptions, dsi_EtlInfo, etlMoreTaskInfo, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
  }

  def sqlEtl(param: Map[String, Any]): Unit = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //etl任务信息
    val sqlTaskInfo = param.getOrElse("sqlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val inputCols: Array[String] = sqlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output

    //过滤条件
    val filter = sqlTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }


    //清空语句
    val clear = sqlTaskInfo.getOrElse("data_sources_clear_output", "").toString


    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandlerSql(spark, task_logs_id, dispatchOptions, sqlTaskInfo, inPut, inPutBaseOptions ++ inputOptions, outPut,
            outPutBaseOptions ++ outputOptions, null, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
  }

  def etl(param: Map[String, Any]): Unit = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //etl任务信息
    val etlTaskInfo = param.getOrElse("etlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    var inputCols: Array[String] = etlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")
    if(etlTaskInfo.getOrElse("data_sources_file_columns", "").equals("")){
      inputCols = etlTaskInfo.getOrElse("data_sources_table_columns", "").toString.split(",")
    }


    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output

    //过滤条件
    val filter = etlTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    //字段映射
    //List(Map(column_alias -> id, column_expr -> id))
    val list_map = etlTaskInfo.getOrElse("column_data_list", null).asInstanceOf[List[Map[String, String]]]
    val outPutCols = list_map.toArray
    //val outPutCols=list_map.map(f=> expr(f.getOrElse("column_expr","")).as(f.getOrElse("column_alias",""))).toArray

    //清空语句
    val clear = etlTaskInfo.getOrElse("data_sources_clear_output", "").toString


    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandler(spark, task_logs_id, dispatchOptions, etlTaskInfo, inPut, inPutBaseOptions ++ inputOptions, filter, inputCols, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
  }


  def droolsEtl(param: Map[String, Any]): Unit ={

    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_EtlInfo = param.getOrElse("dsi_EtlInfo",List.empty[Map[String, Map[String, Any]]]).asInstanceOf[List[Map[String, Map[String, Any]]]]

    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl任务信息
    val etlDroolsTaskInfo = param.getOrElse("etlDroolsTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl-多源任务信息
    val etlMoreTaskInfo = param.getOrElse("etlMoreTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl-sql任务信息
    val sqlTaskInfo = param.getOrElse("sqlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlDroolsTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val outPutCols = null

    //清空语句
    val clear = etlDroolsTaskInfo.getOrElse("data_sources_clear_output", "").toString

    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandlerDrools(spark, task_logs_id, dispatchOptions, dsi_EtlInfo, etlDroolsTaskInfo,etlMoreTaskInfo,sqlTaskInfo, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })

  }

  def apply(param: Map[String, Any]): Unit = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //etl任务信息
    val etlApplyTaskInfo = param.getOrElse("etlApplyTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = etlApplyTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    var inputCols: Array[String] = etlApplyTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")
    if(etlApplyTaskInfo.getOrElse("data_sources_file_columns", "").equals("")){
      inputCols = etlApplyTaskInfo.getOrElse("data_sources_table_columns", "").toString.split(",")
    }


    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output

    //过滤条件
    val filter = etlApplyTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlApplyTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    //字段映射
    //List(Map(column_alias -> id, column_expr -> id))
    val list_map = etlApplyTaskInfo.getOrElse("column_data_list", null).asInstanceOf[List[Map[String, String]]]
    val outPutCols = list_map.toArray
    //val outPutCols=list_map.map(f=> expr(f.getOrElse("column_expr","")).as(f.getOrElse("column_alias",""))).toArray

    //清空语句
    val clear = etlApplyTaskInfo.getOrElse("data_sources_clear_output", "").toString


    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandler(spark, task_logs_id, dispatchOptions, etlApplyTaskInfo, inPut, inPutBaseOptions ++ inputOptions, filter, inputCols, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
  }

  def quality(param: Map[String, Any]): Unit = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //quality任务信息
    val qualityTaskInfo = param.getOrElse("qualityTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = qualityTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    var inputCols: Array[String] = qualityTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")
    if(qualityTaskInfo.getOrElse("data_sources_file_columns", "").equals("")){
      inputCols = qualityTaskInfo.getOrElse("data_sources_table_columns", "").toString.split(",")
    }

    //过滤条件
    val filter = qualityTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //List(Map(column_alias -> id, column_expr -> id))
    val outPutCols=inputCols.map(colName=> Map("column_alias" -> colName, "column_expr" -> colName))

    //清空语句
    val clear = qualityTaskInfo.getOrElse("data_sources_clear_output", "").toString


    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataQualityHandler(spark, task_logs_id, dispatchOptions, qualityTaskInfo, inPut, inPutBaseOptions ++ inputOptions, filter, inputCols, null,
            Map.empty[String,Any], outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
  }

}
