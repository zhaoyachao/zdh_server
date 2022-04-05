package com.zyc.zdh

import java.sql.Timestamp
import java.util

import com.zyc.base.util.{DateUtil, JsonUtil}
import com.zyc.common.MariadbCommon
import com.zyc.zdh.datasources._
import org.apache.log4j.MDC
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.kie.api.KieServices
import org.slf4j.LoggerFactory

object DataSources extends Serializable{

  val logger = LoggerFactory.getLogger(this.getClass)

  val SPARK_ZDH_PROCESS="spark.zdh.process"
  val SPARK_ZDH_LOCAL_PROCESS = "spark.zdh.local.process"
  /**
    * 统一数据源处理入口
    *
    * @param spark
    * @param task_logs_id     任务记录id
    * @param dispatchOption   调度任务信息
    * @param inPut            输入数据源类型
    * @param inputOptions     输入数据源参数
    * @param inputCondition   输入数据源条件
    * @param inputCols        //输入字段
    * @param outPut           输出数据源类型
    * @param outputOptions 输出数据源参数
    * @param outputCols       输出字段
    * @param sql              清空sql 语句
    */
  def DataHandler(spark: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], etlTaskInfo: Map[String, Any], inPut: String, inputOptions: Map[String, Any], inputCondition: String,
                  inputCols: Array[String],
                  outPut: String, outputOptions: Map[String, Any], outputCols: Array[Map[String, String]], sql: String): Unit = {
    implicit val dispatch_task_id = dispatchOption.getOrElse("job_id", "001").toString
    val etl_date = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString;
    val owner = dispatchOption.getOrElse("owner", "001").toString
    val job_context = dispatchOption.getOrElse("job_context", "001").toString
    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)

    MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "23")
    val spark_tmp=spark.newSession()
    spark_tmp.sparkContext.setJobGroup(task_logs_id+"_"+job_context,etlTaskInfo.getOrElse("etl_context",etlTaskInfo.getOrElse("id","").toString).toString+"_"+etl_date+"_"+task_logs_id)
    try {
      logger.info("[数据采集]:数据采集开始")
      logger.info("[数据采集]:数据采集日期:" + etl_date)
      val fileType=etlTaskInfo.getOrElse("file_type_output","csv").toString
      val encoding=etlTaskInfo.getOrElse("encoding_output","utf-8").toString
      val header=etlTaskInfo.getOrElse("header_output","false").toString
      val sep=etlTaskInfo.getOrElse("sep_output",",").toString
      val primary_columns = etlTaskInfo.getOrElse("primary_columns", "").toString

      val merge = etlTaskInfo.getOrElse("merge_output", "-1").toString
      val model = etlTaskInfo.getOrElse("model_output", "append").toString
      val partition_by = etlTaskInfo.getOrElse("partition_by_output", "").toString


      val outputOptions_tmp=outputOptions.asInstanceOf[Map[String,String]].+("fileType"->fileType,"encoding"->encoding,"sep"->sep,
        "header"->header,"merge"-> merge, "model"->model,"partition_by"->partition_by)
      //判断是否有spark conf 参数 spark. 开头的都是conf 配置
      inputOptions.filter(p=>p._1.startsWith("spark.")).asInstanceOf[Map[String,String]].foreach(p=>spark_tmp.conf.set(p._1,p._2))
      spark_tmp.conf.set(SPARK_ZDH_PROCESS,"INPUT")
      val df = inPutHandler(spark_tmp, task_logs_id, dispatchOption, etlTaskInfo, inPut, inputOptions, inputCondition, inputCols, outPut, outputOptions_tmp, outputCols, sql)

      if (!inPut.toString.toLowerCase.equals("kafka") && !inPut.toString.toLowerCase.equals("flume")) {
        //判断是否有spark conf 参数 spark. 开头的都是conf 配置
        outputOptions_tmp.filter(p=>p._1.startsWith("spark.")).foreach(p=>spark_tmp.conf.set(p._1,p._2))
        spark_tmp.conf.set(SPARK_ZDH_PROCESS,"OUTPUT")
        outPutHandler(spark_tmp, df, outPut, outputOptions_tmp, outputCols, sql)
        MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "finish", etl_date, "100")
      } else {
        logger.info("[数据采集]:数据采集检测是实时采集,程序将常驻运行")
      }
      if (outPut.trim.toLowerCase.equals("外部下载")) {
        //获取路径信息
        val root_path = outputOptions_tmp.getOrElse("root_path", "")
        val paths = outputOptions_tmp.getOrElse("paths", "")
        MariadbCommon.insertZdhDownloadInfo(root_path + "/" + paths + ".csv", Timestamp.valueOf(etl_date), owner, job_context)
      }
      logger.info("[数据采集]:数据采集完成")

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[ERROR]:" + ex.getMessage, ex.getCause)
        //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "error", etl_date, "")
        MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOption,etl_date)
      }
    } finally {
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      SparkSession.clearActiveSession()
    }

  }

  /**
    * 多源任务入口
    * @param spark
    * @param task_logs_id
    * @param dispatchOption
    * @param dsi_EtlInfo
    * @param etlMoreTaskInfo
    * @param outPut
    * @param outputOptions
    * @param outputCols
    * @param sql
    */
  def DataHandlerMore(spark: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], dsi_EtlInfo: List[Map[String, Map[String, Any]]],
                      etlMoreTaskInfo: Map[String, Any], outPut: String, outputOptions: Map[String, Any], outputCols: Array[Map[String, String]], sql: String): Unit = {

    implicit val dispatch_task_id = dispatchOption.getOrElse("job_id", "001").toString
    val etl_date = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString;
    val owner = dispatchOption.getOrElse("owner", "001").toString
    val job_context = dispatchOption.getOrElse("job_context", "001").toString
    val drop_tmp_tables=etlMoreTaskInfo.getOrElse("drop_tmp_tables","").toString.trim match {
      case ""=>Array.empty[String]
      case a=>a.split(",")
    }
    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)
    MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "23")
    val spark_tmp=spark.newSession()
    val tables = new util.ArrayList[String]();
    val table_map=new util.HashMap[String,String]()
    try {
      logger.info("[数据采集]:[多源]:数据采集开始")
      logger.info("[数据采集]:[多源]:数据采集日期:" + etl_date)
      val exe_sql = etlMoreTaskInfo.getOrElse("etl_sql", "").toString.replaceAll("\\$zdh_etl_date", "'" + etl_date + "'")
      if (exe_sql.trim.equals("")) {
        //logger.error("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
        throw new Exception("多源任务处理逻辑必须不为空")
      }

      //执行sql
      val exe_sql_ary=exe_sql.trim().split(";\r\n|;\n")

      //生成表名映射
      exe_sql_ary.filter(sql=>sql.startsWith("--table")).foreach(sql=>{
        val tb=sql.drop(7).split("=",2)
        table_map.put(tb(0).trim(),tb(1).trim())
      })

      //多源处理
      dsi_EtlInfo.foreach(f => {

        //调用读取数据源
        //输入数据源信息
        val dsi_Input = f.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
        //输入数据源类型
        val inPut = dsi_Input.getOrElse("data_source_type", "").toString
        val etlTaskInfo = f.getOrElse("etlTaskInfo", Map.empty[String, Any])

        if(!table_map.containsKey(etlTaskInfo.getOrElse("etl_context", "").toString)){
          logger.error("多源任务在SQL开始处必须配置 --table etl任务说明=临时表名; 多个表名必须分多行设置 ")
          throw new Exception("多源任务在SQL开始处必须配置 --table etl任务说明=临时表名; 多个表名必须分多行设置 ")
        }

        spark_tmp.sparkContext.setJobGroup(task_logs_id+"_"+job_context,etlTaskInfo.getOrElse("etl_context",etlTaskInfo.getOrElse("id","").toString).toString+"_"+etl_date+"_"+task_logs_id)
        //参数
        val inputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
          case "" => Map.empty[String, Any]
          case a => JsonUtil.jsonToMap(a)
        }
        //过滤条件
        val filter = etlTaskInfo.getOrElse("data_sources_filter_input", "").toString
        //输入字段
        val inputCols: Array[String] = etlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")
        //输出字段
        val list_map = etlTaskInfo.getOrElse("column_data_list", null).asInstanceOf[List[Map[String, String]]]
        val outPutCols_tmp = list_map.toArray

        //生成table
        //获取表名
//        if (!etlTaskInfo.getOrElse("etl_context", "").toString.contains("#")) {
//          logger.error("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
//          throw new Exception("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
//        }
        if (inPut.toString.toLowerCase.equals("kafka") || inPut.toString.toLowerCase.equals("flume")) {
          logger.error("多源任务对应的单源任务 不支持流数据kafka,flume")
          throw new Exception("多源任务对应的单源任务 不支持流数据kafka,flume")
        }
        spark_tmp.conf.set(SPARK_ZDH_PROCESS,"INPUT")
        val ds = inPutHandler(spark_tmp, task_logs_id, dispatchOption, etlTaskInfo, inPut, dsi_Input ++ inputOptions, filter, inputCols, null, null, outPutCols_tmp, null)


        val tableName = table_map.get(etlTaskInfo.getOrElse("etl_context", "").toString)

        ds.createTempView(tableName)
        tables.add(tableName)
      })


      var result:DataFrame=null
      exe_sql_ary.filter(sql=> !sql.startsWith("--table")).foreach(sql=>{
        if (!sql.trim.equals(""))
          logger.info(sql)
          result = spark_tmp.sql(sql)
      })

      val fileType=etlMoreTaskInfo.getOrElse("file_type_output","csv").toString
      val encoding=etlMoreTaskInfo.getOrElse("encoding_output","utf-8").toString
      val header=etlMoreTaskInfo.getOrElse("header_output","false").toString
      val sep=etlMoreTaskInfo.getOrElse("sep_output",",").toString
      val merge = etlMoreTaskInfo.getOrElse("merge_output", "-1").toString
      val model = etlMoreTaskInfo.getOrElse("model_output", "append").toString
      val partition_by = etlMoreTaskInfo.getOrElse("partition_by_output", "").toString

      val outputOptions_tmp=outputOptions.asInstanceOf[Map[String,String]].+("fileType"->fileType,"encoding"->encoding,"sep"->sep,
        "header"->header,"merge"-> merge, "model"->model,"partition_by"->partition_by)
      //判断是否有spark conf 参数 spark. 开头的都是conf 配置
      outputOptions_tmp.filter(p=>p._1.startsWith("spark.")).foreach(p=>spark_tmp.conf.set(p._1,p._2))
      //写入数据源
      spark_tmp.conf.set(SPARK_ZDH_PROCESS,"OUTPUT")
      outPutHandler(spark_tmp, result, outPut, outputOptions_tmp, null, sql)
      MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "finish", etl_date, "100")
      if (outPut.trim.toLowerCase.equals("外部下载")) {
        //获取路径信息
        val root_path = outputOptions_tmp.getOrElse("root_path", "")
        val paths = outputOptions_tmp.getOrElse("paths", "")
        MariadbCommon.insertZdhDownloadInfo(root_path + "/" + paths + ".csv", Timestamp.valueOf(etl_date), owner, job_context)
      }

      logger.info("[数据采集]:[多源]:数据采集完成")
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        val line=System.getProperty("line.separator")
        val log=ex.getMessage.split(line).mkString(",")
        logger.info("[数据采集]:[多源]:[ERROR]:" +log.trim)
        //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "error", etl_date, "")
        MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOption,etl_date)
      }
    } finally {
      tables.toArray().foreach(table => {
        if (spark.catalog.tableExists(table.toString)) {
          logger.info("[数据采集]:[多源]:任务完成清空临时表:" + table.toString)
          drop_tmp_tables.foreach(table=>{
            spark.sql("drop view if EXISTS "+table).show()
          })
          spark.catalog.dropTempView(table.toString)
        }
      })
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      SparkSession.clearActiveSession()
    }


  }

  /**
    * sql任务入口
    * @param spark
    * @param task_logs_id
    * @param dispatchOption
    * @param sqlTaskInfo
    * @param inPut
    * @param inputOptions
    * @param outPut
    * @param outputOptions
    * @param outputCols
    * @param sql
    */
  def DataHandlerSql(spark: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], sqlTaskInfo: Map[String, Any], inPut: String, inputOptions: Map[String, Any],
                     outPut: String, outputOptions: Map[String, Any], outputCols: Array[Map[String, String]], sql: String): Unit ={

    implicit val dispatch_task_id = dispatchOption.getOrElse("job_id", "001").toString
    val etl_date = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString;
    val owner = dispatchOption.getOrElse("owner", "001").toString
    val job_context = dispatchOption.getOrElse("job_context", "001").toString
    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)
    MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "23")
    val spark_tmp=spark.newSession()
    spark_tmp.sparkContext.setJobGroup(task_logs_id+"_"+job_context,sqlTaskInfo.getOrElse("sql_context",sqlTaskInfo.getOrElse("id","").toString).toString+"_"+etl_date+"_"+task_logs_id)
    try {
      logger.info("[数据采集]:[SQL]:数据采集开始")
      logger.info("[数据采集]:[SQL]:数据采集日期:" + etl_date)
      val etl_sql=sqlTaskInfo.getOrElse("etl_sql","").toString

      logger.info("[数据采集]:[SQL]:"+etl_sql)
      if (etl_sql.trim.equals("")) {
        //logger.error("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
        throw new Exception("SQL任务处理逻辑必须不为空")
      }

      logger.info("[数据采集]:[SQL]:"+etl_sql)
      //判断是否有spark conf 参数 spark. 开头的都是conf 配置
      inputOptions.filter(p=>p._1.startsWith("spark.")).asInstanceOf[Map[String,String]].foreach(p=>spark_tmp.conf.set(p._1,p._2))
      spark_tmp.conf.set(SPARK_ZDH_PROCESS,"INPUT")

      val df=DataWareHouseSources.getDS(spark_tmp,dispatchOption,inPut,inputOptions.asInstanceOf[Map[String,String]],
        null,null,null,outPut,outputOptions.asInstanceOf[Map[String,String]],outputCols,etl_sql)

      val fileType=sqlTaskInfo.getOrElse("file_type_output","csv").toString
      val encoding=sqlTaskInfo.getOrElse("encoding_output","utf-8").toString
      val header=sqlTaskInfo.getOrElse("header_output","false").toString
      val sep=sqlTaskInfo.getOrElse("sep_output",",").toString
      val merge = sqlTaskInfo.getOrElse("merge_output", "-1").toString
      val model = sqlTaskInfo.getOrElse("model_output", "append").toString
      val partition_by = sqlTaskInfo.getOrElse("partition_by_output", "").toString
      val outputOptions_tmp=outputOptions.asInstanceOf[Map[String,String]].+("fileType"->fileType,"encoding"->encoding,"sep"->sep,
        "header"->header,"merge"-> merge, "model"->model,"partition_by"->partition_by)

      //判断是否有spark conf 参数 spark. 开头的都是conf 配置
      outputOptions_tmp.filter(p=>p._1.startsWith("spark.")).foreach(p=>spark_tmp.conf.set(p._1,p._2))
      spark_tmp.conf.set(SPARK_ZDH_PROCESS,"OUTPUT")

      if( outPut!=null && !outPut.toString.trim.equalsIgnoreCase("")){
        outPutHandler(spark_tmp,df,outPut,outputOptions_tmp,outputCols,sql)
      }else{
        logger.info("[数据采集]:[SQL]:无输出数据源")
      }
      MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "finish", etl_date, "100")
      if (outPut.trim.toLowerCase.equals("外部下载")) {
        //获取路径信息
        val root_path = outputOptions.getOrElse("root_path", "")
        val paths = outputOptions.getOrElse("paths", "")
        MariadbCommon.insertZdhDownloadInfo(root_path + "/" + paths + ".csv", Timestamp.valueOf(etl_date), owner, job_context)
      }
      logger.info("[数据采集]:[SQL]:数据采集完成")

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[SQL]:[ERROR]:" + ex.getMessage, ex.getCause)
        //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "error", etl_date, "")
        MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOption,etl_date)
      }
    } finally {
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      SparkSession.clearActiveSession()
    }





  }


  /**
    * drools 任务入口
    * @param spark
    * @param task_logs_id
    * @param dispatchOption
    * @param dsi_EtlInfo
    * @param etlDroolsTaskInfo
    * @param etlMoreTaskInfo
    * @param sqlTaskInfo
    * @param outPut
    * @param outputOptions
    * @param outputCols
    * @param sql
    */
  def DataHandlerDrools(spark: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], dsi_EtlInfo: List[Map[String, Map[String, Any]]],
                      etlDroolsTaskInfo: Map[String, Any],etlMoreTaskInfo: Map[String, Any] ,sqlTaskInfo: Map[String, Any],outPut: String,
                        outputOptions: Map[String, Any], outputCols: Array[Map[String, String]], sql: String): Unit = {

    implicit val dispatch_task_id = dispatchOption.getOrElse("job_id", "001").toString
    val etl_date = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString;
    val owner = dispatchOption.getOrElse("owner", "001").toString
    val job_context = dispatchOption.getOrElse("job_context", "001").toString

    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)
    MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "23")
    val spark_tmp=spark.newSession()
    import spark_tmp.implicits._
    val tables = new util.ArrayList[String]();
    try {
      logger.info("[数据采集]:[Drools]:数据采集开始")
      logger.info("[数据采集]:[Drools]:数据采集日期:" + etl_date)
      val exe_drools = etlDroolsTaskInfo.getOrElse("etl_drools", "").toString.replaceAll("\\$zdh_etl_date", "'" + etl_date + "'")
      if (exe_drools.trim.equals("")) {
        //logger.error("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
        throw new Exception("Drools任务处理逻辑必须不为空")
      }
      logger.info("[数据采集]:[Drools]:数据处理规则:\n"+exe_drools.replaceAll("\"", ""))

      val filter_drools=etlDroolsTaskInfo.getOrElse("data_sources_filter_input", "").toString

      var ds:DataFrame=null
      spark_tmp.conf.set(SPARK_ZDH_PROCESS,"INPUT")
      if(etlDroolsTaskInfo.getOrElse("more_task","").toString.equalsIgnoreCase("单源ETL")){
        //调用读取数据源
        //输入数据源信息
        val dsi_Input = dsi_EtlInfo(0).getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
        //输入数据源类型
        val inPut = dsi_Input.getOrElse("data_source_type", "").toString
        val etlTaskInfo = dsi_EtlInfo(0).getOrElse("etlTaskInfo", Map.empty[String, Any])

        spark_tmp.sparkContext.setJobGroup(task_logs_id+"_"+job_context,etlTaskInfo.getOrElse("etl_context",etlTaskInfo.getOrElse("id","").toString).toString+"_"+etl_date+"_"+task_logs_id)
        //参数
        val inputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
          case "" => Map.empty[String, Any]
          case a => JsonUtil.jsonToMap(a)
        }
        //过滤条件
        val filter = etlTaskInfo.getOrElse("data_sources_filter_input", "").toString
        //输入字段
        val inputCols: Array[String] = etlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")
        //输出字段
        val list_map = etlTaskInfo.getOrElse("column_data_list", null).asInstanceOf[List[Map[String, String]]]
        val outPutCols_tmp = list_map.toArray

        if (inPut.toString.toLowerCase.equals("kafka") || inPut.toString.toLowerCase.equals("flume")) {
          logger.error("Drools任务对应的单源任务 不支持流数据kafka,flume")
          throw new Exception("Drools任务对应的单源任务 不支持流数据kafka,flume")
        }
        ds = inPutHandler(spark_tmp, task_logs_id, dispatchOption, etlTaskInfo, inPut, dsi_Input ++ inputOptions, filter, inputCols, null, null, outPutCols_tmp, null)
      }else if(etlDroolsTaskInfo.getOrElse("more_task","").toString.equalsIgnoreCase("多源ETL")){
        ds=DataHandlerMore2(spark_tmp,task_logs_id,dispatchOption,dsi_EtlInfo,etlMoreTaskInfo,"")(dispatch_task_id)
      }else if(etlDroolsTaskInfo.getOrElse("more_task","").toString.equalsIgnoreCase("SQL")){
        val inputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
          case "" => Map.empty[String, Any]
          case a => JsonUtil.jsonToMap(a)
        }
        ds=DataHandlerSql2(spark_tmp,task_logs_id,dispatchOption,sqlTaskInfo,null,inputOptions,sql)(dispatch_task_id)
      }


      //执行drools 逻辑

      var result:DataFrame=null
      val columns=ds.columns

      val keyColumns=array(columns.map(lit(_)):_*)
      val valueColumns=array(columns.map(col(_)):_*)
      val ds_result=ds.select(map_from_arrays(keyColumns,valueColumns) as "map")


      import  scala.collection.JavaConverters._


      implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[java.util.HashMap[String, String]]


      val tmp_ds=ds_result.mapPartitions(ms=>{
        val kieServices = KieServices.Factory.get();
        val kfs = kieServices.newKieFileSystem();
        kfs.write("src/main/resources/rules/"+job_context+"_"+etl_date+".drl", exe_drools.split("\r\n").mkString("\n").getBytes());
        val kieBuilder = kieServices.newKieBuilder(kfs).buildAll();
        val results = kieBuilder.getResults();
        if (results.hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
          System.out.println(results.getMessages());
          throw new IllegalStateException("errors");
        }
        val kieContainer = kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());
        val kieBase = kieContainer.getKieBase();
        val ksession = kieBase.newKieSession()

        val rs=ms.map(f=>{
          val map=f.getAs[Map[String,String]](0)
          val m=new java.util.HashMap[String,String]()
          map.foreach(a=>m.put(a._1,a._2))
          ksession.insert(m)
          ksession.fireAllRules()
          m.asScala.toMap
        })
        rs
      }).toDF("map").select(to_json(col("map")) as "map").as[String]

      result=spark.read.json(tmp_ds)
      if(!filter_drools.equals("")){
        result=result.filter(filter_drools)
      }

      val fileType=etlDroolsTaskInfo.getOrElse("file_type_output","csv").toString
      val encoding=etlDroolsTaskInfo.getOrElse("encoding_output","utf-8").toString
      val header=etlDroolsTaskInfo.getOrElse("header_output","false").toString
      val sep=etlDroolsTaskInfo.getOrElse("sep_output",",").toString
      val merge = sqlTaskInfo.getOrElse("merge_output", "-1").toString
      val model = sqlTaskInfo.getOrElse("model_output", "append").toString
      val partition_by = sqlTaskInfo.getOrElse("partition_by_output", "").toString
      val outputOptions_tmp=outputOptions.asInstanceOf[Map[String,String]].+("fileType"->fileType,"encoding"->encoding,"sep"->sep,
        "header"->header,"merge"-> merge, "model"->model,"partition_by"->partition_by)

      //判断是否有spark conf 参数 spark. 开头的都是conf 配置
      outputOptions_tmp.filter(p=>p._1.startsWith("spark.")).foreach(p=>spark_tmp.conf.set(p._1,p._2))
      //写入数据源
      spark_tmp.conf.set(SPARK_ZDH_PROCESS,"OUTPUT")
      outPutHandler(spark_tmp, result, outPut, outputOptions_tmp, null, sql)
      MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "finish", etl_date, "100")
      if (outPut.trim.toLowerCase.equals("外部下载")) {
        //获取路径信息
        val root_path = outputOptions_tmp.getOrElse("root_path", "")
        val paths = outputOptions_tmp.getOrElse("paths", "")
        MariadbCommon.insertZdhDownloadInfo(root_path + "/" + paths + ".csv", Timestamp.valueOf(etl_date), owner, job_context)
      }

      logger.info("[数据采集]:[Drools]:数据采集完成")
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        val line=System.getProperty("line.separator")
        val log=ex.getMessage.split(line).mkString(",")
        logger.info("[数据采集]:[Drools]:[ERROR]:" +log.trim)
        //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "error", etl_date, "")
        MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOption,etl_date)
      }
    } finally {
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      SparkSession.clearActiveSession()
    }


  }


  /**
    * drools 任务调用多源任务处理
    * @param spark
    * @param task_logs_id
    * @param dispatchOption
    * @param dsi_EtlInfo
    * @param etlMoreTaskInfo
    * @param sql
    * @param dispatch_task_id
    * @return
    */
  def DataHandlerMore2(spark: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], dsi_EtlInfo: List[Map[String, Map[String, Any]]],
                      etlMoreTaskInfo: Map[String, Any], sql: String)(implicit dispatch_task_id:String): DataFrame = {

    val etl_date = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString;
    val owner = dispatchOption.getOrElse("owner", "001").toString
    val job_context = dispatchOption.getOrElse("job_context", "001").toString
    val drop_tmp_tables=etlMoreTaskInfo.getOrElse("drop_tmp_tables","").toString.trim match {
      case ""=>Array.empty[String]
      case a=>a.split(",")
    }
    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)
    val spark_tmp=spark.newSession()
    val tables = new util.ArrayList[String]();
    try {
      logger.info("[数据采集]:[Drool]:[多源解析]:数据采集开始")
      logger.info("[数据采集]:[Drool]:[多源解析]:数据采集日期:" + etl_date)
      val exe_sql = etlMoreTaskInfo.getOrElse("etl_sql", "").toString.replaceAll("\\$zdh_etl_date", "'" + etl_date + "'")
      if (exe_sql.trim.equals("")) {
        //logger.error("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
        throw new Exception("多源任务处理逻辑必须不为空")
      }

      //多源处理
      dsi_EtlInfo.foreach(f => {

        //调用读取数据源
        //输入数据源信息
        val dsi_Input = f.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
        //输入数据源类型
        val inPut = dsi_Input.getOrElse("data_source_type", "").toString
        val etlTaskInfo = f.getOrElse("etlTaskInfo", Map.empty[String, Any])

        spark_tmp.sparkContext.setJobGroup(task_logs_id+"_"+job_context,etlTaskInfo.getOrElse("etl_context",etlTaskInfo.getOrElse("id","").toString).toString+"_"+etl_date+"_"+task_logs_id)
        //参数
        val inputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
          case "" => Map.empty[String, Any]
          case a => JsonUtil.jsonToMap(a)
        }
        //过滤条件
        val filter = etlTaskInfo.getOrElse("data_sources_filter_input", "").toString
        //输入字段
        val inputCols: Array[String] = etlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")
        //输出字段
        val list_map = etlTaskInfo.getOrElse("column_data_list", null).asInstanceOf[List[Map[String, String]]]
        val outPutCols_tmp = list_map.toArray

        //生成table
        //获取表名
        if (!etlTaskInfo.getOrElse("etl_context", "").toString.contains("#")) {
          logger.error("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
          throw new Exception("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
        }
        if (inPut.toString.toLowerCase.equals("kafka") || inPut.toString.toLowerCase.equals("flume")) {
          logger.error("多源任务对应的单源任务 不支持流数据kafka,flume")
          throw new Exception("多源任务对应的单源任务 不支持流数据kafka,flume")
        }

        val ds = inPutHandler(spark_tmp, task_logs_id, dispatchOption, etlTaskInfo, inPut, dsi_Input ++ inputOptions, filter, inputCols, null, null, outPutCols_tmp, null)

        val tableName = etlTaskInfo.getOrElse("etl_context", "").toString.split("#")(1)
        ds.createTempView(tableName)
        tables.add(tableName)
      })

      //执行sql
      val exe_sql_ary=exe_sql.split(";\r\n|;\n")
      var result:DataFrame=null
      exe_sql_ary.foreach(sql=>{
        if (!sql.trim.equals(""))
          result = spark_tmp.sql(sql)
      })

      return result
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        val line=System.getProperty("line.separator")
        val log=ex.getMessage.split(line).mkString(",")
        logger.info("[数据采集]:[Drool]:[多源解析]:[ERROR]:" +log.trim)
        //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "error", etl_date, "")
        throw ex
      }
    } finally {
      tables.toArray().foreach(table => {
        if (spark.catalog.tableExists(table.toString)) {
          logger.info("[数据采集]:[Drool]:[多源解析]:任务完成清空临时表:" + table.toString)
          drop_tmp_tables.foreach(table=>{
            spark.sql("drop view if EXISTS "+table).show()
          })
          spark.catalog.dropTempView(table.toString)
        }
      })

      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      SparkSession.clearActiveSession()
    }


  }


  /**
    * drools 任务调用sql任务处理
    * @param spark
    * @param task_logs_id
    * @param dispatchOption
    * @param sqlTaskInfo
    * @param inPut
    * @param inputOptions
    * @param sql
    * @param dispatch_task_id
    * @return
    */
  def DataHandlerSql2(spark: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], sqlTaskInfo: Map[String, Any], inPut: String, inputOptions: Map[String, Any],
                      sql: String)(implicit  dispatch_task_id:String): DataFrame ={

    val etl_date = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString;
    val owner = dispatchOption.getOrElse("owner", "001").toString
    val job_context = dispatchOption.getOrElse("job_context", "001").toString
    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)
    val spark_tmp=spark.newSession()
    spark_tmp.sparkContext.setJobGroup(task_logs_id+"_"+job_context,sqlTaskInfo.getOrElse("sql_context",sqlTaskInfo.getOrElse("id","").toString).toString+"_"+etl_date+"_"+task_logs_id)
    try {
      logger.info("[数据采集]:[SQL]:数据采集开始")
      logger.info("[数据采集]:[SQL]:数据采集日期:" + etl_date)
      val etl_sql=sqlTaskInfo.getOrElse("etl_sql","").toString

      logger.info("[数据采集]:[SQL]:"+etl_sql)
      if (etl_sql.trim.equals("")) {
        //logger.error("多源任务对应的单源任务说明必须包含# 格式 'etl任务说明#临时表名'")
        throw new Exception("SQL任务处理逻辑必须不为空")
      }

      logger.info("[数据采集]:[SQL]:"+etl_sql)
      //判断是否有spark conf 参数 spark. 开头的都是conf 配置
      inputOptions.filter(p=>p._1.startsWith("spark.")).asInstanceOf[Map[String,String]].foreach(p=>spark_tmp.conf.set(p._1,p._2))

      val df=DataWareHouseSources.getDS(spark_tmp,dispatchOption,inPut,inputOptions.asInstanceOf[Map[String,String]],
        null,null,null,null,null,null,etl_sql)

      return df
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[SQL]:[ERROR]:" + ex.getMessage, ex.getCause)
        //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "error", etl_date, "")
       throw ex
      }
    } finally {

      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      SparkSession.clearActiveSession()
    }

  }

  def DataQualityHandler(spark: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], qualityTaskInfo: Map[String, Any], inPut: String, inputOptions: Map[String, Any], inputCondition: String,
                  inputCols: Array[String],
                  outPut: String, outputOptions: Map[String, Any], outputCols: Array[Map[String, String]], sql: String): Unit = {
    implicit val dispatch_task_id = dispatchOption.getOrElse("job_id", "001").toString
    val etl_date = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString
    val owner = dispatchOption.getOrElse("owner", "001").toString
    val job_context = dispatchOption.getOrElse("job_context", "001").toString
    MDC.put("job_id", dispatch_task_id)
    MDC.put("task_logs_id",task_logs_id)

    MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "23")
    val spark_tmp=spark.newSession()
    val quality_context = qualityTaskInfo.getOrElse("quality_context","").toString

    spark_tmp.sparkContext.setJobGroup(task_logs_id+"_"+job_context,qualityTaskInfo.getOrElse("quality_context",qualityTaskInfo.getOrElse("id","").toString).toString+"_"+etl_date+"_"+task_logs_id)
    try {
      logger.info("[数据质量检测]:数据采集开始")
      logger.info("[数据质量检测]:数据采集日期:" + etl_date)
      val fileType=qualityTaskInfo.getOrElse("file_type_output","csv").toString
      val encoding=qualityTaskInfo.getOrElse("encoding_output","utf-8").toString
      val header=qualityTaskInfo.getOrElse("header_output","false").toString
      val sep=qualityTaskInfo.getOrElse("sep_output",",").toString
      val primary_columns = qualityTaskInfo.getOrElse("primary_columns", "").toString

      val merge = qualityTaskInfo.getOrElse("merge_output", "-1").toString
      val model = qualityTaskInfo.getOrElse("model_output", "append").toString
      val partition_by = qualityTaskInfo.getOrElse("partition_by_output", "").toString


      //判断是否有spark conf 参数 spark. 开头的都是conf 配置
      inputOptions.filter(p=>p._1.startsWith("spark.")).asInstanceOf[Map[String,String]].foreach(p=>spark_tmp.conf.set(p._1,p._2))
      spark_tmp.conf.set(SPARK_ZDH_PROCESS,"INPUT")
      val df = inPutHandler(spark_tmp, task_logs_id, dispatchOption, qualityTaskInfo, inPut, inputOptions, inputCondition, inputCols, outPut, outputOptions, outputCols, sql)

      //解析数据检测规则
      val quality_rule_config_str:String = qualityTaskInfo.getOrElse("quality_rule_config", "").toString

      val quality_rule_config = JsonUtil.jsonToList[Map[String,Any]](quality_rule_config_str).toArray

      //
      val regex_error_regex = QualityDataSources.dataRegexQuality(spark_tmp,df, quality_rule_config)
      val regex_error_sql = QualityDataSources.dataSqlQuality(spark_tmp,df, quality_rule_config)
      val regex_error_zdh = QualityDataSources.dataZdhQuality(spark_tmp,df, quality_rule_config)
      var report = (regex_error_regex.++:(regex_error_sql).++:(regex_error_zdh)).zipWithIndex.map(p=>{
        (p._2,s"指标:${p._1._1} ,在${p._1._3}规则下,有${p._1._4}条数据不满足")
      }).toMap.asInstanceOf[Map[String,String]]

      if((regex_error_regex.++:(regex_error_sql).++:(regex_error_zdh)).size>0){
        report=report.+(("result", "不通过"))
      }else{
        report=report.+(("result", "通过"))
      }


      MariadbCommon.insertQuality(task_logs_id, dispatch_task_id,job_context, qualityTaskInfo.getOrElse("id","").toString,quality_context, etl_date, report, owner)

      MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "finish", etl_date, "100")

      logger.info("[数据质量检测]:数据质量检测完成")

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据质量检测]:[ERROR]:" + ex.getMessage, ex.getCause)
        //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "error", etl_date, "")
        MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOption,etl_date)
      }
    } finally {
      MDC.remove("job_id")
      MDC.remove("task_logs_id")
      SparkSession.clearActiveSession()
    }

  }


  /**
    * 读取数据源handler
    *
    * @param spark
    * @param dispatchOption
    * @param inPut
    * @param inputOptions
    * @param inputCondition
    * @param inputCols
    * @param outPut
    * @param outputOptionions
    * @param outputCols
    * @param sql
    * @param dispatch_task_id
    */
  def inPutHandler(spark: SparkSession, task_logs_id: String, dispatchOption: Map[String, Any], etlTaskInfo: Map[String, Any], inPut: String, inputOptions: Map[String, Any], inputCondition: String,
                   inputCols: Array[String],
                   outPut: String, outputOptionions: Map[String, Any], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    //调用对应的数据源
    //调用对应的中间处理层
    logger.info("[数据采集]:[输入]:开始设置spark基础参数:"+inputOptions.filter(p=>p._1.startsWith("spark.")).mkString(","))
    //判断是否有spark conf 参数 spark. 开头的都是conf 配置
    inputOptions.filter(p=>p._1.startsWith("spark.")).asInstanceOf[Map[String,String]].foreach(p=>spark.conf.set(p._1,p._2))

    logger.info("[数据采集]:[输入]:开始匹配输入数据源")

    val etl_date = JsonUtil.jsonToMap(dispatchOption.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString;
    val etl_task_id = etlTaskInfo.getOrElse("id", "001").toString
    val context = etlTaskInfo.getOrElse("etl_context", "").toString
    val job_context = dispatchOption.getOrElse("job_context", "").toString
    val owner = dispatchOption.getOrElse("owner", "001").toString
    val error_rate = etlTaskInfo.getOrElse("error_rate", "0.01").toString match {
      case ""=>"0.01"
      case er=>er
    }
    val enable_quality = etlTaskInfo.getOrElse("enable_quality", "off").toString
    val duplicate_columns=etlTaskInfo.getOrElse("duplicate_columns", "").toString.trim match {
      case ""=>Array.empty[String]
      case a=>a.split(",")
    }
    val fileType=etlTaskInfo.getOrElse("file_type_input","csv").toString
    val encoding=etlTaskInfo.getOrElse("encoding_input","utf-8").toString
    val header=etlTaskInfo.getOrElse("header_input","false").toString
    val sep=etlTaskInfo.getOrElse("sep_input",",").toString
    val repartition_num = etlTaskInfo.getOrElse("repartition_num_input", "").toString
    val repartition_cols = etlTaskInfo.getOrElse("repartition_cols_input", "").toString

    val inputOptions_tmp=inputOptions.asInstanceOf[Map[String,String]].+("fileType"->fileType,"encoding"->encoding,"sep"->sep,
      "header"->header,"repartition_num"->repartition_num, "repartition_cols"->repartition_cols)
    val zdhDataSources: ZdhDataSources = inPut.toString.toLowerCase match {
      case "jdbc" => JdbcDataSources
      case "hdfs" => HdfsDataSources
      case "hive" => HiveDataSources
      //hbase 数据源不能直接使用expr 表达式
      case "hbase" => HbaseDataSources
      case "es" => ESDataSources
      case "mongodb" => MongoDBDataSources
      case "kafka" => KafKaDataSources
      case "http" => HttpDataSources
      case "redis" => RedisDataSources
      case "cassandra" => CassandraDataSources
      case "sftp" => SFtpDataSources
      case "kudu" => KuduDataSources
      case "外部上传" => LocalDataSources
      case "flume" => FlumeDataSources
      case "外部下载" => throw new Exception("[数据采集]:[输入]:[外部下载]只能作为输出数据源:")
      case "greenplum"=> GreenplumDataSources
      case "tidb" => TidbDataSources
      case "iceberg" => IcebergDataSources
      case "ftp" => FtpDataSources
      case _ => throw new Exception("数据源类型无法匹配")
    }
    var outputCols_expr: Array[Column] = null
    if (!inPut.toLowerCase.equals("hbase")) {
      outputCols_expr = outputCols.map(f => {
        if (f.getOrElse("column_alias", "").toLowerCase.equals("row_key")) {
          expr(f.getOrElse("column_expr", "")).cast("string").as(f.getOrElse("column_alias", ""))
        } else {
          if (!f.getOrElse("column_type", "").trim.equals("")) {
            //类型转换
            if (f.getOrElse("column_expr", "").contains("$zdh_etl_date")) {
              expr(f.getOrElse("column_expr", "").replaceAll("\\$zdh_etl_date", "'" + etl_date + "'")).cast(f.getOrElse("column_type", "string")).as(f.getOrElse("column_alias", ""))
            } else {
              expr(f.getOrElse("column_expr", "")).cast(f.getOrElse("column_type", "string")).as(f.getOrElse("column_alias", ""))
            }
          } else {
            //默认类型
            if (f.getOrElse("column_expr", "").contains("$zdh_etl_date")) {
              expr(f.getOrElse("column_expr", "").replaceAll("\\$zdh_etl_date", "'" + etl_date + "'")).as(f.getOrElse("column_alias", ""))
            } else {
              expr(f.getOrElse("column_expr", "")).as(f.getOrElse("column_alias", ""))
            }
          }

        }
      })
    }
    if(outputCols_expr==null){
      outputCols_expr=Array.empty[Column]
    }

    val primary_columns = etlTaskInfo.getOrElse("primary_columns", "").toString
    val column_size = etlTaskInfo.getOrElse("column_size", "").toString match {
      case ""=>0
      case cs=>cs.toInt
    }
    val rows_range = etlTaskInfo.getOrElse("rows_range", "").toString

    var column_is_null: Seq[Column] = Seq.empty[Column]
    var column_length: Seq[Column] = Seq.empty[Column]
    var column_regex: Seq[Column] = Seq.empty[Column]

    val zdh_regex=udf((c1:String,re:String)=>{
      val a=re.r()
      c1.matches(a.toString())
    })

    logger.info("开始加载ETL任务转换信息,检测元数据是否合规")
    outputCols.foreach(f => {
      if (f.getOrElse("column_is_null", "true").trim.equals("false")) {
        if (f.getOrElse("column_name", "").trim.equals("")) {
          throw new Exception("字段是否为空检测,需要的原始字段列名为空")
        }
        val c1 = col(f.getOrElse("column_name", "").trim).isNull
        column_is_null = column_is_null.:+(c1)

      }
      if (!f.getOrElse("column_regex", "").trim.equals("")) {
        if (f.getOrElse("column_name", "").trim.equals("")) {
          throw new Exception("字段是否为空检测,需要的原始字段列名为空")
        }
        val c1=zdh_regex(col(f.getOrElse("column_name", "").trim),lit(f.getOrElse("column_regex", "").trim))
        column_regex = column_regex.:+(c1)
      }

      if (!f.getOrElse("column_size", "").trim.equals("")) {

      }

      if (!f.getOrElse("column_length", "").trim.equals("")) {
        if (f.getOrElse("column_name", "").trim.equals("")) {
          throw new Exception("字段长度质量检测,需要的原始字段列名为空")
        }
        val c1 = length(col(f.getOrElse("column_name", "").trim)) =!= f.getOrElse("column_length", "").trim
        column_length = column_length.:+(c1)
      }

    })
    logger.info("完成加载ETL任务转换信息")
    //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "25")
    val df = zdhDataSources.getDS(spark, dispatchOption, inPut, inputOptions_tmp.asInstanceOf[Map[String, String]].+("primary"->""),
      inputCondition, inputCols, duplicate_columns,outPut, outputOptionions.asInstanceOf[Map[String, String]], outputCols, sql)
    //MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "61")

    if (enable_quality.trim.equals("on") && !inPut.equalsIgnoreCase("kafka")) {
      logger.info("任务开启了质量检测,开始进行质量检测")
      val report = zdhDataSources.dataQuality(spark, df, error_rate, primary_columns, column_size, rows_range, column_is_null, column_length,column_regex)

      MariadbCommon.insertQuality(task_logs_id, dispatch_task_id,job_context, etl_task_id,context, etl_date, report, owner)
      if (report.getOrElse("result", "").equals("不通过")) {
        throw new Exception("ETL 任务做质量检测时不通过,具体请查看质量检测报告")
      }
      logger.info("完成质量检测")
    }else{
      logger.info("未开启质量检测,如果想开启,请打开ETL任务中质量检测开关,提示:如果输入数据源是kafka 不支持质量检测")
    }
    val result=zdhDataSources.process(spark, df, outputCols_expr, etl_date)

//    val repartition_num=inputOptions_tmp.getOrElse("repartition_num","").toString
//    val repartition_cols=inputOptions_tmp.getOrElse("repartition_cols","").toString


    if( !repartition_num.equals("")&& !repartition_cols.equals("")){
      logger.info("数据重分区规则,重分区个数:"+repartition_num+",重分区字段:"+repartition_cols)
      return result.repartition(repartition_num.toInt,repartition_cols.split(",").map(col(_)):_*)
    }
    if(repartition_num.equals("")&& !repartition_cols.equals("")){
      logger.info("数据重分区规则,重分区字段:"+repartition_cols+",无分区个数")
      return result.repartition(repartition_cols.split(",").map(col(_)):_*)
    }
    if(!repartition_num.equals("")&& repartition_cols.equals("")){
      logger.info("数据重分区规则,重分区个数:"+repartition_num+",无分区字段")
      return result.repartition(repartition_num.toInt)
    }

    result
  }


  /**
    * 输出数据源处理
    *
    * @param spark
    * @param df
    * @param outPut
    * @param outputOptionions
    * @param outputCols
    * @param sql
    * @param dispatch_task_id
    */
  def outPutHandler(spark: SparkSession, df: DataFrame,
                    outPut: String, outputOptionions: Map[String, Any], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): Unit = {
    try {
      import spark.implicits._
      logger.info("[数据采集]:[输出]:开始设置spark基础参数:"+outputOptionions.filter(p=>p._1.startsWith("spark.")).mkString(","))
      //判断是否有spark conf 参数 spark. 开头的都是conf 配置
      outputOptionions.filter(p=>p._1.startsWith("spark.")).asInstanceOf[Map[String,String]].foreach(p=>spark.conf.set(p._1,p._2))

      logger.info("[数据采集]:[输出]:开始匹配输出数据源")
      //调用写入数据源
      val zdhDataSources: ZdhDataSources = outPut.toString.toLowerCase match {
        case "jdbc" => {
          logger.info("[数据采集]:[输出]:输出源为[JDBC]")
          JdbcDataSources
        }
        case "hive" => {
          logger.info("[数据采集]:[输出]:输出源为[HIVE]")
          HiveDataSources
        }
        case "hdfs" => {
          logger.info("[数据采集]:[输出]:输出源为[HDFS]")
          HdfsDataSources
        }
        case "hbase" => {
          logger.info("[数据采集]:[输出]:输出源为[HBASE]")
          HbaseDataSources
        }
        case "es" => {
          logger.info("[数据采集]:[输出]:输出源为[ES]")
          ESDataSources
        }
        case "mongodb" => {
          logger.info("[数据采集]:[输出]:输出源为[MONGODB]")
          MongoDBDataSources
        }
        case "kafka" => {
          logger.info("[数据采集]:[输出]:输出源为[KAFKA]")
          KafKaDataSources
        }
        case "redis" => {
          logger.info("[数据采集]:[输出]:输出源为[REDIS]")
          RedisDataSources
        }
        case "cassandra" => {
          logger.info("[数据采集]:[输出]:输出源为[CASSANDRA]")
          CassandraDataSources
        }
        case "sftp" => {
          logger.info("[数据采集]:[输出]:输出源为[SFTP]")
          SFtpDataSources
        }
        case "kudu" => {
          logger.info("[数据采集]:[输出]:输出源为[KUDU]")
          KuduDataSources
        }
        case "外部上传" => throw new Exception("[数据采集]:[输出]:[外部上传]只能作为输入数据源:")
        case "外部下载" => {
          logger.info("[数据采集]:[输出]:输出源为[外部下载]")
          DownDataSources
        }
        case "greenplum" => {
          logger.info("[数据采集]:[输出]:输出源为[Greenplum]")
          GreenplumDataSources
        }
        case "tidb" =>{
          logger.info("[数据采集]:[输出]:输出源为[TIDB]")
          TidbDataSources
        }
        case "iceberg" => {
          logger.info("[数据采集]:[输出]:输出源为[ICEBERG]")
          IcebergDataSources
        }
        case "ftp" => {
          logger.info("[数据采集]:[输出]:输出源为[FTP]")
          FtpDataSources
        }
        case x => throw new Exception("[数据采集]:[输出]:无法识别输出数据源:" + x)
      }

      zdhDataSources.writeDS(spark, df, outputOptionions.asInstanceOf[Map[String, String]], sql)

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[输出]:[ERROR]:" + ex.getMessage)
        throw ex
      }
    }

  }


}
