package com.zyc.zdh.datasources

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

/**
  * 通过配置文件方式读写hive
  */
object HiveDataSources extends ZdhDataSources{

  val logger=LoggerFactory.getLogger(this.getClass)

  override def getSchema(spark: SparkSession, options: Map[String, String])(implicit dispatch_task_id:String): Array[StructField] = {
    logger.info(s"获取hive表的schema信息table:${options.getOrElse("table","")},option:${options.mkString(",")}")
    spark.table(options.getOrElse("table","")).schema.fields
  }

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:输入源为[HIVE]")
      val tableName=inputOptions.getOrElse("tableName","").toString
      if(tableName.trim.equals("")){
        throw new Exception("[zdh],hive数据源读取:tableName为空")
      }

      logger.info("[数据采集]:[HIVE]:[READ]:[table]:"+tableName+"[FILTER]:"+inputCondition)
      val df=spark.table(tableName)
      filter(spark,df,inputCondition,duplicateCols)

    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[HIVE]:[READ]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id:String): DataFrame ={
    try{
      logger.info("[数据采集]:[HIVE]:[SELECT]")
      logger.debug("[数据采集]:[HIVE]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[HIVE]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[HIVE]:[SELECT]:[ERROR]"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

  override def writeDS(spark: SparkSession,df:DataFrame,options: Map[String,String], sql: String="")(implicit dispatch_task_id:String): Unit = {
    try{
      logger.info("[数据采集]:[HIVE]:[WRITE]:[options]:"+options.mkString(","))

      //默认是append
      val model=options.getOrElse("model","").toString.toLowerCase match {
        case "overwrite"=>SaveMode.Overwrite
        case "append"=>SaveMode.Append
        case "errorifexists"=>SaveMode.ErrorIfExists
        case "ignore"=>SaveMode.Ignore
        case _=>SaveMode.Append
      }

      //如果需要建立外部表需要options中另外传入path 参数 example 外部表t1 path:/dir1/dir2/t1
      val format=options.getOrElse("format","orc")
      val tableName=options.getOrElse("paths","")
      val partitionBy=options.getOrElse("partitionBy","")

      //合并小文件操作
      var df_tmp = merge(spark,df,options)

      if(spark.catalog.tableExists(tableName)){
        val cols=spark.table(tableName).columns
        df_tmp.select(cols.map(col(_)):_*)
          .write.mode(model).insertInto(tableName)
      }else{
        if(partitionBy.equals("")){
          df_tmp.write.mode(model).format(format).options(options).saveAsTable(tableName)
        }else{
          df_tmp.write.mode(model).format(format).partitionBy(partitionBy).options(options).saveAsTable(tableName)
        }

      }
    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.error("[数据采集]:[HIVE]:[WRITE]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

}
