package com.zyc.zdh.datasources

import java.io.{FileWriter, PrintWriter}
import java.net.URL

import com.zyc.base.util.JsonSchemaBuilder
import com.zyc.zdh.ZdhDataSources
import org.apache.spark.SparkFiles
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.slf4j.LoggerFactory

object FtpDataSources extends ZdhDataSources {
  val logger = LoggerFactory.getLogger(this.getClass)


  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:[FTP]:匹配文件格式")
      var url=inputOptions.getOrElse("url","")
      var port="21"
      if(url.contains(":")){
        port=url.split(":")(1)
        url=url.split(":")(0)
      }
      if(url.trim.equals("")){
        throw new Exception("[zdh],ftp数据源读取:url为空")
      }

      var paths=inputOptions.getOrElse("paths","")

      if(paths.trim.equals("")){
        throw new Exception("[zdh],ftp数据源读取:paths为空")
      }
      if(!paths.startsWith("/")){
        paths = "/"+paths
      }

      val sep=inputOptions.getOrElse("sep",",")

      val username=inputOptions.getOrElse("user","")
      val password=inputOptions.getOrElse("password","")

      val fileType=inputOptions.getOrElse("fileType", "csv").toString.toLowerCase

      logger.info("[数据采集]:[FTP]:[CSV]:[READ]:分割符为多位" + sep + ",如果是以下符号会自动转义( )*+ -/ [ ] { } ? ^ | .")
      if (inputCols == null || inputCols.isEmpty) {
        throw new Exception("[数据采集]:[FTP]:[CSV]:[READ]:分割符为多位" + sep + ",数据结构必须由外部指定")
      }
      var sep_tmp = sep.replace("\\", "\\\\")
      if (sep_tmp.contains('$')) {
        sep_tmp = sep_tmp.replace("$", "\\$")
      }
      if (sep_tmp.contains('(') || sep_tmp.contains(')')) {
        sep_tmp = sep_tmp.replace("(", "\\(").replace(")", "\\)")
      }
      if (sep_tmp.contains('*')) {
        sep_tmp = sep_tmp.replace("*", "\\*")
      }
      if (sep_tmp.contains('+')) {
        sep_tmp = sep_tmp.replace("+", "\\+")
      }
      if (sep_tmp.contains('-')) {
        sep_tmp = sep_tmp.replace("-", "\\-")
      }
      if (sep_tmp.contains('[') || sep_tmp.contains(']')) {
        sep_tmp = sep_tmp.replace("[", "\\[").replace("]", "\\]")
      }
      if (sep_tmp.contains('{') || sep_tmp.contains('}')) {
        sep_tmp = sep_tmp.replace("{", "\\{").replace("}", "\\}")
      }
      if (sep_tmp.contains('^')) {
        sep_tmp = sep_tmp.replace("^", "\\^")
      }
      if (sep_tmp.contains('|')) {
        sep_tmp = sep_tmp.replace("|", "\\|")
      }

      var ncols = inputCols.zipWithIndex.map(f => col("value").getItem(f._2) as f._1)
      //ncols2=ds.columns.mkString(",").split(sep_tmp).zipWithIndex.map(f => col("value").getItem(f._2) as f._1)
      val schema=JsonSchemaBuilder.getJsonSchema(inputCols.mkString(","))
      logger.info("[数据采集]:[FTP]:[READ]:paths:"+url+":"+port+paths)
      var filename = s"ftp://${username}:${password}@${url}${paths}"
      import spark.implicits._
      spark.sparkContext.addFile(filename)

      var df:DataFrame = spark.emptyDataFrame
      var df_tmp = spark.sparkContext.textFile(SparkFiles.get(filename.split("/").last))
      if(fileType.equalsIgnoreCase("csv")){
        df = df_tmp.map(line=>line.split(sep_tmp)).toDF("value")
          .select(ncols: _*)
          .filter(col(inputCols.head) =!= inputCols.head) //过滤表头信息
      }
      if(fileType.equalsIgnoreCase("json")){
        df =  df_tmp.toDF("value")
          .as[String].select(from_json(col("value").cast("string"), schema) as "data")
          .select(col("data.*"))
      }


      filter(spark,df,inputCondition,duplicateCols)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[FTP]:[READ]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:[FTP]:[SELECT]")
      logger.debug("[数据采集]:[FTP]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[FTP]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[FTP]:[SELECT]:[ERROR]"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    try {
      logger.info("[数据采集]:[FTP]:[WRITE]:[options]:"+options.mkString(","))
      var url=options.getOrElse("url","")
      var port="21"
      if(url.contains(":")){
        port=url.split(":")(1)
        url=url.split(":")(0)
      }

      var paths=options.getOrElse("paths","")
      val sep=options.getOrElse("sep",",")
      if(!paths.startsWith("/")){
        paths = "/"+paths
      }
      val username=options.getOrElse("user","")
      val password=options.getOrElse("password","")

      val filtType=options.getOrElse("fileType", "csv").toString.toLowerCase

      //合并小文件操作
      var df_tmp = merge(spark,df,options)
      var filename = s"ftp://${username}:${password}@${url}:21${paths}"
      logger.info("[数据采集]:[FTP]:[WRITE]:当前只支持覆盖写入ftp文件,不支持追加")
      val header = df_tmp.columns.mkString(sep)
      df_tmp.repartition(1)
        .foreachPartition(rows=>{
          val ftp = new URL(filename)
          val pw = new PrintWriter(ftp.openConnection().getOutputStream)
          pw.write(header+"\n")
          rows.foreach(row=> pw.write(row.mkString(sep)+"\n"))
          pw.flush()
          pw.close()
        })
      df_tmp
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[FTP]:[WRITE]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }


}
