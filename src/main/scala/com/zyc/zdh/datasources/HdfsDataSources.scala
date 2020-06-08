package com.zyc.zdh.datasources

import com.zyc.zdh.ZdhDataSources
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

object HdfsDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  /**
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
    * @return
    */
  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols: Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:输入源为[HDFS],开始匹配对应参数")
    val paths = inputOptions.getOrElse("paths", "").toString
    if (paths.trim.equals("")) {
      throw new Exception("[zdh],hdfs数据源读取:paths为空")
    }
    if (!paths.startsWith("/")) {
      throw new Exception("[zdh],hdfs数据源读取:路径必须是绝对路径,以/开头")
    }

    val sep = inputOptions.getOrElse("sep", ",").toString

    val hdfs = inputOptions.getOrElse("url", "")

    if (hdfs.contains(",")) {
      throw new Exception("[zdh],hdfs数据源读取:数据源连接串中包含特殊字符,或者不支持多个ip")
    }

    logger.info("[数据采集]:输入源为[HDFS],匹配对应参数完成")

    val options2 = inputOptions.filterKeys(key => !key.equals("paths") || !key.equals("sep"))
    val df = getDS(spark, hdfs + paths, sep, options2, inputCols, inputCondition)
    filter(spark, df, "", duplicateCols)
  }


  def getDS(spark: SparkSession, paths: String, sep: String, options: Map[String, String], cols: Array[String],
            inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:[HDFS]:匹配文件格式")

      options.getOrElse("fileType", "csv").toString.toLowerCase match {
        case "csv" => csv(spark, paths, sep, options, cols, inputCondition)
        case "orc" => orc(spark, paths, options, cols, inputCondition)
        case "paquet" => parquet(spark, paths, options, cols, inputCondition)
        case "json" => json(spark, paths, options, cols, inputCondition)
        case "excel" => excel(spark, paths, options, cols, inputCondition)
        case _ => other(spark, paths, sep, options, cols, inputCondition)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[HDFS]:[ERROR]:" + ex.getMessage.replace("\"", "'"))
        throw ex
      }
    }


  }

  def other(spark: SparkSession, paths: String, sep: String, options: Map[String, String], cols: Array[String],
            inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
    val fileType = options.getOrElse("fileType", "csv").toString.toLowerCase
    logger.info(s"[数据采集]:[HDFS]:[${fileType}]:[READ]:[cols]:" + cols.mkString(",") + "[options]:" + options.mkString(",") + " [FILTER]:" + inputCondition)
    val ds = spark.read.format(fileType).options(options).load(paths)
      .select(cols.map(col(_)): _*)
    if (inputCondition.trim.equals(""))
      ds
    else
      ds.filter(inputCondition)
  }

  def csv(spark: SparkSession, paths: String, sep: String, options: Map[String, String], cols: Array[String],
          inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:[HDFS]:[CSV]:[READ]:[cols]:" + cols.mkString(",") + "[options]:" + options.mkString(",") + " [FILTER]:" + inputCondition)

    import spark.implicits._
    val ncols = cols.zipWithIndex.map(f => col("value").getItem(f._2) as f._1)
    var ds: DataFrame = null
    if (sep.size == 1 && options.getOrElse("header", "false").equalsIgnoreCase("true")) {
      ds = spark.read.format("csv").options(options).option("sep", sep).load(paths)
    } else {
      logger.info("[数据采集]:[HDFS]:[CSV]:[READ]:分割符为多位" + sep + ",如果是以下符号会自动转义( )*+ -/ [ ] { } ? ^ | .")
      if (cols == null || cols.isEmpty) {
        throw new Exception("[数据采集]:[HDFS]:[CSV]:[READ]:分割符为多位" + sep + ",数据结构必须由外部指定")
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
      logger.info("[数据采集]:[HDFS]:[CSV]:[READ]:分割符为多为:" + sep + ",转义之后的分割符为:" + sep_tmp)
      ds = spark.read.format("csv").options(options).option("sep", ",").load(paths)
        .map(f => f.mkString(",").split(sep_tmp)).toDF("value")
        .select(ncols: _*)
    }

    if (inputCondition.trim.equals(""))
      ds
    else
      ds.filter(inputCondition)

  }

  def orc(spark: SparkSession, paths: String, options: Map[String, String], cols: Array[String],
          inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:[HDFS]:[ORC]:[READ]:[cols]:" + cols.mkString(",") + "[options]:" + options.mkString(",") + " [FILTER]:" + inputCondition)
    var ds = spark.read.format("orc").options(options).load(paths)

    if (cols != null && !cols.isEmpty) {
      ds = ds.select(cols.map(col(_)): _*)
    }

    if (inputCondition.trim.equals(""))
      ds
    else
      ds.filter(inputCondition)
  }

  def parquet(spark: SparkSession, paths: String, options: Map[String, String], cols: Array[String],
              inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:[HDFS]:[PARQUET]:[READ]:[cols]:" + cols.mkString(",") + "[options]:" + options.mkString(",") + " [FILTER]:" + inputCondition)
    var ds = spark.read.format("parquet").options(options).load(paths)
    if (cols != null && !cols.isEmpty) {
      ds = ds.select(cols.map(col(_)): _*)
    }

    if (inputCondition.trim.equals(""))
      ds
    else
      ds.filter(inputCondition)
  }

  def json(spark: SparkSession, paths: String, options: Map[String, String], cols: Array[String],
           inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:[HDFS]:[JSON]:[READ]:[cols]:" + cols.mkString(",") + "[options]:" + options.mkString(",") + " [FILTER]:" + inputCondition)
    var ds = spark.read.format("json").options(options).load(paths)
    if (cols != null && !cols.isEmpty) {
      ds = ds.select(cols.map(col(_)): _*)
    }
    if (inputCondition.trim.equals(""))
      ds
    else
      ds.filter(inputCondition)
  }

  def xml(spark: SparkSession, paths: String, options: Map[String, String], cols: Array[String],
          inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:[HDFS]:[XML]:[READ]:[cols]:" + cols.mkString(",") + "[options]:" + options.mkString(",") + " [FILTER]:" + inputCondition)
    val ds = spark.read.format("com.databricks.spark.xml").options(options)
      .schema("")
      .load(paths)
      .select(cols.map(col(_)): _*)

    if (inputCondition.trim.equals(""))
      ds
    else
      ds.filter(inputCondition)
  }

  def excel(spark: SparkSession, paths: String, options: Map[String, String], cols: Array[String],
            inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:[HDFS]:[EXCEL]:[READ]:[cols]:" + cols.mkString(",") + "[options]:" + options.mkString(",") + " [FILTER]:" + inputCondition)
    var ds = spark.read.format("com.crealytics.spark.excel")
      .options(options)
      //      .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
      //      .option("header", "true") // Required
      //      .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
      //      .option("inferSchema", "false") // Optional, default: false
      //      .option("addColorColumns", "true") // Optional, default: false
      //      .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
      //      .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files
      //      .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
      //      .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
      //      .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
      .load(paths)
      .toDF(cols: _*)

    if (cols != null && !cols.isEmpty) {
      ds = ds.select(cols.map(col(_)): _*)
    }

    if (inputCondition.trim.equals(""))
      ds
    else
      ds.filter(inputCondition)
  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column], zdh_etl_date: String)(implicit dispatch_task_id: String): DataFrame = {
    try {
      logger.info("[数据采集]:[HDFS]:[SELECT]")
      logger.debug("[数据采集]:[HDFS]:[SELECT]:" + select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[HDFS]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[HDFS]:[SELECT]:[ERROR]" + ex.getMessage.replace("\"", "'"))
        throw ex
      }
    }

  }


  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    val model = options.getOrElse("model", "").toString.toLowerCase match {
      case "overwrite" => SaveMode.Overwrite
      case "append" => SaveMode.Append
      case "errorifexists" => SaveMode.ErrorIfExists
      case "ignore" => SaveMode.Ignore
      case _ => SaveMode.Append
    }

    val fileType = options.getOrElse("fileType", "csv").toString
    val partitionBy = options.getOrElse("partitionBy", "").toString
    val outputPath = options.getOrElse("paths", "").toString
    if (outputPath.trim.equals("")) {
      throw new Exception("数据采集最终输出的路径为空")
    }
    if (!outputPath.startsWith("/")) {
      throw new Exception("数据采集最终输出的路径必须是以/开头的绝对路径")
    }
    val hdfs = options.getOrElse("url", "").toString

    var df_tmp = df
    //合并小文件操作
    if (!options.getOrElse("merge", "-1").equals("-1")) {
      df_tmp = df.repartition(options.getOrElse("merge", "200").toInt)
    }

    writeDS(spark, df_tmp, fileType, hdfs + outputPath, model, options, partitionBy)
  }

  def writeDS(spark: SparkSession, df: DataFrame, outPut: String, path: String, model: SaveMode, options: Map[String, String],
              partitionBy: String)(implicit dispatch_task_id: String): Unit = {
    try {
      logger.info("[数据采集]:[HDFS]:[WRITE]:[outPut]:" + outPut + "[path]:" + path + "[mode]:" + model.name() + "[options]:" + options.mkString(",") + "[partitionBy]:" + partitionBy)
      var format = outPut
      if (outPut.toLowerCase.equals("xlsx") || outPut.toLowerCase.equals("xls")) {
        format = "com.crealytics.spark.excel"
      }
      if (outPut.toLowerCase.equals("xml")) {
        format = "com.databricks.spark.xml"
      }

      if (!partitionBy.equals("")) {
        df.write.format(format).mode(model).partitionBy(partitionBy.mkString(",")).options(options).save(path)
      } else {
        df.write.format(format).mode(model).options(options).save(path)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[HDFS]:[WRITE]:[ERROR]:" + ex.getMessage.replace("\"", "'"))
        throw ex
      }
    }


  }

}
