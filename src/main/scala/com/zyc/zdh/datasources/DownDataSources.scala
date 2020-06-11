package com.zyc.zdh.datasources

import java.io._

import com.zyc.base.util.JsonSchemaBuilder
import com.zyc.zdh.ZdhDataSources
import org.apache.poi.xssf.streaming.SXSSFWorkbook
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

object DownDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {

    throw new Exception("[数据采集]:[外部下载]:[READ]:不支持外部下载读取数据源")

  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    throw new Exception("[数据采集]:[外部下载]:[SELECT]:不支持外部下载读取数据源")
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    try {
      logger.info("[数据采集]:[外部下载]:[WRITE]:[options]:" + options.mkString(","))
      var url = options.getOrElse("url", "")
      var port = "22"
      if (url.contains(":")) {
        port = url.split(":")(1)
        url = url.split(":")(0)
      }

      val paths = options.getOrElse("paths", "")
      val sep = options.getOrElse("sep", ",")

      val username = options.getOrElse("user", "")
      val password = options.getOrElse("password", "")

      val filtType = options.getOrElse("fileType", "csv").toString.toLowerCase

      val root_path = options.getOrElse("root_path", "")
      var df_tmp = df
      //合并小文件操作
      if (!options.getOrElse("merge", "-1").equals("-1")) {
        df_tmp = df.repartition(options.getOrElse("merge", "200").toInt)
      }

      if (!url.equals("")) {
        logger.info("[数据采集]:[外部下载]:[WRITE]:输出到sftp数据源,文件路径:" + root_path + "/" + paths+".csv")
        df_tmp.write.
          format("com.springml.spark.sftp").
          options(options).
          option("host", url).
          option("port", port).
          option("username", username).
          option("password", password).
          option("fileType", filtType).
          option("delimiter", sep).
          save(root_path + "/" + paths+".csv")
      } else {
        logger.info("[数据采集]:[外部下载]:[WRITE]:未配置sftp 服务器,输出到本地数据源")
        val file=new File(root_path)
        if(!file.exists()){
          file.mkdirs()
          logger.info("[数据采集]:[外部下载]:[WRITE]:输出到本地数据源,自动创建目录"+root_path)
        }
        writeFile3(spark, df_tmp, root_path + "/" + paths, null);
      }


      df_tmp

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[外部下载]:[WRITE]:[ERROR]:" + ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  def writeFile2(spark: SparkSession, dataset: Dataset[_], path: String, style: Map[String, String], sheetName: String = "sheet1"): Unit = {
    import spark.implicits._
    val columns = dataset.columns
    val rs = dataset.na.fill("").select(array(col("*"))).as[Array[String]].collect()

    println("生成文件数据量:" + rs.length + ",文件路径:" + path)
    val sxssf = new SXSSFWorkbook()
    // val cellStyle = getHeaderStyle(sxssf,style.getOrElse("headerColor","52").toShort)

    val sheet = sxssf.createSheet(sheetName)

    val header = sheet.createRow(0)
    for (j <- 0 until columns.length) {
      sheet.setColumnWidth(j, 5000)
      val cell = header.createCell(j)
      cell.setCellValue(columns(j))
    }

    for (i <- 0 until rs.length) {
      val row = sheet.createRow(i + 1)
      for (j <- 0 until columns.length) {
        var cell1 = row.createCell(j)
        cell1.setCellValue(rs(i)(j).toString)
      }
    }
    var out = new FileOutputStream(path + ".csv");
    var a = new OutputStreamWriter(out, "utf-8")
    sxssf.write(out);
    out.close();
    sxssf.close()

  }

  def writeFile3(spark: SparkSession, dataset: Dataset[_], path: String, style: Map[String, String], sheetName: String = "sheet1"): Unit = {

    val writeFile = new File(path + ".csv");
    import spark.implicits._
    val columns = dataset.columns
    val rs = dataset.na.fill("").select(array(col("*"))).as[Array[String]].collect()
    println("生成文件数据量:" + rs.length + ",文件路径:" + path + ".csv")
    try {
      //第二步：通过BufferedReader类创建一个使用默认大小输出缓冲区的缓冲字符输出流
      val writeText = new BufferedWriter(new FileWriter(writeFile));

      //第三步：将文档的下一行数据赋值给lineData，并判断是否为空，若不为空则输出

      writeText.write(columns.mkString(","));
      for (i <- 0 until rs.length) {
        writeText.newLine(); //换行
        writeText.write(rs(i).mkString(","));
      }
      //使用缓冲区的刷新方法将数据刷到目的地中
      writeText.flush();
      //关闭缓冲区，缓冲区没有调用系统底层资源，真正调用底层资源的是FileWriter对象，缓冲区仅仅是一个提高效率的作用
      //因此，此处的close()方法关闭的是被缓存的流对象
      writeText.close();
    } catch {
      case e: Exception => {
        e.printStackTrace()
        throw e
      }
    }
  }


}
