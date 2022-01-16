package com.zyc.zdh.datasources.sftp

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

/**
 * Abstract relation class for reading data from file
 */
case class SftpRelation(
    fileLocation: String,
    fileType: String,
    inferSchema: String,
    header: String,
    delimiter: String,
    quote: String,
    escape: String,
    multiLine: String,
    rowTag: String,
    customSchema: StructType,
    parameters:Map[String,String],
    sqlContext: SQLContext) extends BaseRelation with TableScan {

    private val logger = Logger.getLogger(classOf[SftpRelation])

    val df = read()

    private def read(): DataFrame = {
      import sqlContext.implicits._
      var dataframeReader = sqlContext.read
      if (customSchema != null) {
        dataframeReader = dataframeReader.schema(customSchema)
      }

      var df: DataFrame = null

      df = fileType match {
        case "avro" => dataframeReader.format("avro").options(parameters).load(fileLocation)
        case "txt" => dataframeReader.format("text").options(parameters).load(fileLocation)
        case "xml" => dataframeReader.format(constants.xmlClass)
          .option(constants.xmlRowTag, rowTag)
          .load(fileLocation)
        case "csv" => {
          val sep_tmp=resolveSep(parameters.getOrElse("delimiter",","))
          var ds=dataframeReader.format("csv").options(parameters).option("sep",",").load(fileLocation)
          var columns=Array.empty[Column]

          var ds1=ds.map(f => f.mkString(",").split(sep_tmp)).toDF("value")
          if(header.equalsIgnoreCase("true")){
            columns=ds.columns.mkString(",").split(sep_tmp).zipWithIndex.map(f => col("value").getItem(f._2) as f._1)
            ds1=ds1.select(columns:_*)
          }else{
            val inputCols=parameters.getOrElse("inputCols","")
            logger.info("[sftp:csv]:"+inputCols)
            if(!inputCols.trim.equals("")){
              columns=inputCols.split(",").zipWithIndex.map(f => col("value").getItem(f._2) as f._1)
            }
            ds1=ds1.select(columns:_*)
          }
          ds1
        }
        case "excel"=>dataframeReader.format("com.crealytics.spark.excel").options(parameters).load(fileLocation)
        case _ => dataframeReader.format(fileType).load(fileLocation)
      }
     df
    }

    override def schema: StructType = {
      df.schema
    }

    override def buildScan(): RDD[Row] = {
      df.rdd
    }

    def resolveSep(sep:String): String ={
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
      sep_tmp
    }

}
