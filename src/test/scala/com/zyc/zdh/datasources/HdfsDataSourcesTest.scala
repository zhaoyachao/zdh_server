package com.zyc.zdh.datasources

import java.util

import com.zyc.TEST_TRAIT2
import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.sql.{Row, RowFactory, SaveMode}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.Test
import org.scalatest.FunSuite

import scala.collection.JavaConversions

@Test
class HdfsDataSourcesTest extends  TEST_TRAIT2{

  @Test
  def testWriteDS {

    val dt=Seq(
      Row("1","a", Row("zhaoyachao","man")),
      Row("2","b", Row("zhaoyachao","man"))
    )
    val schema = StructType(Seq(StructField("id", StringType, nullable = true), StructField("tag", StringType, nullable = true),
      StructField("other", StructType(Seq(StructField("name", StringType, nullable = true),StructField("sex", StringType, nullable = true))), nullable = true)
      ))
    import spark.implicits._

    val df =spark.createDataFrame(spark.sparkContext.parallelize(dt), schema)

    val options = Map(
      "precombine_field_opt_key"->"id",
      "recordkey_field_opt_key"->"id"

    )
    HdfsDataSources.writeDS(spark, df, "hudi", "/data/hudi/t11", SaveMode.Overwrite, options, "")("001")

  }

  @Test
  def testWriteDS2 {

    val dt=Seq(
      Row("1","a", "zhaoyachao","man"),
      Row("2","b", "zhaoyachao","man")
    )
    val schema = StructType(Seq(StructField("id", StringType, nullable = true), StructField("tag", StringType, nullable = true),
      StructField("name", StringType, nullable = true),StructField("sex", StringType, nullable = true)
    ))
    import spark.implicits._

    val df =spark.createDataFrame(spark.sparkContext.parallelize(dt), schema)

    var options = Map(
      "precombine_field_opt_key"->"id",
      "recordkey_field_opt_key"->"id"

    )
    val path="/data/hudi/t13"
    val basePath=path.substring(0,path.lastIndexOf("/"))
    val tableName=path.substring(path.lastIndexOf("/")+1)
    // TABLENAME
    val cols=df.columns
    if(options.getOrElse("precombine_field_opt_key","").toString.equalsIgnoreCase("") && !cols.contains("ts")){
      throw new Exception("[数据采集]:[HDFS]:[WRITE]:[ERROR]:写入hudi数据文件时必须指定主键,请设置etl任务中的主键字段precombine_field_opt_key参数")
    }
    if(options.getOrElse("recordkey_field_opt_key","").toString.equalsIgnoreCase("") && !cols.contains("uuid")){
      throw new Exception("[数据采集]:[HDFS]:[WRITE]:[ERROR]:写入hudi数据文件时必须指定主键,请设置etl任务中的主键字段recordkey_field_opt_key参数")
    }
    val recordkey_field_opt_key=options.getOrElse("recordkey_field_opt_key","")
    val precombine_field_opt_key=options.getOrElse("precombine_field_opt_key","")
    val operation_opt_key=options.getOrElse("operation_opt_key","upsert")

    options=options.+( HoodieWriteConfig.TABLE_NAME->tableName).+(
      DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY->precombine_field_opt_key,
      DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY->recordkey_field_opt_key,
      DataSourceWriteOptions.OPERATION_OPT_KEY->operation_opt_key)

    df.write.format("hudi").mode(SaveMode.Overwrite).options(options).save(path)
  }
}
