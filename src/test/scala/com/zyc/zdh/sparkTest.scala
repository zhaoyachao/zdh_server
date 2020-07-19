package com.zyc.zdh

import java.sql.{DriverManager, ResultSet, Statement}

import com.zyc.TEST_TRAIT2
import com.zyc.base.util.{DateUtil, JsonSchemaBuilder}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SaveMode}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.SizeEstimator

class sparkTest extends FunSuite with TEST_TRAIT2 {


  test("OverwritePartition") {

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val ds1 = spark.range(0, 10).select(col("id") as "id", to_date(lit("2020-01-27")) as "etl_date")

    val ds2 = spark.range(0, 10).select(concat(col("id"), lit("na")) as "name", col("id") as "id", to_date(lit("2020-01-28")) as "etl_date")

    ds1.write.format("orc").mode(SaveMode.Overwrite).partitionBy("ETL_DATE").saveAsTable("t1")


    ds2.createTempView("t2")
    spark.sql("insert overwrite table t1 partition(etl_date) select name,etl_date from t2")
    // ds2.write.format("orc").mode(SaveMode.Overwrite).partitionBy("ETL_DATE").saveAsTable("t1")

    spark.table("t1").show()
  }

  test("sparkSqlHive") {

    spark.sql("select * from t1").show()

  }

  test("readJSON") {

    import spark.implicits._

    spark.read.
      //schema("path string,date string,tableName string,dataLevel string,repartitionNum string,repartitionColName string").
      json("/zjson.json")
      .show()


  }

  test("testType") {

    //    val ds2=spark.range(0,10).select(col("id"),lit(0.1)  as "amt",lit(2L) as "num",
    //      to_timestamp(lit("20200219"),"yyyyMMdd") as "UPDATE_DATE")
    //    ds2.printSchema()
    //    ds2.write.mode(SaveMode.Overwrite).format("orc").save("/t1")
    ////
    //    val ds=spark.range(0,10).select(col("id"),lit(0.1) as "amt",
    //      to_timestamp(lit("20200218"),"yyyyMMdd") as "UPDATE_DATE")
    //
    //    ds.write.mode(SaveMode.Overwrite).format("orc").save("/t2")
    //    ds.printSchema()


    spark.read.orc("/t1").show()
  }

  test("sparkShuffle") {

    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(List("name", "age").map(fileName => StructField(fileName, StringType, true))))
      .show()
    //
    //    import spark.implicits._
    //    spark.conf.set("spark.local.dir","/tmp/spark_shuffle")
    //    //shuffle 20
    //    val ds=spark.range(0,1000).select(col("id"),lit("a") as "name").repartition(100).map(f=>(f.getAs[String]("name"),f.getAs[Long]("id")))
    //      .rdd.reduceByKey(_+_).persist(StorageLevel.DISK_ONLY)
    //
    //    ds.count()
    //    ds.collect()
    //
    //    ds.top(10)
    //    spark.range(0,1000).select(col("id"),lit("a") as "name").repartition(100).groupBy("id").agg(first("name") as "name").count()
    //    while(true){
    //
    //    }

  }

  test("delta") {


    val ds = spark.range(0, 20).select(col("id"), rand() as "age", rand() as "name")

    ds.write.format("delta").mode(SaveMode.Overwrite).saveAsTable("t1")


    spark.sql("select * from t1").show(false)

    import io.delta.tables._

    val dt = DeltaTable.forPath(spark, "spark-warehouse/t1")
    dt.toDF.show
    dt.delete("id<10")


    //spark.sql("delete from t1 where id<10").show()

    spark.sql("select * from t1").show(false)

  }

  test("readExcel") {


    val ds = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "false")
      .option("dataAddress", "'Sheet1'!A1")
      .load("file:///t1.xlsx")
    ds.show(100, false)

  }

  test("readXML") {

    import spark.implicits._
    import com.databricks.spark.xml.functions.from_xml
    import com.databricks.spark.xml.schema_of_xml
    import com.databricks.spark.xml.schema_of_xml_df
    val ds = spark.read.format("com.databricks.spark.xml")
      .option("rowTag", "Item")
      .load("file:///t1.xml")
      .toDF("value")


    val schema = schema_of_xml_df(ds.select("value"))
    schema.printTreeString()
    //println()

    ds.show(100, false)


  }

  test("readES") {

    import org.elasticsearch.spark.sql

    spark.read.format("org.elasticsearch.spark.sql")
    // .options(cfg)


    spark.conf
  }

  test("fsfsafsfsd") {


    spark.conf.set("spark.sql.shuffle.partitions", "100")

    println(spark.conf.get("spark.sql.shuffle.partitions"))

    println(spark.sparkContext.getConf.get("spark.sql.shuffle.partitions"))


    val spark2 = spark.newSession()


    println(spark2.conf.get("spark.sql.shuffle.partitions"))
  }

  test("fsdfsafsfsafsafsad") {


    val df = spark.range(0, 1000000).select(concat(lit("name"), col("id")) as "name", col("id"), lit("fsdfadsfsadfsafadsf") as "c3")

    df.write.mode(SaveMode.Overwrite).save("/size_t1")
    val long = SizeEstimator.estimate(df)

    println(long)
  }

  test("spark Hadoop Ha") {

    spark.conf
      .set("spark.hadoop.dfs.nameservices", "iptvcluster")
    spark.conf.
      set("spark.hadoop.dfs.client.failover.proxy.provider.iptvcluster", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    spark.conf.
      set("spark.hadoop.dfs.ha.namenodes.iptvcluster", "nn1,nn2")
    spark.conf.
      set("spark.hadoop.dfs.namenode.rpc-address.iptvcluster.nn1", "10.136.1.218:8020")
    spark.conf.
      set("spark.hadoop.dfs.namenode.rpc-address.iptvcluster.nn2", "10.136.1.213:8020")

    spark.read.format("csv").load("")


  }


  test("hive_jdbc") {


    val outputOptions = Map("url" -> "jdbc:hive2://192.168.65.11:10000/default",
      "driver" -> "org.apache.hive.jdbc.HiveDriver",
      "dbtable" -> "zt2",
      "user" -> "zyc",
      "password" -> "",
      "fetchsize" -> "100")

    //    spark.conf
    //      .set("spark.hadoop.dfs.nameservices","yldcluster")
    //    spark.conf.
    //      set("spark.hadoop.dfs.client.failover.proxy.provider.iptvcluster","org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")
    //    spark.conf.
    //      set("spark.hadoop.dfs.ha.namenodes.yldcluster", "nn1,nn2")
    //    spark.conf.
    //      set("spark.hadoop.dfs.namenode.rpc-address.yldcluster.nn1", "192.168.65.10:8020")
    //    spark.conf.
    //      set("spark.hadoop.dfs.namenode.rpc-address.yldcluster.nn2", "192.168.65.11:8020")


    spark.sparkContext.setLogLevel("debug")

    val ds = spark.range(150, 160).select(col("id"), concat(lit("name"), col("id")) as "name", lit(20) as "sex")
    ds.show(false)

    ds.write.format("org.apache.spark.sql.hive_jdbc.datasources.hive.HiveRelationProvider")
      // ds.write.format("jdbc")
      .options(outputOptions)
      .mode(SaveMode.Append)
      .save()


    //    spark.read.format("org.apache.spark.sql.hive_jdbc.datasources.hive.HiveRelationProvider")
    //      .options(outputOptions)
    //      .load()
    //      .show()

  }

  test("cdhReadHiveJdbc") {
    val url = "jdbc:datadirect:sparksql://192.168.65.11:10000;DatabaseName=default;";
    try {
      Class.forName("com.ddtek.jdbc.sparksql.SparkSQLDriver");
    } catch {
      // TODO Auto-generated catch block
      case e: Exception => e.printStackTrace();
    }
    val conn = DriverManager.getConnection(url, "zyc", "");
    val stmt: Statement = conn.createStatement();
    val sql = "SELECT * from zt1";
    //stmt.setQueryTimeout(0)
    var res: ResultSet = stmt.executeQuery("show tables");
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
  }

  test("testWindow") {

    import spark.implicits._


    spark.range(0, 17).select(col("id") % 5 as "organ", col("id"), col("id") % 10 as "t")
      .withColumn("total", count(col("t")) over (Window.partitionBy("organ")))
      .show(false)


  }

  test("testJson") {

    import spark.implicits._
    JsonSchemaBuilder.getJsonSchema("name,age,job.money,job.jobTp").printTreeString()
    spark.read.schema(JsonSchemaBuilder.getJsonSchema("name,age,job.money,job.jobTp")).json(spark.range(1).select(lit("{\"name\":\"zyc\",\"age\":\"20\"}") as "js1").as[String]).show(false)

    val js = "{\"string\":\"string1\",\"int\":1,\"array\":[1,2,3],\"dict\": {\"key\": \"value1\"}}\n{\"string\":\"string2\",\"int\":2,\"array\":[2,4,6],\"dict\": {\"key\": \"value2\"}}\n{\"string\":\"string3\",\"int\":3,\"array\":[3,6,9],\"dict\": {\"key\": \"value3\", \"extra_key\": \"extra_value3\"}}"


    val df = spark.read.json(spark.sparkContext.parallelize(Seq(js)))
    df.show(false)
    println(df.schema.toDDL)


    //    spark.range(1).select(lit("{\"name\":\"zyc\",\"age\":\"20\"}") as "js1").select(from_json(col("js1"),JsonSchemaBuilder.getJsonSchema("name,age,job.money,job.jobTp")))
    //      .show(false)

    JsonSchemaBuilder.getJsonSchema("name,age,job.money,job.jobTp").toDDL
  }

  test("testCSVLr") {


    import spark.implicits._
    //    val df=Seq("111111111\r222222","aaaaaaa\rbbbbbbbbbb","cccccccccccccccc","ddddddddd\reeeeeeeeeeee").toDF("value").as[String]
    //   df.show()
    val ds = spark.read.option("multiLine", "true").option("header", "true").option("parserLib", "univocity")
      .option("quote", "")
      .csv("/csvm.txt")
    ds.show(false)


  }

  test("createExterTable") {


    spark.range(0, 10).select(col("id"), col("id") as "name").write.option("path", "/exteranl").saveAsTable("t1")
    //    spark.sql("show tables").show()
    //    spark.sql("drop table t1").show()
    //
    //    spark.sql("show tables").show()

    spark.table("t1").show(false)

    spark.range(0, 10).select(col("id"), col("id") as "name").write.saveAsTable("t2")

    spark.sql("drop table t2").show()


  }


  test("writeCSV") {

    import spark.implicits._

    import spark.implicits._
    val user_seq = Seq(("20200325", "001", "p1", 1),
      ("20200325", "001", "p1", 1),
      ("20200325", "001", "p1", 1),
      ("20200325", "001", "p1", 1),
      ("20200325", "001", "p1", 1),
      ("20200325", "001", "p1", 1),
      ("20200325", "002", "p1", 1),
      ("20200325", "003", "p1", 1),
      ("20200325", "001", "p2", 1),
      ("20200325", "001", "p3", 1)
    )

    val ds = user_seq.toDF("day", "user_id", "page_id", "time")

    ds.groupBy("day", "user_id", "page_id").agg(count("time"))
      .withColumn("range", rank() over (Window.partitionBy("day", "user_id", "page_id")))
      .filter(col("range") <= 10)


  }


  test("readHttp") {

    import spark.implicits._
    spark.read.format("com.zyc.zdh.datasources.http.datasources.HttpRelationProvider")
      .option("url", "http://127.0.0.1:9091").option("schema", "a").option("paths", "zyc/static/t1.json").load().show()


  }

  test("readImage") {
    val df = spark.read.format("image").option("dropInvalid", true)
      .load("C:/Users/zhaoyachao/Pictures/个人/反.jpg")
    df.printSchema()
    df.select("image.data").show()
  }


  test("writeExternalTable") {

    import spark.implicits._

    val ds = spark.range(0, 100).select(col("id"), lit("abc") as "name")

    //spark 创建外部表--指定path 路径,注意路径的最后一个目录最好表名保持一致
    ds.write.format("orc").option("path", "/hive_table/t3").saveAsTable("t3")

    //解析分割符存在空数据问题
    println("a|b|c||||".split("\\|", -1).mkString(","))


  }


  test("writeOrc_t1") {

    import spark.implicits._

    //

    Seq((1, 2)).toDF

    val ds = spark.range(0, 100).select(col("id"), concat(lit("a"), col("id")) as "name")


    ds.write.format("orc").mode(SaveMode.Overwrite).save("/tt1")

    spark.read.orc("/tt1").show(100, false)

  }

  test("dateFormat") {

    import spark.implicits._

    spark.range(0, 10).select(date_format(lit("2020-10-01"), "yyyyMMdd")).show(false)


  }

  test("stringConvertRdd") {
    import spark.implicits._

    val ds = spark.createDataset(Seq("1", "2", "3"))
    ds.filter("CUST_TYPE is null").select("CUST_ID", "CUST_CATEGORY", "UPDATE_DATE").dropDuplicates("UPDATE_DATE").show(1000)
    ds.dropDuplicates()
  }

  test("substr") {
    // spark.range(10).select( substring(lit("001"),0,1),substring(lit("001"),0,2),substring(lit("001"),0,3),substring(lit("001"),0,4)).show(false)

    println(DateUtil.parse("20200101").before(DateUtil.parse("20200301")))


  }

  test("split_") {

    import spark.implicits._

    val ds = spark.read.orc("/postal_data_test/full_orc/FD_CUSTOMERINFO")

    //    case 2 => 1
    //    case 3 => 2
    //    case 4 => 3
    //客户类型，1：个人，2：公司,3:同业,4:小企业
    val ds_tmp = spark.read.option("header", "true").csv("自己的路径")
      .withColumnRenamed("CUSTOMERID", "CUST_ID")
      .withColumn("IS_UPDATE", lit("1"))
      .withColumn("NEW_CUST_TYPE", when(col("CUSTOMERTYPE") =!= 1, col("CUSTOMERTYPE") - 1).otherwise(col("CUSTOMERTYPE")))


    val result=ds.join(ds_tmp, Seq("CUST_ID"), "left").withColumn("CUST_TYPE", when(col("IS_UPDATE") === 1, ds_tmp("NEW_CUST_TYPE")))
      .drop("IS_UPDATE", "NEW_CUST_TYPE", "CUSTOMERTYPE", "CUSTOMERKIND")

    result



  }

  test("sparkReadExcel"){

    import spark.implicits._

    spark.read.option("header","true").format("com.crealytics.spark.excel").load("/poi.xls").show()

  }

  test("exprLit"){
    val zdh_etl_date="2020-01-01 00:00:00"

    val ud=udf((a:String)=>{
      a
    })

    spark.range(0,10).select(expr("ud(zdh_etl_date)")).show(false)
  }

  test("regex"){

    import spark.implicits._


    val ds=spark.range(0,10).select(lit("1") as "c1",lit("1234") as "c2",lit("5") as "c3")

    val u=udf((c1:String,re:String)=>{
      val a=re.r()
      (c1.matches(a.toString())).toString
    })


    ds.withColumn("re",u($"c1",lit("^([123])"))).withColumn("re2",u($"c3",lit("^([123])"))).show()
//    表相关检测
    //    字段总个数
    //    行数范围
    //    主键-不可重复
//    单字段检测
    //    单个字段长度
    //    字段类型
    //    字段是否为空
    //    字段范围
    //    正则表达式

    //前端 增加了etl 任务中内容-需要修改
    // 1增加了原始字段 2 增加了质量检测相关

    //kafka 数据源未添加过滤
    //hdfs 数据源 处理不统一
  }

  test("remove_tables"){

    import spark.implicits._

    val ds=spark.range(0,10).select(col("id"))

    ds.createOrReplaceTempView("t0")
   // spark.sql("CREATE OR REPLACE TEMPORARY VIEW t1 as select * from t0")

   // spark.sql("select * from t1").show(false)

   // spark.sql("CREATE OR REPLACE TEMPORARY VIEW t1 as select * from t0\r\n" +
   //   "drop view if EXISTS t1").show(false)

   // spark.sql("create table t1(name String,age int)").show(false)

    //spark.sql("create database d1").show()
   // spark.sql("create table d1.t1(name String,age int)").show(false)
    spark.sql("show databases").show(false)

    spark.sql("use d1").show()
    spark.sql("show tables").show(false)

    spark.sql("desc default.t1").show(false)
    spark.sql("desc d1.t1").show(false)

  }

}
