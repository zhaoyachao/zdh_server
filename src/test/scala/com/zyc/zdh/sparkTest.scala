package com.zyc.zdh

import java.sql.{DriverManager, ResultSet, Statement}

import com.zyc.TEST_TRAIT2
import com.zyc.base.util.{DateUtil, JsonSchemaBuilder}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.util.SizeEstimator
import org.junit.Test
import org.kie.api.KieServices
@Test
class sparkTest extends TEST_TRAIT2 {

  @Test
  def OverwritePartition {

    spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
    val ds1 = spark.range(0, 10).select(col("id") as "id", to_date(lit("2020-01-27")) as "etl_date")

    val ds2 = spark.range(0, 10).select(concat(col("id"), lit("na")) as "name", col("id") as "id", to_date(lit("2020-01-28")) as "etl_date")

    ds1.write.format("orc").mode(SaveMode.Overwrite).partitionBy("ETL_DATE").saveAsTable("t1")


    ds2.createTempView("t2")
    spark.sql("insert overwrite table t1 partition(etl_date) select name,etl_date from t2")
    // ds2.write.format("orc").mode(SaveMode.Overwrite).partitionBy("ETL_DATE").saveAsTable("t1")

    spark.table("t1").show()
  }
  @Test
  def sparkSqlHive {

    spark.sql("select * from t1").show()

  }
  @Test
  def readJSON {

    spark.read.
      //schema("path string,date string,tableName string,dataLevel string,repartitionNum string,repartitionColName string").
      json("/zjson.json")
      .show()


  }
  @Test
  def testType {

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
  @Test
  def sparkShuffle {

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
  @Test
  def delta {


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
  @Test
  def readExcel {


    val ds = spark.read
      .format("com.crealytics.spark.excel")
      .option("header", "false")
      .option("dataAddress", "'Sheet1'!A1")
      .load("file:///t1.xlsx")
    ds.show(100, false)

  }
  @Test
  def readXML {

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
  @Test
  def readES {

    spark.read.format("org.elasticsearch.spark.sql")
    // .options(cfg)


    spark.conf
  }
  @Test
  def fsfsafsfsd {


    spark.conf.set("spark.sql.shuffle.partitions", "100")

    println(spark.conf.get("spark.sql.shuffle.partitions"))

    println(spark.sparkContext.getConf.get("spark.sql.shuffle.partitions"))


    val spark2 = spark.newSession()


    println(spark2.conf.get("spark.sql.shuffle.partitions"))
  }
  @Test
  def fsdfsafsfsafsafsad {


    val df = spark.range(0, 1000000).select(concat(lit("name"), col("id")) as "name", col("id"), lit("fsdfadsfsadfsafadsf") as "c3")

    df.write.mode(SaveMode.Overwrite).save("/size_t1")
    val long = SizeEstimator.estimate(df)

    println(long)
  }
  @Test
  def spark_Hadoop_Ha {

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

  @Test
  def hive_jdbc {


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
  @Test
  def cdhReadHiveJdbc {
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
  @Test
  def testWindow {


    spark.range(0, 17).select(col("id") % 5 as "organ", col("id"), col("id") % 10 as "t")
      .withColumn("total", count(col("t")) over (Window.partitionBy("organ")))
      .show(false)


  }
  @Test
  def testJson {

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
  @Test
  def testCSVLr {
    //    val df=Seq("111111111\r222222","aaaaaaa\rbbbbbbbbbb","cccccccccccccccc","ddddddddd\reeeeeeeeeeee").toDF("value").as[String]
    //   df.show()
    val ds = spark.read.option("multiLine", "true").option("header", "true").option("parserLib", "univocity")
      .option("quote", "")
      .csv("/csvm.txt")
    ds.show(false)


  }
  @Test
  def createExterTable {


    spark.range(0, 10).select(col("id"), col("id") as "name").write.option("path", "/exteranl").saveAsTable("t1")
    //    spark.sql("show tables").show()
    //    spark.sql("drop table t1").show()
    //
    //    spark.sql("show tables").show()

    spark.table("t1").show(false)

    spark.range(0, 10).select(col("id"), col("id") as "name").write.saveAsTable("t2")

    spark.sql("drop table t2").show()


  }

  @Test
  def writeCSV {

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

  @Test
  def readHttp {
    spark.read.format("com.zyc.zdh.datasources.http.datasources.HttpRelationProvider")
      .option("url", "http://127.0.0.1:9091").option("schema", "a").option("paths", "zyc/static/t1.json").load().show()


  }
  @Test
  def readImage {
    val df = spark.read.format("image").option("dropInvalid", true)
      .load("C:/Users/zhaoyachao/Pictures/个人/反.jpg")
    df.printSchema()
    df.select("image.data").show()
  }

  @Test
  def writeExternalTable {

    val ds = spark.range(0, 100).select(col("id"), lit("abc") as "name")

    //spark 创建外部表--指定path 路径,注意路径的最后一个目录最好表名保持一致
    ds.write.format("orc").option("path", "/hive_table/t3").saveAsTable("t3")

    //解析分割符存在空数据问题
    println("a|b|c||||".split("\\|", -1).mkString(","))


  }

  @Test
  def writeOrc_t1 {

    import spark.implicits._

    //

    Seq((1, 2)).toDF

    val ds = spark.range(0, 100).select(col("id"), concat(lit("a"), col("id")) as "name")


    ds.write.format("orc").mode(SaveMode.Overwrite).save("/tt1")

    spark.read.orc("/tt1").show(100, false)

  }
  @Test
  def dateFormat {

    spark.range(0, 10).select(date_format(lit("2020-10-01"), "yyyyMMdd")).show(false)


  }
  @Test
  def stringConvertRdd {
    import spark.implicits._

    val ds = spark.createDataset(Seq("1", "2", "3"))
    ds.filter("CUST_TYPE is null").select("CUST_ID", "CUST_CATEGORY", "UPDATE_DATE").dropDuplicates("UPDATE_DATE").show(1000)
    ds.dropDuplicates()
  }
  @Test
  def substr {
    // spark.range(10).select( substring(lit("001"),0,1),substring(lit("001"),0,2),substring(lit("001"),0,3),substring(lit("001"),0,4)).show(false)

    println(DateUtil.parse("20200101").before(DateUtil.parse("20200301")))


  }
  @Test
  def split_ {

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
  @Test
  def sparkReadExcel {

    spark.read.option("header","true").format("com.crealytics.spark.excel").load("/poi.xls").show()

  }
  @Test
  def exprLit {
    val zdh_etl_date="2020-01-01 00:00:00"

    val ud=udf((a:String)=>{
      a
    })

    spark.range(0,10).select(expr("ud(zdh_etl_date)")).show(false)
  }
  @Test
  def regex {

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
  @Test
  def remove_tables {

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
  @Test
  def spark_col_map {

    import org.apache.spark.sql.functions._
    import spark.implicits._

   // spark.range(10).select(col("id"),lit("z") as "name").select(map_concat(col("id"),col("name")) as "m1" ).show()
   val rules="package rules\n " +
     "import com.zyc.drools.D1\n"+
     "import java.util.HashMap\n"+
     "rule \"alarm\"\n"+
     "no-loop true\n"+
     "when\n"+
     "d1:HashMap(d1.get(\"id\")>=5)\n"+
     "then\n"+
     "d1.put(\"dsts\",\"true\");\n"+
     "update(d1);\n"+
    "D1 d2=new D1();\n"+
    "d2.update(\"hello world\");\n"+
     "System.out.print(\"dddddddddd=\"+d1.get(\"id\"));\n"+
     "end"

    println(rules)



    val ds=spark.range(10).select(col("id"),lit("z") as "name")
      .select(map_from_arrays(array(lit("id"),lit("name")),array(col("id"),col("name"))) as "map")


    import scala.collection.JavaConverters._


    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[java.util.HashMap[String, String]]

    val re=ds.mapPartitions(ms=>{
      val kieServices = KieServices.Factory.get();
      val kfs = kieServices.newKieFileSystem();
      kfs.write("src/main/resources/rules/rules.drl", rules.getBytes());
      val kieBuilder = kieServices.newKieBuilder(kfs).buildAll();
      val results = kieBuilder.getResults();
      if (results.hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
        System.out.println(results.getMessages());
        throw new IllegalStateException("### errors ###");
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
    }).toDF("map")

re.show(false)

    spark.read.json(re.select(to_json(col("map"))).as[String]).show()


//    ds.map(f=>{
//      val kieServices = KieServices.Factory.get();
//      val kfs = kieServices.newKieFileSystem();
//      kfs.write("src/main/resources/rules/rules.drl", rules.getBytes());
//      val kieBuilder = kieServices.newKieBuilder(kfs).buildAll();
//      val results = kieBuilder.getResults();
//      if (results.hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
//        System.out.println(results.getMessages());
//        throw new IllegalStateException("### errors ###");
//      }
//      val kieContainer = kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());
//      val kieBase = kieContainer.getKieBase();
//      val ksession = kieBase.newKieSession()
//      val map=f.getAs[Map[String,String]](0)
//      val m=new java.util.HashMap[String,String]()
//      map.foreach(a=>{
//        m.put(a._1,a._2)
//      })
//      ksession.insert(m)
//      val res=ksession.fireAllRules()
//      ksession.dispose()
//      m.asScala.toMap
//    }).show(false)



  }
  @Test
  def web_port {

    val web_port=spark.conf.get("spark.ui.port","4040")

    println(spark.sparkContext.uiWebUrl.get)
    while (true){
      Thread.sleep(10000)
    }
  }
}
