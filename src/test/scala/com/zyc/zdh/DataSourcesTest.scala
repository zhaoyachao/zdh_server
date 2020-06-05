package com.zyc.zdh

import java.sql.{DriverManager, ResultSet, Statement}
import java.util.Properties

import com.zyc.TEST_TRAIT2
import org.apache.hadoop.conf.Configuration
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class DataSourcesTest extends FunSuite with TEST_TRAIT2{

  test("testDataHandlerJDBC") {


    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")


    val opt=Map(
      "driver"->"com.mysql.cj.jdbc.Driver",
      "url"->"jdbc:mysql://127.0.0.1:3306/zyc?serverTimezone=GMT%2B8",
      "dbtable"->"z1",
      "user"->"zyc",
      "password"->"123456"
    )
    ds.write.format("jdbc").mode(SaveMode.Overwrite).options(opt).save()

    val input="jdbc"
    val outputOptions=Map( "url"->"jdbc:mysql://127.0.0.1:3306/zyc?serverTimezone=GMT%2B8",
      "driver"->"com.mysql.cj.jdbc.Driver",
      "dbtable"->"z1_tmp",
      "user"->"zyc",
      "password"->"123456")
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))
    val output="jdbc"
    val sql="delete from z1_tmp where etl_date='2020-01-26'"

    DataSources.DataHandler(spark,null,null,null,input,opt,"1=1",null,output,outputOptions,cols,sql)


  }


  test("testDataHandlerJDBC2Hive") {


    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")


    val opt=Map(
      "driver"->"com.mysql.cj.jdbc.Driver",
      "url"->"jdbc:mysql://127.0.0.1:3306/zyc?serverTimezone=GMT%2B8",
      "dbtable"->"z1",
      "user"->"zyc",
      "password"->"123456"
    )
    ds.write.format("jdbc").mode(SaveMode.Overwrite).options(opt).save()

    val input="jdbc"
    val outputOptions=Map(
      "tableName"->"hive_z1"

    )
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))
    val output="hive"
    val sql=""

    DataSources.DataHandler(spark,null,null,null,input,opt,"1=1",null,output,outputOptions,cols,sql)

  }


  test("testDataHandlerJDBC2Hive2") {


    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")


    val opt=Map(
      "driver"->"com.mysql.cj.jdbc.Driver",
      "url"->"jdbc:mysql://127.0.0.1:3306/zyc?serverTimezone=GMT%2B8",
      "dbtable"->"z1",
      "user"->"zyc",
      "password"->"123456"
    )
    ds.write.format("jdbc").mode(SaveMode.Overwrite).options(opt).save()

    val input="jdbc"
    val outputOptions=Map(
      "tableName"->"hive_z1"

    )
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))
    val output="hive"
    val sql=""

    DataSources.DataHandler(spark,null,null,null,input,opt,"1=1",null,output,outputOptions,cols,sql)


    DataSources.DataHandler(spark,null,null,null,input,opt,"1=1",null,output,outputOptions,cols,sql)

    spark.table("hive_z1").show()

  }


  test("testDataHandlerJDBC2Csv") {


    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")


    val opt=Map(
      "driver"->"com.mysql.cj.jdbc.Driver",
      "url"->"jdbc:mysql://127.0.0.1:3306/zyc?serverTimezone=GMT%2B8",
      "dbtable"->"z1",
      "user"->"zyc",
      "password"->"123456"
    )
    ds.write.format("jdbc").mode(SaveMode.Overwrite).options(opt).save()

    val input="jdbc"
    val outputOptions=Map(
      "tableName"->"z1",
      "path"->"/data/csv/z1",
      "model"->"overwrite",
      "header"->"true"
    )
    val inputCols=Array("id","name","etl_date")
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))
    val output="csv"
    val sql=""

    DataSources.DataHandler(spark,null,null,null,input,opt,"1=1",inputCols,output,outputOptions,cols,sql)


    DataSources.DataHandler(spark,null,null,null,input,opt,"1=1",inputCols,output,outputOptions,cols,sql)


    spark.read.format("csv").option("header","true").load("/data/csv/z1").show()

  }


  test("testDataHandlerCSV2Csv") {


    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")


    val inputOptions=Map(
      "paths"->"/data/csv/z2",
      "model"->"overwrite"
    )
    ds.write.format("csv").mode(SaveMode.Overwrite).options(inputOptions).save("/data/csv/z2")

    val input="csv"
    val outputOptions=Map(
      "path"->"/data/csv/z1",
      "model"->"overwrite",
      "header"->"true",
      "sep"->"|"
    )
    val inputCols=Array("id","name","etl_date")
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))
    val output="csv"
    val sql=""

    DataSources.DataHandler(spark,null,null,null,input,inputOptions,"1=1",inputCols,output,outputOptions,cols,sql)


    DataSources.DataHandler(spark,null,null,null,input,inputOptions,"1=1",inputCols,output,outputOptions,cols,sql)


    spark.read.format("csv").option("header","true").load("/data/csv/z1").show()

  }

  test("testDataHandlerCSV2Jdbc") {


    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")


    val inputOptions=Map(
      "paths"->"/data/csv/z2",
      "model"->"overwrite"
    )
    ds.write.format("csv").mode(SaveMode.Overwrite).options(inputOptions).save("/data/csv/z2")

    val input="csv"
    val outputOptions=Map( "url"->"jdbc:mysql://127.0.0.1:3306/zyc?serverTimezone=GMT%2B8",
      "driver"->"com.mysql.cj.jdbc.Driver",
      "dbtable"->"z3",
      "user"->"zyc",
      "password"->"123456")

    val inputCols=Array("id","name","etl_date")
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))
    val output="jdbc"
    val sql="delete from z3 where etl_date='2020-01-26'"


    DataSources.DataHandler(spark,null,null,null,input,inputOptions,"1=1",inputCols,output,outputOptions,cols,sql)


  }


  test("testDataHandlerHive2Jdbc") {


    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")


    val inputOptions=Map(
      "tableName"->"z3"
    )
    ds.write.format("orc").mode(SaveMode.Overwrite).options(inputOptions).saveAsTable("z3")

    val input="hive"
    val outputOptions=Map( "url"->"jdbc:mysql://127.0.0.1:3306/zyc?serverTimezone=GMT%2B8",
      "driver"->"com.mysql.cj.jdbc.Driver",
      "dbtable"->"z3",
      "user"->"zyc",
      "password"->"123456")

    val inputCols=Array("id","name","etl_date")
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))
    val output="jdbc"
    val sql="delete from z3 where etl_date='2020-01-26'"


    DataSources.DataHandler(spark,null,null,null,input,inputOptions,"1=1",inputCols,output,outputOptions,cols,sql)


  }


  test("testDataHandlerHive2csv") {


    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")


    val inputOptions=Map(
      "tableName"->"z3"
    )
    ds.write.format("orc").mode(SaveMode.Overwrite).options(inputOptions).saveAsTable("z3")

    val input="hive"
    val outputOptions=Map(
      "path"->"/data/csv/z3",
      "model"->"overwrite",
      "header"->"true",
      "sep"->"|"
    )
    val inputCols=Array("id","name","etl_date")
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))
    val output="csv"
    val sql=""

    DataSources.DataHandler(spark,null,null,null,input,inputOptions,"1=1",inputCols,output,outputOptions,cols,sql)


    DataSources.DataHandler(spark,null,null,null,input,inputOptions,"1=1",inputCols,output,outputOptions,cols,sql)


    spark.read.format("csv").option("header","true").load("/data/csv/z3").show()


  }

  test("testHbase"){


    val input="hbase"

    val table="t1"
    val inputOptions=Map("url"->"192.168.65.10",
      "paths"->"t1"
    )

    val inputCols=Array("cf1:name","cf2:age","cf1:sex")
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"cf1:name","column_alias"->"name"),
      Map("column_expr"->"cf2:age","column_alias"->"age"),
      Map("column_expr"->"cf1:sex","column_alias"->"sex"))
    val output="jdbc"

    val outputOptions=Map( "url"->"jdbc:mysql://127.0.0.1:3306/mydb?serverTimezone=GMT%2B8",
      "driver"->"com.mysql.cj.jdbc.Driver",
      "dbtable"->"z3",
      "user"->"zyc",
      "password"->"123456")

    val sql="drop table z3"

    DataSources.DataHandler(spark,null,null,Map(),input,inputOptions,"0,1",inputCols,output,outputOptions,cols,sql)

  }


  test("testHbase2"){


    val input="hbase"

    val table="t1"
    val inputOptions=Map("url"->"192.168.65.10",
      "paths"->"t1"
    )

    val inputCols=Array("cf1:name","cf2:age","cf1:sex")
    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"cf1:name","column_alias"->"cf1:name"),
      Map("column_expr"->"cf2:age","column_alias"->"cf2:age"),
      Map("column_expr"->"cf1:sex","column_alias"->"cf2:sex"))
    val output="hbase"

    val outputOptions=Map( "url"->"192.168.65.10",
      "paths"->"t2"
    )

    val sql=""

    DataSources.DataHandler(spark,null,null,Map(),input,inputOptions,"0,1",inputCols,output,outputOptions,cols,sql)

  }

  test("phoenix"){

    val ds=spark.range(0,10).select(col("id") as "id",col("id") as "name",to_date(lit("2020-01-26")) as "etl_date")
    val inputOptions=Map(
      "driver"->"com.mysql.cj.jdbc.Driver",
      "url"->"jdbc:mysql://127.0.0.1:3306/zyc?serverTimezone=GMT%2B8",
      "dbtable"->"z1",
      "user"->"zyc",
      "password"->"123456"
    )
    ds.write.format("jdbc").mode(SaveMode.Overwrite).options(inputOptions).save()

    val input="jdbc"
    val inputCols=Array("id","name","etl_date")

    val output="jdbc"

    val outputOptions=Map( "url"->"jdbc:phoenix:192.168.65.10:2181",
      "driver"->"org.apache.phoenix.jdbc.PhoenixDriver",
      "dbtable"->"z1",
      "user"->"",
      "password"->"")

    val cols:Array[Map[String,String]]=Array(Map("column_expr"->"id+1","column_alias"->"id"),
      Map("column_expr"->"name","column_alias"->"name"),
      Map("column_expr"->"etl_date","column_alias"->"etl_date"))

    val sql=""

    DataSources.DataHandler(spark,null,null,Map(),input,inputOptions,"",inputCols,output,outputOptions,cols,sql)


  }

  test("readPhoenix"){


    import java.sql.Connection
    import java.sql.DriverManager
    import java.sql.SQLException
    val driver: String ="org.apache.phoenix.jdbc.PhoenixDriver"
    try
      Class.forName(driver)
    catch {
      case e: ClassNotFoundException =>
        e.printStackTrace()
    }

    var stmt:Statement  = null
    var rs:ResultSet = null
    val con = DriverManager.getConnection("jdbc:phoenix:master:2181")
    stmt = con.createStatement
    val sql = "select * from account"
    rs = stmt.executeQuery(sql)
    while ( {
      rs.next
    }) {
      System.out.print("id:" + rs.getString("id"))
      System.out.println(",name:" + rs.getString("name"))
    }
    stmt.close
    con.close()

  }


  test("readJDBCPhoenix"){
    val outputOptions=Map( "url"->"jdbc:phoenix:192.168.65.10:2181",
      "driver"->"org.apache.phoenix.jdbc.PhoenixDriver",
      "dbtable"->"account")

    spark.read.format("jdbc").options(outputOptions).load().show()

  }

  test("readPhoenix2"){

//    import org.apache.phoenix.spark._
//    val conf=spark.sparkContext.hadoopConfiguration
//    //conf.set("hbase.zookeeper.quorum","192.168.65.10")
//
//    val df = spark.sqlContext.phoenixTableAsDataFrame(
//      "tableName",
//      Seq("USER_ID"),
//      zkUrl = Some("192.168.65.10")
//    )
//    df.show()

  }

  test("readHiveJDBC"){

    val outputOptions=Map( "url"->"jdbc:hive2://192.168.65.10:10000/default",
      "driver"->"org.apache.hive.jdbc.HiveDriver",
      "dbtable"->"t1",
      "user"->"zyc",
      "password"->"")

    spark.sparkContext.setLogLevel("debug")
    val df = spark
      .read
      .option("url", "jdbc:hive2://192.168.65.10:10000/default")
      .option("dbtable", "zt1")
      .format("jdbc")
      .load


  }

  test("jdbcHive2"){

    spark.sparkContext.setLogLevel("debug")
    val url = "jdbc:hive2://192.168.65.10:10000/default";
    try {
      Class.forName("org.apache.hive.jdbc.HiveDriver");
    } catch {
      // TODO Auto-generated catch block
      case e:Exception=>e.printStackTrace();
    }
    val conn = DriverManager.getConnection(url, "", "");
    val stmt:Statement  = conn.createStatement();
    val sql = "SELECT * from zt1";
    //stmt.setQueryTimeout(0)
    var res:ResultSet  = stmt.executeQuery("show tables");
    while (res.next()) {
      System.out.println(res.getString(1) + "\t" + res.getString(2));
    }
//    res = stmt.executeQuery(sql2);
  //  res = stmt.executeQuery(sql3);
    //        while (res.next()) {
    //            System.out.println("id: " + res.getInt(1) + "\ttype: " + res.getString(2) + "\tauthors: " + res.getString(3) + "\ttitle: " + res.getString(4) + "\tyear:" + res.getInt(5));
    //        }



  }

  test("readHiveByMeta"){

    import spark.implicits._

    // 	ThriftJDBC URL for LLAP HiveServer2 	jdbc:hive2://localhost:10000
    //spark.datasource.hive.warehouse.load.staging.dir 	Temp directory for batch writes to Hive 	/tmp
    //  spark.hadoop.hive.llap.daemon.service.hosts 	App name for LLAP service 	@llap0
   // spark.hadoop.hive.zookeeper.quorum 	Zookeeper hosts used by LLAP 	host1:2181;host2:2181;host3:2181

    spark.conf.set("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://192.168.65.10:10000")
   // spark.conf.set()
//   val hive = com.hortonworks.spark.sql.hive.llap.HiveWarehouseBuilder.session(spark).build()
//
//    val df = hive.table("zt2")
//    df.show()




  }




}
