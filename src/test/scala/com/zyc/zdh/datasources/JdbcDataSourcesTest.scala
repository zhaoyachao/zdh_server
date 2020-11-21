package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.scalatest.FunSuite

class JdbcDataSourcesTest extends FunSuite with TEST_TRAIT2{

  test("testGetDS") {

    implicit val id="001"
    val dispatchOption = null
    val inPut = "jdbc"
    val inputOptions=Map(
      "driver"->"com.github.housepower.jdbc.ClickHouseDriver",
      "url"->"jdbc:clickhouse://192.168.110.10:9000",
      "dbtable"->"datasets.z1",
      "user"->"default",
      "password"->"",
      "numPartitions"->"1",
      "isolationLevel"->"NONE"
    )
    val df=JdbcDataSources.getDS(spark,dispatchOption,inPut,inputOptions,"",null,null,null,null,null,null )

    df.show(false)

//    var pro = new java.util.Properties
//    pro.put("driver","com.github.housepower.jdbc.ClickHouseDriver")
//    val format="org.apache.spark.sql.hive_jdbc.datasources.clickhouse.ClickHouseRelationProvider"
//    df.write.format(format).mode("append").option("batchsize", "50000").option("isolationLevel", "NONE").option("numPartitions", "1").options(inputOptions)
//      .save()


  }

  test("testWrteDS"){
    implicit val id="001"
    val dispatchOption = null
    val inPut = "jdbc"
    val inputOptions=Map(
      "driver"->"com.github.housepower.jdbc.ClickHouseDriver",
      "url"->"jdbc:clickhouse://192.168.110.10:9000",
      "dbtable"->"datasets.z1",
      "user"->"default",
      "password"->"",
      "numPartitions"->"1",
      "isolationLevel"->"NONE"
    )
    import org.apache.spark.sql.functions._
    val df=spark.range(10).select(concat(lit("zhaoyachao"),col("id")) as "name",lit(123) as "age",lit("woman") as "sex",lit(100.5f).cast("float") as "money")
    JdbcDataSources.writeDS(spark,df,inputOptions,"alter table datasets.z1 delete where 1=1")
  }

  test("getGreenplum"){

    implicit val id="001"
    val dispatchOption = null
    val inPut = "jdbc"
    val inputOptions=Map(
      "driver"->"com.pivotal.jdbc.GreenplumDriver",
      "url"->"jdbc:pivotal:greenplum://192.168.110.10:5432;DatabaseName=postgres",
      "dbtable"->"t1",
      "user"->"zyc",
      "password"->"123456",
      "numPartitions"->"1",
      "isolationLevel"->"NONE"
    )
    import org.apache.spark.sql.functions._
    val dt=spark.range(10).select(col("id"))
    JdbcDataSources.writeDS(spark,dt,inputOptions,"")

    val df=JdbcDataSources.getDS(spark,dispatchOption,inPut,inputOptions,"",null,null,null,null,null,null )

    df.show(false)

  }

  test("getGreenplum2"){

    implicit val id="001"
    val dispatchOption = null
    val inPut = "jdbc"
    val options =Map(
      "url"->"jdbc:postgresql://192.168.110.10:5432/",
    "delimiter"-> "\t",
      "dbschema"-> "postgres",
    "dbtable"-> "t1",
    "user"-> "zyc",
    "password"-> "123456")

    import org.apache.spark.sql.functions._
    val df=spark.read.format("org.apache.spark.sql.execution.datasources.greenplum.DefaultSource")
        .options(options).load()

    df.show(false)

  }


  test("testUpdateJDBC"){

    val opt=Map(
      "driver"->"com.mysql.cj.jdbc.Driver",
      "url"->"jdbc:mysql://127.0.0.1:3306/mydb?serverTimezone=GMT%2B8",
      "dbtable"->"task_log_instance",
      "user"->"zyc",
      "password"->"123456"
    )
    import spark.implicits._
    JdbcDataSources.updateJDBC(spark,null,opt,"")("001")

  }



}
