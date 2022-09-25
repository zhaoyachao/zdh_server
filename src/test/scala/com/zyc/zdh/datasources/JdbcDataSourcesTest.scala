package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.junit.Test
import org.scalatest.FunSuite
@Test
class JdbcDataSourcesTest extends  TEST_TRAIT2{

  @Test
  def testGetDS  {

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
  @Test
  def testWrteDS {
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

  @Test
  def getGreenplum{

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
    //JdbcDataSources.writeDS(spark,dt,inputOptions,"")

    val df=JdbcDataSources.getDS(spark,dispatchOption,inPut,inputOptions,"",null,null,null,null,null,null )

    df.show(false)

  }
  @Test
  def getGreenplum2 {

    implicit val id="001"
    val dispatchOption = null
    val inPut = "jdbc"
    val options =Map(
      "url"->"jdbc:postgresql://192.168.110.10:5432/postgres",
    "delimiter"-> "\t",
      "dbschema"-> "public",
    "dbtable"-> "t1",
    "user"-> "zyc",
    "password"-> "123456")
    val df=spark.read.format("greenplum")
        .options(options).load()

    df.show(false)

  }

  @Test
  def testUpdateJDBC {

    val opt=Map(
      "driver"->"com.mysql.cj.jdbc.Driver",
      "url"->"jdbc:mysql://127.0.0.1:3306/mydb?serverTimezone=GMT%2B8",
      "dbtable"->"task_log_instance",
      "user"->"zyc",
      "password"->"123456"
    )
    //JdbcDataSources.updateJDBC(spark,null,opt,"")("001")

  }



}
