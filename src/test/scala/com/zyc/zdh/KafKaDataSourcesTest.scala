package com.zyc.zdh

import com.zyc.TEST_TRAIT2
import com.zyc.zdh.datasources.KafKaDataSources
import org.apache.spark.sql.functions._
import org.junit.Test
import org.scalatest.FunSuite
@Test
class KafKaDataSourcesTest extends TEST_TRAIT2{
  @Test
  def testGetDS {
    val outputOptions=Map( "url"->"jdbc:mysql://127.0.0.1:3306/mydb?serverTimezone=GMT%2B8",
      "driver"->"com.mysql.cj.jdbc.Driver",
      "dbtable"->"z10",
      "user"->"zyc",
      "password"->"123456")
    spark.sparkContext.setLogLevel("error")
    KafKaDataSources.createKafkaDataSources(spark,"localhost:9092","topic1","g1",Map("msgType"->"csv"),Array("name","age"),"",null,outputOptions,"","")("001")


  while (true){
    Thread.sleep(10000)
    print("============")
  }
  }
  @Test
  def writeKafka{

    val outputOptions=Map( "url"->"localhost:9092",
      "paths"->"t1",
      "dbtable"->"z10",
      "user"->"zyc",
      "password"->"123456")

    val df=spark.range(0,10).select(col("id") as  "key",col("id") as "value")
    KafKaDataSources.writeDS(spark,df,outputOptions,"")("001")


  }

}
