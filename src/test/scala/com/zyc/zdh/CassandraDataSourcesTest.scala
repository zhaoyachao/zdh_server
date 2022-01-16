package com.zyc.zdh

import com.zyc.TEST_TRAIT2
import com.zyc.zdh.datasources.CassandraDataSources
import org.junit.Test
@Test
class CassandraDataSourcesTest extends TEST_TRAIT2{
  @Test
  def testGetDS {

    val inputOptions=Map(
      "url"->"localhost:9042",
      "paths"->"ks_test.tb1"
    )
    CassandraDataSources.getDS(spark,null,null,inputOptions,null,null,null,null,null,null,null)("").show()
    //spark.conf.set("spark.cassandra.connection.host","localhost:9042")


//      spark.range(0,100).select(col("id"),col("id") as "name",lit("man") as "sex")
//      .write
//      .format("org.apache.spark.sql.cassandra")
//      .mode("overwrite")
//      .options(Map( "table" -> "tb1","keyspace"->"ks_test"))
//       .option("confirm.truncate","true")
//      .save()


//    val df = spark
//      .read
//      .format("org.apache.spark.sql.cassandra")
//      .options(Map( "table" -> "tb1","keyspace"->"ks_test"))
//      .load()
//
//    df.show()

  }
}
