package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.apache.spark.sql.functions._
import org.junit.Test
import org.scalatest.FunSuite
@Test
class ESDataSourcesTest extends  TEST_TRAIT2{

  @Test
  def testGetDS {
    val options=Map("url"->"localhost:9200",
      "paths"->"persion3"
    )

    ESDataSources.getDS(spark,null,null,options,null,null,null,null,null,null,null)("001").show(false)


  }

  @Test
  def testWriteDS{
    val df=spark.range(0,10).select(col("id"),concat(lit("name"),col("id")) as "name",lit(25) as "age")
    val options=Map("url"->"localhost:9200",
      "paths"->"persion3"
     // "es.write.operation"->"update"
    )

    ESDataSources.writeDS(spark,df,options,"")("001")
  }

}
