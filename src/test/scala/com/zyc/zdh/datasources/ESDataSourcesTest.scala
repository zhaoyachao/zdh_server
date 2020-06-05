package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class ESDataSourcesTest extends FunSuite with TEST_TRAIT2{

  test("testGetDS") {

    import spark.implicits._
    val options=Map("url"->"localhost:9200",
      "paths"->"persion3"
    )

    ESDataSources.getDS(spark,null,null,options,null,null,null,null,null,null,null)("001").show(false)


  }

  test("testWriteDS"){
    val df=spark.range(0,10).select(col("id"),concat(lit("name"),col("id")) as "name",lit(25) as "age")
    val options=Map("url"->"localhost:9200",
      "paths"->"persion3"
     // "es.write.operation"->"update"
    )

    ESDataSources.writeDS(spark,df,options,"")("001")
  }

}
