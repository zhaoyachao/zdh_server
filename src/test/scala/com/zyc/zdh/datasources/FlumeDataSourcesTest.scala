package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.scalatest.FunSuite

class FlumeDataSourcesTest extends FunSuite with TEST_TRAIT2{

  test("testGetDS") {

    import spark.implicits._

    val inputOptions=Map("url"->"localhost:9999")
    val inputCols=Array("name","age")

    val output="jdbc"

    val outputOptions=Map( "url"->"jdbc:mysql://127.0.0.1:3306/mydb?serverTimezone=GMT%2B8",
      "driver"->"com.mysql.cj.jdbc.Driver",
      "dbtable"->"flume_t1",
      "user"->"zyc",
      "password"->"123456")



    FlumeDataSources.getDS(spark,null,"",inputOptions,"",inputCols,null,output,outputOptions,null,null)("001")

    while(true){

    }

  }

}
