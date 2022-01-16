package com.zyc.zdh

import com.zyc.TEST_TRAIT2
import com.zyc.zdh.datasources.RedisDataSources
import org.junit.Test
import org.scalatest.FunSuite
@Test
class RedisDataSourcesTest extends  TEST_TRAIT2{
  @Test
  def testGetDS {
    import spark.implicits._
    val options=Map("url"->"127.0.0.1:6379",
    "paths"->"persion",
      "data_type"->"hash",
      "password"->"yld"
    )
    val inputCols=Array("name","age")
//    spark.conf.set("spark.redis.host", "localhost")
//    spark.conf.set("spark.redis.port", "6379")
//    spark.conf.set("spark.redis.auth", "yld")

    //RedisDataSources.getDS(spark,null,null,options,null,inputCols,null,null,null,null)("").show()


    val options2=Map("url"->"127.0.0.1:6379",
      "paths"->"persion",
      "data_type"->"table",
      "password"->"yld",
      "key.column"->"id"
    )

    val df=Seq((1,"zyc",20),(2,"abc",30)).toDF("id","name","age")
    RedisDataSources.writeDS(spark,df,options2,"")("")
  }

}
