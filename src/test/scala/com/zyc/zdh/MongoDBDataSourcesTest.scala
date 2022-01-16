package com.zyc.zdh

import com.zyc.TEST_TRAIT2
import com.zyc.zdh.datasources.MongoDBDataSources
import org.junit.Test
import org.scalatest.FunSuite
@Test
class MongoDBDataSourcesTest extends  TEST_TRAIT2{
  @Test
  def testGetDS {

    val dispatch=null
    val inputOptions=Map("url"->"mongodb://localhost:27017/admin","paths"->"zyc")
    val df=MongoDBDataSources.getDS(spark,dispatch,"mongodb",inputOptions,null,null,null,null,null,null,
      "delete from t1 where item='canvas'")("001").drop("_id")

      df.show(false)

    val outputOptions=Map("url"->"mongodb://localhost:27017/admin","paths"->"zyc1","model"->"append")
    val df1=MongoDBDataSources.writeDS(spark,df,outputOptions,"delete from t1 where item='canvas'")("001")
   // val readConfig=ReadConfig(Map("uri"->"mongodb://localhost:27017","database"->"admin","collection"->"zyc"))
   // MongoSpark.loadAndInferSchema(spark,readConfig).show(false)
//    val struct=JsonSchemaBuilder.getJsonSchema("_id,item,qty,size.h,size.w,size.uom,tags")
//    val outputOptions=Map("spark.mongodb.output.uri"->"mongodb://localhost:27017/admin","spark.mongodb.output.collection"->"zyc1")
//    df.drop("_id").write.format("mongo").mode(SaveMode.Append).options(outputOptions).save()






  }

}
