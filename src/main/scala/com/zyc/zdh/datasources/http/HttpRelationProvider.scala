package com.zyc.zdh.datasources.http

import com.zyc.base.util.JsonSchemaBuilder
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.sources.{CreatableRelationProvider, DataSourceRegister, RelationProvider}

class HttpRelationProvider extends CreatableRelationProvider
  with RelationProvider with DataSourceRegister{

  override def shortName() = "http"

  override def createRelation(sqlContext: SQLContext, mode: SaveMode, parameters: Map[String, String], data: DataFrame) = {

    println("http参数1:"+parameters.mkString(","))
    createRelation(sqlContext,parameters)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]) = {
    import sqlContext.implicits._
    println("http参数2:"+parameters.mkString(","))
    val httpOptions = new HttpOptions(parameters)
    val schema=JsonSchemaBuilder.getJsonSchema(httpOptions.getSchema())
    val parts = null
    HttpRelation(schema, parts, httpOptions)(sqlContext.sparkSession)
  }

}
