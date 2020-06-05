package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.FunSuite
import org.apache.spark.sql.functions._

class KuduDataSourcesTest extends FunSuite with TEST_TRAIT2{

  test("testGetDS") {

    import spark.implicits._
    val inputOptions=Map("url"->"192.168.65.10:7051","paths"->"k1")

    val kuduContext = new KuduContext(inputOptions.getOrElse("url",""), spark.sparkContext)

    val kuduTableSchema = StructType(
      StructField("name", StringType, false) ::
        StructField("sex", StringType, true) ::
        StructField("age", IntegerType, true) :: Nil)
    val kuduTableOptions = new CreateTableOptions()
    import scala.collection.JavaConverters._
    kuduTableOptions.setRangePartitionColumns(List("name").asJava).setNumReplicas(1);
    val kuduPrimaryKey = Seq("name")
    if(!kuduContext.tableExists(inputOptions.getOrElse("paths",""))){
      kuduContext.createTable(inputOptions.getOrElse("paths","").toString, kuduTableSchema,kuduPrimaryKey,kuduTableOptions)
    }

    val df=KuduDataSources.getDS(spark,null,"kudu",inputOptions,null,null,null,null,null,null,null)("")

    df.show()

  }

  test("writeDS"){

    import spark.implicits._
    val options=Map("url"->"192.168.65.10:7051","paths"->"k2")

    val kuduContext = new KuduContext(options.getOrElse("url",""), spark.sparkContext)

    val df =spark.range(0,100).select(col("id").cast("string") as "name",col("id").cast("string") as "sex",
      col("id").cast("int") as "age")

    KuduDataSources.writeDS(spark,df,options,"")("")



  }

}
