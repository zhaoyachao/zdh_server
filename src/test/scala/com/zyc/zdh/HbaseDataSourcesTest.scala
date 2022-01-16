package com.zyc.zdh

import com.zyc.TEST_TRAIT2
import com.zyc.zdh.datasources.HbaseDataSources
import org.junit.Test
//import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.apache.spark.sql.functions._
import org.scalatest.FunSuite

@Test
class HbaseDataSourcesTest extends TEST_TRAIT2{
  @Test
  def testGetDS {

    val table="t1"
    val cols=Array("cf1:name","cf1:age")
    val map=Map("url"->"127.0.0.1","paths"->"t1")
    HbaseDataSources.getDS(spark,null,"",map,"0,1",cols,null,null,null,Array.empty[Map[String,String]],"")("1").show(false)

  }
    @Test
  def testHbaseSHC {

    val table="t1"
    val cols=Array("cf1:name")
  //  HbaseDataSources.hbaseSHC(spark,table,"",null,cols,"rowkey=002")("1")


    val colStr = cols.map(col => {
      val cf = col.split(":")(0)
      val colName = col.split(":")(1)
      s"""|"$colName":{"cf":"$cf","col":"$colName","type":"string"}  """
    }).mkString(",")
    val catalog =
      s"""{
         |"table":{"namespace":"default", "name":"$table"},
         |"rowkey":"key",
         |"columns":{
         |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
         ${colStr}
         |}
         |}""".stripMargin

//        spark.read
//          .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
//          .format("org.apache.spark.sql.execution.datasources.hbase")
//          .load()
//      .show()



  }
  @Test
  def testHbaseNerdammer {

    val table="t1"
    val cols=Array("cf1:name")
//  //  HbaseDataSources.hbaseNerdammer(spark,table,"",null,cols,"rowkey=002")("1")
//
//        import it.nerdammer.spark.hbase._
//
//        import spark.implicits._
//
//        spark.conf.set("spark.hbase.host", "192.168.65.10") //e.g. 192.168.1.1 or localhost or your hostanme
//
//        // For Example If you have an HBase Table as 'Document' with ColumnFamily 'SMPL' and qualifier as 'DocID, Title' then:
//
//        val docRdd = spark.sparkContext.hbaseTable[(Option[String], Option[String])](table)
//          .select("cf1:name","cf1:name")
//          .withStartRow("0")
//          .withStopRow("0")
//        docRdd.map(f=>f._1.get).toDF().show()

  }
  @Test
  def testGetDS2 {

    val table="t1"
    val cols=Array("cf1:name","cf2:age","cf1:sex")
    val map=Map("url"->"192.168.65.10:2181")
   // HbaseDataSources.getDS(spark,table,map,cols,"0,1")("1").show(false)

  }
  @Test
  def loadHFile {
    val options=Map("paths"->"t1")


    val df=spark.range(0,100).select(concat(col("id"),lit("a")) as "row_key",lit("1a") as "cf1:index",lit("zyc") as "cf1:name",lit("zhaoyachao") as "cf2:user")

    HbaseDataSources.writeHFile(spark,df,options)("001")

  }

}
