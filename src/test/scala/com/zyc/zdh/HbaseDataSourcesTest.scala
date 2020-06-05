package com.zyc.zdh

import com.zyc.TEST_TRAIT2
//import org.apache.hadoop.hbase.spark.datasources.HBaseTableCatalog
import org.scalatest.FunSuite

class HbaseDataSourcesTest extends FunSuite with TEST_TRAIT2{

  test("testGetDS") {

    val table="t1"
    val cols=Array("cf1:name","cf1:age")
    val map=Map("url"->"10.136.1.37,10.136.1.38,10.136.1.39")
  //  HbaseDataSources.getDS(spark,table,map,cols,"0,1")("1").show(false)

  }

  test("testHbaseSHC") {

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

  test("testHbaseNerdammer") {

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

  test("testGetDS2") {

    val table="t1"
    val cols=Array("cf1:name","cf2:age","cf1:sex")
    val map=Map("url"->"192.168.65.10:2181")
   // HbaseDataSources.getDS(spark,table,map,cols,"0,1")("1").show(false)

  }

}
