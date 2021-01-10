package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.scalatest.FunSuite

class TidbDataSourcesTest extends FunSuite with TEST_TRAIT2{

  test("getDS"){

    var tiDBOptions=Map[String,String]("tidb.user"->"root",
    "tidb.password"->"",
      "tidb.addr"->"192.168.110.10",
      "tidb.port" -> "4000"
    )

    //spark.conf.set("spark.sql.extensions","org.apache.spark.sql.TiExtensions")
    //spark.conf.set("spark.tispark.pd.addresses","192.168.110.10:2379")
    val df=spark.sql("select * from d1.t1")

//    val df=spark.read.format("tidb")
//      .options(tiDBOptions)
//      .option("database","d1")
//      .option("table","t1")
//      .load()

    df.show()

    df.write.
      format("tidb").
      option("tidb.user", "root").
      option("tidb.password", "").
      option("database", "d1").
      option("table", "t2").
      options(tiDBOptions).
      mode("overwrite").
      save()

  }


  test("jdbc"){

    var tiDBOptions=Map[String,String]("tidb.user"->"root",
      "tidb.password"->"",
      "tidb.addr"->"192.168.110.10",
      "tidb.port" -> "4000"
    )
    val inputOptions=Map(
      "driver"->"com.mysql.cj.jdbc.Driver",
      "url"->"jdbc:mysql://192.168.110.10:4000",
      "dbtable"->"d1.t1",
      "user"->"root",
      "password"->"",
      "numPartitions"->"1",
      "isolationLevel"->"NONE",
      "useSSL"->"false"
    )

    val df=spark.read.format("jdbc").options(inputOptions).load()

    df.write.
      format("jdbc").
      options(inputOptions).
      option("dbtable","d1.t3").
      mode("overwrite").
      save()

  }
}
