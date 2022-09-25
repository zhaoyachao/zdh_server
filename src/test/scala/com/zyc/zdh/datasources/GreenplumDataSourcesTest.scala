package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.junit.Test


class GreenplumDataSourcesTest extends TEST_TRAIT2 {

    @Test
    def getDS(): Unit = {
      implicit val id="001"
      val options =Map(
        "url"->"jdbc:postgresql://192.168.110.10:5432/postgres",
        //"dbschema"-> "public",
        "dbtable"-> "t1",
        "user"-> "zyc",
        "password"-> "123456")
      val df=spark.read.format("greenplum")
        .options(options).load()

      df.show(false)

    }
}