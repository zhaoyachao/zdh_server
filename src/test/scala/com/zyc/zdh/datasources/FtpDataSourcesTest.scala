package com.zyc.zdh.datasources

import com.zyc.TEST_TRAIT2
import org.junit.Test
import org.scalatest.FunSuite
@Test
class FtpDataSourcesTest extends  TEST_TRAIT2{

  @Test
  def testGetDS {
    val inputOptions=Map("sep"->"|",
      "url"->"10.136.1.217",
    "paths"->"/app/zyc/zyc.txt",
      "user"->"zyc",
      "password"->"123456",
      "header"->"false"
    )
    val inputCols=Array("name","sex","age")
    val df=SFtpDataSources.getDS(spark,null,"ftp",inputOptions,"",inputCols,null,null,null,null,null)("")
      df.show()
    val options=Map("sep"->",",
      "url"->"10.136.1.217",
      "paths"->"/app/zyc/zyc1.txt",
      "user"->"zyc",
      "password"->"123456",
      "header"->"false"
    )

    SFtpDataSources.writeDS(spark,df,options,"")("")

  }

}
