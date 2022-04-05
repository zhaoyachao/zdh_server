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

  @Test
  def testGetDSCSV {
    val inputOptions=Map("sep"->"|",
      "url"->"192.168.110.10",
      "paths"->"/a.txt",
      "user"->"zyc",
      "sep"-> "|",
      "password"->"123456",
      "header"->"false",
      "fileType"->"csv"
    )
    val inputCols=Array("name","age")
    val df=FtpDataSources.getDS(spark,null,"ftp",inputOptions,"",inputCols,null,null,null,null,null)("")
    df.show()


  }

  @Test
  def testWriteDSCSV {
    val inputOptions=Map("sep"->"|",
      "url"->"192.168.110.10",
      "paths"->"/a.txt",
      "user"->"zyc",
      "sep"-> "|",
      "password"->"123456",
      "header"->"false",
      "fileType"->"csv"
    )
    val outputOptions=Map(
      "url"->"192.168.110.10",
      "paths"->"/a1.txt",
      "user"->"zyc",
      "sep"-> "||",
      "password"->"123456",
      "header"->"false",
      "fileType"->"csv"
    )
    val inputCols=Array("name","age")
    val df=FtpDataSources.getDS(spark,null,"ftp",inputOptions,"",inputCols,null,null,null,null,null)("")

    FtpDataSources.writeDS(spark,df,outputOptions,null)("")


  }

  @Test
  def testGetDSJSON {
    val inputOptions=Map("sep"->"|",
      "url"->"192.168.110.10",
      "paths"->"b.json",
      "user"->"zyc",
      "sep"-> "|",
      "password"->"123456",
      "header"->"false",
      "fileType"->"json"
    )
    val inputCols=Array("name","age")
    val df=FtpDataSources.getDS(spark,null,"ftp",inputOptions,"",inputCols,null,null,null,null,null)("")
    df.show()
  }

}
