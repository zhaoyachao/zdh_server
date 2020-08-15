package com.zyc

import com.typesafe.config.ConfigFactory
import com.zyc.common.{HACommon, MariadbCommon, SparkBuilder}
import com.zyc.netty.NettyServer
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory

object SystemInit {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    MDC.put("job_id", "001")
    val configLoader=ConfigFactory.load("application.conf")
    val port = configLoader.getConfig("server").getString("port")
    val zdh_instance = configLoader.getString("instance")
    logger.info("开始初始化SparkSession")
    val spark = SparkBuilder.getSparkSession()
    val uiWebUrl = spark.sparkContext.uiWebUrl.get
    try{
      MariadbCommon.insertZdhHaInfo(zdh_instance, uiWebUrl.split(":")(1).substring(2), port, uiWebUrl.split(":")(2))
      logger.info("开始初始化netty server")
      new NettyServer().start()
    }catch {
      case ex:Exception=>{

      }
    }finally {
      MariadbCommon.updateZdhHaInfo(zdh_instance, uiWebUrl.split(":")(0), port, uiWebUrl.split(":")(1))
    }


  }


}
