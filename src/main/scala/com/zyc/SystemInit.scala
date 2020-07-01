package com.zyc

import com.typesafe.config.ConfigFactory
import com.zyc.common.{HACommon, MariadbCommon, SparkBuilder}
import com.zyc.netty.NettyServer
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory

object SystemInit {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    MDC.put("job_id","001")
    //检测到服务死亡
    // 1 创建一个临时目录 谁创建成功 谁就获得锁,其他未获得锁的 监控这个临时目录,当这个临时目录消失时,再次监控服务
    //拿锁---拿到锁的启动项目,未拿到锁的暂时不能监控
    //为拿到锁的 需要监控一个节点
    //启动项目后,其他未拿到锁的才能监控
    if (HACommon.etcdHA("application.conf")){
      logger.info("开始初始化SparkSession")
      val spark = SparkBuilder.getSparkSession()
      logger.info("开始初始化netty server")
      new Thread(new Runnable {
        override def run(): Unit = {
          Thread.sleep(5000)
          println("开始删除锁")
          HACommon.updateEtcd()
          HACommon.deleteEtcdLock()
        }
      }).start()
      val web_port=spark.conf.get("spark.ui.port","4040")
      if(!HACommon.current_host.equals("")){
        MariadbCommon.insertZdhHaInfo(HACommon.zdh_instance,HACommon.current_host,HACommon.port,web_port)
      }else{
        val config = ConfigFactory.load("application.conf")
        val port=config.getConfig("server").getString("port")
        MariadbCommon.insertZdhHaInfo(HACommon.zdh_instance,"127.0.0.1",port,web_port)
      }
      new NettyServer().start()

    }
  }


}
