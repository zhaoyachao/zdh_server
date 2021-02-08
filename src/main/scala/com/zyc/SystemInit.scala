package com.zyc

import com.typesafe.config.ConfigFactory
import com.zyc.base.util.HttpUtil
import com.zyc.common.{HACommon, MariadbCommon, ServerSparkListener, SparkBuilder}
import com.zyc.netty.NettyServer
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory

object SystemInit {

  val logger = LoggerFactory.getLogger(this.getClass)
  //心跳检测路径
  val keeplive_url="/api/v1/zdh/keeplive"

  def main(args: Array[String]): Unit = {
    MDC.put("job_id", "001")
    val configLoader=ConfigFactory.load("application.conf")
    val port = configLoader.getConfig("server").getString("port")
    val zdh_instance = configLoader.getString("instance")
    val time_interval = configLoader.getString("time_interval").toLong
    val spark_history_server = configLoader.getString("spark_history_server")
    val online = configLoader.getString("online")

    logger.info("开始初始化SparkSession")
    val spark = SparkBuilder.getSparkSession()
    val uiWebUrl = spark.sparkContext.uiWebUrl.get
    val host=uiWebUrl.split(":")(1).substring(2)
    val applicationId=spark.sparkContext.applicationId
    val master=spark.sparkContext.master
    spark.sparkContext.master
    try{

      MariadbCommon.insertZdhHaInfo(zdh_instance,host , port, uiWebUrl.split(":")(2),applicationId,spark_history_server,master,online)
      logger.info("开始初始化netty server")
      new Thread(new Runnable {
        override def run(): Unit = new NettyServer().start()
      }).start()

      while (true){
        val list=MariadbCommon.getZdhHaInfo()

        list.filter(map=> !map.getOrElse("zdh_host","").equals(host) || !map.getOrElse("zdh_port","").equals(port))
          .foreach(map=>{
            val remote_host=map.getOrElse("zdh_host","")
            val remote_port=map.getOrElse("zdh_port","")
            val remote_url="http://"+remote_host+":"+remote_port+keeplive_url
            try{
              val rs=HttpUtil.get(remote_url,Seq.empty[(String,String)])
            }catch {
              case ex:Exception=>MariadbCommon.delZdhHaInfo(map.getOrElse("id","-1"))
            }
          })

        if(list.filter(map=> map.getOrElse("zdh_host","").equals(host) && map.getOrElse("zdh_port","").equals(port)).size<1){
          logger.debug("当前节点丢失,重新注册当前节点")
          MariadbCommon.insertZdhHaInfo(zdh_instance,host , port, uiWebUrl.split(":")(2),applicationId,spark_history_server,master,online)
        }else{
          logger.debug("当前节点存在,更新当前节点")
          val instance=list.filter(map=> map.getOrElse("zdh_host","").equals(host) && map.getOrElse("zdh_port","").equals(port))(0)
          val id=instance.getOrElse("id","-1")
          MariadbCommon.updateZdhHaInfoUpdateTime(id)
          if(instance.getOrElse("online","false").equalsIgnoreCase("false")){
            if(ServerSparkListener.jobs.size()<=0){
              logger.info("当前节点下线成功")
              System.exit(0)
            }
            logger.info("当前节点存在正在执行的任务,任务执行完成,自动下线")
          }

        }

        Thread.sleep(time_interval*1000)
      }


    }catch {
      case ex:Exception=>{
        logger.error(ex.getMessage)
      }
    }finally {
      MariadbCommon.delZdhHaInfo("enabled",host, port)
    }


  }


}
