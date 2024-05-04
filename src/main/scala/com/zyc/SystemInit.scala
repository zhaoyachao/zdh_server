package com.zyc

import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.typesafe.config.{Config, ConfigFactory}
import com.zyc.base.util.{HttpUtil, JsonUtil}
import com.zyc.common.{MariadbCommon, ServerSparkListener, SparkBuilder}
import com.zyc.netty.NettyServer
import com.zyc.rqueue.{RQueueManager, RQueueMode}
import com.zyc.zdh.ZdhHandler
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory

object SystemInit {

  val logger = LoggerFactory.getLogger(this.getClass)
  //心跳检测路径
  val keeplive_url="/api/v1/zdh/keeplive"

  private val threadpool = new ThreadPoolExecutor(
    1, // core pool size
    1, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable]()
  )

  def main(args: Array[String]): Unit = {
    MDC.put("job_id", "001")
    val configLoader=ConfigFactory.load("application.conf")
    var host = configLoader.getConfig("server").getString("host")
    val port = configLoader.getConfig("server").getString("port")
    val zdh_instance = configLoader.getString("instance")
    val time_interval = configLoader.getString("time_interval").toLong
    val spark_history_server = configLoader.getString("spark_history_server")
    val online = configLoader.getString("online")

    initRQueue(configLoader)

    logger.info("开始初始化SparkSession")
    val spark = SparkBuilder.getSparkSession()
    val uiWebUrl = spark.sparkContext.uiWebUrl.get
    if(host.trim.equals("")){
      host=uiWebUrl.split(":")(1).substring(2)
    }
    val applicationId=spark.sparkContext.applicationId
    val master=spark.sparkContext.master
    spark.sparkContext.master
    try{
      //org.apache.hadoop.conf.Configuration
      MariadbCommon.insertZdhHaInfo(zdh_instance,host , port, uiWebUrl.split(":")(2),applicationId,spark_history_server,master,online)
      logger.info("开始初始化netty server")
      new Thread(new Runnable {
        override def run(): Unit = new NettyServer().start()
      }).start()

      consumer(configLoader)

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
          if(instance.getOrElse("online","0").equalsIgnoreCase("2")){
            if(ServerSparkListener.jobs.size()<=0){
              logger.info("当前节点物理下线成功")
              MariadbCommon.delZdhHaInfo("enabled",host,port)
              System.exit(0)
            }else{
              logger.info("当前节点存在正在执行的任务,任务执行完成,自动物理下线")
            }
          }else if(instance.getOrElse("online","0").equalsIgnoreCase("0")){
            if(ServerSparkListener.jobs.size()<=0){
              logger.info("当前节点逻辑下线成功")
            }else{
              logger.info("当前节点存在正在执行的任务,任务执行完成,自动逻辑下线")
            }
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

  def initRQueue(config: Config): Unit = {
    val url = config.getString("redis.url")
    val auth = config.getString("redis.password")
    RQueueManager.buildDefault(url, auth)
  }

  def consumer(config: Config): Unit ={

    val queue_pre = config.getString("queue.pre_key")
    val instance = config.getString("instance")
    val queue = queue_pre + "_" + instance
    logger.info("加载当前queue: "+queue)
    threadpool.execute(new Runnable {
      override def run(): Unit = {

        //延迟启动30s
        Thread.sleep(1000*30)
        while(true){
          import util.control.Breaks._
          breakable {
            var rqueueClient = RQueueManager.getRQueueClient(queue, RQueueMode.BLOCKQUEUE)
            var o = rqueueClient.poll()
            if(o == null){
              break()
            }

            val param:Map[String, Any] = JsonUtil.jsonToMap(o.toString)
            val dispatchOptions = param.getOrElse("tli", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
            val dispatch_task_id = dispatchOptions.getOrElse("job_id", "001").toString
            val task_logs_id=param.getOrElse("task_logs_id", "001").toString
            val etl_date = JsonUtil.jsonToMap(dispatchOptions.getOrElse("params", "").toString).getOrElse("ETL_DATE", "").toString
            MariadbCommon.updateTaskStatus(task_logs_id, dispatch_task_id, "etl", etl_date, "22")

            try{
              //消费队列,调用
              val more_task = dispatchOptions.getOrElse("more_task", "")
              if(more_task.equals("ETL")){
                ZdhHandler.etl(param)
              }else if(more_task.equals("MORE_ETL")){
                ZdhHandler.moreEtl(param)
              }else if(more_task.toString.toUpperCase.equals("QUALITY")){
                ZdhHandler.quality(param)
              }else if(more_task.toString.toUpperCase.equals("APPLY")){
                ZdhHandler.apply(param)
              }else if(more_task.toString.toUpperCase.equals("DROOLS")){
                ZdhHandler.droolsEtl(param)
              }else if(more_task.toString.toUpperCase.equals("SQL")){
                ZdhHandler.sqlEtl(param)
              }else{
                throw new Exception("未知的more_task: "+more_task)
              }

            }catch {
              case ex:Exception=>{
                logger.error("[数据采集]:[consumer]:[ERROR]:" + ex.getMessage, ex.getCause)
                MariadbCommon.updateTaskStatus2(task_logs_id,dispatch_task_id,dispatchOptions,etl_date)
              }
            }

          }



        }

      }
    })
  }


}
