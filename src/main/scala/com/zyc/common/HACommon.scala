package com.zyc.common

import java.net.{InetSocketAddress, Socket}

import com.typesafe.config.ConfigFactory
import com.zyc.SystemInit.logger
import com.zyc.base.util.{HttpUtil, JsonUtil}

object HACommon {

  var enable=false
  var port=""
  var zdh_instance=""
  var current_host = ""
  var etcd_cluster =""
  var timeout = 1000*5
  val checkServiceTime=1000*5
  var etcd_host=Array.empty[String]

  val ha_key="/v2/keys/zdh_ha"
  val ha_lock_key="/v2/keys/zdh_ha_lock"
  val ha_preExist="?prevExist=false&value="
  val ha_update="?value="

  /**
    * 返回 true 时 外部可以启动任务
    * @param configFile
    */
  def etcdHA(configFile:String): Boolean ={

    val config = ConfigFactory.load(configFile)
    zdh_instance = config.getString("instance")
    if (config.getConfig("zdh_ha").getBoolean("enable")) {
       enable=true
       port = config.getConfig("server").getString("port")
       current_host = config.getConfig("zdh_ha").getString("current_host")

       etcd_cluster = config.getConfig("zdh_ha").getString("etcd_cluster")
       timeout = config.getConfig("zdh_ha").getInt("timeout")
       etcd_host = etcd_cluster.split(",")

      return HAProcess()
    }
    enable=false
    true
  }


  def HAProcess(): Boolean ={
    //检测到服务死亡
    checkService(true,etcd_host)

    //获取锁
    val lock=getEtcdLock(true)

    while (!lock){
      //否则开始监听锁,返回true 表示监听到了事件
      waitLock(true)
      HAProcess()
    }

    lock
  }

  /**
    * 检测服务
    * @param enable
    * @param etcd_host
    */
  def checkService(enable:Boolean,etcd_host:Array[String]): Unit = {
    println("检测HA")
    if (enable) {

      //默认false 表示不起新服务
      var retry = false
      var cluster_index = 0

      //etcd 请求创建Ha 标识,返回true 标识创建成功,
      val result = putEtcd(etcd_host,  ha_key+ha_preExist+current_host)

      if (result == false) {
        println("项目已经..开始检查远程服务器是否存活")
        retry = isRetry(etcd_host, ha_key, port, timeout)
      }


      while (retry == false) {
        println("检查远程服务器存活")
        Thread.sleep(checkServiceTime)
        retry = isRetry(etcd_host, ha_key, port, timeout)
      }
      println("检查远程服务器不可达...准备启动新服务")

    }
  }

  /**
    * 等待锁
    */
  def waitLock(enable:Boolean): Unit ={

    println("等待锁")
    var change=false
    val config = ConfigFactory.load("application.conf")
    if (enable) {

      var cluster_index = 0
      //url="/v2/keys/zdh_ha?value="+current_host
      var retry = true
      var result = ""
      while (retry) {
        try {
          result = HttpUtil.get("http://" + etcd_host(cluster_index) + ha_lock_key+"?wait=true", Seq.empty)
          retry = false
          change=true
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
            logger.info("请求etcd 出错,重新尝试")
            cluster_index = cluster_index + 1
            if (cluster_index > etcd_host.size) {
              retry = false
            }

          }
        }

      }

    }

    change

  }

  /**
    * 获取锁
    * @param enable
    * @return
    */
  def getEtcdLock(enable:Boolean): Boolean = {
    println("尝试获取锁")
    var lock=false
    if (enable) {
      lock = putEtcd(etcd_host,  ha_lock_key+"?prevExist=false&value=" + current_host)
    }

    lock

  }


  def deleteEtcdLock(): Unit ={

    if(enable){
      deleteEtcd(etcd_host,ha_lock_key)
    }
  }

  def updateEtcd(): Unit ={
    if(enable){
      putEtcd(etcd_host,ha_key+ha_update+current_host)
    }
  }

  /**
    *
    * @param etcd_host
    * @param url
    * @return 返回值表示 是否插入成功
    */
  def putEtcd(etcd_host: Array[String], url: String): Boolean = {
    var cluster_index = 0
    //url="/v2/keys/zdh_ha?value="+current_host
    var retry = true
    var result = ""
    while (retry) {
      try {
        result = HttpUtil.put("http://" + etcd_host(cluster_index) + url, Seq.empty)
        retry = false
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          logger.info("请求etcd 出错,重新尝试")
          cluster_index = cluster_index + 1
          if (cluster_index > etcd_host.size) {
            retry = false
          }

        }
      }

    }


    //判断返回结果
    if (result.equals("") || result.contains("errorCode"))
      return false
    else
      true

  }

  /**
    * 获取kv,
    * @param etcd_host
    * @param url
    * @return
    */
  def getEtcd(etcd_host: Array[String], url: String): String = {
    var cluster_index = 0
    //url="/v2/keys/zdh_ha?value="+current_host
    var retry = true
    var result = "{}"
    while (retry) {
      try {
        result = HttpUtil.get("http://" + etcd_host(cluster_index) + url, Seq.empty)
        retry = false
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          logger.info("请求etcd 出错,重新尝试")
          cluster_index = cluster_index + 1
          if (cluster_index > etcd_host.size) {
            retry = false
          }

        }
      }

    }

    result

  }

  def deleteEtcd(etcd_host: Array[String], url: String): String = {
    println("删除ha 临时锁")
    var cluster_index = 0
    //url="/v2/keys/zdh_ha?value="+current_host
    var retry = true
    var result = "{}"
    while (retry) {
      try {
        result = HttpUtil.delete("http://" + etcd_host(cluster_index) + url, Seq.empty)
        retry = false
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          logger.info("请求etcd 出错,重新尝试")
          cluster_index = cluster_index + 1
          if (cluster_index > etcd_host.size) {
            retry = false
          }

        }
      }

    }

    result

  }

  /**
    * 判断主程序是否重启
    *
    * @param etcd_host
    * @param url
    * @param port
    * @param timeout
    * @return false/true
    */
  def isRetry(etcd_host: Array[String], url: String, port: String, timeout: Int): Boolean = {

    val socket = new Socket();
    val value = getEtcd(etcd_host, url)

    var retry = false
    //获取远程ip
    val remote_host = JsonUtil.jsonToMap(value).getOrElse("node", Map.empty[String, String]).asInstanceOf[Map[String, String]].getOrElse("value", "")

    try {
      //测试远程服务器存活
      println("连接远程服务器ip:"+remote_host+",port:"+port)
      socket.connect(new InetSocketAddress(remote_host, Integer.parseInt(port)), timeout);
    } catch {
      case ex: Exception => {
        println("远程服务器不可达")
        retry = true
      }
    }
    retry


  }
}



