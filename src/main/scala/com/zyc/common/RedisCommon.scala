package com.zyc.common

import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory
import redis.clients.jedis.{HostAndPort, Jedis, JedisCluster, JedisPool, JedisPoolConfig, Pipeline}

object RedisCommon {

  val logger = LoggerFactory.getLogger(this.getClass)

  private var model="";
  var jedis:Jedis=null;
  var jedisCluster:JedisCluster=null;

  def isRedis(): String ={
    model
  }
  def connect(configFile:String): Unit ={
    val config = ConfigFactory.load(configFile).getConfig("redis")
    if(config.getString("model").equalsIgnoreCase("signle")){
      logger.info("初始化单机版本的redis")
      model="signle"
      val Array(host,port)=config.getString("url").split(":",2)
      val passwd=config.getString("password")
      val jedisConf: JedisPoolConfig = new JedisPoolConfig()
      val jedisPool = new JedisPool(jedisConf, host.toString, port.toInt, 10000, passwd)
      jedis=jedisPool.getResource

    }else if(config.getString("model").equalsIgnoreCase("cluster")){
      logger.info("初始化集群版本的is")
      model="cluster"
      val hosts=config.getString("url").split(",")
      val hostAndPortsSet = new java.util.HashSet[HostAndPort]()
      hosts.foreach(host=>{
        val Array(hs,port)=host.split(":",2)
        hostAndPortsSet.add(new HostAndPort(hs, port.toInt))
      })
      jedisCluster = new JedisCluster(hostAndPortsSet)
    }
  }


  def set(key:String,value:String): Unit ={

    if(model.equals("signle")){
      jedis.set(key,value)
    }else if(model.equalsIgnoreCase("cluster")){
      jedisCluster.set(key,value)
    }

  }

  def get(key:String): String ={
    if(model.equals("signle")){
      jedis.get(key)
    }else if(model.equalsIgnoreCase("cluster")){
      jedisCluster.get(key)
    }else{
      ""
    }
  }

  def keys(par:String): java.util.Set[String] ={
    if(model.equals("signle")){
      jedis.keys(par)
    }else if(model.equalsIgnoreCase("cluster")){
      jedisCluster.keys(par)
    }else{
      null
    }
  }

  def pipeline(): Pipeline ={
    if(model.equals("signle")){
    jedis.pipelined()
    }else if(model.equalsIgnoreCase("cluster")){
      throw new Exception("暂时不支持集群事务")
    }else{
      null
    }
  }


}
