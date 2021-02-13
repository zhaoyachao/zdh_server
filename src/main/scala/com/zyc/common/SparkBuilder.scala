package com.zyc.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBuilder{
  private var sparkSession:SparkSession=null;
  def initSparkSession(): Unit ={
    val sparkConf = new SparkConf()
    val system = System.getProperty("os.name");
    if(system.toLowerCase().startsWith("win")){
      sparkConf.setMaster("local[*]")
    }
    //sparkConf.setAppName("Spark Shenzhen SERVER")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    sparkConf.set("hive.orc.splits.include.file.footer","true")
//    sparkConf.set("hive.exec.orc.default.stripe.size","268435456")
//    sparkConf.set("hive.exec.orc.split.strategy", "BI")
//    sparkConf.set("spark.sql.orc.impl","hive")
//    sparkConf.set("spark.sql.hive.convertMetastoreOrc","false")
//    sparkConf.set("spark.sql.orc.enableVectorizedReader","false")
    sparkConf.set("spark.sql.crossJoin.enabled","true")
    sparkConf.set("spark.extraListeners", classOf[ServerSparkListener].getName)
//    sparkConf.set("spark.sql.shuffle.partitions","2000")
//    sparkConf.set("spark.sql.extensions","org.apache.spark.sql.TiExtensions")
//    sparkConf.set("spark.tispark.pd.addresses","192.168.110.10:2379")
    val sparkSession = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
   // sparkSession.sql("set spark.sql.orc.filterPushdown=true")
    this.sparkSession=sparkSession
  }


  def getSparkSession(): SparkSession ={
    synchronized{
      if(sparkSession==null){
        initSparkSession()
      }
    }
    sparkSession
  }
}
