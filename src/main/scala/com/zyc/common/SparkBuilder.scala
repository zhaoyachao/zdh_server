package com.zyc.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkBuilder{
  private var sparkSession:SparkSession=null;
  def initSparkSession(): Unit ={
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[7]")
    //sparkConf.setAppName("Spark Shenzhen SERVER")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    sparkConf.set("hive.orc.splits.include.file.footer","true")
//    sparkConf.set("hive.exec.orc.default.stripe.size","268435456")
//    sparkConf.set("hive.exec.orc.split.strategy", "BI")
//    sparkConf.set("spark.sql.orc.impl","hive")
//    sparkConf.set("spark.sql.hive.convertMetastoreOrc","false")
//    sparkConf.set("spark.sql.orc.enableVectorizedReader","false")
    sparkConf.set("spark.sql.crossJoin.enabled","true")
//    sparkConf.set("spark.sql.shuffle.partitions","2000")
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
