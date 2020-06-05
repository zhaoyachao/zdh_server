package com.zyc

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

trait TEST_TRAIT2 {
  val spark = getSparkSession()

  val date_s = "20191108"


  def getSparkSession(): SparkSession = {

    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[5]")
    sparkConf.setAppName("Spark demo")
    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    sparkConf.set("spark.executor.instances", "13")
    sparkConf.set("spark.executor.cores", "3")
    sparkConf.set("spark.repartition.min.num", "2")
    sparkConf.set("spark.repartition.normal.num", "5")
    sparkConf.set("spark.repartition.big.num", "10")
    sparkConf.set("spark.repartition.huge.num", "50")
    sparkConf.set("spark.sql.orc.impl", "native")
    sparkConf.set("spark.sql.orc.enableVectorizedReader", "true")
    sparkConf.set("spark.sql.crossJoin.enabled", "true")
    sparkConf.set("spark.sql.shuffle.partitions", "20")
    sparkConf.set("spark.default.parallelism", "3")
    //sparkConf.set("spark.sql.codegen.maxFields", "1000")
    //sparkConf.set("spark.sql.codegen.fallback", "true")
    //sparkConf.set("spark.sql.codegen.hugeMethodLimit",(65535*2).toString)
    sparkConf.set("spark.sql.sources.partitionOverwriteMode","dynamic")
    // sparkConf.set("spark.driver.extraJavaOptions", "-Dlog4j.configuration=file:/log4j.properties")

    //    sparkConf.registerKryoClasses(Array(classOf[Hold]))
    val sparkSession = SparkSession
      .builder()
      .appName("Spark Zdh Report")
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()
    //sparkSession.sparkContext.setLogLevel("INFO")
    sparkSession
  }



}
