package com.zyc.zdh.datasources

import java.net.InetSocketAddress
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.zyc.base.util.JsonSchemaBuilder
import com.zyc.zdh.{DataSources, ZdhDataSources}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.flume.FlumeUtils
import org.slf4j.LoggerFactory

//Kafka 	spark-streaming-kafka-0-10_2.12
//Flume 	spark-streaming-flume_2.12
//Kinesis
//spark-streaming-kinesis-asl_2.12 [Amazon Software License]

object FlumeDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  val flumeInstance = new java.util.concurrent.ConcurrentHashMap[String, StreamingContext]()

  //程线程池
  private val threadpool = new ThreadPoolExecutor(
    1, // core pool size
    10, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable]()
  )

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:输入源为[FLUME],开始匹配对应参数")
    val brokers = inputOptions.getOrElse("url", "")
    if (brokers.equals("")) {
      throw new Exception("[zdh],flume数据源读取:url为空")
    }

    if (!outPut.toLowerCase.equals("jdbc")) {
      throw new Exception("[zdh],flume数据源读取:输出数据源只支持jdbc数据源,请修改输出数据源为jdbc")
    }


    createFlumeDataSources(spark, brokers, "", "", inputOptions, inputCols,outPut, outputCols, outputOptionions, inputCondition,sql)

    null
  }

  def createFlumeDataSources(spark: SparkSession, brokers: String, topics: String, groupId: String, options: Map[String, String], cols: Array[String],
                             outPut:String,
                             outputCols: Array[Map[String, String]], outputOptions: Map[String, String],
                             inputCondition: String,sql:String)(implicit dispatch_task_id: String): Unit = {
    logger.info("[数据采集]:[FLUME]:[READ]:其他参数:," + options.mkString(",") + " [FILTER]:" + inputCondition)
    //获取jdbc 配置
    if (flumeInstance.size() < 10) {

      threadpool.execute(new Runnable {
        override def run(): Unit = {
          import org.apache.spark.streaming._
          import spark.implicits._
          val ssc = new StreamingContext(spark.sparkContext, Seconds(5))
          flumeInstance.put(dispatch_task_id, ssc)

          val address = brokers.split(",").map(f => new InetSocketAddress(f.split(":")(0), f.split(":")(1).toInt)).toSeq

          val stream = FlumeUtils.createPollingStream(ssc, address, StorageLevel.MEMORY_ONLY_SER_2)
          val sep = options.getOrElse("sep", ",")
          //判断消息类型
          val msgType = options.getOrElse("msgType", "csv")
          //多行模式
          val multiline = options.getOrElse("multiline", "false")

          val ncols = cols.zipWithIndex.map(f => trim(col("value").getItem(f._2)) as f._1)
          val message = stream.map(f => new String(f.event.getBody.array()))

          message.foreachRDD(rdd => {
            var tmp: DataFrame = null
            if (msgType.equals("csv")) {
              tmp = rdd.map(f=>f.split(sep)).toDF("value").select(ncols: _*)
            } else {
              val schema = JsonSchemaBuilder.getJsonSchema(cols.mkString(","))
              import spark.implicits._
              val outCols = outputCols.map(f => expr(f.getOrElse("column_expr", "")) as f.getOrElse("column_alias", ""))
              tmp = rdd.toDF("value").as[String].select(from_json(col("value").cast("string"), schema) as "data")
                .select($"data.*").select(outCols: _*)
            }

            if (tmp != null && !tmp.isEmpty)
              DataSources.outPutHandler(spark, tmp, outPut, outputOptions, outputCols, sql)
          })

          ssc.start()
          ssc.awaitTermination()
        }
      })


    } else {

    }

  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id: String): DataFrame = {
    null
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    logger.info("[数据采集]:[FLUME]:[WRITE]:")
    throw new Exception("[数据采集]:[FLUME]:[WRITE]:[ERROR]:不支持写入flume数据源")

  }

}
