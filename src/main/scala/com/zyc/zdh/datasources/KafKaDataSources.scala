package com.zyc.zdh.datasources

import java.util.Properties
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.zyc.base.util.JsonSchemaBuilder
import com.zyc.common.RedisCommon
import com.zyc.zdh.{DataSources, ZdhDataSources}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.streaming.StreamingContext
import org.slf4j.LoggerFactory
import redis.clients.jedis.Pipeline

import scala.collection.JavaConverters

//Kafka 	spark-streaming-kafka-0-10_2.12
//Flume 	spark-streaming-flume_2.12
//Kinesis
//spark-streaming-kinesis-asl_2.12 [Amazon Software License]

object KafKaDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)

  val kafkaInstance = new java.util.concurrent.ConcurrentHashMap[String, StreamingContext]()

  //程线程池
  private val threadpool = new ThreadPoolExecutor(
    1, // core pool size
    10, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable]()
  )

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols: Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:输入源为[KAFKA],开始匹配对应参数")
    val brokers = inputOptions.getOrElse("url", "")
    if (brokers.equals("")) {
      throw new Exception("[zdh],kafka数据源读取:url为空")
    }
    val topics = inputOptions.getOrElse("paths", "")
    if (topics.equals("")) {
      throw new Exception("[zdh],kafka数据源读取:paths为空,请在界面配置表名或者文件名")
    }

    if (!outPut.toLowerCase.equals("jdbc")) {
      throw new Exception("[zdh],kafka数据源读取:输出数据源只支持jdbc数据源,请修改输出数据源为jdbc")
    }

    val groupId = inputOptions.getOrElse("groupId", "g1")


    createKafkaDataSources(spark, brokers, topics, groupId, inputOptions, inputCols, outPut, outputCols, outputOptionions, inputCondition, sql)

    null
  }

  def createKafkaDataSources(spark: SparkSession, brokers: String, topics: String, groupId: String, options: Map[String, String], cols: Array[String], outPut: String, outputCols: Array[Map[String, String]], outputOptions: Map[String, String],
                             inputCondition: String, sql: String)(implicit dispatch_task_id: String): Unit = {
    logger.info("[数据采集]:[KAFKA]:[READ]:TOPIC:" + topics + ",其他参数:," + options.mkString(",") + " [FILTER]:" + inputCondition)
    //获取jdbc 配置
    if (kafkaInstance.size() < 10) {

      threadpool.execute(new Runnable {
        override def run(): Unit = {

          if (RedisCommon.isRedis().isEmpty) {
            logger.info("[数据采集]:[KAFKA]:[READ]:不使用第三方存储offset")
            defOffset(spark, brokers, topics, groupId, options, cols, outPut, outputCols, outputOptions, inputCondition, sql)
          } else {
            logger.info("[数据采集]:[KAFKA]:[READ]:使用redis存储offset")
            redisOffset(spark, brokers, topics, groupId, options, cols, outPut, outputCols, outputOptions, inputCondition, sql)
          }
        }
      })
    } else {

    }

  }

  def redisOffset(spark: SparkSession, brokers: String, topics: String, groupId: String, options: Map[String, String], cols: Array[String],
                  outPut: String,
                  outputCols: Array[Map[String, String]], outputOptions: Map[String, String],
                  inputCondition: String, sql: String)(implicit dispatch_task_id: String): Unit = {
    import org.apache.spark.streaming._
    import org.apache.spark.streaming.kafka010._
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    kafkaInstance.put(dispatch_task_id, ssc)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    var sep = options.getOrElse("sep", ",")
    if (sep.size > 1) {
      sep = sepConvert(sep)
    }
    //判断消息类型
    val msgType = options.getOrElse("msgType", "csv")
    //多行模式
    val multiline = options.getOrElse("multiline", "false")


    var kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    kafkaParams = options ++ kafkaParams

    //设置每个分区起始的Offset
    import scala.collection.JavaConverters._
    val fromOffsets = topicsSet.map(topic => {
      val keys = RedisCommon.keys(topic + "_*")
      val fromOffsets_tmp = keys.toArray.zipWithIndex.map(p => {

        val offset: Int = RedisCommon.get(topic + "_" + p._2) match {
          case null => 0
          case "" => 0
          case a => a.toInt
        }
        (new TopicPartition(topic, p._2) -> offset.asInstanceOf[java.lang.Long])
      })
      fromOffsets_tmp
    }).flatten.toMap
    logger.info("获取offset 下标:" + fromOffsets.mkString(","))
    val fromOffsets2 = JavaConverters.mapAsJavaMapConverter(fromOffsets).asJava


    val p1: Pipeline = RedisCommon.pipeline()
    p1.multi() //开启事务
    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet.asJavaCollection, kafkaParams.asJava, fromOffsets2))
    val ncols = cols.zipWithIndex.map(f => col("value").getItem(f._2) as f._1)
    var ds: Array[DataFrame] = Array.empty[DataFrame]

    messages.foreachRDD(message => {
      val offsetRanges = message.asInstanceOf[HasOffsetRanges].offsetRanges
      import spark.implicits._
      var tmp: DataFrame = null
      if (msgType.equals("csv")) {
        tmp = message.map(_.value()).map(f => f.split(sep)).toDF("value").select(ncols: _*)
      } else {
        val schema = JsonSchemaBuilder.getJsonSchema(cols.mkString(","))
        import spark.implicits._
        val outCols = outputCols.map(f => expr(f.getOrElse("column_expr", "")) as f.getOrElse("column_alias", ""))
        tmp = message.map(_.value()).toDF("value").as[String].select(from_json(col("value").cast("string"), schema) as "data")
          .select($"data.*").select(outCols: _*)
        //spark.read.options(options).json(rdd.toDF("value").as[String])
      }

      if (tmp != null && !tmp.isEmpty)
        DataSources.outPutHandler(spark, tmp, outPut, outputOptions, outputCols, sql)

      offsetRanges.foreach { offsetRange =>
        println("partition : " + offsetRange.partition + " fromOffset:  " + offsetRange.fromOffset + " untilOffset: " + offsetRange.untilOffset)
        val topic_partition_key = offsetRange.topic + "_" + offsetRange.partition
        p1.set(topic_partition_key, offsetRange.untilOffset + "")
      }
      p1.exec()
      p1.sync()

    })


    ssc.start()
    ssc.awaitTermination()


  }

  def defOffset(spark: SparkSession, brokers: String, topics: String, groupId: String, options: Map[String, String], cols: Array[String],
                outPut: String,
                outputCols: Array[Map[String, String]], outputOptions: Map[String, String],
                inputCondition: String, sql: String)(implicit dispatch_task_id: String): Unit = {
    import org.apache.spark.streaming._
    import org.apache.spark.streaming.kafka010._
    val ssc = new StreamingContext(spark.sparkContext, Seconds(2))
    kafkaInstance.put(dispatch_task_id, ssc)

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet
    var sep = options.getOrElse("sep", ",")
    if (sep.size > 1) {
      sep = sepConvert(sep)
    }

    //判断消息类型
    val msgType = options.getOrElse("msgType", "csv")
    //多行模式
    val multiline = options.getOrElse("multiline", "false")


    var kafkaParams = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer])

    kafkaParams = options ++ kafkaParams

    val messages = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
    val ncols = cols.zipWithIndex.map(f => col("value").getItem(f._2) as f._1)
    var ds: Array[DataFrame] = Array.empty[DataFrame]

    messages.foreachRDD(message => {
      val offsetRanges = message.asInstanceOf[HasOffsetRanges].offsetRanges
      import spark.implicits._
      var tmp: DataFrame = null
      if (msgType.equals("csv")) {
        tmp = message.map(_.value()).map(f => f.split(sep)).toDF("value").select(ncols: _*)
      } else {
        val schema = JsonSchemaBuilder.getJsonSchema(cols.mkString(","))
        import spark.implicits._
        val outCols = outputCols.map(f => expr(f.getOrElse("column_expr", "")) as f.getOrElse("column_alias", ""))
        tmp = message.map(_.value()).toDF("value").as[String].select(from_json(col("value").cast("string"), schema) as "data")
          .select($"data.*").select(outCols: _*)
        //spark.read.options(options).json(rdd.toDF("value").as[String])
      }

      if (tmp != null && !tmp.isEmpty)
        DataSources.outPutHandler(spark, tmp, outPut, outputOptions, outputCols, sql)
    })


    ssc.start()
    ssc.awaitTermination()

  }

  def sepConvert(sep: String): String = {
    var sep_tmp = sep.replace("\\", "\\\\")
    if (sep_tmp.contains('$')) {
      sep_tmp = sep_tmp.replace("$", "\\$")
    }
    if (sep_tmp.contains('(') || sep_tmp.contains(')')) {
      sep_tmp = sep_tmp.replace("(", "\\(").replace(")", "\\)")
    }
    if (sep_tmp.contains('*')) {
      sep_tmp = sep_tmp.replace("*", "\\*")
    }
    if (sep_tmp.contains('+')) {
      sep_tmp = sep_tmp.replace("+", "\\+")
    }
    if (sep_tmp.contains('-')) {
      sep_tmp = sep_tmp.replace("-", "\\-")
    }
    if (sep_tmp.contains('[') || sep_tmp.contains(']')) {
      sep_tmp = sep_tmp.replace("[", "\\[").replace("]", "\\]")
    }
    if (sep_tmp.contains('{') || sep_tmp.contains('}')) {
      sep_tmp = sep_tmp.replace("{", "\\{").replace("}", "\\}")
    }
    if (sep_tmp.contains('^')) {
      sep_tmp = sep_tmp.replace("^", "\\^")
    }
    if (sep_tmp.contains('|')) {
      sep_tmp = sep_tmp.replace("|", "\\|")
    }
    sep_tmp
  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column], zdh_etl_date: String)(implicit dispatch_task_id: String): DataFrame = {
    null
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    try {
      spark.sparkContext.setLocalProperty(DataSources.SPARK_ZDH_LOCAL_PROCESS,"OUTPUT")
      logger.info("[数据采集]:[KAFKA]:[WRITE]:topic:" + options.getOrElse("paths", "") + "," + options.mkString(","))
      val url = options.getOrElse("url", "")
      if (url.equals("")) {
        throw new Exception("KAFKA写入数据源时无法找到对应的KAFKA服务器,请检查KAFKA数据源连接串")
      }
      val topic = options.getOrElse("paths", "")
      if (topic.equals("")) {
        throw new Exception("KAFKA写入数据源时无法找到对应的KAFKA-TOPIC,请检查ETL任务中文件名或者表名")
      }

      val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
        val kafkaProducerConfig = {
          val p = new Properties()
          p.setProperty("bootstrap.servers", url)
          p.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          p.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          p
        }
        spark.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
      }
      df.foreachPartition(rdd => {
        rdd.foreach(record => {
          kafkaProducer.value.send(topic, record.getAs[Any]("key").toString, record.getAs[Any]("value").toString)
        })
      })
    } catch {
      case ex: Exception => {
        logger.info("[数据采集]:[KAFKA]:[WRITE]:TOPIC:" + options.getOrElse("paths", "") + "," + "[ERROR]:" + ex.getMessage.replace("\"", "'"))
        throw ex
      }
    }


  }

}

class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): java.util.concurrent.Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, key, value))

  def send(topic: String, value: V): java.util.concurrent.Future[RecordMetadata] =
    producer.send(new ProducerRecord[K, V](topic, value))
}

object KafkaSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}
