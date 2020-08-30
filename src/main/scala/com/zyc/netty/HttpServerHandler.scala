package com.zyc.netty

import java.net.URLDecoder
import java.util.Date
import java.util.concurrent.{LinkedBlockingQueue, ThreadPoolExecutor, TimeUnit}

import com.zyc.base.util.JsonUtil
import com.zyc.common.SparkBuilder
import com.zyc.zdh.DataSources
import com.zyc.zdh.datasources.{DataWareHouseSources, FlumeDataSources, KafKaDataSources}
import io.netty.channel.{ChannelFutureListener, ChannelHandlerContext, ChannelInboundHandlerAdapter}
import io.netty.handler.codec.http._
import io.netty.util.CharsetUtil
import org.apache.commons.lang.StringEscapeUtils
import org.apache.log4j.MDC
import org.slf4j.LoggerFactory
import org.apache.spark.sql.functions._


class HttpServerHandler extends ChannelInboundHandlerAdapter with HttpBaseHandler {
  val logger = LoggerFactory.getLogger(this.getClass)


  //单线程线程池，同一时间只会有一个线程在运行,保证加载顺序
  private val threadpool = new ThreadPoolExecutor(
    1, // core pool size
    1, // max pool size
    500, // keep alive time
    TimeUnit.MILLISECONDS,
    new LinkedBlockingQueue[Runnable]()
  )

  override def channelRead(ctx: ChannelHandlerContext, msg: scala.Any): Unit = {
    println("接收到netty 消息:时间" + new Date(System.currentTimeMillis()))
    val request = msg.asInstanceOf[FullHttpRequest]
    val keepAlive = HttpUtil.isKeepAlive(request)
    val response = diapathcer(request)
    if (keepAlive) {
      response.headers().set(Connection, KeepAlive)
      ctx.writeAndFlush(response)
    } else {
      ctx.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE)
    }
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    ctx.writeAndFlush(defaultResponse(serverErr)).addListener(ChannelFutureListener.CLOSE)
    //    logger.error(cause.getMessage)
    //    logger.error("error:", cause)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush
  }

  /**
    * 分发请求
    *
    * @param request
    * @return
    */
  def diapathcer(request: FullHttpRequest): HttpResponse = {

    try {
      val uri = request.uri()
      //数据采集请求
      val param = getReqContent(request)

      val dispatchOptions = param.getOrElse("quartzJobInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
      val dispatch_task_id = dispatchOptions.getOrElse("job_id", "001").toString
      MDC.put("job_id", dispatch_task_id)
      logger.info(s"接收到请求uri:$uri")

      logger.info(s"接收到请求uri:$uri,参数:${param.mkString(",").replaceAll("\"", "")}")
      if (uri.contains("/api/v1/zdh/more")) {
        moreEtl(param)
      } else if (uri.contains("/api/v1/zdh/sql")) {
        sqlEtl(param)
      } else if(uri.contains("/api/v1/zdh/drools")){
        droolsEtl(param)
      }else if (uri.contains("api/v1/zdh/show_databases")) {
        val spark = SparkBuilder.getSparkSession()
        val result = DataWareHouseSources.show_databases(spark)
        defaultResponse(result)
      } else if (uri.contains("api/v1/zdh/show_tables")) {
        val spark = SparkBuilder.getSparkSession()
        val databaseName = param.getOrElse("databaseName", "default").toString
        val result = DataWareHouseSources.show_tables(spark, databaseName)
        defaultResponse(result)
      } else if (uri.contains("api/v1/zdh/desc_table")) {
        val spark = SparkBuilder.getSparkSession()
        val table = param.getOrElse("table", "").toString
        val result = DataWareHouseSources.desc_table(spark, table)
        defaultResponse(result)
      } else if (uri.contains("/api/v1/zdh/keeplive")) {
        defaultResponse(cmdOk)
      } else if (uri.contains("/api/v1/zdh")) {
        etl(param)
      } else if (uri.contains("/api/v1/del")) {
        val param = getReqContent(request)
        val key = param.getOrElse("job_id", "")
        logger.info("删除实时任务:" + key)
        if (param.getOrElse("del_type", "").equals("kafka")) {
          if (KafKaDataSources.kafkaInstance.containsKey(key)) {
            KafKaDataSources.kafkaInstance.get(key).stop(false)
            KafKaDataSources.kafkaInstance.remove(key)
          }
        } else {
          if (FlumeDataSources.flumeInstance.containsKey(key)) {
            FlumeDataSources.flumeInstance.get(key).stop(false)
            FlumeDataSources.flumeInstance.remove(key)
          }
        }
        defaultResponse(cmdOk)
      } else {
        defaultResponse(noUri)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        defaultResponse(noUri)
      }
    } finally {
      MDC.remove("job_id")
    }

  }

  private def getBody(content: String): Map[String, Any] = {
    JsonUtil.jsonToMap(content)
  }

  private def getParam(uri: String): Map[String, Any] = {
    val path = URLDecoder.decode(uri, chartSet)
    val cont = uri.substring(path.lastIndexOf("?") + 1)
    if (cont.contains("="))
      cont.split("&").map(f => (f.split("=")(0), f.split("=")(1))).toMap[String, Any]
    else
      Map.empty[String, Any]
  }

  private def getReqContent(request: FullHttpRequest): Map[String, Any] = {
    request.method() match {
      case HttpMethod.GET => getParam(request.uri())
      case HttpMethod.POST => getBody(request.content.toString(CharsetUtil.UTF_8))
    }
  }


  private def moreEtl(param: Map[String, Any]): DefaultFullHttpResponse = {

    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_EtlInfo = param.getOrElse("dsi_EtlInfo", List.empty[Map[String, Map[String, Any]]]).asInstanceOf[List[Map[String, Map[String, Any]]]]

    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl任务信息
    val etlMoreTaskInfo = param.getOrElse("etlMoreTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //调度任务信息
    val dispatchOptions = param.getOrElse("quartzJobInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlMoreTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val outPutCols = null

    //清空语句
    val clear = etlMoreTaskInfo.getOrElse("data_sources_clear_output", "").toString

    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandlerMore(spark, task_logs_id, dispatchOptions, dsi_EtlInfo, etlMoreTaskInfo, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)

  }

  private def sqlEtl(param: Map[String, Any]): DefaultFullHttpResponse = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //etl任务信息
    val sqlTaskInfo = param.getOrElse("sqlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("quartzJobInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val inputCols: Array[String] = sqlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output

    //过滤条件
    val filter = sqlTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = sqlTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }


    //清空语句
    val clear = sqlTaskInfo.getOrElse("data_sources_clear_output", "").toString


    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandlerSql(spark, task_logs_id, dispatchOptions, sqlTaskInfo, inPut, inPutBaseOptions ++ inputOptions, outPut,
            outPutBaseOptions ++ outputOptions, null, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)
  }

  private def etl(param: Map[String, Any]): DefaultFullHttpResponse = {
    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString
    //输入数据源信息
    val dsi_Input = param.getOrElse("dsi_Input", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //etl任务信息
    val etlTaskInfo = param.getOrElse("etlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]
    //调度任务信息
    val dispatchOptions = param.getOrElse("quartzJobInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输入数据源类型
    val inPut = dsi_Input.getOrElse("data_source_type", "").toString

    //输入数据源基础信息
    val inPutBaseOptions = dsi_Input

    //输入数据源其他信息k:v,k1:v1 格式
    val inputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_input", "").toString.trim match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val inputCols: Array[String] = etlTaskInfo.getOrElse("data_sources_file_columns", "").toString.split(",")

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output

    //过滤条件
    val filter = etlTaskInfo.getOrElse("data_sources_filter_input", "").toString


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    //字段映射
    //List(Map(column_alias -> id, column_expr -> id))
    val list_map = etlTaskInfo.getOrElse("column_data_list", null).asInstanceOf[List[Map[String, String]]]
    val outPutCols = list_map.toArray
    //val outPutCols=list_map.map(f=> expr(f.getOrElse("column_expr","")).as(f.getOrElse("column_alias",""))).toArray

    //清空语句
    val clear = etlTaskInfo.getOrElse("data_sources_clear_output", "").toString


    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandler(spark, task_logs_id, dispatchOptions, etlTaskInfo, inPut, inPutBaseOptions ++ inputOptions, filter, inputCols, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)
  }


  private def droolsEtl(param: Map[String, Any]): DefaultFullHttpResponse ={

    //此处任务task_logs_id
    val task_logs_id = param.getOrElse("task_logs_id", "001").toString

    //输入数据源信息
    val dsi_EtlInfo = param.getOrElse("dsi_EtlInfo",List.empty[Map[String, Map[String, Any]]]).asInstanceOf[List[Map[String, Map[String, Any]]]]

    //输出数据源信息
    val dsi_Output = param.getOrElse("dsi_Output", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl任务信息
    val etlDroolsTaskInfo = param.getOrElse("etlDroolsTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl-多源任务信息
    val etlMoreTaskInfo = param.getOrElse("etlMoreTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //etl-sql任务信息
    val sqlTaskInfo = param.getOrElse("sqlTaskInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //调度任务信息
    val dispatchOptions = param.getOrElse("quartzJobInfo", Map.empty[String, Any]).asInstanceOf[Map[String, Any]]

    //输出数据源类型
    val outPut = dsi_Output.getOrElse("data_source_type", "").toString

    //输出数据源基础信息
    val outPutBaseOptions = dsi_Output


    //输出数据源其他信息k:v,k1:v1 格式
    val outputOptions: Map[String, Any] = etlDroolsTaskInfo.getOrElse("data_sources_params_output", "").toString match {
      case "" => Map.empty[String, Any]
      case a => JsonUtil.jsonToMap(a)
    }

    val outPutCols = null

    //清空语句
    val clear = etlDroolsTaskInfo.getOrElse("data_sources_clear_output", "").toString

    threadpool.execute(new Runnable() {
      override def run() = {
        try {
          val spark = SparkBuilder.getSparkSession()
          DataSources.DataHandlerDrools(spark, task_logs_id, dispatchOptions, dsi_EtlInfo, etlDroolsTaskInfo,etlMoreTaskInfo,sqlTaskInfo, outPut,
            outPutBaseOptions ++ outputOptions, outPutCols, clear)
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        }
      }
    })
    defaultResponse(cmdOk)

  }
}
