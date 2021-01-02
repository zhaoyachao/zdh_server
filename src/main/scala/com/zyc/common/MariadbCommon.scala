package com.zyc.common

import java.sql.{Connection, DriverManager, Timestamp}
import java.util
import java.util.{Calendar, Date, Properties}

import com.zyc.base.util.{DateUtil, JsonUtil}
import org.slf4j.LoggerFactory

/**
  * airflow 相关操作通用表
  */
object MariadbCommon {

  val logger = LoggerFactory.getLogger(this.getClass)

  var connection: Connection = null

  /**
    * 任务状态插入
    *
    */
  def insertJob(id:String,log_time:Timestamp,msg:String,level:String): Unit = {

    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }

    try {
      logger.info(s"开始插入zdh_logs:${id},日志时间:${log_time},日志:${msg}")
      //ETL_DATE,MODEL_NAME,STATUS,START_TIME,END_TIME
      val sql = s"insert into zdh_logs (job_id,log_time,msg,level) values(?,?,?,?)"
      val statement = connection.prepareStatement(sql)
      statement.setString(1, id)
      statement.setTimestamp(2, log_time)
      statement.setString(3,msg)
      statement.setString(4,level)
      statement.execute()
      logger.info(s"完成插入zdh_logs:${id},日志时间:${log_time}")
    } catch {
      case ex: Exception => {
        logger.error("ZDH_LOGS插入数据时出现错误", ex.getCause)
        throw ex
      }
    }

  }


  def insertZdhHaInfo(zdh_instance:String,zdh_host:String,zdh_port:String,web_port:String,applicationId:String,spark_history_server:String,master:String): Unit ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    try {
      delZdhHaInfo("enabled",zdh_host,zdh_port)
      logger.info(s"开始插入zdh_ha_info:${zdh_instance},IP:${zdh_host},PORT:${zdh_port},WEB_PORT:${web_port}")
      //ETL_DATE,MODEL_NAME,STATUS,START_TIME,END_TIME
      val sql = s"insert into zdh_ha_info (zdh_instance,zdh_url,zdh_host,zdh_port,web_port,zdh_status,application_id,history_server,master) values(?,?,?,?,?,?,?,?,?)"
      val statement = connection.prepareStatement(sql)
      statement.setString(1, zdh_instance)
      statement.setString(2, "http://"+zdh_host+":"+zdh_port+"/api/v1/zdh")
      statement.setString(3, zdh_host)
      statement.setString(4,zdh_port)
      statement.setString(5,web_port)
      statement.setString(6,"enabled")
      statement.setString(7,applicationId)
      statement.setString(8,spark_history_server)
      statement.setString(9,master)
      statement.execute()
      logger.info(s"完成插入zdh_ha_info:${zdh_instance},IP:${zdh_host},PORT:${zdh_port},WEB_PORT:${web_port}")
    } catch {
      case ex: Exception => {
        logger.error("ZDH_HA_INFO插入数据时出现错误", ex.getCause)
        throw ex
      }
    }

  }

  def updateZdhHaInfo(zdh_instance:String,zdh_host:String,zdh_port:String,web_port:String,status:String="enabled"): Unit ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    try {
      logger.info(s"开始更新zdh_ha_info状态为enabled")
      val update_sql=s"update zdh_ha_info set zdh_status='disabled' where zdh_status='${status}' and zdh_instance=? and zdh_url=? and zdh_host=?" +
        " and zdh_port=? and web_port=?"
      val stat_update=connection.prepareStatement(update_sql)
      stat_update.setString(1,zdh_instance)
      stat_update.setString(2,"http://"+zdh_host+":"+zdh_port+"/api/v1/zdh")
      stat_update.setString(3,zdh_host)
      stat_update.setString(4,zdh_port)
      stat_update.setString(5,web_port)
      stat_update.execute()
      logger.info(s"完成更新zdh_ha_info")

    } catch {
      case ex: Exception => {
        logger.error("ZDH_HA_INFO插入数据时出现错误", ex.getCause)
        throw ex
      }
    }

  }

  def delZdhHaInfo(id:String): Unit ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    try {
      logger.info(s"开始删除zdh_ha_info,id:${id}")
      val update_sql=s"delete from zdh_ha_info where zdh_status='enabled' and id=${id} "
      val stat_update=connection.prepareStatement(update_sql)
      stat_update.execute()
      logger.info(s"删除zdh_ha_info")

    } catch {
      case ex: Exception => {
        logger.error("ZDH_HA_INFO插入数据时出现错误", ex.getCause)
        throw ex
      }
    }
  }

  def delZdhHaInfo(status:String,host:String,port:String): Unit ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    try {
      logger.info(s"开始删除zdh_ha_info")
      val update_sql=s"delete from zdh_ha_info where zdh_status='${status}' and zdh_host='${host}' and zdh_port='${port}' "
      val stat_update=connection.prepareStatement(update_sql)
      stat_update.execute()
      logger.info(s"删除zdh_ha_info")

    } catch {
      case ex: Exception => {
        logger.error("ZDH_HA_INFO插入数据时出现错误", ex.getCause)
        throw ex
      }
    }
  }

  def updateZdhHaInfoUpdateTime(id:String): Unit ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    try {
      logger.info(s"开始更新zdh_ha_info")
      val update_sql=s"update zdh_ha_info set update_time= '${DateUtil.getCurrentTime()}'"
      val stat_update=connection.prepareStatement(update_sql)
      stat_update.execute()
      logger.info(s"更新zdh_ha_info")

    } catch {
      case ex: Exception => {
        logger.error("ZDH_HA_INFO更新数据时出现错误", ex.getCause)
        throw ex
      }
    }
  }

  def getZdhHaInfo(): Seq[Map[String,String]] ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    try {
      logger.debug(s"开始查询zdh_ha_info状态为enabled")
      val query_sql=s"select * from zdh_ha_info where zdh_status='enabled'"
      val stat_query=connection.prepareStatement(query_sql)
      val resultSet=stat_query.executeQuery()
      val list=new util.ArrayList[Map[String,String]]()
      while (resultSet.next()) {
        list.add(Map("zdh_host"->resultSet.getString("zdh_host"),"zdh_port"->resultSet.getString("zdh_port"),"id"->resultSet.getString("id")))
      }
      import collection.mutable._
      import collection.JavaConverters._
      list.asScala.toSeq
    } catch {
      case ex: Exception => {
        logger.error("ZDH_HA_INFO插入数据时出现错误", ex.getCause)
        throw ex
      }
    }
  }

  def insertZdhDownloadInfo(file_name:String,etl_date:Timestamp,owner:String,job_context:String): Unit ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }
    try {
      logger.info(s"开始更新zdh_download_info状态为")
      val insert_sql="insert into zdh_download_info(file_name,create_time,down_count,etl_date,owner,job_context) values(?,?,?,?,?,?) "
      val stat_insert=connection.prepareStatement(insert_sql)
      stat_insert.setString(1, file_name)
      stat_insert.setTimestamp(2,new Timestamp(new Date().getTime))
      stat_insert.setInt(3, 0)
      stat_insert.setTimestamp(4,etl_date)
      stat_insert.setString(5,owner)
      stat_insert.setString(6,job_context)
      stat_insert.execute()
      logger.info(s"完成更新zdh_download_info")

    } catch {
      case ex: Exception => {
        logger.error("zdh_download_info插入数据时出现错误", ex.getCause)
        throw ex
      }
    }

  }

  def updateTaskStatus(task_logs_id:String,quartzJobInfo_job_id:String,last_status:String,etl_date:String,process:String): Unit ={
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }

    try {
      logger.info(s"开始更新调度任务状态:${quartzJobInfo_job_id},状态:${last_status}")
      //ETL_DATE,MODEL_NAME,STATUS,START_TIME,END_TIME
//      val sql = s"update quartz_job_info set last_status=? where job_id=?"
//      val statement = connection.prepareStatement(sql)
//      statement.setString(1, last_status)
//      statement.setString(2, quartzJobInfo_job_id)
//      statement.execute()
//      statement.close()

      logger.info(s"开始更新任务日志状态:${task_logs_id},状态:${last_status}")
      var sql2 = s"update task_log_instance set status=? , process=? ,update_time= ? ,server_ack='1' where job_id=? and etl_date=? and id=?"
      if(process==null || process.equals("")){
        sql2 = s"update task_log_instance set status=? ,update_time= ? ,server_ack='1' where job_id=? and etl_date=? and id=?"
        val statement2 = connection.prepareStatement(sql2)
        statement2.setString(1, last_status)
        statement2.setTimestamp(2, new Timestamp(new Date().getTime))
        statement2.setString(3, quartzJobInfo_job_id)
        statement2.setString(4, etl_date)
        statement2.setString(5, task_logs_id)
        statement2.execute()
        statement2.close()
      }else{
        val statement2 = connection.prepareStatement(sql2)
        statement2.setString(1, last_status)
        statement2.setString(2,process)
        statement2.setTimestamp(3, new Timestamp(new Date().getTime))
        statement2.setString(4, quartzJobInfo_job_id)
        statement2.setString(5, etl_date)
        statement2.setString(6, task_logs_id)
        statement2.execute()
        statement2.close()
      }


      logger.info(s"完成更新:${quartzJobInfo_job_id},状态:${last_status}")
    } catch {
      case ex: Exception => {
        logger.error("quartz_job_info更新数据时出现错误", ex.getCause)
        throw ex
      }
    }

  }

  def updateTaskStatus2(task_logs_id:String,quartzJobInfo_job_id:String,dispatchOption: Map[String, Any],etl_date:String): Unit ={
    var status = "error"
    try{
      var msg = "ETL任务失败存在问题,重试次数已达到最大,状态设置为error"
      if (dispatchOption.getOrElse("plan_count","3").toString.equalsIgnoreCase("-1") || dispatchOption.getOrElse("plan_count","3").toString.toLong > dispatchOption.getOrElse("count","1").toString.toLong) { //重试
        status = "wait_retry"
        msg = "ETL任务失败存在问题,状态设置为wait_retry等待重试"
        if (dispatchOption.getOrElse("plan_count","3").toString.equalsIgnoreCase("-1")) msg = msg + ",并检测到重试次数为无限次"
      }
      logger.info(msg)
      val interval_time = if(dispatchOption.getOrElse("interval_time","").toString.equalsIgnoreCase("")) 5 else dispatchOption.getOrElse("interval_time","5").toString.toInt
      val retry_time=DateUtil.add(new Timestamp(new Date().getTime()),Calendar.SECOND,interval_time);
      if (connection == null){
        synchronized {
          logger.info("数据库未初始化连接,尝试初始化连接")
          if (connection == null) {
            if(!getConnect())
              throw new Exception("connection mariadb fail,could not get connection ")
          }
          logger.info("数据库完成初始化连接")
        }
      }

      if (connection != null && !connection.isValid(5000)) {
        logger.info("数据库连接失效,尝试重新连接")
        connection.close()
        if (!getConnect())
          throw new Exception("connection mariadb fail,could not get connection ")
        logger.info("数据库连接失效,重连成功")
      }

      if(status.equals("wait_retry")){
        var sql3 = s"update task_log_instance set status=?,retry_time=?,update_time= ? where job_id=? and etl_date=? and id=?"
        val statement2 = connection.prepareStatement(sql3)
        statement2.setString(1, status)
        statement2.setTimestamp(2, retry_time)
        statement2.setTimestamp(3, new Timestamp(new Date().getTime))
        statement2.setString(4, quartzJobInfo_job_id)
        statement2.setString(5, etl_date)
        statement2.setString(6, task_logs_id)
        statement2.execute()
        statement2.close()

//        val sql = s"update quartz_job_info set last_status=? where job_id=?"
//        val statement = connection.prepareStatement(sql)
//        statement.setString(1, status)
//        statement.setString(2, quartzJobInfo_job_id)
//        statement.execute()
//        statement.close()

      }else{
        updateTaskStatus(task_logs_id, quartzJobInfo_job_id, status, etl_date,"")
      }
    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        updateTaskStatus(task_logs_id, quartzJobInfo_job_id, status, etl_date,"")
      }
    }



  }

  def insertQuality(task_log_id:String,dispatch_task_id:String,etl_task_id:String,etl_date:String,report:Map[String,String],owner:String): Unit ={
    //调度id,etl 任务id,调度日期,生成报告日期
    if (connection == null){
      synchronized {
        logger.info("数据库未初始化连接,尝试初始化连接")
        if (connection == null) {
          if(!getConnect())
            throw new Exception("connection mariadb fail,could not get connection ")
        }
        logger.info("数据库完成初始化连接")
      }
    }

    if (connection != null && !connection.isValid(5000)) {
      logger.info("数据库连接失效,尝试重新连接")
      connection.close()
      if (!getConnect())
        throw new Exception("connection mariadb fail,could not get connection ")
      logger.info("数据库连接失效,重连成功")
    }

    try {

      logger.info(s"开始生成质量检测报告:${dispatch_task_id},状态:${report.getOrElse("result","").toString}")
      //ETL_DATE,MODEL_NAME,STATUS,START_TIME,END_TIME
      val sql = s"insert into quality(id,dispatch_task_id,etl_task_id,etl_date,status,report,create_time,owner) values(?,?,?,?,?,?,?,?)"
      val statement = connection.prepareStatement(sql)
      statement.setString(1, task_log_id)
      statement.setString(2, dispatch_task_id)
      statement.setString(3, etl_task_id)
      statement.setString(4, etl_date)
      statement.setString(5, report.getOrElse("result","").toString)
      statement.setString(6, JsonUtil.toJson(report))
      statement.setTimestamp(7,new Timestamp(new Date().getTime))
      statement.setString(8, owner)
      statement.execute()
      statement.close()
      logger.info(s"完成质量检测报告:${dispatch_task_id}")
    } catch {
      case ex: Exception => {
        logger.error("quality生成质量报告数据时出现错误", ex.getCause)
        throw ex
      }
    }


  }

  /**
    * 获取数据库连接
    */
  def getConnect(): Boolean = {

    val props = new Properties();
    val inStream = this.getClass.getResourceAsStream("/datasources.propertites")
    props.load(inStream)
    if(props.getProperty("enable").equals("false")){
      logger.info("读取配置文件datasources.propertites,但是发现未启用此配置enable is false")
      return false
    }

    val url = props.getProperty("url")
    //驱动名称
    val driver = props.getProperty("driver")
    //用户名
    val username = props.getProperty("username")
    //密码
    val password = props.getProperty("password")


    try {
      //注册Driver
      Class.forName(driver)
      //得到连接
      connection = DriverManager.getConnection(url, username, password)
      true
    } catch {
      case ex: Exception => {
        logger.error("连接airflow时出现错误", ex.getCause)
        throw ex
      }
    }
  }

}
