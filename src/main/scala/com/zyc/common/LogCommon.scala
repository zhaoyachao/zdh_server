package com.zyc.common

import java.sql.Timestamp
import java.util.Date
import java.util.concurrent.LinkedBlockingDeque

case class zhd_logs(id:String,log_time:Timestamp,msg:String,level:String)

object LogCommon {

  val linkedBlockingDeque=new LinkedBlockingDeque[zhd_logs]()



  def info(msg:String,level:String="info")(implicit id:String): Unit ={

    val lon_time=new Timestamp(new Date().getTime)
    linkedBlockingDeque.add(zhd_logs(id,lon_time,msg,level))
    //System.out.println("id:"+id+",log_time:"+lon_time+",msg:"+msg)

  }

  new Thread(new Runnable {
    override def run(): Unit = {

      while (true){
        val log=linkedBlockingDeque.take()
       // MariadbCommon.insertJob(log.id,log.log_time,log.msg,log.level);
      }
    }
  }).start()

}
