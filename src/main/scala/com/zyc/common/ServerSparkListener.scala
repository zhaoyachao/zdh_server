package com.zyc.common

import com.zyc.zdh.DataSources
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobEnd, SparkListenerJobStart, SparkListenerStageCompleted, SparkListenerTaskEnd, StageInfo}

object ServerSparkListener{

  //存储jobid 和进度
  val jobs=new java.util.concurrent.ConcurrentHashMap[Int,Int]
  //job->stage
  val stages=new java.util.concurrent.ConcurrentHashMap[Int,Seq[StageInfo]]
  //job->tasknum
  val tasks=new java.util.concurrent.ConcurrentHashMap[Int,Int]
  // stage->job
  val stage_job=new java.util.concurrent.ConcurrentHashMap[Int,Int]
  // job -> task_log_instance
  val job_tli=new java.util.concurrent.ConcurrentHashMap[Int,String]


}

class ServerSparkListener extends SparkListener {

  //INPUT 25,50
  //OUTPUT 50,100

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    jobStart.properties.keySet().toArray.foreach(key=> println(key+"==="+jobStart.properties.getProperty(key.toString)))
    var PROCESS=jobStart.properties.getProperty(DataSources.SPARK_ZDH_PROCESS)
    println("Process:"+PROCESS)
    if(PROCESS == null){
      PROCESS=jobStart.properties.getProperty(DataSources.SPARK_ZDH_LOCAL_PROCESS, "OUTPUT")
    }
    val pro_num:Int = PROCESS match {
      case "INPUT" => 25
      case "OUTPUT" => 61
    }
    //获取对应的task_log_instance id
    val tli_id=jobStart.properties.getProperty("spark.jobGroup.id").split("_")(0)
    if(tli_id == null || tli_id.length() != 18){
      return ;
    }
    ServerSparkListener.job_tli.put(jobStart.jobId,tli_id)
    MariadbCommon.updateTaskStatus3(tli_id,pro_num)
    ServerSparkListener.jobs.put(jobStart.jobId,pro_num)
    val total_tasks=jobStart.stageInfos.map(stage => stage.numTasks).sum
    ServerSparkListener.stages.put(jobStart.jobId,jobStart.stageInfos)
    ServerSparkListener.tasks.put(jobStart.jobId,total_tasks)
    jobStart.stageIds.map(sid=> ServerSparkListener.stage_job.put(sid,jobStart.jobId))
  }

  override def onJobEnd(jobEnd: SparkListenerJobEnd): Unit = {
    if(!ServerSparkListener.jobs.containsKey(jobEnd.jobId)){
     return ;
    }
    ServerSparkListener.jobs.remove(jobEnd.jobId)
    ServerSparkListener.stages.get(jobEnd.jobId).foreach(stage=>{
      ServerSparkListener.stage_job.remove(stage.stageId)
    })
    ServerSparkListener.tasks.remove(jobEnd.jobId)
    ServerSparkListener.job_tli.remove(jobEnd.jobId)
    ServerSparkListener.stages.remove(jobEnd.jobId)
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    println("stategId:"+taskEnd.stageId)
  }


  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId=stageCompleted.stageInfo.stageId
    if(!ServerSparkListener.stage_job.containsKey(stageId)){
      return ;
    }
    //stage 获取job
    val job_id=ServerSparkListener.stage_job.get(stageId)
    //获取所有的任务数
    val total_tasks=ServerSparkListener.tasks.get(job_id)
    //获取job_id对应的task_log_instance id
    val tli_id=ServerSparkListener.job_tli.get(job_id)
    val new_pro_num=ServerSparkListener.jobs.get(job_id)+35*(stageCompleted.stageInfo.numTasks/total_tasks)
    ServerSparkListener.jobs.put(job_id,new_pro_num)
    MariadbCommon.updateTaskStatus3(tli_id,new_pro_num)
  }




}
