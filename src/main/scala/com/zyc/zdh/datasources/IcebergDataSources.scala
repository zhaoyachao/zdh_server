package com.zyc.zdh.datasources

import com.zyc.zdh.ZdhDataSources
import org.apache.hadoop.hdfs.server.namenode.ha.proto.HAZKInfoProtos
import org.apache.iceberg.{BaseMetastoreCatalog, PartitionSpec, Schema}
import org.apache.iceberg.catalog.{Namespace, TableIdentifier}
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.iceberg.types.Types
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.zookeeper.ZooKeeper
import org.apache.zookeeper.data.Stat
import org.slf4j.LoggerFactory

/**
  * 读写iceberg
  */
object IcebergDataSources extends ZdhDataSources{

  val logger=LoggerFactory.getLogger(this.getClass)

  override def getSchema(spark: SparkSession, options: Map[String, String])(implicit dispatch_task_id:String): Array[StructField] = {
    logger.info(s"获取iceberg表的schema信息table:${options.getOrElse("table","")},option:${options.mkString(",")}")
    spark.table(options.getOrElse("table","")).schema.fields
  }

  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String],duplicateCols:Array[String], outPut: String, outputOptionions: Map[String, String], outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {
    try{
      logger.info("[数据采集]:输入源为[ICEBERG]")
      val tableName=inputOptions.getOrElse("tableName","").toString
      if(tableName.trim.equals("")){
        throw new Exception("[zdh],iceberg数据源读取:tableName为空")
      }

      // 获取元数据类型hadoop,hive
      val driver: String = inputOptions.getOrElse("driver", "hadoop").toString

      val url: String = inputOptions.getOrElse("url", "").toString
      if(url.trim.equals("")){
        throw new Exception("[zdh],iceberg数据源读取:url为空")
      }

      var path=url
      if(driver.equalsIgnoreCase("hadoop")){
        //如何元数据是hadoop,需判断是指定的hadoop地址还是zk地址
        if (url.contains(",")) {
          logger.info("[zdh],iceberg数据源读取:检测多个为zk连接,连接zk")
          val cluster = url.split("/").last
          val zk_url=url.split("/")(0)
          val zk=new ZooKeeper(zk_url,10000,null)
          val hadoop_ha=s"/hadoop-ha/${cluster}/ActiveStandbyElectorLock"
          val bytes=zk.getData(hadoop_ha,true,new Stat())
          val info=HAZKInfoProtos.ActiveNodeInfo.parseFrom(bytes)
          path=s"hdfs://${info.getHostname}:${info.getPort}"
          logger.info(s"[zdh],hdfs数据源读取:连接zk获取hdfs地址为:${path}")
        }
        path=path+"/"+tableName
      }else if(driver.equalsIgnoreCase("hive")){
        path=tableName
      }

      logger.info("[数据采集]:[ICEBERG]:[READ]:[table]:"+path+"[FILTER]:"+inputCondition)
      val df=spark.read.format("iceberg").load(path)
      filter(spark,df,inputCondition,duplicateCols)

    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[ICEBERG]:[READ]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }
  }

  override def process(spark: SparkSession, df: DataFrame, select: Array[Column],zdh_etl_date:String)(implicit dispatch_task_id:String): DataFrame ={
    try{
      logger.info("[数据采集]:[ICEBERG]:[SELECT]")
      logger.debug("[数据采集]:[ICEBERG]:[SELECT]:"+select.mkString(","))
      if(select==null || select.isEmpty){
        logger.debug("[数据采集]:[ICEBERG]:[SELECT]:[智能识别字段]" +df.columns.mkString(","))
        return df
      }
      df.select(select: _*)
    }catch {
      case ex:Exception=>{
        logger.error("[数据采集]:[ICEBERG]:[SELECT]:[ERROR]"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

  override def writeDS(spark: SparkSession,df:DataFrame,options: Map[String,String], sql: String="")(implicit dispatch_task_id:String): Unit = {
    try{
      logger.info("[数据采集]:[ICEBERG]:[WRITE]:[options]:"+options.mkString(","))

      //默认是append
      val model=options.getOrElse("model","").toString.toLowerCase match {
        case "overwrite"=>SaveMode.Overwrite
        case "append"=>SaveMode.Append
        case "errorifexists"=>SaveMode.ErrorIfExists
        case "ignore"=>SaveMode.Ignore
        case _=>SaveMode.Append
      }

      //如果需要建立外部表需要options中另外传入path 参数 example 外部表t1 path:/dir1/dir2/t1
      val format="iceberg"
      val tableName=options.getOrElse("paths","")
      val partitionBy=options.getOrElse("partitionBy","")

      //获取元数据类型是hadoop,hive

      val driver=options.getOrElse("driver","hadoop")
      val url: String = options.getOrElse("url", "").toString

      var path=url
      if(driver.equalsIgnoreCase("hadoop")){
        //如何元数据是hadoop,需判断是指定的hadoop地址还是zk地址
        if (url.contains(",")) {
          logger.info("[zdh],iceberg数据源读取:检测多个为zk连接,连接zk")
          val cluster = url.split("/").last
          val zk_url=url.split("/")(0)
          val zk=new ZooKeeper(zk_url,10000,null)
          val hadoop_ha=s"/hadoop-ha/${cluster}/ActiveStandbyElectorLock"
          val bytes=zk.getData(hadoop_ha,true,new Stat())
          val info=HAZKInfoProtos.ActiveNodeInfo.parseFrom(bytes)
          path=s"hdfs://${info.getHostname}:${info.getPort}"
          logger.info(s"[zdh],hdfs数据源读取:连接zk获取hdfs地址为:${path}")
        }
        path=path+"/"+tableName
      }else if(driver.equalsIgnoreCase("hive")){
        path=tableName
      }

      if(url.trim.equals("")){
        throw new Exception("[zdh],iceberg数据源读取:url为空")
      }

      //合并小文件操作
      var df_tmp = merge(spark,df,options)

      val get_t = (typename:String)=>{
        typename.toLowerCase match {
          case "string" => Types.StringType.get()
          case "integer" => Types.IntegerType.get()
          case "byte" => Types.IntegerType.get()
          case "short" => Types.IntegerType.get()
          case "long" => Types.LongType.get()
          case "float" => Types.FloatType.get()
          case "double" => Types.DoubleType.get()
          case "decimal" => Types.DecimalType.of(16,4)
          case "binary" => Types.BinaryType.get()
          case "boolean" => Types.BooleanType.get()
          case "date" => Types.DateType.get()
          case "timestamp" => Types.DateType.get()
          case "map" => Types.MapType.ofOptional(0,1,Types.StringType.get(),Types.StringType.get())
        }

      }

      var name: TableIdentifier = TableIdentifier.of(tableName)
      var tname=Array("default", tableName)
      if(tableName.contains("/")){
        tname=tableName.split("\\/",2)
        logger.info("[数据采集]:[ICEBERG]:[WRITE]:database:"+tname(0)+",table:"+tname(1))
        name= TableIdentifier.of(tname(0),tname(1))
      }

      val schema = SparkSchemaUtil.convert(df_tmp.schema)
      var catalog:BaseMetastoreCatalog = null

      if(driver.equalsIgnoreCase("hadoop")){
        catalog = new HadoopCatalog(spark.sparkContext.hadoopConfiguration,path)
        path=path+"/"+tableName
      }else if(driver.equalsIgnoreCase("hive")){
        catalog = new HiveCatalog(spark.sparkContext.hadoopConfiguration)
        path=tableName
      }

      if(!catalog.tableExists(name)){
        var spec:PartitionSpec=PartitionSpec.unpartitioned()
        if(!partitionBy.equalsIgnoreCase("")){
          var pt_schema = new Schema(partitionBy.split(",").zipWithIndex.map(f=>Types.NestedField.required(f._2,f._1,Types.StringType.get())):_*)
          spec=PartitionSpec.builderFor(pt_schema).build()
        }
        if(!catalog.asInstanceOf[HadoopCatalog].namespaceExists(Namespace.of(tname(0)))){
          catalog.asInstanceOf[HadoopCatalog].createNamespace(Namespace.of(tname(0)))
        }
        catalog.createTable(name, schema, spec)
      }

      if(partitionBy.equals("")){
        df_tmp.write.mode(model).format(format).options(options).save(path)
      }else{
        df_tmp.write.mode(model).format(format).partitionBy(partitionBy).options(options).save(tableName)
      }


    }catch {
      case ex:Exception=>{
        ex.printStackTrace()
        logger.error("[数据采集]:[ICEBERG]:[WRITE]:[ERROR]:"+ex.getMessage.replace("\"","'"))
        throw ex
      }
    }

  }

}
