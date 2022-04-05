package com.zyc.zdh.datasources

import java.util

import com.zyc.common.LogCommon
import com.zyc.zdh.{DataSources, ZdhDataSources}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.{HFileOutputFormat, LoadIncrementalHFiles, TableInputFormat, TableMapReduceUtil}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.Partitioner
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.slf4j.LoggerFactory

import scala.collection.mutable

object HbaseDataSources extends ZdhDataSources {

  val logger = LoggerFactory.getLogger(this.getClass)
  val defFamily="cf1"

  /**
    *
    * @param spark
    * @param dispatchOption
    * @param inPut
    * @param inputOptions   参数-必须包含 url 表示hbase 对应的zk的主机,paths 表示hbase 查询的表名
    * @param inputCondition rowkey 格式:起始,结束 examp: 0001,0005
    * @param inputCols      输入字段 cf:col1,cf:col2 格式
    * @param outPut
    * @param outputOptionions
    * @param outputCols     输出字段 数组 格式(column_expr->cf:col1),(column_alias->col1)
    * @param sql
    * @param dispatch_task_id
    * @return
    */
  override def getDS(spark: SparkSession, dispatchOption: Map[String, Any], inPut: String, inputOptions: Map[String, String],
                     inputCondition: String, inputCols: Array[String], duplicateCols: Array[String], outPut: String, outputOptionions: Map[String, String],
                     outputCols: Array[Map[String, String]], sql: String)(implicit dispatch_task_id: String): DataFrame = {

    try {
      logger.info("[数据采集]:输入源为[HBASE],开始匹配对应参数")

      val outputCols_expr = outputCols.map(f => {
        val column_expr = f.getOrElse("column_expr", "")
        val column_alias = f.getOrElse("column_alias", "")
        //      expr(column_expr) as column_alias
        if (column_expr.contains(":")) {
          val cf = column_expr.split(":", 2)(0)
          val col = column_expr.split(":", 2)(1)
          expr(col) as column_alias
        } else {
          expr(column_expr) as column_alias
        }
      })

      val paths = inputOptions.getOrElse("paths", "").toString
      if (paths.trim.equals("")) {
        throw new Exception("[zdh],hbase数据源读取:paths为空")
      }

      val url: String = inputOptions.getOrElse("url", "").toString
      if (url.trim.equals("")) {
        throw new Exception("[zdh],hbase数据源读取:url为空")
      }

      if (inputCols.size < 1) {
        throw new Exception("[zdh],hbase数据源读取:输入字段为空")
      }
      if (inputCondition.trim.equals("")) {
        throw new Exception("[zdh],hbase数据源读取:过滤条件必须按照指定格式 startrow_key,endrow_key")
      }

      logger.info("[数据采集]:[HBASE]:[READ]:表名:" + paths + "," + inputOptions.mkString(",") + " [FILTER]:" + inputCondition)

      //过滤条件
      var scan = new Scan()
      val startRow = Bytes.toBytes(inputCondition.split(",")(0))

      scan.setStartRow(startRow)
      if (inputCondition.split(",").size == 2) {
        val endRow = Bytes.toBytes(inputCondition.split(",")(1))
        scan.setStopRow(endRow)
      }

      //配置查询的列
      inputCols.map(col => {
        val cf = Bytes.toBytes(col.split(":")(0))
        val cn = Bytes.toBytes(col.split(":")(1))
        scan.addColumn(cf, cn)
      })

      //配置文件信息
      val conf = getConfHbase(inputOptions)
      val tableName = TableName.valueOf(paths)
      val scan_str = TableMapReduceUtil.convertScanToString(scan)
      conf.set(TableInputFormat.INPUT_TABLE, paths)
      conf.set(TableInputFormat.SCAN, scan_str)

      val hBaseRDD = spark.sparkContext.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])

      val result = hBaseRDD.map(_._2).map(result => {
        var resultMap: Map[String, String] = Map()
        val row_key = Bytes.toString(result.getRow)
        var str = "row_key=" + row_key
        val cells = result.rawCells()
        for (cell <- cells) {
          val cf = Bytes.toString(CellUtil.cloneFamily(cell))
          val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
          val col_value = Bytes.toString(CellUtil.cloneValue(cell))
          val map_key = cf + ":" + col_name.toLowerCase
          println(cf + ":" + col_name.toLowerCase)
          resultMap = resultMap ++ Map(map_key -> col_value)
        }
        Row.fromSeq(inputCols.map(f => resultMap.getOrElse(f.toLowerCase, "")).+:(row_key))
      })


      val schema_cols = inputCols.map(f => if (f.contains(":")) f.split(":")(1) else f).+:("row_key")


      val schema = StructType(schema_cols.map(fileName => StructField(fileName, StringType, true)))

      val df = spark.createDataFrame(result, schema)

      filter(spark, df, "", duplicateCols)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[HBASE]:[READ]:表名:" + inputOptions.getOrElse("paths", "").toString + ":[ERROR]:" + ex.getMessage.replace("\"", "'"))
        throw ex
      }
    } finally {
    }


  }


  /**
    *
    * @param spark
    * @param df     数据集
    * @param select 字段表达式
    * @param dispatch_task_id
    * @return
    */
  override def process(spark: SparkSession, df: DataFrame, select: Array[Column], zdh_etl_date: String)(implicit dispatch_task_id: String): DataFrame = {
    logger.info("[数据采集]:[HBASE]:[SELECT]")
    logger.debug("[数据采集]:[HBASE]:[SELECT]:" + select.mkString(","))
    if (select == null || select.isEmpty) {
      logger.debug("[数据采集]:[HBASE]:[SELECT]:[智能识别字段]" + df.columns.mkString(","))
      return df
    }
    df.select(select: _*)
  }

  override def writeDS(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String = "")(implicit dispatch_task_id: String): Unit = {
    spark.sparkContext.setLocalProperty(DataSources.SPARK_ZDH_LOCAL_PROCESS,"OUTPUT")
    var conn: Connection = null
    // var HbaseTale: Table = null
    val table = options.getOrElse("paths", "").toString

    logger.info("[数据采集]:[HBASE]:[WRITE]:如果设置write_mode 为hfile 会使用hfile文件方式写入hbase,此方法适用于大数据量写入hbase")
    if (options.getOrElse("write_mode", "").equalsIgnoreCase("hfile")) {
      logger.info("[数据采集]:[HBASE]:[WRITE]:选择HFILE 方式写入")
      writeHFile(spark, df, options, sql)
      return
    }

    try {
      logger.info("[数据采集]:[HBASE]:[WRITE]:表名:" + table + "[options]:" + options.mkString(","))
      conn = getConnHbase(options)

      val conf = getConfHbase(options)

      val jobConf = new JobConf(conf)
      jobConf.setOutputFormat(classOf[TableOutputFormat])
      jobConf.set(TableOutputFormat.OUTPUT_TABLE, table)

      //写入hbase 数据源必须指定rowkey 字段--规定必须有一个字段为row_key

      val Htable = TableName.valueOf(table)

      val columns = df.columns
      if (!columns.contains("row_key")) {
        logger.info("[数据采集]:[HBASE]:[WRITE]:表名:" + table + "[ERROR]:输出的例必须包含row_key")
        throw new Exception("输出的例必须包含row_key");
        return
      }

      println("=================" + columns.mkString(","))
      val defalutFamily = getDefaultFamily(Htable,conn)
      //列族
      val cfs = columns.filter(!_.equalsIgnoreCase("row_key")).map(f => {
        if (f.contains(":")) {
          if(f.split(":").size!=2 && f.split(":")(1).trim.equalsIgnoreCase("")){
            throw new Exception("hbase列簇及字段配置异常,"+f)
          }
          f.split(":")(0)
        } else {
          defalutFamily
        }
      }).toSet[String]

      createHbaseTable(Htable, cfs, conn)

      //合并小文件操作
      var df_tmp = merge(spark,df,options)

      df_tmp.rdd.map(row => {
        val put = new Put(Bytes.toBytes(row.getAs[Any]("row_key").toString))
        columns.foreach(f => {
          if (!f.equals("row_key")) {
            if (f.contains(":")) {
              //说明有指定的列族
              val col = f.split(":")
              val value = row.getAs[Any](f)
              if (value != null && !value.toString.equals(""))
                put.addColumn(Bytes.toBytes(col(0)), Bytes.toBytes(col(1)), Bytes.toBytes(value.toString))
            } else {
              //println(row.getAs(f).getClass.getName)
              val value = row.getAs[Any](f)
              if (value != null && !value.toString.equals(""))
                put.addColumn(Bytes.toBytes(defalutFamily), Bytes.toBytes(f), Bytes.toBytes(value.toString))
            }
          }
        })
        (new ImmutableBytesWritable, put)
      }).saveAsHadoopDataset(jobConf)

    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("[数据采集]:[HBASE]:[WRITE]:表名:" + table + "[ERROR]:" + ex.getMessage.replace("\"", "'"))
        throw ex
      }
    } finally {
      conn.close()
    }


  }

  def deleteJDBC(spark: SparkSession, table: String, options: Map[String, String], sql: String)(implicit dispatch_task_id: String): Unit = {
    LogCommon.info("[数据采集]:[JDBC]:[CLEAR]:table" + table + "," + options.mkString(","))
    logger.info("[数据采集]:[JDBC]:[CLEAR]:table" + table + "," + options.mkString(","))

    //多个host 逗号分割
    val zk_host = options.getOrElse("url", "127.0.0.1")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zk_host)
    //conf.set("hbase.rootdir","file")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //conf.set("zookeeper.znode.parent", "/hbase")

    val conn = ConnectionFactory.createConnection(conf)


    val Htable = TableName.valueOf(table)

    val HbaseTale = conn.getTable(Htable)

    var scan = new Scan()
    if (sql.equals("")) {
      return
    }

    if (sql.contains(",")) {
      val startRow = Bytes.toBytes(sql.split(",")(0))
      scan.setStartRow(startRow)
      if (sql.split(",").size == 2) {
        val endRow = Bytes.toBytes(sql.split(",")(1))
        scan.setStopRow(endRow)
      }
    } else {
      val startRow = Bytes.toBytes(sql.split(",")(0))
      scan.setStartRow(startRow)
    }

    val scanner = HbaseTale.getScanner(scan)
    val row_keys = new util.ArrayList[Delete]()

    val iterator = scanner.iterator()
    while (iterator.hasNext) {
      row_keys.add(new Delete(iterator.next().getRow))
    }
    scanner.close()
    if (row_keys.size() > 0) {
      HbaseTale.delete(row_keys)
    }
  }


  def getConfHbase(options: Map[String, String])(implicit dispatch_task_id: String): Configuration = {
    LogCommon.info("[数据采集]:[HBASE]:获取HBASE配置信息")
    logger.info("[数据采集]:[HBASE]:获取HBASE配置信息")
    //多个host 逗号分割
    val zk_host = options.getOrElse("url", "127.0.0.1")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zk_host)
    //conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf
  }

  def getConnHbase(options: Map[String, String])(implicit dispatch_task_id: String): Connection = {

    LogCommon.info("[数据采集]:[HBASE]:获取HBASE连接")
    logger.info("[数据采集]:[HBASE]:获取HBASE连接")
    //多个host 逗号分割
    val zk_host = options.getOrElse("url", "127.0.0.1")

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", zk_host)
    //conf.set("hbase.rootdir","file")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    //conf.set("zookeeper.znode.parent", "/hbase")

    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  def getDefaultFamily(table: TableName, conn: Connection)(implicit dispatch_task_id: String): String = {
    val admin: Admin = conn.getAdmin
    try {
      if (admin.tableExists(table)) {
        val cf = conn.getTable(table).getTableDescriptor.getColumnFamilies.map(cf => Bytes.toString(cf.getName)).head
        if (cf != null && cf.isEmpty) return cf
      }
      return defFamily
    } catch {
      case ex: Exception => throw ex
    } finally {
      admin.close()
    }

  }

  def createHbaseTable(table: TableName, cfs: Set[String], conn: Connection)(implicit dispatch_task_id: String): Unit = {
    var admin: Admin = null
    try {
      logger.info("[数据采集]:[HBASE]:检查表是否存在:" + table.getNameAsString)
      admin = conn.getAdmin
      //如果表不存在添加 表
      if (!admin.tableExists(table)) {
        logger.info("[数据采集]:[HBASE]:检查表不存在,添加:" + table.getNameAsString + " 列族:" + cfs.mkString(","))
        val htd = new HTableDescriptor(table)
        if (cfs.size > 0) {
          cfs.foreach(cf => {
            htd.addFamily(new HColumnDescriptor(cf))
          })
        } else {
          htd.addFamily(new HColumnDescriptor(defFamily))
        }
        //创建表
        admin.createTable(htd)
        if (admin.isTableDisabled(table)) {
          admin.enableTable(table)
        }
      } else {
        logger.info("[数据采集]:[HBASE]:检查表已存在,检查 列族:" + cfs.mkString(","))
        val tableDescriptor = conn.getTable(table).getTableDescriptor
        var is_update = false
        cfs.foreach(cf => if (tableDescriptor.getFamily(Bytes.toBytes(cf)) == null) {
          tableDescriptor.addFamily(new HColumnDescriptor(cf))
          is_update = true
        })
        if (is_update) {
          logger.info("[数据采集]:[HBASE]:检查到有新增的列族:" + tableDescriptor.getColumnFamilies.map(cf => Bytes.toString(cf.getName)).mkString(","))
          admin.modifyTable(table, tableDescriptor)
          admin.enableTable(table)
        }

      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.info("[数据采集]:[HBASE]:检查表不存在,添加:" + table.getNameAsString + " 列族:" + cfs.mkString(","))
        throw ex
      }
    } finally {
      admin.close()
    }


  }

  def hbaseNerdammer(spark: SparkSession, table: String, sep: String, options: Map[String, String], cols: Array[String],
                     inputCondition: String)(implicit dispatch_task_id: String): Unit = {
    //    import it.nerdammer.spark.hbase._
    //
    //    import spark.implicits._
    //
    //    spark.conf.set("spark.hbase.host", "127.0.0.1") //e.g. 192.168.1.1 or localhost or your hostanme
    //
    //    // For Example If you have an HBase Table as 'Document' with ColumnFamily 'SMPL' and qualifier as 'DocID, Title' then:
    //
    //    val docRdd = spark.sparkContext.hbaseTable[(Option[String], Option[String])](table)
    //      .select("cf1:name","cf1:name")
    //      .withStartRow("0")
    //      .withStopRow("0")
    //    docRdd.map(f=>f._1.get).toDF().show()
  }

  def writeHFile(spark: SparkSession, df: DataFrame, options: Map[String, String], sql: String = "")(implicit dispatch_task_id: String) {

    import spark.implicits._
    var conn = getConnHbase(options)

    val conf = getConfHbase(options)

    val path = options.getOrElse("paths", "")

    val Htable = TableName.valueOf(path)

    val startKeys = conn.getRegionLocator(Htable).getStartKeys

    val columns = df.columns
    if (!columns.contains("row_key")) {
      logger.info("[数据采集]:[HBASE]:[WRITE]:表名:" + "[ERROR]:输出的例必须包含row_key")
      return
    }
    val defalutFamily = getDefaultFamily(Htable,conn)
    val cfs = columns.map(f => {
      if (f.contains(":")) {
        f.split(":")(0)
      } else {
        defalutFamily
      }
    }).toSet[String]

    createHbaseTable(Htable, cfs, conn)

    println("=================" + columns.mkString(","))


    val df_tmp = df.rdd.map(f => {
      val d = columns.map(c => {
        val map_f = c match {
          case a if a.contains(":") => Map(c -> f.getAs[Any](c).toString)
          case _ => Map(defalutFamily+":" + c -> f.getAs[Any](c).toString)
        }
        map_f
      }
      )
      f.getAs[String]("row_key") -> d.flatten
    }).repartitionAndSortWithinPartitions(new HFilePartitioner(conf, startKeys, 1))
      .flatMap {
        case (key, columns) =>
          val rowkey = new ImmutableBytesWritable()
          rowkey.set(Bytes.toBytes(key)) //设置rowkey
        val kvs = new util.TreeSet[KeyValue](KeyValue.COMPARATOR)
          columns.foreach(ele => {
            val (column, value) = ele // 每一条数据两个列族  对应map里面的两列
            kvs.add(new KeyValue(rowkey.get(), Bytes.toBytes(column.split(":")(0)), Bytes.toBytes(column.split(":")(1)), Bytes.toBytes(value)))
          })
          kvs.toArray.toSeq.map(kv => (rowkey, kv))
      }

    // conf.set(TableOutputFormat.OUTPUT_TABLE,path)
    val hdfs_path = new Path(path); // 取第1个表示输出目录参数（第0个参数是输入目录）
    val fileSystem = hdfs_path.getFileSystem(conf); // 根据path找到这个文件
    if (fileSystem.exists(hdfs_path)) {
      fileSystem.delete(hdfs_path, true); // true的意思是，就算output有东西，也一带删除
    }
    df_tmp.saveAsNewAPIHadoopFile(path, classOf[ImmutableBytesWritable], classOf[KeyValue], classOf[HFileOutputFormat])
    loadHFile(conf, conn, path)

  }

  def loadHFile(conf: Configuration, conn: Connection, path: String)(implicit dispatch_task_id: String): Unit = {
    //开始即那个HFile导入到Hbase,此处都是hbase的api操作
    val load = new LoadIncrementalHFiles(conf)
    //hbase的表名
    val tableName = path
    //创建hbase的链接,利用默认的配置文件,实际上读取的hbase的master地址

    //根据表名获取表
    val table: Table = conn.getTable(TableName.valueOf(tableName))
    var admin: Admin = null
    var regionLocator: RegionLocator = null
    try {
      //获取hbase表的region分布
      regionLocator = conn.getRegionLocator(TableName.valueOf(tableName))

      //创建一个hadoop的mapreduce的job
      val job = Job.getInstance(conf)
      //设置job名称
      job.setJobName("DumpFile")
      //此处最重要,需要设置文件输出的key,因为我们要生成HFil,所以outkey要用ImmutableBytesWritable
      job.setMapOutputKeyClass(classOf[ImmutableBytesWritable])
      //输出文件的内容KeyValue
      job.setMapOutputValueClass(classOf[KeyValue])
      //配置HFileOutputFormat2的信息
      HFileOutputFormat.configureIncrementalLoad(job, table.asInstanceOf[HTable])

      admin = conn.getAdmin

      //开始导入
      load.doBulkLoad(new Path(path), admin, table, regionLocator)
    } finally {
      table.close()
      regionLocator.close()
      if (admin != null) {
        admin.close()
      }
      conn.close()
    }
  }


  private class HFilePartitioner(conf: Configuration, splits: Array[Array[Byte]], numFilesPerRegion: Int) extends Partitioner {
    val fraction = 1 max numFilesPerRegion min 128

    override def getPartition(key: Any): Int = {
      def bytes(n: Any) = n match {
        case s: String => Bytes.toBytes(s)
        case s: Long => Bytes.toBytes(s)
        case s: Int => Bytes.toBytes(s)
      }

      val h = (key.hashCode() & Int.MaxValue) % fraction
      for (i <- 1 until splits.length)
        if (Bytes.compareTo(bytes(key), splits(i)) < 0) return (i - 1) * fraction + h

      (splits.length - 1) * fraction + h
    }

    override def numPartitions: Int = splits.length * fraction
  }


  /**
    * 暂时不可用---spark 2.4 版本 暂时不支持此种方法
    *
    * @param spark
    * @param table
    * @param sep
    * @param options
    * @param cols
    * @param inputCondition
    * @param dispatch_task_id
    */
  def hbaseSHC(spark: SparkSession, table: String, sep: String, options: Map[String, String], cols: Array[String],
               inputCondition: String)(implicit dispatch_task_id: String): Unit = {

    val colStr = cols.map(col => {
      val cf = col.split(":")(0)
      val colName = col.split(":")(1)
      s"""|"$colName":{"cf":"$cf","col":"$colName","type":"string"}  """
    }).mkString(",")
    val catalog =
      s"""{
         |"table":{"namespace":"default", "name":"$table"},
         |"rowkey":"key",
         |"columns":{
         |"rowkey":{"cf":"rowkey", "col":"key", "type":"string"},
         ${colStr}
         |}
         |}""".stripMargin

    //    spark.read
    //      .options(Map(HBaseTableCatalog.tableCatalog -> catalog))
    //      .format("org.apache.spark.sql.execution.datasources.hbase")
    //      .load()
    //      .filter(inputCondition)

  }


  //  def getDS(spark: SparkSession, table: String, options: Map[String, String], cols: Array[String],
  //            inputCondition: String)(implicit dispatch_task_id: String): DataFrame = {
  //
  //    var conn: Connection = null
  //    var HbaseTale: Table = null
  //    try {
  //      LogCommon.info("[数据采集]:[HBASE]:[READ]:" + options.mkString(",") + " [FILTER]:" + inputCondition)
  //      logger.info("[数据采集]:[HBASE]:[READ]:" + options.mkString(",") + " [FILTER]:" + inputCondition)
  //      import spark.implicits._
  //
  //      conn = getConnHbase(options)
  //
  //      val Htable = TableName.valueOf(table)
  //
  //      HbaseTale = conn.getTable(Htable)
  //
  //      var scan = new Scan()
  //      val startRow = Bytes.toBytes(inputCondition.split(",")(0))
  //      scan.setStartRow(startRow)
  //      if (inputCondition.split(",").size == 2) {
  //        val endRow = Bytes.toBytes(inputCondition.split(",")(1))
  //        scan.setStopRow(endRow)
  //      }
  //
  //      //scan.withStartRow(startRow).withStopRow(endRow)
  //
  //      cols.map(col => {
  //        val cf = Bytes.toBytes(col.split(":")(0))
  //        val cn = Bytes.toBytes(col.split(":")(1))
  //        scan.addColumn(cf, cn)
  //      })
  //
  //      import scala.collection.JavaConversions._
  //      val scanner = HbaseTale.getScanner(scan)
  //      var resValues: List[Map[String, String]] = List()
  //      scanner.foreach(result => {
  //        var resultMap: Map[String, String] = Map()
  //        val cells = result.rawCells();
  //        val row_key = Bytes.toString(result.getRow)
  //        resultMap = resultMap ++ Map("row_key" -> row_key)
  //        for (cell <- cells) {
  //          val col_name = Bytes.toString(CellUtil.cloneQualifier(cell))
  //          val col_value = Bytes.toString(CellUtil.cloneValue(cell))
  //          resultMap = resultMap ++ Map(col_name -> col_value)
  //        }
  //        val resultLst = List(resultMap)
  //        resValues = resValues ::: resultLst
  //      })
  //      scanner.close()
  //
  //
  //      //get all unique columns from the list
  //      val uniqcolList = cols.map(col => col.split(":")(1)).toList ::: List("row_key") //List("name","age") //colList.reduce((x,y)=>(x ++ y))
  //
  //
  //      if (resValues.size < 1) {
  //        return spark.createDataFrame(spark.sparkContext.emptyRDD[Row], StructType(uniqcolList.map(fileName => StructField(fileName, StringType, true))))
  //      }
  //
  //
  //      val newColValMap = resValues.map(eleMap => {
  //        uniqcolList.map(col => (col, eleMap.getOrElse(col, ""))).toMap
  //      })
  //
  //      val rows = newColValMap.map(m => Row(m.values.toSeq: _*))
  //      val header = newColValMap.head.keys.toList
  //      val schema = StructType(header.map(fileName => StructField(fileName, StringType, true)))
  //      //val rdd=spark.sparkContext.parallelize(rows)
  //      val resultDF = spark.createDataFrame(rows, schema)
  //
  //      resultDF
  //    } catch {
  //      case ex: Exception => {
  //        ex.printStackTrace()
  //        throw ex
  //      }
  //    } finally {
  //      HbaseTale.close()
  //      conn.close()
  //    }
  //
  //
  //  }


  //      HbaseTale = conn.getTable(Htable)
  //
  //      df.collect.foreach(row => {
  //
  //        var puts = new util.ArrayList[Put]()
  //
  //        val row_key = row.getAs("row_key").toString
  //        val put = new Put(Bytes.toBytes(row_key));
  //        columns.foreach(f => {
  //          if (!f.equals("row_key")) {
  //            if (f.contains(":")) {
  //              //说明有指定的列族
  //              val col = f.split(":")
  //              val value = row.getAs[String](f)
  //              if (!value.equals(""))
  //                put.addColumn(Bytes.toBytes(col(0)), Bytes.toBytes(col(1)), Bytes.toBytes(value))
  //            } else {
  //              //println(row.getAs(f).getClass.getName)
  //              val value = row.getAs(f).toString
  //              if (!value.equals(""))
  //                put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes(f), Bytes.toBytes(value))
  //            }
  //          }
  //        })
  //        puts.add(put)
  //        HbaseTale.put(puts)
  //      })

}
