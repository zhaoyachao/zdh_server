//package com.zyc.zdh
//
//import java.io.{BufferedReader, ByteArrayInputStream, InputStreamReader}
//import java.net.URI
//
//import cn.hutool.core.io.FileUtil
//import com.amazonaws.AmazonWebServiceClient
//import com.amazonaws.services.s3.AmazonS3Client
//import com.univocity.parsers.common.ArgumentUtils
//import com.zyc.TEST_TRAIT2
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.TaskContext
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.execution.datasources.{CodecStreams, HadoopFileLinesReader}
//import org.apache.spark.sql.execution.datasources.csv.UnivocityParser
//import org.junit.Test
//
//@Test
//class MinioDataSourcesTest extends TEST_TRAIT2{
//  @Test
//  def testGetDS {
//
//    val inputOptions=Map(
//      "url"->"localhost:9042",
//      "paths"->"ks_test.tb1"
//    )
//
//
////    val conf = new SparkConf().setAppName("minio-loader").setMaster("local[*]")
////    conf.set("spark.hadoop.fs.s3a.access.key", "minio")
////    conf.set("spark.hadoop.fs.s3a.secret.key", "minio123")
////    conf.set("spark.hadoop.fs.s3a.endpoint", "http://192.168.0.1:9000")
////    conf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
////    conf.set("spark.hadoop.fs.s3a.path.style.access", "true")
////    conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
////    conf.set("spark.debug.maxToStringFields", "2048")
//
//
//
//    // Forbidden (Service: Amazon S3; Status Code: 403; Error Code: null; 权限失败,可能是minio 权限设置问题,服务器时间和应用时间不一直,有可能是hadoop版本不一致
//    //write file to minio bucket
//
//    import spark.implicits._
////    spark.sparkContext.getConf.set("spark.hadoop.fs.s3a.access.key", "minioadmin")
////    spark.sparkContext.getConf.set("spark.hadoop.fs.s3a.secret.key", "minioadmin")
////    spark.sparkContext.getConf.set("spark.hadoop.fs.s3a.endpoint", "http://192.168.110.10:9900")
////    spark.sparkContext.getConf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//    //spark.sparkContext.getConf.set("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
////    spark.sparkContext.getConf.set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
////    spark.sparkContext.getConf.set("spark.hadoop.fs.s3a.path.style.access", "true")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "wjut1PNxKy3u2NHmDxFR")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "o9aqPxuNOIAI76LbZDBH1fZ5KEunWkVKsGt725d2")
////    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", "minioadmin")
////    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", "minioadmin")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "http://192.168.110.10:9900")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//    //spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.connection.ssl.enabled", "false")
//    spark.sparkContext.hadoopConfiguration.set("fs.s3a.path.style.access", "true")
//    val data = Seq(("李彪", 30), ("林进", 39), ("林磊", 23))
//    var dfWriter = data.toDF("Name", "Age")
//
//    val bucketName = "s11"
//    val minioPath = "s3a://" + bucketName + "/csv"
//    //setting write concurrent
//    dfWriter = dfWriter.repartition(2)
//    dfWriter.write.mode("overwrite").format("csv")
//      .option("header", "true")
//      .option("fs.s3a.access.key", "minioadmin")
//      .option("fs.s3a.secret.key", "minioadmin")
//      .option("fs.s3a.endpoint", "http://192.168.110.10:9900")
//      .option("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//      .save(minioPath)
//
//    //read file from minio
//    val dfReader = spark.read.format("csv")
//      .option("header", "true")
//      .option("inferSchema", "true")
//      //.option("spark.hadoop.fs.s3a.access.key", "minioadmin")
//      //.option("spark.hadoop.fs.s3a.secret.key", "minioadmin")
//      //.option("spark.hadoop.fs.s3a.endpoint", "http://192.168.110.10:9900")
//      .load(minioPath)
//    dfReader.show(5)
//
//  }
//
//  @Test
//  def testHadoopWriteMinio(): Unit ={
//    val minioServerUrl = "http://192.168.110.10:9900"
//    // 存储桶的名称
//    val bucketName = "s11"
//    // 对象的键（即文件路径）
//    val objectKey = "csv"
//
//    // 配置MinIO服务器
//    val conf = new Configuration()
//    conf.set("fs.s3a.endpoint", minioServerUrl)
//    conf.set("fs.s3a.access.key", "minioadmin")
//    conf.set("fs.s3a.secret.key", "minioadmin")
//    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//    conf.setBoolean("fs.s3a.path.style.access", true)
//
//    import com.amazonaws.AmazonClientException
//    import com.amazonaws.AmazonServiceException
//    import com.amazonaws.ClientConfiguration
//    import com.amazonaws.auth.AWSCredentials
//
//    import com.amazonaws.auth.BasicAWSCredentials
//    import com.amazonaws.regions.Regions
//    import com.amazonaws.services.s3.AmazonS3
//
//    import com.amazonaws.services.s3.model.GetObjectRequest
//    import com.amazonaws.services.s3.model.PutObjectRequest
//    import com.amazonaws.services.s3.model.S3Object
//
//
//    val credentials = new BasicAWSCredentials("YOUR-ACCESSKEYID", "OUR-SECREYTACCESSKEY")
//    val clientConfiguration = new ClientConfiguration
//
//
//    val s2 =  new AmazonS3Client(credentials)
//    s2.setEndpoint(minioServerUrl)
//
//
//    import spark.implicits._
//    val data = Seq(("李彪", 30), ("林进", 39), ("林磊", 23))
//    var dfWriter = data.toDF("Name", "Age")
//
//    val putObjectRequest = new PutObjectRequest(bucketName, objectKey, )
//    s2.putObject(putObjectRequest)
//
//
//
//    val is = new ByteArrayInputStream("".getBytes())
//    // 创建FileSystem实例
//    val fileSystem = FileSystem.get(new URI("s3a://"+bucketName),conf)
//
//    // 构建S3对象的路径
//    val path = new Path("s3a://" + bucketName + "/" + objectKey);
//
//
//    // 打开文件读取流
//    val inputStream = fileSystem.open(path)
//
//
//
//    val reader = new BufferedReader(new InputStreamReader(inputStream,"utf-8"))
//
//    while (true){
//      val line = reader.readLine()
//      import util.control.Breaks._
//      breakable {
//        if(line == null){
//          break()
//        }
//      }
//
//      println(line)
//
//    }
//
//
//
//    //    val linesReader = new HadoopFileLinesReader(inputStream, conf)
//    //    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
//    //    linesReader.map { line =>
//    //      new String(line.getBytes, 0, line.getLength, "utf-8")
//    //    }
//    //    new HadoopFileLinesReader(inputStream, textOptions.lineSeparatorInRead, confValue)
//    //
//    //    ArgumentUtils.newReader(inputStream)
//    //    UnivocityParser.parseStream(
//    //      CodecStreams.createInputStreamWithCloseResource(conf, new Path(new URI(file.filePath))),
//    //      parser.options.headerFlag,
//    //      parser,
//    //      requiredSchema,
//    //      checkHeader)
//    //    spark.readStream.format("").
//    //
//    //    // 读取数据（根据需要）
//    //    // ...
//    //
//    //    // 关闭输入流
//    //    inputStream.close()
//    fileSystem.close()
//  }
//
//  @Test
//  def testHadoopGetMinio(): Unit ={
//    val minioServerUrl = "http://192.168.110.10:9900"
//    // 存储桶的名称
//    val bucketName = "s11"
//    // 对象的键（即文件路径）
//    val objectKey = "csv"
//
//    // 配置MinIO服务器
//    val conf = new Configuration()
//    conf.set("fs.s3a.endpoint", minioServerUrl)
//    conf.set("fs.s3a.access.key", "minioadmin")
//    conf.set("fs.s3a.secret.key", "minioadmin")
//    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
//    conf.setBoolean("fs.s3a.path.style.access", true)
//
//    // 创建FileSystem实例
//    val fileSystem = FileSystem.get(new URI("s3a://"+bucketName),conf)
//
//    // 构建S3对象的路径
//    val path = new Path("s3a://" + bucketName + "/" + objectKey);
//
//
//    // 打开文件读取流
//    val inputStream = fileSystem.open(path)
//
//
//
//    val reader = new BufferedReader(new InputStreamReader(inputStream,"utf-8"))
//
//    while (true){
//      val line = reader.readLine()
//      import util.control.Breaks._
//      breakable {
//        if(line == null){
//          break()
//        }
//      }
//
//      println(line)
//
//    }
//
//
//
////    val linesReader = new HadoopFileLinesReader(inputStream, conf)
////    Option(TaskContext.get()).foreach(_.addTaskCompletionListener[Unit](_ => linesReader.close()))
////    linesReader.map { line =>
////      new String(line.getBytes, 0, line.getLength, "utf-8")
////    }
////    new HadoopFileLinesReader(inputStream, textOptions.lineSeparatorInRead, confValue)
////
////    ArgumentUtils.newReader(inputStream)
////    UnivocityParser.parseStream(
////      CodecStreams.createInputStreamWithCloseResource(conf, new Path(new URI(file.filePath))),
////      parser.options.headerFlag,
////      parser,
////      requiredSchema,
////      checkHeader)
////    spark.readStream.format("").
////
////    // 读取数据（根据需要）
////    // ...
////
////    // 关闭输入流
////    inputStream.close()
//    fileSystem.close()
//  }
//}
