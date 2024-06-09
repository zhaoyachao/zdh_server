package com.zyc.zdh.datasources.http

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost, HttpPut}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicNameValuePair
import org.apache.http.util.EntityUtils
import org.apache.spark.Partition
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Row, SQLContext, SparkSession}
import org.json4s.{CustomSerializer, DefaultFormats, JLong}


case class HttpRelation(
                         override val schema: StructType,
                         parts: Array[Partition],
                         httpOptions: HttpOptions)(@transient val sparkSession: SparkSession)
  extends BaseRelation
    with PrunedFilteredScan {
  case object TimestampSerializer extends CustomSerializer[java.sql.Timestamp](format => ( {
    case _ => null
  }, {
    case ts: Timestamp =>JLong(ts.getTime)
  })
  )
  implicit val formats = DefaultFormats

  override def sqlContext: SQLContext =sparkSession.sqlContext

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    import sparkSession.implicits._
    //println(httpOptions.getHttp_Url)
    //println(httpOptions.getSep())
    //连接http 获取数据
    var http_result=requestUrl(httpOptions.getHttp_Url,httpOptions.parameters)
    import org.json4s.jackson.Serialization.read
    import org.json4s.jackson.Serialization.write

    if(!httpOptions.getResultColumn().isEmpty){
      var result = read[Map[String,Any]](http_result)
      var tmp:Any = null
      var tmp2:AnyRef = null
      val columns = httpOptions.getResultColumn().split("\\.")
      Seq.range(0, columns.size).foreach(index=>{
        if(index == 0){
          tmp = result.get(columns(index)).get
        }else{
          result = tmp.asInstanceOf[Map[String,Any]]
          tmp = result.get(columns(index)).get
        }
        tmp2 = tmp.asInstanceOf[AnyRef]
      })
      http_result = write(tmp2)
    }

    if(httpOptions.getFileType().toLowerCase.equals("csv")){
      val sep=httpOptions.getSep()
      val ncols = requiredColumns.zipWithIndex.map(f => col("value").getItem(f._2) as f._1)
      val result=sparkSession.sparkContext.parallelize(http_result.split("\n").toSeq)
        .map(f=>f.split(sep)).toDF("value")
        .select(ncols:_*)
      result.rdd
    }else{
      sparkSession.read.schema(schema).options(httpOptions.parameters).json(Seq(http_result).toDS()).rdd
    }

  }

  def requestUrl(url: String, params: Map[String, String]):String = {

    httpOptions.getMethod() match {
      case "get"=>get(url,params.toSeq)
      case "post"=>post(url,params.toSeq)
      case "delete"=>delete(url,params.toSeq)
      case "put"=>put(url,params.toSeq)
      case _=>get(url,params.toSeq)
    }
  }

  /**
    * 超时时间 单位：毫秒
    */
  def HttpClient(): CloseableHttpClient ={
    val httpClient:CloseableHttpClient = HttpClientBuilder.create()
      .setConnectionTimeToLive(httpOptions.getTimeOut(), TimeUnit.MILLISECONDS)
      .build()
    httpClient
  }

  /**
    *
    * @param addr 接口地址
    * @param param 请求参数
    * @return
    */
  def get(addr:String,param: Seq[(String,String)]):String={
    val builder=new URIBuilder(addr)
    if(param.nonEmpty){
      param.foreach(r=>{
        if(!r._1.startsWith("header.")){
          builder.addParameter(r._1,r._2)
        }
      })
    }
    val client=HttpClient()
    val httpGet = new HttpGet(builder.build())
    if(param.nonEmpty){
      param.foreach(r=>{
        if(r._1.startsWith("header.")){
          httpGet.setHeader(r._1.substring(7),r._2)
        }
      })
    }
    val httpResponse = client.execute(httpGet)
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      content=EntityUtils.toString(entity)
    }
    client.close()
    content
  }

  def put(addr:String,param: Seq[(String,String)]):String={
    val builder=new URIBuilder(addr)
    if(param.nonEmpty){
      param.foreach(r=>{
        if(!r._1.startsWith("header.")){
          builder.addParameter(r._1,r._2)
        }
      })
    }
    val client=HttpClient()
    val httpPut = new HttpPut(builder.build())
    if(param.nonEmpty){
      param.foreach(r=>{
        if(r._1.startsWith("header.")){
          httpPut.setHeader(r._1.substring(7),r._2)
        }
      })
    }
    val httpResponse = client.execute(httpPut)
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      content=EntityUtils.toString(entity)
    }
    client.close()
    content
  }

  def delete(addr:String,param: Seq[(String,String)]):String={
    val builder=new URIBuilder(addr)
    if(param.nonEmpty){
      param.foreach(r=>{
        if(!r._1.startsWith("header.")){
          builder.addParameter(r._1,r._2)
        }
      })
    }
    val client=HttpClient()
    val httpDelete = new HttpDelete(builder.build())
    if(param.nonEmpty){
      param.foreach(r=>{
        if(r._1.startsWith("header.")){
          httpDelete.setHeader(r._1.substring(7),r._2)
        }
      })
    }
    val httpResponse = client.execute(httpDelete)
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      content=EntityUtils.toString(entity)
    }
    client.close()
    content
  }

  def post(addr:String,param: Seq[(String,String)]):String={
    val req=new HttpPost(addr)

    import org.json4s.jackson.Serialization.write
    implicit val formats = org.json4s.DefaultFormats
    //import scala.collection.JavaConverters._
    //val entity=new UrlEncodedFormEntity(listParms.toList.asJava,"utf-8")
    val entity = new StringEntity(write(param.toMap))
    req.setEntity(entity)
    val client=HttpClient()

    if(param.nonEmpty){
      param.foreach(r=>{
        if(r._1.startsWith("header.")){
          req.setHeader(r._1.substring(7),r._2)
        }
      })
    }

    val httpResponse = client.execute(req)
    val resEntity = httpResponse.getEntity()
    var content = ""
    if (resEntity != null) {
      content=EntityUtils.toString(resEntity)
    }
    client.close()
    content
  }

}

