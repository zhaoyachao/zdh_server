package com.zyc.base.util

import java.util.concurrent.TimeUnit

import org.apache.http.NameValuePair
import org.apache.http.client.entity.UrlEncodedFormEntity
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost, HttpPut}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClientBuilder}
import org.apache.http.message.BasicNameValuePair

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object HttpUtil {
  /**
    * 超时时间 单位：毫秒
    */
  val connTimout:Long=5000
  def HttpClient(): CloseableHttpClient ={
    val httpClient:CloseableHttpClient = HttpClientBuilder.create()
      .setConnectionTimeToLive(connTimout, TimeUnit.MILLISECONDS)
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
        builder.addParameter(r._1,r._2)
      })
    }
    val client=HttpClient()
    println(builder.build().toString)
    val httpResponse = client.execute(new HttpGet(builder.build()))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    client.close()
    content
  }

  def put(addr:String,param: Seq[(String,String)]):String={
    val builder=new URIBuilder(addr)
    if(param.nonEmpty){
      param.foreach(r=>{
        builder.addParameter(r._1,r._2)
      })
    }
    val client=HttpClient()
    println(builder.build().toString)
    val httpResponse = client.execute(new HttpPut(builder.build()))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    client.close()
    content
  }

  def delete(addr:String,param: Seq[(String,String)]):String={
    val builder=new URIBuilder(addr)
    if(param.nonEmpty){
      param.foreach(r=>{
        builder.addParameter(r._1,r._2)
      })
    }
    val client=HttpClient()
    println(builder.build().toString)
    val httpResponse = client.execute(new HttpDelete(builder.build()))
    val entity = httpResponse.getEntity()
    var content = ""
    if (entity != null) {
      val inputStream = entity.getContent()
      content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    client.close()
    content
  }

  def post(addr:String,param: Seq[(String,String)]):String={
    val req=new HttpPost(addr)
    val listParms=new ArrayBuffer[NameValuePair]()
    param.foreach(r=>{
      listParms+=new BasicNameValuePair(r._1,r._2)
    })
    import scala.collection.JavaConverters._
    val entity=new UrlEncodedFormEntity(listParms.toList.asJava,"utf-8")
    req.setEntity(entity)
    val client=HttpClient()
    val httpResponse = client.execute(req)
    val resEntity = httpResponse.getEntity()
    var content = ""
    if (resEntity != null) {
      val inputStream = resEntity.getContent()
      content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    client.close()
    content
  }

  def  postJson(addr:String,json:String):String={
    val req=new HttpPost(addr)
    val entity=new StringEntity(json,"utf-8")
    req.setEntity(entity)
    val client=HttpClient()
    val httpResponse = client.execute(req)
    val resEntity = httpResponse.getEntity()
    var content = ""
    if (resEntity != null) {
      val inputStream = resEntity.getContent()
      content = Source.fromInputStream(inputStream).getLines.mkString
      inputStream.close
    }
    client.close()
    content
  }

  def main(args: Array[String]): Unit = {
    println(get("http://127.0.0.1:60001/test",Seq("asdddddd"->"d你是谁啊")))
  }


}
