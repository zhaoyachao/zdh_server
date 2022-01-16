package com.zyc.netty

import java.net.URLDecoder

import io.netty.buffer.Unpooled
import io.netty.handler.codec.http.{DefaultFullHttpResponse, HttpResponseStatus, HttpVersion}
import io.netty.util.AsciiString

trait HttpBaseHandler {
  val ContentType = AsciiString.cached("Content-Type")
  val ContentLength = AsciiString.cached("Content-Length")
  val Connection = AsciiString.cached("Connection")
  val KeepAlive = AsciiString.cached("keep-alive")
  val noParam="{\"code\":500,\"msg\":\"no params\"}"
  val noService="{\"code\":500,\"msg\":\"no match reportService\"}"
  val noUri="{\"code\":500,\"msg\":\"request uri is wrong\"}"
  val unknownParam="{\"code\":500,\"msg\":\"unknown cmd\"}"
  val cmdOk="{\"code\":200,\"msg\":\"command executed\"}"
  val execErr="{\"code\":500,\"msg\":\"command execute error\"}"
  val serverErr="{\"code\":500,\"msg\":\"server error\"}"
  val cacheIsNull="{\"code\":501,\"msg\":\"model cache is null\"}"
  val chartSet:String="utf-8"


  def defaultResponse(respContent: String):DefaultFullHttpResponse={
    val response = new DefaultFullHttpResponse(
      HttpVersion.HTTP_1_1,
      HttpResponseStatus.OK,
      Unpooled.wrappedBuffer(respContent.getBytes())
    )
    response.headers().set(ContentType, "application/json")
    response.headers().setInt(ContentLength, response.content().readableBytes())
    response
  }
  def parseGetParam(uri: String):Map[String,String]={
    var map=Map.empty[String,String]
    val array=URLDecoder.decode(uri,chartSet).split("\\?")
    if(array.length>1){
      val params=array.apply(1).split("&").map(_.trim)
      params.foreach(str=>{
        val strArr=str.split("=")
        if(strArr.length>1){
          map+=(strArr.apply(0)->strArr.apply(1))
        }else if(strArr.length>0){
          //有可能出现等号后面什么也没有
          map+=(strArr.apply(0)->"")
        }
      })
    }
    map
  }
}
