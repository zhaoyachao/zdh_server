package com.zyc.base.util

import java.sql.Timestamp



import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization
import org.json4s.{JValue, _}

/**
  * json解析 工具类
  * 基于scala json4s
  */
object JsonUtil {
  case object TimestampSerializer extends CustomSerializer[java.sql.Timestamp](format => ( {
    case _ => null
  }, {
    case ts: Timestamp =>JLong(ts.getTime)
  })
  )
  implicit val formats = DefaultFormats

  /**
    * json 转 map
    * @param content
    * @return
    */
  def jsonToMap(content: String):Map[String,Any]={
    parse(content).extract[Map[String,Any]]
  }

  /**
    * json 转对象
    * @param content
    * @tparam T
    * @return
    */
  def jsonToObj[T: Manifest](content:String):T={
    org.json4s.jackson.JsonMethods.parse(content).extract[T]
  }

  def jsonToObj[T: Manifest](content: AnyRef): T = {
    org.json4s.jackson.JsonMethods.parse(toJson(content)).extract[T]
  }

  /**
    * json 转 list
    * @param content
    * @tparam T
    * @return
    */
  def jsonToList[T:Manifest](content:String):List[T]={
    org.json4s.jackson.JsonMethods.parse(content).extract[List[T]]
  }

  /**
    * 对象转json
    * @param obj
    * @return
    */
  def toJson(obj:AnyRef):String={
    try{
      Serialization.write(obj)
    }catch {
      case ex:Exception=>throw new Exception("转json异常",ex.getCause)
    }
  }

  def toJson2(obj: List[Map[String,Any]]): String = {
    Serialization.write(obj)
  }
  sealed trait QueryCacheClusterData
  case class QueryCacheClusterSuccess(total: Int, totalPage: Int, rows: List[Map[String, Any]], columns: List[Map[String, Any]]) extends QueryCacheClusterData
  def main(args: Array[String]): Unit = {
//    val list=Seq(Map("A"->"B"),Map("A"->Map("b"->"asd")),Map("A"->"B"))
//    val str=toJson(list)
    val str2="{\"cityCode\":\"43019884\",\"cityName\":\"凤凰县支行\",\"branchList\":[{\"branchCode\":\"43020126\",\"branchName\":\"凤凰县吉信镇营业所\",\"level\":\"4\"},{\"branchCode\":\"43020138\",\"branchName\":\"凤凰县禾库镇营业所\",\"level\":\"4\"},{\"branchCode\":\"43020140\",\"branchName\":\"凤凰县腊尔山镇营业所\",\"level\":\"4\"},{\"branchCode\":\"43020153\",\"branchName\":\"凤凰县凤凰路营业所\",\"level\":\"4\"},{\"branchCode\":\"43020165\",\"branchName\":\"凤凰县虹桥路支行\",\"level\":\"4\"},{\"branchCode\":\"43020177\",\"branchName\":\"凤凰县木江坪镇营业所\",\"level\":\"4\"},{\"branchCode\":\"4399935Q\",\"branchName\":\"凤凰县支行\",\"level\":\"4\"},{\"branchCode\":\"43020189\",\"branchName\":\"凤凰县山江镇营业所\",\"level\":\"4\"},{\"branchCode\":\"43020191\",\"branchName\":\"凤凰县茶田镇营业所\",\"level\":\"4\"},{\"branchCode\":\"43020203\",\"branchName\":\"凤凰县新场乡营业所\",\"level\":\"4\"},{\"branchCode\":\"43020215\",\"branchName\":\"凤凰县廖家桥镇营业所\",\"level\":\"4\"},{\"branchCode\":\"43020227\",\"branchName\":\"凤凰县阿拉镇营业所\",\"level\":\"4\"},{\"branchCode\":\"43022707\",\"branchName\":\"凤凰县柳薄乡营业所\",\"level\":\"4\"},{\"branchCode\":\"43022729\",\"branchName\":\"凤凰县米良乡营业所\",\"level\":\"4\"}]}"
//    println(str)
//    println(jsonToList[Map[String,Any]](str))
    val a="[{\"ORGAN_ID\":\"35000011\",\"ORGAN_NAME\":\"aaaa\",\"QUOTA\":77178.16113114754}]"

    println(jsonToMap(str2))

    val total=100
    val totalPage=10
    val rows=List(Map("ORGAN_ID"->"35000011","ORGAN_NAME"->"aaaa","QUOTA"->77178.16113114754))
    QueryCacheClusterSuccess(total,totalPage,rows,rows)
    println(QueryCacheClusterSuccess)
    println(Serialization.write(QueryCacheClusterSuccess(total,totalPage,rows,rows)))
  }
}

