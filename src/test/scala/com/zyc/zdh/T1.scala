package com.zyc.zdh

import java.io.StringReader
import java.net.URI
import java.util
import java.util.Properties

import net.sf.jsqlparser.expression.Expression
import net.sf.jsqlparser.parser.CCJSqlParserManager
import net.sf.jsqlparser.statement.delete.Delete
import org.apache.hadoop.hive.ql.tools.LineageInfo
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.junit.Test
import org.kie.api.KieServices
import org.scalatest.FunSuite
@Test
class T1 {
  @Test
  def fdfdsf {
    val s="$zdh_etl_date_abc"

    println(s.contains("\\$zdh_etl_date"))
    println(s.replaceAll("\\$zdh_etl_date","1"))

  }
  @Test
  def abc {


    println("a|b|c|||".split("\\|",-1).mkString(","))
    val redisUri=URI.create("redis://:yld@127.0.0.1,127.0.0.2,127.0.0.3:6379")

    println(redisUri.getAuthority)

    println("20200101".substring(0,6))
  }
  @Test
  def covert {
    val s='|'
    val s1="\\"
    println(s)
    println(s1.replace("\\","\\\\"))

  }

  @Test
  def sort2 {

    var s1=Array(1,2,3,4)
    var s2=Array(5,6,7,8)
    sort(s1,s2)
    def sort(s1:Seq[Int],s2:Seq[Int]){
      //定义新数组
      var sortSeq:Array[Int]=new Array(8)
      val size1=s1.size
      val size2=s2.size
      var s1_index=0
      var s2_index=0
      var tmp:Int=0
      //如果
      while(s1_index<=s1.size-1 && s2_index<=s2.size-1) {
        if (s1(s1_index) >= s2(s2_index)) {
          sortSeq(tmp) = s2(s2_index)
          s2_index = s2_index + 1
          tmp = tmp + 1
        } else if (s1(s1_index) < s2(s2_index)) {
          sortSeq(tmp) = s1(s1_index)
          s1_index = s1_index + 1
          tmp = tmp + 1
        }
      }

      println(sortSeq.mkString(","))
      println(s1_index+"=="+s2_index+"=="+tmp)

        //s1 到达数组界限,s2 数组数据直接添加到新数组
        while(s1_index==size1 && s2_index<size2){
          sortSeq(tmp)=s2(s2_index)
        s2_index=s2_index+1
        tmp=tmp+1
      }

      while(s2_index==size2 && s1_index<size1){
        sortSeq(tmp)=s1(s1_index)
        s1_index=s1_index+1
        tmp=tmp+1
      }

      println(sortSeq.mkString(","))
    }


  }
  @Test
  def sort {
    //二维数组,M*N 每一行已经拍好序
    //结果求top(K)
    val datas=Array(Array(10,9),Array(8,7),Array(4,3),Array(6,5),Array(2,1))
    //定义分割几块
    val num=10
    //定义新数组
    val result=Array.empty[Array[Int]]

    splitDatas(datas,0,datas.length)
    //分割数组
    def splitDatas(datas:Array[Array[Int]],start:Int,end:Int): Unit ={
      //根据定义好的几块分割数组,循环调用topK
      //每个返回值数组放入新数组
    }
    def topK(datas:Array[Array[Int]]): Unit ={
      //长度为k
      val topAry=Array.empty[Int]
      //定义第一个值
      topAry(0)=datas(0)(0)
      //循环遍历datas
      //和topAry 做比较,如果大于则占用此下标,删除最后一位数据，此下标之后的数据全部后移一位
      //最终返回一个topk 数组
    }
    //最终再次调用topK
    topK(result)







  }

  @Test
  def kafka {

    val prop = new Properties
    // 指定请求的kafka集群列表
    prop.put("bootstrap.servers", "127.0.0.1:9092")// 指定响应方式
    //prop.put("acks", "0")
    prop.put("acks", "all")
    // 请求失败重试次数
    //prop.put("retries", "3")
    // 指定key的序列化方式, key是用于存放数据对应的offset
    prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 指定value的序列化方式
    prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // 配置超时时间
    prop.put("request.timeout.ms", "60000")


    // 3. 构建Producer对象
    val producer = new KafkaProducer[String,String](prop)

    producer.send(new ProducerRecord[String, String]("topic1",  "{\"name\":\"z\",\"age\":\"26\",\"job\":{\"addr\":\"ad1\",\"context\":\"first job\"}}")).get()
    // 4. 发送数据到服务器，并发线程发送

  }
  @Test
  def testURI {

    val sql = "delete from t1 where t1>='1'     and t2='a'    and t3='' or t4=4"
    val parser = new CCJSqlParserManager();
    val list = new util.ArrayList[String]();
    val stmt = parser.parse(new StringReader(sql));


    if (stmt.isInstanceOf[Delete]) {
      val whereExpr: Expression = stmt.asInstanceOf[Delete].getWhere
      println(whereExpr.toString)
      whereExpr.getASTNode
//      whereExpr.accept(
//        new ExpressionVisitorAdapter() {
//          override def visit(expr: AndExpression): Unit = {
//            if (expr.getLeftExpression.isInstanceOf[AndExpression])
//              expr.getLeftExpression.accept(this)
//            else if (expr.getLeftExpression.isInstanceOf[EqualsTo]) {
//              expr.getLeftExpression.accept(this)
//              System.out.println(expr.getLeftExpression)
//            }else if (expr.getLeftExpression.isInstanceOf[GreaterThan]) {
//              expr.getLeftExpression.accept(this)
//              System.out.println(expr.getLeftExpression)
//            }else if (expr.getLeftExpression.isInstanceOf[GreaterThanEquals]) {
//              expr.getLeftExpression.accept(this)
//              System.out.println(expr.getLeftExpression)
//            }else if (expr.getLeftExpression.isInstanceOf[MinorThan]) {
//              expr.getLeftExpression.accept(this)
//              System.out.println(expr.getLeftExpression)
//            }else if (expr.getLeftExpression.isInstanceOf[MinorThanEquals]) {
//              expr.getLeftExpression.accept(this)
//              System.out.println(expr.getLeftExpression)
//            }
//            expr.getRightExpression.accept(this)
//            System.out.println(expr.getRightExpression)
//          }
//
//          override def visit(expr: EqualsTo): Unit = {
//            //          System.out.println(expr.getLeftExpression)
//            //          System.out.println(expr.getStringExpression)
//            //          System.out.println(expr.getRightExpression)
//          }
//        })
//      whereExpr.getASTNode
    }


    val paths = "/t1.txt"


    val zk = "jdbc:hive2://127.0.0.1:2181,127.0.0.2:2181/dbName"


    // val uri=URI.create(zk.substring("jdbc:".length))


    val hadoop = "hdfs://192.168.65.10:8020,192.168.65.11:8020/p1/f1"
    var uri = URI.create(hadoop)
    val hadoop1 = "hdfs://192.168.65.10:8020/p1/f1"
    uri = URI.create(hadoop1)
    val hadoop2 = "/p1/f1"
    uri = URI.create(hadoop2)

    print(uri.toString)

  }
  @Test
  def drools {
    val rules="package rules\n " +
      "import com.zyc.drools.D1\n"+
      "import java.util.HashMap\n"+
      "rule \"alarm\"\n"+
      "no-loop true\n"+
      "when\n"+
      "d1:HashMap(1==1)\n"+
      "then\n"+
      "d1.put(\"age\",\"22\");\n"+
      "update(d1);\n"+
      "D1 d2=new D1();\n"+
      "d2.update(\"hello world\");\n"+
      "System.out.print(\"dddddddddd=\"+d1.get(\"id\"));\n"+
      "end"



    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]


    val kieServices = KieServices.Factory.get();
    val kfs = kieServices.newKieFileSystem();
    kfs.write("src/main/resources/rules/rules.drl", rules.getBytes());
    val kieBuilder = kieServices.newKieBuilder(kfs).buildAll();
    val results = kieBuilder.getResults();
    if (results.hasMessages(org.kie.api.builder.Message.Level.ERROR)) {
      System.out.println(results.getMessages());
      throw new IllegalStateException("### errors ###");
    }
    val kieContainer = kieServices.newKieContainer(kieServices.getRepository().getDefaultReleaseId());
    val kieBase = kieContainer.getKieBase();
    val ksession = kieBase.newKieSession()
    import scala.collection.JavaConverters._

    val map=Map("id"->"1")
   // var m2=map.asJava
    var m2=new util.HashMap[String,String]()
    m2.put("id","1")
    ksession.insert(m2)
    val res=ksession.fireAllRules()
    println(m2.asScala.mkString(","))



  }
  @Test
  def line_query {
    val li=new LineageInfo()
    val sql="insert overwrite table dwd.d1 select * from (select * from ods.d1) s1 where abc=''"
    li.getLineageInfo(sql)

    for( table <- li.getInputTableList.toArray()){

      println(table)
    }
    for( table <- li.getOutputTableList.toArray()){

      println(table)
    }



  }
}
