# 技术栈

   + spark 2.4.4
   + hadoop 3.1.x
   + hive > 2.3.3
   + kafka 1.x,2.x
   + scala 2.11.12
   + java 1.8
   
# 项目介绍

    数据采集ETL处理,通过spark 平台抽取数据,并根据etl 相关函数,做数据处理
    新增数据源需要继承ZdhDataSources 公共接口,重载部分函数即可
 
# 项目编译打包
    项目采用gradle 管理
    打包命令,在当前项目目录下执行
    window: gradlew.bat release -x test
    linux : ./gradlew release -x test
    
    项目需要的jar 会自动生成到relase/libs 目录下
    
    如果想单独打包项目代码
    window: gradlew.bat jar
    linux : ./gradlew jar
    
# 部署
    拷贝release/libs 下所有的jar 到spark的classpath 目录下(SPARK_HOME/jars)
    
# 启动脚本
    注意项目需要用到log4j.properties 需要单独放到driver 机器上,启动采用client 模式
    nohup ${SPARK_HOME}/bin/spark-submit \
       --class com.zyc.SystemInit \
       --driver-memory 2g \
       --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:/app/zyc/log4j.properties" \
       --conf spark.sql.catalogImplementation=hive \
       --jars /app/zyc/spark_zdh_sources/spark_zdh/release/libs/* \
       /app/zyc/spark_zdh_sources/spark_zdh/release/libs/zdh.jar \
      > spark_zdh.log &
    
# 停止脚本
     kill `ps -ef |grep SparkSubmit |grep zdh_server |awk -F ' ' '{print $2}'`

# 个人联系方式
    邮件：1209687056@qq.com