# 技术栈

   + spark 2.4.4
   + hadoop 3.1.x
   + hive > 2.3.3
   + kafka 1.x,2.x
   + scala 2.11.12
   + java 1.8
   + hbase < 1.3.6 (可选)

# 提示
    zdh_server改名为zdh_spark
    zdh 分2部分,前端配置+后端数据ETL处理,此部分只包含ETL处理
    前端配置项目 请参见项目 https://github.com/zhaoyachao/zdh_web
    zdh_web 和zdh_server 保持同步 大版本会同步兼容 如果zdh_web 选择版本1.0 ,zdh_server 使用1.x 都可兼容
    二次开发同学 请选择dev 分支,dev 分支只有测试通过才会合并master,所以master 可能不是最新的,但是可保证可用性
    
#  在线预览
    http://zycblog.cn:8081/login
    用户名：zyc
    密码：123456
    
    服务器资源有限,界面只供预览,不包含数据处理部分,谢码友们手下留情    
   
# 项目介绍

    数据采集ETL处理,通过spark 平台抽取数据,并根据etl 相关函数,做数据处理
    新增数据源需要继承ZdhDataSources 公共接口,重载部分函数即可
 
# 项目编译打包
    项目采用maven 管理
    打包命令,在当前项目目录下执行
    window: mvn package -Dmaven.test.skip=true 
   
    项目需要的jar 会自动生成到zdh_spark-xxxx-RELEASE 目录下
   
    
# 部署
    1 拷贝zdh_spark-xxxx-RELEASE到服务器(linux)
    2 拷贝zdh_spark-xxxx-RELEASE/copy_spark_jars 目录下的jar 拷贝到spark home 目录下的jars 目录
    3 修改zdh_spark-xxxx-RELEASE/conf/datasources.propertites
    4 修改zdh_spark-xxxx-RELEASE/conf/log4j.propertites
    5 配置系统spark环境变变量SPARK_HOME
    6 启动脚本 start_server.sh
    
# 启动脚本
    注意项目需要用到log4j.properties 需要单独放到driver 机器上,启动采用client 模式
      
    
# 停止脚本
     kill `ps -ef |grep SparkSubmit |grep zdh_server |awk -F ' ' '{print $2}'`

# 个人联系方式
    邮件：1209687056@qq.com
    
# FAQ
    使用tidb 连接时,需要在zdh_server 启动配置文件中添加如下配置
    spark.tispark.pd.addresses 192.168.1.100:2379
    spark.sql.extensions org.apache.spark.sql.TiExtensions
    
# 版本更新说明
   + v5.1.1 修复http数据源
   + v5.3.0 优化pom文件
   
   + v5.3.4 支持消息队列获取任务
   + v5.3.5 升级优先级队列版本
   + v5.3.6 无改动仅配合版本变更
   
   + v5.4.0 修复启动jar缺失redisson,hutool
   
   + v5.6.3 hdfs支持kerberos认证