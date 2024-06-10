set ff=unix
BIN_PATH=$(cd `dirname $0`; pwd)
BASE_RUN_PATH=$(cd "$BIN_PATH/../"; pwd)
files=`sh $BASE_RUN_PATH/bin/ljars.sh $BASE_RUN_PATH`
echo $files
nohup ${SPARK_HOME}/bin/spark-submit \
 --class com.zyc.SystemInit \
 --driver-memory 800M \
 --conf "spark.driver.extraJavaOptions=-Dlog4j.configuration=file:$BASE_RUN_PATH/conf/log4j.properties" \
 --driver-class-path $BASE_RUN_PATH/conf:$BASE_RUN_PATH/libs \
 --files $BASE_RUN_PATH/conf/application.conf,$BASE_RUN_PATH/conf/datasources.properties \
 --jars $files \
 $BASE_RUN_PATH/zdh_spark.jar \
 >zdh_spark.log &
