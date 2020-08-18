#!/usr/bin/env bash
set -e
umask 0000

APP_HOME="$(cd $(dirname $0)/..; pwd -P)"
ROOT_PATH="$(cd $APP_HOME/..; pwd -P)"
currentTime=`date '+%Y-%m-%d_%H-%M-%S'`

if [ $# -eq 1 ];then
  params="-c qujia_prd.yaml -d $1"
elif [ $# -eq 2 ];then
  params="-c $2 -d $1"
# debug
# params="-c $2 -d $1 --debug"
else
  echo "useage: $0 <date-yyyyMMdd> <conf>"
  exit 1
fi
STREAM_APP="com.hellowzk.light.spark.App" # 程序入口
echo "__________________________ light-spark start __________________________"

CLASSPATH="$APP_HOME/conf"

confList=`find $APP_HOME/conf/ -type f|sed ':a;N;s/\n/,/;ta;'` # 扫描目录下所有配置信息
APP_JAR="light-spark-assembly.jar" # 指定 jar 包名称
cmd="spark-submit"
cmd="$cmd --master yarn --deploy-mode cluster"
cmd="$cmd --files $confList"
cmd="$cmd --conf spark.app.name=light-spark-$1" # 指定 hadoop 应用名称
cmd="$cmd --conf spark.driver.extraClassPath=$CLASSPATH"
cmd="$cmd --conf spark.yarn.submit.waitAppCompletion=false"
cmd="$cmd --class $STREAM_APP $APP_HOME/lib/$APP_JAR $params"

echo "command to execute $0: $cmd" # > $APP_HOME/log/light-spark-$1.log
$cmd