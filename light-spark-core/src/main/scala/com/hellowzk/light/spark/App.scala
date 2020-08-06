package com.hellowzk.light.spark

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeFilter
import com.hellowzk.light.spark.beans.BusinessConfig
import com.hellowzk.light.spark.config.{BusConfig, CacheConstants}
import com.hellowzk.light.spark.constants.{AppConstants, SysConstants}
import com.hellowzk.light.spark.stages.{BatchPip, StreamPip}
import com.hellowzk.light.spark.uitils.{Logging, SparkUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.JavaConversions._
/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 15:08
 * <p>
 * 星期：
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object App extends Logging {
  def main(args: Array[String]): Unit = {
    val appConfig = BusConfig.apply.parseOptions(args)
    logger.info(s"load config success, event date is ${appConfig.eventDate}, config file is ${appConfig.configFile}.")
    val confMap: java.util.Map[String, String] = AppConstants.variables
    val strConf = JSON.toJSONString(confMap, new Array[SerializeFilter](0))

    val sparkConf = SparkUtil.getSparkConf(appConfig, strConf)
    if (appConfig.isStreaming) {
      stream(appConfig, sparkConf)
    } else {
      batch(appConfig, sparkConf)
    }
    cleanUp()
    logger.info(s"context exit success.")
  }

  private def batch(appConfig: BusinessConfig, sparkConf: SparkConf): Unit = {
    logger.info("start batch process.")
    implicit val sparkSession: SparkSession = SparkUtil.initSparkSession(sparkConf, appConfig.hiveEnabled)
    BatchPip.startPip(appConfig)
    sparkSession.stop()
  }

  private def stream(appConfig: BusinessConfig, sparkConf: SparkConf): Unit = {
    logger.info("start stream process.")
    implicit val sparkSession: SparkSession = SparkUtil.initSparkSession(sparkConf, appConfig.hiveEnabled)
    implicit val ssc: StreamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(appConfig.streamBatchSeconds))
    StreamPip.startPip(appConfig)
    ssc.stop()
    sparkSession.stop()
  }

  private def cleanUp(): Unit = {
    AppConstants.variables.clear()
    SysConstants.SYS_SPARK_CONFIG.clear()
    SysConstants.SYS_DEFALUT_VARIABLES.clear()
    SysConstants.SYS_DEFINED_TABLES.clear()
    CacheConstants.rdds.clear()
    CacheConstants.tables.clear()
  }
}