package com.hellowzk.light.spark.stages

import com.hellowzk.light.spark.beans.BaseConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * <p>
 * 日期： 2020/7/10
 * <p>
 * 时间： 9:49
 * <p>
 * 星期： 星期五
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
trait StreamBaseInputWorker extends BaseWorker {
  def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {}

  def initDS(bean: BaseConfig)(implicit ss: SparkSession, ssc: StreamingContext): DStream[ConsumerRecord[String, String]]
}
