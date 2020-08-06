package com.hellowzk.light.spark.stages.output

import com.hellowzk.light.spark.stages.BaseWorker
import com.hellowzk.light.spark.uitils.KafkaUtils

/**
 * <p>
 * 日期： 2020/4/15
 * <p>
 * 时间： 14:13
 * <p>
 * 星期： 星期三
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
trait KafkaOutputWorker extends BaseWorker {
  def sendData(iter: scala.Iterator[String], brokers: String, topic: String): Unit = {
    val toSend = iter.toList
    if (toSend.nonEmpty) {
      val producer = KafkaUtils.getKafkaProducer(brokers)
      toSend.foreach(line => KafkaUtils.sendMessage(producer, topic, line))
      producer.close()
    }
  }
}
