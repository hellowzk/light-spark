package com.hellowzk.light.spark.beans.output

import com.hellowzk.light.spark.stages.output.KafkaFieldOutputWorker

import scala.beans.BeanProperty
/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:13
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class KafkaFieldOutputConfig extends KafkaOutputConfig {
  @BeanProperty
  var fs: String = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("srcName", "brokers", "topic", "fs")
  }

  setWorkerClass(classOf[KafkaFieldOutputWorker].getName)
}
