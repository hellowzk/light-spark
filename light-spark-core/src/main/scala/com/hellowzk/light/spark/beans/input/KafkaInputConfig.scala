package com.hellowzk.light.spark.beans.input

import com.hellowzk.light.spark.stages.input.KafkaInputWorker

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
class KafkaInputConfig extends StreamInputConfig {
  @BeanProperty
  var items: java.util.List[KafkaInputItem] = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("clazz", "items")
  }

  setWorkerClass(classOf[KafkaInputWorker].getName)
}
