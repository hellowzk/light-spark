package com.hellowzk.light.spark.beans.input

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
class KafkaInputItem extends Serializable {
  @BeanProperty
  var brokers: String = _
  @BeanProperty
  var topic: String = _
  @BeanProperty
  var groupId: String = _
  @BeanProperty
  var offersetReset: String = "earliest"
  @BeanProperty
  var autoCommit: java.lang.Boolean = false
  @BeanProperty
  var commitOffset: java.lang.Boolean = true
  /**
   * kafkaParams
   */
  @BeanProperty
  var params: java.util.List[String] = _
}
