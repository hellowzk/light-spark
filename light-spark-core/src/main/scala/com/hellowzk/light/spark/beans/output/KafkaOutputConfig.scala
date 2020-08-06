package com.hellowzk.light.spark.beans.output

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
class KafkaOutputConfig extends BaseOutputConfig {
  @BeanProperty
  var srcName: String = _
  @BeanProperty
  var brokers: String = _
  @BeanProperty
  var topic: String = _
}
