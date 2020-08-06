package com.hellowzk.light.spark.beans

import java.util

import scala.beans.BeanProperty

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 14:47
 * <p>
 * 星期：
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class EnvConfig extends Serializable {
  @BeanProperty
  var spark: java.util.List[String] = new util.ArrayList[String]()
}
