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
class CustomInputConfig extends BaseInputConfig {
  @BeanProperty
  var clazz: String = _
  @BeanProperty
  var path: String = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("clazz", "path")
  }

}
