package com.hellowzk.light.spark.beans.transform

import com.hellowzk.light.spark.stages.Transform.SQLTransformWorker

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
class SQLTransformConfig extends BaseTransformConfig {

  setWorkerClass(classOf[SQLTransformWorker].getName)

  @BeanProperty
  var dimKey: String = _

  @BeanProperty
  var allPlaceholder: String = "all"

  override protected def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("sql")
  }
}
