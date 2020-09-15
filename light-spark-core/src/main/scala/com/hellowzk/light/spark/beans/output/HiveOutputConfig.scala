package com.hellowzk.light.spark.beans.output

import com.hellowzk.light.spark.stages.output.HiveOutputWorker

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
class HiveOutputConfig extends BaseOutputConfig {

  @BeanProperty
  var database: String = _
  @BeanProperty
  var mode: String = "overwrite"
  /**
   * src table , dist table
   */
  @BeanProperty
  var tables: java.util.Map[String, String] = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("database", "tables")
  }

  setWorkerClass(classOf[HiveOutputWorker].getName)
}
