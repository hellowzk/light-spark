package com.hellowzk.light.spark.beans.input

import com.hellowzk.light.spark.stages.input.HDFSCsvInputWorker

import scala.beans.BeanProperty

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:22
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class HDFSCsvInputConfig extends BaseInputConfig {

  @BeanProperty
  var columns: String = _
  @BeanProperty
  var path: String = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("columns", "path")
  }

  setWorkerClass(classOf[HDFSCsvInputWorker].getName)
}
