package com.hellowzk.light.spark.beans.output

import com.hellowzk.light.spark.stages.output.HdfsOutputWorker

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
class HDFSOutputConfig extends BaseOutputConfig {
  @BeanProperty
  var format: String = _
  @BeanProperty
  var path: String = _
  @BeanProperty
  var fs: String = "\u0001"
  @BeanProperty
  var srcName: String = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("format", "path", "srcName")
  }

  setWorkerClass(classOf[HdfsOutputWorker].getName)
}
