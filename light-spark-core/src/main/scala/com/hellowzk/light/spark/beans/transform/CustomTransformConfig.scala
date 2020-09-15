package com.hellowzk.light.spark.beans.transform

import com.hellowzk.light.spark.stages.Transform.CustomTransformWorker

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
class CustomTransformConfig extends BaseTransformConfig {

  setWorkerClass(classOf[CustomTransformWorker].getName)

  override protected def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("clazz")
  }
}
