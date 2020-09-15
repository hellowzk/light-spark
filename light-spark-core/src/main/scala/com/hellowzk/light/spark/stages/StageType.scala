package com.hellowzk.light.spark.stages

/**
 * <p>
 * 日期： 2020/7/10
 * <p>
 * 时间： 9:49
 * <p>
 * 星期： 星期五
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object StageType extends Enumeration {
  type StageType = Value

  val inputs, processes, outputs = Value
}
