package com.hellowzk.light.spark.beans

/**
 * <p>
 * 日期： 2020/7/3
 * <p>
 * 时间： 17:38
 * <p>
 * 星期： 星期五
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object NodeTypes extends Enumeration {
  type NodeTypes = Value
  val root,
  //  和 BusinessConfig 中属性一致
  inputs,
  //  和 BusinessConfig 中属性一致
  outputs,
  //  和 BusinessConfig 中属性一致
  processes = Value
}
