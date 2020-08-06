package com.hellowzk.light.spark.beans
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
object PersistTypes extends Enumeration {
  type PersistTypes = Value
  val hdfs, hive = Value
}
