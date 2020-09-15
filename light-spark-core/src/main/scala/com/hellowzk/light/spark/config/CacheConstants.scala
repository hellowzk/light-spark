package com.hellowzk.light.spark.config

import scala.collection.mutable.ListBuffer

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:13
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：全局 cached 表信息存储
 * <p>
 * 作者： zhaokui
 *
 **/
object CacheConstants {
  val tables: ListBuffer[String] = ListBuffer.empty[String]


  val rdds = new java.util.ArrayList[Object]()
}
