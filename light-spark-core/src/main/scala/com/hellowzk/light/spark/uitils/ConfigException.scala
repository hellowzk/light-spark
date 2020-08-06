package com.hellowzk.light.spark.uitils

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 20:34
 * <p>
 * 星期：
 * <p>
 * 描述：自定义异常
 * <p>
 * 作者： zhaokui
 *
 **/
class ConfigException extends Throwable with Logging {
  def this(msg: String, e: Throwable) {
    this()
    logger.error(msg, e)
  }

  def this(msg: String) {
    this()
    logger.error(msg)
  }
}