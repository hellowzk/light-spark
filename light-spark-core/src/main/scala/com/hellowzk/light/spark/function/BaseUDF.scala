package com.hellowzk.light.spark.function

import com.hellowzk.light.spark.uitils.Logging
import org.apache.spark.sql.SparkSession

/**
 * <p>
 * 日期： 2020/7/13
 * <p>
 * 时间： 16:28
 * <p>
 * 星期： 星期一
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
trait BaseUDF extends Logging {
  def setup()(implicit ss: SparkSession): Unit
}
