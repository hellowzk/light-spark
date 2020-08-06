package com.hellowzk.light.spark.stages

import com.hellowzk.light.spark.function.BaseUDF
import org.apache.spark.sql.{SparkSession, functions}

/**
 * <p>
 * 日期： 2020/7/10
 * <p>
 * 时间： 16:38
 * <p>
 * 星期： 星期五
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 **/
class MyUDF extends BaseUDF {
  def addSuffix(str: String): String = {
    s"${str}_suffix"
  }

  override def setup()(implicit ss: SparkSession): Unit = {
    ss.udf.register("myudf1", functions.udf[String, String](addSuffix))
  }
}


