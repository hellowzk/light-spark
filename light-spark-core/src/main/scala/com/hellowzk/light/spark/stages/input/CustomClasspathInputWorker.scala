package com.hellowzk.light.spark.stages.input

import com.hellowzk.light.spark.beans.BaseConfig
import com.hellowzk.light.spark.beans.input.CustomInputConfig
import com.hellowzk.light.spark.stages.BaseWorker
import com.hellowzk.light.spark.stages.custom.CustomBaseInput
import com.hellowzk.light.spark.uitils.{HDFSUtils, ReflectUtils}
import org.apache.spark.sql.SparkSession
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
class CustomClasspathInputWorker extends BaseWorker {

  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[CustomInputConfig]
    val rdd = HDFSUtils.apply.loadClasspathFile(item.path)(ss.sparkContext)
    ReflectUtils.apply.getInstance[CustomBaseInput](item.clazz).process(rdd, item.getName)
    afterProcess(item)
  }
}
