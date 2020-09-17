package com.hellowzk.light.spark.stages.input

import com.hellowzk.light.spark.beans.BaseConfig
import com.hellowzk.light.spark.beans.input.CustomInputConfig
import com.hellowzk.light.spark.stages.custom.CustomBaseInput
import com.hellowzk.light.spark.uitils.ReflectUtils
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
class CustomFileInputWorker extends HDFSInputWorker {
  /**
   * 加载数据
   *
   * @param bean BaseInputConfig
   * @param ss   SparkSession
   */
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[CustomInputConfig]
    // 如果数据 path 不存在，且配置定义该数据允许为空，则给一个空的 rdd
    val rdd = loadAsText(item.path, item.nullable)(ss.sparkContext)

    ReflectUtils.apply.getInstance[CustomBaseInput](item.clazz).process(rdd, item.getName)
    afterProcess(item)
  }
}
