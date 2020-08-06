package com.hellowzk.light.spark.stages.Transform

import com.hellowzk.light.spark.beans.BaseConfig
import com.hellowzk.light.spark.beans.transform.CustomTransformConfig
import com.hellowzk.light.spark.stages.BaseWorker
import com.hellowzk.light.spark.stages.custom.CustomBaseTransform
import com.hellowzk.light.spark.uitils.ReflectUtils
import org.apache.spark.sql.SparkSession

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 15:43
 * <p>
 * 星期：
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object CustomTransformWorker {
  def apply: CustomTransformWorker = new CustomTransformWorker()
}

class CustomTransformWorker extends BaseWorker {
  override def process(item: BaseConfig)(implicit ss: SparkSession): Unit = {
    val config = item.asInstanceOf[CustomTransformConfig]
    ReflectUtils.apply.getInstance[CustomBaseTransform](config.clazz).doProcess(config)
    if (ss.catalog.tableExists(item.name)) {
      afterProcess(item)
    }
  }
}
