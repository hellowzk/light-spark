package com.hellowzk.light.spark.stages.input

import com.hellowzk.light.spark.beans.BaseConfig
import com.hellowzk.light.spark.beans.input.FileInputConfig
import com.hellowzk.light.spark.uitils.{AppUtil, HDFSUtils}
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
object HDFSTxtInputWorker {
  def apply(): HDFSTxtInputWorker = new HDFSTxtInputWorker()
}

class HDFSTxtInputWorker extends HDFSInputWorker {
  /**
   * 加载数据
   *
   * @param bean InputItemBean
   * @param ss   SparkSession
   */
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[FileInputConfig]
    val data = HDFSUtils.apply.loadHdfsTXT(item.path, item.fs, item.nullable)(ss.sparkContext)
    AppUtil.rddToTable(data, item.fs, item.columns, item.name)
    afterProcess(item)
  }
}
