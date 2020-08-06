package com.hellowzk.light.spark.stages.input

import com.hellowzk.light.spark.beans.BaseConfig
import com.hellowzk.light.spark.beans.input.FileInputConfig
import com.hellowzk.light.spark.stages.BaseWorker
import com.hellowzk.light.spark.uitils.{AppUtil, HDFSUtils}
import org.apache.spark.sql.SparkSession

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 15:45
 * <p>
 * 星期：
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 **/
object ClasspathFileInputWorker {
  def apply(): ClasspathFileInputWorker = new ClasspathFileInputWorker()
}

class ClasspathFileInputWorker extends BaseWorker {
  /**
   * 加载数据
   *
   * @param bean InputItemBean
   * @param ss   SparkSession
   */
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = bean.asInstanceOf[FileInputConfig]
    val data = HDFSUtils.apply.loadClasspathFile(item.path, item.fs, item.nullable)(ss.sparkContext)
    AppUtil.rddToTable(data, item.fs, item.columns, item.name)
    afterProcess(item)
  }
}
