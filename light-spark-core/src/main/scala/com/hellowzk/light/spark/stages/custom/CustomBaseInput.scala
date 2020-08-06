package com.hellowzk.light.spark.stages.custom

import com.hellowzk.light.spark.beans.BaseConfig
import com.hellowzk.light.spark.stages.BaseWorker
import org.apache.spark.rdd.RDD
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
trait CustomBaseInput extends BaseWorker {
  override def process(bean: BaseConfig)(implicit ss: SparkSession): Unit = {}

  /**
   * 自定义处理任务，处理 RDD 数据，生成 SparkSQL 表
   *
   * @param rdd  要处理的数据，框架已经加载为 RDD
   * @param name 建议处理结果生这个表名
   */
  def process(rdd: RDD[String], name: String)(implicit ss: SparkSession)
}
