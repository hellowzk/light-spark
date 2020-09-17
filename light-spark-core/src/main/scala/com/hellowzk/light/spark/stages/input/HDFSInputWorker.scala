package com.hellowzk.light.spark.stages.input

import com.hellowzk.light.spark.stages.BaseWorker
import com.hellowzk.light.spark.uitils.HDFSUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
 * <p>
 * 日期： 2020/9/17
 * <p>
 * 时间： 15:48
 * <p>
 * 星期： 星期四
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 * <p>
 * 版权： Copyright © 2019 北京四维智联科技有限公司
 **/
trait HDFSInputWorker extends BaseWorker {

  protected def loadAsText(path: String, nullable: Boolean)(implicit sc: SparkContext): RDD[String] = {
    if (nullable && !HDFSUtils.apply.exists(path)) {
      sc.emptyRDD[String]
    } else {
      sc.textFile(path)
    }
  }
}
