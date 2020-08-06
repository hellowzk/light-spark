package com.hellowzk.light.spark.beans.input

import com.hellowzk.light.spark.stages.input.HDFSTxtInputWorker

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:20
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class TxtInputConfig extends FileInputConfig {
  setWorkerClass(classOf[HDFSTxtInputWorker].getName)
}
