package com.hellowzk.light.spark.beans.input

import com.hellowzk.light.spark.stages.input.CustomFileInputWorker
/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:13
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class CustomHDFSInputConfig extends CustomInputConfig {
  setWorkerClass(classOf[CustomFileInputWorker].getName)
}
