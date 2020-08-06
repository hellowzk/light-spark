package com.hellowzk.light.spark.beans.input

import com.hellowzk.light.spark.constants.SysConstants
import org.apache.commons.lang3.StringUtils

import scala.beans.BeanProperty

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
class FileInputConfig extends BaseInputConfig {

  @BeanProperty
  var columns: String = _
  @BeanProperty
  var path: String = _
  @BeanProperty
  var fs: String = _

  override def setFieldDefault(): Unit = {
    Option(this).filter(obj => StringUtils.isBlank(obj.fs))
      .foreach(obj => {
        obj.fs = SysConstants.apply.HDFS_DATA_DEFAULT_FIELD_SEPARATOR
        logger.info(s"in '$tag', item '$name' field 'fs' is undefind, set default '\\u0001'.")
      })
  }

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("columns", "path", "fs")
  }
}
