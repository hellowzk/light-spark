package com.hellowzk.light.spark.beans.output

import com.hellowzk.light.spark.beans.{BaseConfig, NodeTypes}
import org.apache.commons.lang3.StringUtils

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:16
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class BaseOutputConfig extends BaseConfig {
  tag = NodeTypes.outputs.toString

  override def nameCheck(): Unit = {
    super.nameCheck()
    require(StringUtils.isNotBlank(this.`type`), s"In node '${this.tag}', 'type' is required in item '${this.name}'!")
  }
}
