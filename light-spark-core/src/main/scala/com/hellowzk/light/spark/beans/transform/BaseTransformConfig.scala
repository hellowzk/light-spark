package com.hellowzk.light.spark.beans.transform

import com.hellowzk.light.spark.beans.{BaseConfig, NodeTypes}

import scala.beans.BeanProperty

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:18
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class BaseTransformConfig extends BaseConfig {
  tag = NodeTypes.processes.toString

  @BeanProperty
  var clazz: String = _

  @BeanProperty
  var sql: String = _

  override protected def checkAnyIsNotBlank(): Unit = {
    validateAnyIsNotBlank("clazz", "sql")
  }

  override def getDefinedTables(): List[String] = {
    List(name)
  }
}
