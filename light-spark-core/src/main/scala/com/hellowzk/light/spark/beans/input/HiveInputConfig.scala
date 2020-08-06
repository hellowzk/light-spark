package com.hellowzk.light.spark.beans.input

import com.hellowzk.light.spark.stages.input.HiveInputWorker

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

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
class HiveInputConfig extends BaseInputConfig {

  @BeanProperty
  var database: String = _
  @BeanProperty
  var dbtable: java.util.Map[String, String] = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("database", "dbtable")
  }

  override def getDefinedTables(): List[String] = {
    dbtable.values().toList
  }

  setWorkerClass(classOf[HiveInputWorker].getName)
}
