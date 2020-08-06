package com.hellowzk.light.spark.beans.output

import com.hellowzk.light.spark.stages.output.JdbcOutptWorker

import scala.beans.BeanProperty
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
class JDBCOutputConfig extends BaseOutputConfig {
  @BeanProperty
  var driver: String = _
  @BeanProperty
  var url: String = _
  @BeanProperty
  var user: String = _
  @BeanProperty
  var password: String = _
  @BeanProperty
  var tables: java.util.Map[String, String] = _
  @BeanProperty
  var mode: String = "append"
  @BeanProperty
  var opts: java.util.Map[String, String] = _
  @BeanProperty
  var preSQL: java.util.List[String] = _

  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("driver", "url", "user", "password", "tables")
  }

  setWorkerClass(classOf[JdbcOutptWorker].getName)
}
