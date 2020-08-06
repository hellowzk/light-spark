package com.hellowzk.light.spark.beans.input

import com.hellowzk.light.spark.stages.input.JdbcInputWorker

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:19
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：详细参数 http://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
 * <p>
 * 作者： zhaokui
 *
 **/
class JDBCInputConfig extends BaseInputConfig {

  @BeanProperty
  var driver: String = _
  @BeanProperty
  var url: String = _
  @BeanProperty
  var user: String = _
  @BeanProperty
  var password: String = _
  /**
   * MySQL表 -> SparkSQL表
   */
  @BeanProperty
  var dbtable: java.util.Map[String, String] = _

  // TODO other param
  override def checkNoneIsBlank(): Unit = {
    validateNoneIsBlank("driver", "url", "user", "password", "dbtable")
  }

  setWorkerClass(classOf[JdbcInputWorker].getName)

  override def getDefinedTables(): List[String] = {
    dbtable.values().toList
  }
}
