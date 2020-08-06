package com.hellowzk.light.spark.core.beans.`new`.input

import com.hellowzk.light.spark.beans.input.JDBCInputConfig
import org.apache.commons.collections.{CollectionUtils, Predicate}
import org.apache.commons.lang3.StringUtils
import org.junit.Test

import scala.collection.JavaConversions._

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 16:40
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class ConfigTest {
  @Test
  def testCheck(): Unit = {
    val jdbcInputConfig = new JDBCInputConfig()
    jdbcInputConfig.setName("testinput")
    jdbcInputConfig.setDriver("aa")
    jdbcInputConfig.setUrl("jdbc:xxx")
    jdbcInputConfig.setUser("user-root")
    jdbcInputConfig.setPassword("passwd-root")
    jdbcInputConfig.setDbtable(Map("a" -> "b"))
    jdbcInputConfig.doCheck()
    println("finish")
  }

  @Test
  def isAllBlank(): Unit = {

    val b = Map("a" -> "b")
    val a = StringUtils.isAnyBlank("null", "a", "b", null)

    val c = CollectionUtils.select(b, new Predicate() {
      override def evaluate(o: Any): Boolean = {
        o
        println(o)
        true
      }
    })
    c.foreach(println)
  }
}
