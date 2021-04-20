package com.hellowzk.light.spark.stages.output

import java.sql.DriverManager

import com.hellowzk.light.spark.beans.BaseConfig
import com.hellowzk.light.spark.beans.output.JDBCOutputConfig
import com.hellowzk.light.spark.stages.BaseWorker
import com.hellowzk.light.spark.uitils.JDBCSparkUtils
import org.apache.commons.collections4.CollectionUtils
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConversions._

/**
 * <p>
 * 日期： 2020/1/6
 * <p>
 * 时间： 13:52
 * <p>
 * 星期： 星期一
 * <p>
 * 描述：结果写入到 jdbc
 * <p>
 * 作者： zhaokui
 *
 **/
class JdbcOutptWorker extends BaseWorker {
  override def process(config: BaseConfig)(implicit ss: SparkSession): Unit = {
    val item = config.asInstanceOf[JDBCOutputConfig]
    // 执行前置sql逻辑，一般为删除操作
    Option(item.preSQL).filter(pre => CollectionUtils.isNotEmpty(pre)).foreach(pre => {
      executePreSQL(item)
    })
    val filterd = JDBCSparkUtils.filterValues(item)
    if (item.opts != null && item.opts.nonEmpty) {
      for ((key, v) <- item.opts) {
        filterd.put(key, v)
        logger.info(s"use jdbc opts - $key -> $v")
      }
    }

    filterd.remove("tables")
    item.tables.foreach { case (src, dist) =>
      logger.info(s"jdbc output, start save '$src' to '$dist'...")
      val t1 = System.currentTimeMillis()
      val dfWriter = ss.table(src).write.mode(item.mode).format("jdbc")
      filterd.put("dbtable", dist)
      dfWriter.options(filterd).save()
      logger.info(s"jdbc output, save '$src' to '$dist' success cost ${System.currentTimeMillis() - t1}.")
    }
  }

  /**
   * 前置 SQL 逻辑
   *
   * @param item JDBC 配置
   */
  def executePreSQL(item: JDBCOutputConfig): Unit = {
    logger.info(s"connect url: ${item.url}")
    Class.forName(item.driver)
    val conn = DriverManager.getConnection(item.url, item.user, item.password)
    item.preSQL.foreach(sql => {
      logger.info(s"start execute preSQL: $sql")
      val stmt = conn.prepareStatement(sql)
      val result = stmt.executeUpdate()
      logger.info(s"execute preSQL success, result: $result.")
      stmt.close()
    })
    conn.close()
  }
}
