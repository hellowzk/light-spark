package com.hellowzk.light.spark.uitils

import org.apache.hadoop.hbase.client.{ConnectionFactory, Result, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, HConstants, TableName}
/**
 * <p>
 * 日期： 2020/7/10
 * <p>
 * 时间： 9:49
 * <p>
 * 星期： 星期五
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object HBaseUtil {
  /**
   * 通过连接池方式获取 HTable
   *
   */
  def getTable(tableName: String, zookeeperQuorum: String, port: String): Table = {
    val config = HBaseConfiguration.create()
    config.set(HConstants.ZOOKEEPER_QUORUM, zookeeperQuorum)
    config.set(HConstants.ZOOKEEPER_CLIENT_PORT, port)
    val connection = ConnectionFactory.createConnection(config)
    connection.getTable(TableName.valueOf(tableName))
  }

  def getColumnValueString(result: Result, columnFamily: String, columnName: String, default: String): String = {
    val byteVale = getColumnValueBytes(result, columnFamily, columnName)
    if (byteVale != null) {
      Bytes.toString(byteVale)
    } else {
      default
    }
  }

  def getColumnValueInt(result: Result, columnFamily: String, columnName: String, default: Int): Int = {
    val byteVale = getColumnValueBytes(result, columnFamily, columnName)
    if (byteVale != null) {
      Bytes.toInt(byteVale)
    } else {
      default
    }
  }

  def getColumnValueDouble(result: Result, columnFamily: String, columnName: String, default: Double): Double = {
    val byteVale = getColumnValueBytes(result, columnFamily, columnName)
    if (byteVale != null) {
      if (byteVale.length == 4) {
        Bytes.toInt(byteVale).toDouble
      } else {
        Bytes.toDouble(byteVale)
      }
    } else {
      default
    }
  }

  def getColumnValueLong(result: Result, columnFamily: String, columnName: String, default: Long): Long = {
    val byteVale = getColumnValueBytes(result, columnFamily, columnName)
    if (byteVale != null) {
      Bytes.toLong(byteVale)
    } else {
      default
    }
  }

  def getColumnValueBoolean(result: Result, columnFamily: String, columnName: String, default: Boolean): Boolean = {
    val byteVale = getColumnValueBytes(result, columnFamily, columnName)
    if (byteVale != null) {
      Bytes.toBoolean(byteVale)
    } else {
      default
    }
  }

  def getColumnValueBytes(result: Result, columnFamily: String, columnName: String): Array[Byte] = {
    result.getValue(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName))
  }
}
