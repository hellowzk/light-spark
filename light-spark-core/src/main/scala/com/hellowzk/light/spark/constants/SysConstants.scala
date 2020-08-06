package com.hellowzk.light.spark.constants

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 11:20
 * <p>
 * 星期：
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object SysConstants {

  private[spark] val SYS_DEFALUT_VARIABLES = scala.collection.mutable.Map.empty[String, String]
  private[spark] val SYS_SPARK_CONFIG = scala.collection.mutable.Map.empty[String, String]
  private[spark] val SYS_DEFINED_TABLES = scala.collection.mutable.Set.empty[String]

  def apply: SysConstants = new SysConstants()
}

class SysConstants {
  /*列分隔符*/
  val HDFS_DATA_DEFAULT_FIELD_SEPARATOR: String = "\u0001"
  // 匹配 ${}
  val VARIABLE_REGEX = "\\$\\{((?!\\$)[^}]*)\\}"
  // 匹配 DATE()，获取日期表达式
  val DATE_REGEX = "DATE\\s*\\((.*?)\\)"
  // 匹配 "$   {", 用于匹配出来后去除空串, 替换成 "${"
  val REGEX_HEAD = "\\$\\s*\\{"
  // dim SQL 维度表达式
  val DIM_FIELDS_EXPR = "DIM_FIELDS"
  // dim SQL 分组表达式
  val DIM_GROUP_EXPR = "DIM_GROUP"
}