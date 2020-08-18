package com.hellowzk.light.spark.uitils

import java.lang.reflect.Field
import java.util.regex.Pattern

import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ListBuffer

/**
 * <p>
 * 日期： 2020/4/25
 * <p>
 * 时间： 14:21
 * <p>
 * 星期： 星期六
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object AppUtil extends Logging {
  /**
   * bean 转 List， javaBean 的属性值放到一个 List 中，顺序和 bean 中属性索引位置一致
   *
   * @param bean
   * @return
   */
  def beanToList(bean: Object): List[Object] = {
    val values = ListBuffer.empty[Object]
    val fields = bean.getClass.getDeclaredFields
    for (i <- fields.indices) {
      val field = fields(i)
      field.setAccessible(true)
      values.append(field.get(bean))
    }
    values.toList
  }

  /**
   * rdd 转 table
   *
   * @param data    要处理的 数据
   * @param fs      列分隔符
   * @param columns 要处理的数据分割后对应的列名，用逗号 "," 分隔
   * @param tableName
   * @param ss
   */
  def rddToTable(data: RDD[String], fs: String, columns: String, tableName: String)(implicit ss: SparkSession): Unit = {
    val columnNames = columns.split(",", -1).map(_.trim)
    val rdd = data.map(line =>
      line.split(Pattern.quote(fs), -1).map(_.trim)
    ).filter(cols =>
      if (cols.length == columnNames.length)
        true
      else {
        logger.warn(s"malformed line, fields count must be ${columnNames.length}, but got ${cols.length}.")
        false
      }
    )
    val rows = rdd.map(cols => Row.fromSeq(cols))
    val columnsStruct = columnNames.map(StructField(_, StringType))
    ss.createDataFrame(rows, StructType(columnsStruct)).createOrReplaceTempView(tableName)
    logger.info(s"load $tableName success.")
  }

  /**
   *
   * @param tarBean     目标对象
   * @param srcValues   源数据数据
   * @param targetStart bean field 起始 index
   * @param targetEnd   bean field 结束 index (包含)
   * @param srcStart    value 起始 index
   * @param srcEnd      value 结束 index (包含)
   */
  def listToBean(tarBean: Object, srcValues: List[Any], targetStart: Int, targetEnd: Int, srcStart: Int, srcEnd: Int): Unit = {
    require(targetEnd - targetStart == srcEnd - srcStart, s"bEnd - bStart(${targetEnd - targetStart}) != vEnd - vStart(${srcEnd - srcStart})")
    val fields = tarBean.getClass.getDeclaredFields
    val beaFileds = ListBuffer.empty[Field]
    val value2Set = ListBuffer.empty[Any]
    for (i <- targetStart to targetEnd) {
      beaFileds.append(fields(i))
    }

    for (i <- srcStart to srcEnd) {
      value2Set.append(srcValues(i))
    }

    for (i <- value2Set.indices) {
      val field = beaFileds(i)
      val theValue = if (value2Set(i) != null) value2Set(i).toString else null
      if (theValue != null) {
        val fieldtype = field.getType.getName
        try {
          field.setAccessible(true)
          var value: Any = null
          if (fieldtype == classOf[String].getName) {
            value = theValue
          } else if (fieldtype == classOf[Int].getName) {
            value = if (StringUtils.isBlank(theValue)) -1 else theValue.toInt
          } else if (fieldtype == classOf[Long].getName) {
            value = if (StringUtils.isBlank(theValue)) -1l else theValue.toLong
          } else if (fieldtype == classOf[Double].getName) {
            value = if (StringUtils.isBlank(theValue)) -1.0 else theValue.toDouble
          } else if (fieldtype == classOf[Float].getName) {
            value = if (StringUtils.isBlank(theValue)) -1.0 else theValue.toFloat
          } else {
            throw new RuntimeException(s"unsupport field type $fieldtype.")
          }
          field.set(tarBean, value)
        } catch {
          case e: Throwable =>
            logger.error(s"${field.getName}: $fieldtype.", e)
            throw e
        }
      }
    }
  }

  /**
   * 配置字符串
   *
   * @param regex
   * @param str
   * @return
   */
  private def regexFind(regex: String, str: String, group: Int) = {
    val p = Pattern.compile(regex)
    val lst = ListBuffer.empty[String]
    val m = p.matcher(str)
    while (m.find) {
      val str = m.group(group)
      lst.append(str)
    }
    lst.toList
  }

  /**
   * 配置字符串 包含外面的括号
   */
  def regexFindWithRule(regex: String, str: String): List[String] = {
    regexFind(regex, str, 0)
  }

  /**
   * 配置字符串 不包含外面的括号
   */
  def regexFindWithoutRule(regex: String, str: String): List[String] = {
    regexFind(regex, str, 1).map(_.trim)
  }


  private def doFind(string: String, leftFlag: Int, rightFlag: Int, flagDetail: scala.collection.mutable.ListBuffer[ExprEntity], lst: scala.collection.mutable.ListBuffer[String]): Unit = {
    val expired = scala.collection.mutable.ListBuffer.empty[ExprEntity]
    for (i <- flagDetail.indices) {
      if (flagDetail(i).flag == leftFlag && flagDetail(i + 1).flag == rightFlag) {
        expired.append(flagDetail(i), flagDetail(i + 1))
        val str = string.substring(flagDetail(i).index, flagDetail(i + 1).index)
        lst.append(str)
      }
    }
    val surplusList = flagDetail.diff(expired)
    val surplusSize = surplusList.size
    val leftSize = surplusList.count(_.flag == leftFlag)
    val rightSize = surplusList.count(_.flag == rightFlag)
    if (surplusSize != 0 && surplusSize != leftSize && surplusSize != rightSize) {
      doFind(string, leftFlag, rightFlag, surplusList, lst)
    }
  }

  /**
   * 获取变量，找出字符串中 变量表达式
   *
   * @param str
   * @return map(变量表达式, xxx)
   */
  def getVariableExprMap(str: String): Map[String, String] = {
    getVariableExprList(str).map(str => (str, str.substring(2, str.length - 1).trim)).toMap
  }

  /**
   * 处理 "$     {" 为 "${", 去除中间的空串
   *
   * @param str
   * @return
   */
  def formatVariableStr(str: String): String = {
    // 替换 "$   {" 为 "${"
    val regex = "\\$\\s*\\{"
    val dropBlankExpr = "${"
    val strings = AppUtil.regexFindWithRule(regex, str)
    var dropBlankStr = str
    strings.foreach(r => {
      dropBlankStr = dropBlankStr.replace(r, dropBlankExpr)
    })
    dropBlankStr
  }

  /**
   * 获取变量，找出字符串中 变量表达式
   *
   * @param str
   * @return List(变量表达式)
   */
  def getVariableExprList(str: String): List[String] = {
    val dropBlankStr = formatVariableStr(str)
    // 0 代表 "${", 1 代表 "}"
    val leftFlag = 0
    val rightFlag = 1
    // 两种类型 "${" "}" 的下标放入容器
    val flagDetail = scala.collection.mutable.ListBuffer.empty[ExprEntity]
    for (i <- dropBlankStr.indices) {
      if (i > 0 && dropBlankStr.charAt(i) == '{' && dropBlankStr.charAt(i - 1) == '$') {
        flagDetail.append(ExprEntity(i - 1, leftFlag))
      } else if (i > 1 && dropBlankStr.charAt(i) == '}') {
        flagDetail.append(ExprEntity(i + 1, rightFlag))
      }
    }
    val lst = scala.collection.mutable.ListBuffer.empty[String]
    doFind(dropBlankStr, leftFlag, rightFlag, flagDetail, lst)
    lst.toList
  }

  private[uitils] case class ExprEntity(index: Int, flag: Int)

}
