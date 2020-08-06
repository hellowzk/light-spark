package com.hellowzk.light.spark.uitils

//import java.text.SimpleDateFormat

import java.util.{Calendar, Date}

import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.time.{DateFormatUtils, DateUtils}

/**
 * <p>
 * 日期： 2020/6/30
 * <p>
 * 时间： 16:20
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object DateUtil {
  /**
   * 计算日期，如表达式 "DATE([yyyy-MM-dd, 2020-08-01][+3d])" 中第一位指定基准日期和输出格式，方法中也传入了基准日期，表达式中的优先级高，基准日期定义格式有：<br>
   * 1、第一个参数，可以是 [yyyy-MM-dd,] 表示基准日期为当天日期，表达式结果格式为 yyyy-MM-dd。<br>
   * 2、第一个参数，可以是 [yyyy-MM-dd, 2020-08-01] 表示基准日期为2020-08-01，表达式结果格式为 yyyy-MM-dd。<br>
   * 3、第一个参数，可以是 [yyyy-MM-dd,20200801,yyyyMMdd] 表示基准日期为 20200801，基准日期格式为 yyyyMMdd，表达式结果格式为 yyyy-MM-dd。<br>
   *
   * @param str  时间表达式
   * @param date 基准日期
   * @return
   */
  def parseExprByString(str: String, date: Date = new Date()): String = {
    // 匹配 [] 中的内容
    val parts = AppUtil.regexFindWithoutRule("\\[(.*?)]", str)
    var realTime = date
    val splits = parts.head.split(",", -1).map(_.trim)
    val formatStr = splits(0)
    if (splits.length == 2) {
      realTime = DateUtils.parseDate(splits(1), formatStr)
    } else if (splits.length == 3) {
      realTime = DateUtils.parseDate(splits(1), splits(2))
    }
    val current: Calendar = Calendar.getInstance()
    current.setTime(realTime)
    // 根据表达式计算时间
    for (i <- parts.indices if i > 0) {
      val exprOrig = parts(i)
      val expr = exprOrig.toUpperCase.trim
      // 有 + =
      if (expr.startsWith("+") || expr.startsWith("-")) {
        val count = expr.substring(0, expr.length - 1).toInt
        expr.substring(expr.length - 1) match {
          case "Y" => yearPlus(current, count)
          case "M" => monthPlus(current, count)
          case "D" => dayPlus(current, count)
          case _ => throw new Exception(s"error DATE expr '$str' '$exprOrig'.")
        }
        // MF 月第一天， ML 月最后一天
      } else if (StringUtils.containsAny(expr, "MF", "ML")) {
        specialDay(current, expr)
      } else {
        throw new Exception(s"error DATE expr '$str' '$expr'.")
      }
    }
    DateFormatUtils.format(current.getTime, formatStr)
  }

  /**
   * 年计算
   *
   */
  private def yearPlus(current: Calendar, count: Int) = {
    current.add(Calendar.YEAR, count)
  }

  /**
   * 月份计算
   *
   */
  private def monthPlus(current: Calendar, count: Int) = {
    current.add(Calendar.MONTH, count)
  }

  /**
   * 日计算
   *
   */
  private def dayPlus(current: Calendar, count: Int) = {
    current.add(Calendar.DATE, count)
  }

  /**
   * 特殊日期，MF 指定日期所在月 第一天， ML 指定日期所在月最后一天
   *
   */
  private def specialDay(current: Calendar, expr: String) = {
    val day = if (expr == "MF") {
      current.getActualMinimum(Calendar.DAY_OF_MONTH)
    } else {
      current.getActualMaximum(Calendar.DAY_OF_MONTH)
    }
    current.set(Calendar.DAY_OF_MONTH, day)
  }
}
