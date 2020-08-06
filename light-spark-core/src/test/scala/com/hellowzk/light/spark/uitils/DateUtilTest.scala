package com.hellowzk.light.spark.uitils

import java.util.Date

import org.junit.Test

/**
 * <p>
 * 日期： 2020/6/30
 * <p>
 * 时间： 16:28
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class DateUtilTest {
  @Test
  def testParseExprByString(): Unit = {
    val d = new Date()
    val lst = Seq("DATE([yyyy-MM-dd])",
      "DATE([yyyy-MM-dd][+3d])",
      "DATE([yyyy-MM-dd][-3d])",
      "DATE([yyyy-MM-dd, 2020-08-01][+3d])",
      "DATE([yyyy-MM-dd, 2020-08-01][-3d])",
      "DATE([yyyy-MM-dd][+2m])",
      "DATE([yyyy-MM-dd][-2m])",
      "DATE([yyyy-MM-dd][+3y])",
      "DATE([yyyy-MM-dd][-3d])",
      "DATE([yyyy-MM-dd][-3d][MF])",
      "DATE([yyyy-MM-dd][-3d][ML])",
      "DATE([yyyy-MM-dd][+3d][-2m][+1y])")
    lst.foreach(line => {
      println(line)
      println(DateUtil.parseExprByString(line, d))
      println("--------------------")
    })
  }

  @Test
  def testExpr() = {
    import java.util.regex.Pattern
    val wpp = "jdbc:mysql://${wpp1}:${wpp2}/${wpp3}?&useSSL=false&characterEncoding=utf-8&serverTimezone=Asia/Shanghai&zeroDateTimeBehavior=convertToNull"
    //\u0024\u007B\u0028\u002E\u002A\u003F\u0029}
    // 匹配方式
    val p = Pattern.compile("\\$\\{(.*?)}")
    // 匹配】
    val matcher = p.matcher(wpp)
    // 处理匹配到的值
    while ( {
      matcher.find
    }) System.out.println("woo: " + matcher.group(0))
  }

  @Test
  def testExpr2() = {
    val wpp = "${DATE \t ([yyyy-MM-dd][-2m])}"
    val regex = "DATE\\s*\\((.*?)\\)"
    val strings = AppUtil.regexFindWithRule(regex, wpp)
    strings.foreach(println)
  }


  @Test
  def testExpr3() = {
    val wpp = "female{{initiator} updated {person}’s role to {role}}"
    val regex = "\\{[^{}]+}"
    val strings = AppUtil.regexFindWithRule(regex, wpp)
    strings.foreach(println)
  }

  @Test
  def testExpr5() = {
    val wpp = "female${${initiator} updated ${person}’s role to ${role}  ${role1}}"
    val regex = "\\$\\{([^}]*)\\}"
    val strings = AppUtil.regexFindWithRule(regex, wpp)
    strings.foreach(println)
  }

  @Test
  def testExpr6() = {
    val wpp = "female${${initiator} updated ${person}’s role to ${role}  ${role1}}"
    val regex = "\\$\\{((?!\\$)[^}]*)\\}"
    val strings = AppUtil.regexFindWithRule(regex, wpp)
    strings.foreach(println)
  }

  @Test
  def testAntLR() = {
    val wpp =
      """
        |female        $  {$   {initiator} updated $  {person}’s role to $ {role}  $
        |   {role1}}
        |""".stripMargin
    val regex = "\\$\\s*\\{"
    val strings = AppUtil.regexFindWithRule(regex, wpp)
    strings.foreach(println)
    println("-----")
    var res = wpp
    strings.foreach(r => {
      res = res.replace(r, "${")
    })
    println(res)
  }

  @Test
  def testAntLR2() = {
    val wpp =
      """
        |female        $  {$   {initiator} updated $  {person}’s role to $ {role}  $
        |   {role1}}}
        |""".stripMargin
    val strings = AppUtil.getVariableExprMap(wpp)
    strings.foreach(println)
  }
}
