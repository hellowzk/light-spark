package com.hellowzk.light.spark

import com.hellowzk.light.spark.uitils.ReflectUtils

/**
 * <p>
 * 日期： 2020/7/3
 * <p>
 * 时间： 11:16
 * <p>
 * 星期： 星期五
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 **/
object AppTest {
  def main(args: Array[String]): Unit = {
    setUp()
    testBatch1()
  }

  def setUp(): Unit = {
    val path1 = "example/src/main/resources/localcluster"
    ReflectUtils.apply.addClasspath(path1)
    val path2 = "example/src/main/resources/data"
    ReflectUtils.apply.addClasspath(path2)
  }

  def testBatch1(): Unit = {
    val configFile1 = "full-batch.yaml"
    val configFile2 = "variables.yaml"
    val date = "20191211"
    App.main(Array("-d", date, "-c", configFile1, "--debug"))
  }
}
