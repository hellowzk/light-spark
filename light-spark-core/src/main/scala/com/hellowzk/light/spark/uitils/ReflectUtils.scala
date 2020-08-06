package com.hellowzk.light.spark.uitils

import java.io.File
import java.net.{URL, URLClassLoader}

import org.apache.commons.lang3.StringUtils

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 14:59
 * <p>
 * 星期：
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object ReflectUtils {
  def apply: ReflectUtils = new ReflectUtils()
}

class ReflectUtils extends Logging {
  /**
   *
   * @param clazz 要反射的类
   * @tparam T 要反射生成的类型
   * @return 类的对象
   */
  def getInstance[T](clazz: String): T = {
    Option(StringUtils.EMPTY).filter(s => StringUtils.isBlank(clazz))
      .foreach(s => throw new Exception(s"do not find the class [$clazz] .Please check the configuration file"))
    try {
      val con = Class.forName(clazz).getConstructors
      val obj = con(0).newInstance()
      obj.asInstanceOf[T]
    } catch {
      case ex: Throwable => throw new Exception(s"the class [$clazz] is not Instance of the class [T]", ex)
    }
  }

  /**
   * 获取class中所有属性
   *
   * @param clazz 目标class
   * @return 属性name value 的map
   */
  def getFieldAsMap(clazz: String): Map[String, String] = {
    val objCls = this.getClass.getClassLoader.loadClass(clazz)
    val obj = objCls.newInstance()
    objCls.getDeclaredFields.map(f => {
      f.setAccessible(true)
      (f.getName, f.get(obj).toString)
    }).toMap
  }

  /**
   * 指定目录加载到 classpath
   *
   * @param path 指定目录
   */
  def addClasspath(path: String): Unit = {
    val programRootDir = new File(path)
    val classLoader = ClassLoader.getSystemClassLoader.asInstanceOf[URLClassLoader]
    val add = classOf[URLClassLoader].getDeclaredMethod("addURL", classOf[URL])
    add.setAccessible(true)
    add.invoke(classLoader, programRootDir.toURI.toURL)
  }
}
