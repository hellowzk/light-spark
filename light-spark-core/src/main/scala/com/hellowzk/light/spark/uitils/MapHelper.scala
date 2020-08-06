package com.hellowzk.light.spark.uitils

/**
 * <p>
 * 日期： 2019/12/15
 * <p>
 * 时间： 1:46
 * <p>
 * 星期： 星期日
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object MapHelper {
  def apply(map: Map[String, Any]): MapHelper = new MapHelper(map)
}

class MapHelper(map: Map[String, Any]) extends Logging {

  def getInteger(key: String, default: java.lang.Integer = null): java.lang.Integer = get(key, default)

  def getLong(key: String, default: java.lang.Long = null): java.lang.Long = get(key, default)

  def getDouble(key: String, default: java.lang.Double = null): java.lang.Double = get(key, default)

  def getFloat(key: String, default: java.lang.Float = null): java.lang.Float = get(key, default)

  def getString(key: String, default: String = null): String = get(key, default)

  private def get[T](key: String, default: T): T = {
    if (map.contains(key)) {
      var value = map(key)
      Option(default).filter(d => null == value).foreach(d => value = d)
      value match {
        case t: T => t
        case e => throw new Exception(s"error: key '$key' exist in map, but value type is ${e.getClass.getName}, not match.")
      }
    }
    else {
      throw new Exception(s"error: key '$key' not exist in map.")
    }
  }

  def getBool(key: String, default: java.lang.Boolean = null): java.lang.Boolean = get(key, default)
}
