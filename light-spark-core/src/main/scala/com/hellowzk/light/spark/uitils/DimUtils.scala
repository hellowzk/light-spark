package com.hellowzk.light.spark.uitils

import scala.collection.mutable
import scala.collection.mutable.Map

/**
 * 排列组合
 *
 * @author 郭振
 *         2019/4/24 15:26
 */
object DimUtils {

  /**
   * 获取维度的所有排列组合 <br>
   * 输入: "brand", "series", "channel", "version" <br>
   * 返回: <br>
   * brand, series, channel, version -> , brand, series, channel, version<br>
   * 'all' as brand, 'all' as series, channel, 'all' as version -> , channel<br>
   * 'all' as brand, series, channel, 'all' as version -> , series, channel<br>
   * 'all' as brand, 'all' as series, 'all' as channel, version -> , version<br>
   * 'all' as brand, series, channel, version -> , series, channel, version<br>
   * brand, series, channel, 'all' as version -> , brand, series, channel<br>
   * brand, 'all' as series, 'all' as channel, 'all' as version -> , brand<br>
   * brand, 'all' as series, channel, 'all' as version -> , brand, channel<br>
   * brand, 'all' as series, channel, version -> , brand, channel, version<br>
   * 'all' as brand, series, 'all' as channel, 'all' as version -> , series<br>
   * brand, series, 'all' as channel, version -> , brand, series, version<br>
   * brand, 'all' as series, 'all' as channel, version -> , brand, version<br>
   * 'all' as brand, series, 'all' as channel, version -> , series, version<br>
   * 'all' as brand, 'all' as series, channel, version -> , channel, version<br>
   * 'all' as brand, 'all' as series, 'all' as channel, 'all' as version -> <br>
   * brand, series, 'all' as channel, 'all' as version -> , brand, series<br>
   *
   * @param dimList        维度信息
   * @param allPlaceholder 维度默认值
   * @return
   */
  def getCombinations(dimList: List[String], allPlaceholder: String): mutable.Map[String, String] = {
    val pMap: Map[String, String] = mutable.Map()
    DimUtils.combinations(0, "", dimList, allPlaceholder, pMap)
    pMap.map { case (k, v) => (k.substring(0, k.length - 1), v) }
  }

  private def combinations(i: Int, str: String, arr: List[String], allPlaceholder: String, targetMap: mutable.Map[String, String]): Unit = {
    if (i == arr.length) {
      val arrValue = str.split(",", -1).filter(!_.isEmpty).map(_.trim)
      val keyStr: StringBuilder = new StringBuilder(4)
      arr.foreach(item => {
        if (!arrValue.contains(item)) {
          keyStr.append(s"'$allPlaceholder' as $item, ")
        }
        else {
          keyStr.append(s"$item, ")
        }
      })
      val strLength = str.length
      var strFormat = str
      if (strLength > 0) {
        strFormat = ", " + str.substring(0, strLength - 2)
      }
      targetMap += (keyStr.toString().trim -> strFormat.trim)
      return
    }
    combinations(i + 1, str, arr, allPlaceholder, targetMap)
    combinations(i + 1, str + arr(i) + ", ", arr, allPlaceholder, targetMap)
  }
}
