package com.hellowzk.light.spark.uitils

import java.net.URL

import org.apache.commons.lang3.StringUtils

/**
 * @author zhaokui
 **/
object ResourceUtil {
  def apply: ResourceUtil = new ResourceUtil()
}

class ResourceUtil extends Logging {
  var CLUSTER_FLAG = true

  def get(confFile: String): URL = {
    var url: URL = null
    var fileName = confFile
    Option(confFile).filter(StringUtils.isNotBlank).foreach(conf => {
      url = this.getClass.getClassLoader.getResource(confFile)
    })
    Option(confFile).filter(c => null == url).foreach(c => throw new RuntimeException(s"file $confFile not exist."))
    logger.info(s"confFileName = [$fileName], url = [$url].")
    url
  }

}
