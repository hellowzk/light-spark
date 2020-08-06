package com.hellowzk.light.spark.config

import java.text.SimpleDateFormat
import java.util.Date

import com.hellowzk.light.spark.beans.BusinessConfig
import org.apache.commons.lang3.time.DateFormatUtils

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 16:21
 * <p>
 * 星期：
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object BusConfig {
  private var baseWorkdir: String = _
  private var eventDate: String = DateFormatUtils.format(new Date(), "yyyyMMdd")
  private var eventDate10: String = _
  private var busConfig: BusinessConfig = _

  def apply: BusConfig = new BusConfig()
}

class BusConfig extends BaseConfigLoader(Options.parse) {


  /**
   * 解析输入参数
   *
   * @param options 输入参数
   * @return 参数封装的BusConfigBean
   */
  def parseOptions(options: Array[String]): BusinessConfig = {
    parse(options)
    val configFile = getString("app.opts.config")
    BusConfig.eventDate = getString("app.opts.date")
    val isDebug = getBool("app.opts.debug")
    val config = AppConfigLoader.loadAppConfig(configFile)
    config.configFile = configFile
    config.eventDate = BusConfig.eventDate
    config.isDebug = isDebug
    BusConfig.busConfig = config
    BusConfig.baseWorkdir = config.persistDir
    logger.debug(s"pipeline config loaded from $configFile")
    config
  }

  /**
   * 获取工作目录
   *
   * @return 工作目录
   */
  def getBaseWorkdir(): String = {
    BusConfig.baseWorkdir
  }

  def getEventDate8(): String = {
    BusConfig.eventDate
  }

  def getEventDateByDate(): Date = {
    new SimpleDateFormat("yyyyMMdd").parse(BusConfig.eventDate)
  }

  def getEventDate10(): String = {
    val date = getEventDateByDate()
    val str = new SimpleDateFormat("yyyy-MM-dd").format(date)
    str
  }

  def getConfig(): BusinessConfig = {
    BusConfig.busConfig
  }
}
