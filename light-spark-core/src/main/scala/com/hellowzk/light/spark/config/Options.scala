package com.hellowzk.light.spark.config

import java.text.SimpleDateFormat
import java.util.Date

import com.typesafe.config.{ConfigFactory, Config => ConfigObject}

import scala.collection.JavaConverters._

/**
 * @author zhaokui
 * @date 2019/11/19 14:55
 */
case class Options(
                    date: String = "",
                    config: String = "",
                    debug: Boolean = false
                  ) {
}

object Options {
  def parse(args: Array[String]): ConfigObject = {
    val parser = new scopt.OptionParser[Options]("sparkprocess") {

      opt[String]('d', "date").valueName("<event date>").text(
        "on what date the statistics based"
      ).required.action((x, c) =>
        c.copy(date = x)
      ).validate(s =>
        if (s.matches("""\d{4}\d{2}\d{2}""")) success
        else failure("date format must be YYYYMMDD")
      )

      opt[String]('c', "config").valueName("<config file>").text(
        "config file to load"
      ).required.action((x, c) =>
        c.copy(config = x)
      )

      opt[Unit]("debug").text(
        "开发模式"
      ).hidden().action((_, c) => {
        c.copy(debug = true)
      })

      help("help").abbr("h").text("prints this usage text")

      note("Happy Spark Statistics.")
    }

    val options = parser.parse(args, Options()) getOrElse {
      System.err.printf("Wrong options: %s\n", args.mkString(","))
      System.exit(1)
      null
    }

    ConfigFactory.parseMap(Map(
      "app.opts.debug" -> options.debug,
      "app.opts.date" -> options.date,
      "app.opts.config" -> options.config
    ).asJava)
  }

  def parseDate(date: String): Date = new SimpleDateFormat("yyyyMMdd").parse(date)
}