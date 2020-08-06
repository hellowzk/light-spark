package com.hellowzk.light.spark.stages

import com.hellowzk.light.spark.beans.{BaseConfig, BusinessConfig}
import com.hellowzk.light.spark.function.BaseUDF
import com.hellowzk.light.spark.uitils.{Logging, ReflectUtils, SparkUtil}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction

import scala.collection.JavaConversions._

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 11:23
 * <p>
 * 星期：
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/

object BatchPip extends Logging {
  /**
   * 开始处理加工逻辑
   *
   * @param config BusConfigBean
   * @param ss     SparkSession
   */
  def startPip(config: BusinessConfig)(implicit ss: SparkSession): Unit = {
    logger.info(s"pipline ${config.configFile} ${config.eventDate} start.")
    // 加载 udaf
    Option(config.udaf).filter(_.nonEmpty).foreach(clazzs =>
      clazzs.foreach { case (udafName, udafCls) =>
        val instans = Class.forName(udafCls).newInstance()
        Option(instans).filter(obj => obj.isInstanceOf[UserDefinedAggregateFunction]).foreach(obj => {
          ss.udf.register(udafName, obj.asInstanceOf[UserDefinedAggregateFunction])
          logger.info(s"registerd UDAF: '$udafCls' => '$udafName'.")
        })
      })
    Option(config.udf).filter(_.nonEmpty).foreach(clazzs =>
      clazzs.foreach { case udf =>
        ReflectUtils.apply.getInstance[BaseUDF](udf).setup()
      })
    // 加载输入数据，注册成表
    logger.info("----------------------start inputs----------------------")
    processStage(config.inputs, StageType.inputs.toString)
    logger.info("----------------------start processes----------------------")
    processStage(config.processes, StageType.processes.toString)
    logger.info("----------------------start outputs----------------------")
    processStage(config.outputs, StageType.outputs.toString)
    SparkUtil.uncacheData()
    logger.info(s"pipline ${config.configFile} ${config.eventDate} finished.")
  }

  /**
   * pipeline 处理
   *
   * @param items     items
   * @param stageName stageName
   * @param ss        ss
   */
  def processStage(items: java.util.List[_ <: BaseConfig], stageName: String)(implicit ss: SparkSession): Unit = {
    Option(items).filter(!_.isEmpty).foreach(lst => {
      for (i <- lst.indices) {
        val item = lst(i)
        logger.info(s"start $stageName, step${i + 1}, item '${item.name}'.")
        ReflectUtils.apply.getInstance[BaseWorker](item.workerClass).process(item)
      }
    })
  }
}
