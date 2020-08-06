package com.hellowzk.light.spark.stages

import com.hellowzk.light.spark.beans.input.{BaseInputConfig, StreamInputConfig}
import com.hellowzk.light.spark.beans.{BaseConfig, BusinessConfig}
import com.hellowzk.light.spark.function.BaseUDF
import com.hellowzk.light.spark.stages.custom.CustomBaseInput
import com.hellowzk.light.spark.uitils.{Logging, ReflectUtils, SparkUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges}

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

object StreamPip extends Logging {

  /**
   * 开始处理加工逻辑
   *
   * @param config BusConfigBean
   * @param ss     SparkSession
   */
  def startPip(config: BusinessConfig)(implicit ss: SparkSession, ssc: StreamingContext): Unit = {
    logger.info(s"pipline ${config.configFile} ${config.eventDate} start.")
    // 加载 udf
    Option(config.udf).filter(_.nonEmpty).foreach(clazzs =>
      clazzs.foreach { case udf =>
        ReflectUtils.apply.getInstance[BaseUDF](udf).setup()
      })
    // 加载输入数据，注册成表
    val kafkaInput = config.inputs.head.asInstanceOf[StreamInputConfig]
    val dstream = getDstream(kafkaInput)
    dstream.map(record => record.value()).foreachRDD(rdd => {
      logger.info(s"batch start.")
      if (!rdd.isEmpty()) {
        ReflectUtils.apply.getInstance[CustomBaseInput](kafkaInput.getClazz).process(rdd, kafkaInput.name)
        logger.info("----------------------start processes----------------------")
        processStage(config.processes, StageType.processes.toString)
        logger.info("----------------------start outputs----------------------")
        processStage(config.outputs, StageType.outputs.toString)
        SparkUtil.uncacheData()
        logger.info(s"batch finished.")
      }
    })

    if (!config.isDebug) {
      dstream.foreachRDD { rdd =>
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        dstream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }

  def getDstream(item: BaseConfig)(implicit ss: SparkSession, ssc: StreamingContext): DStream[ConsumerRecord[String, String]] = {
    val bean = item.asInstanceOf[BaseInputConfig]
    ReflectUtils.apply.getInstance[StreamBaseInputWorker](bean.workerClass).initDS(bean)
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
