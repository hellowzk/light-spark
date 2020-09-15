package com.hellowzk.light.spark.stages.input

import com.hellowzk.light.spark.beans.BaseConfig
import com.hellowzk.light.spark.beans.input.{KafkaInputConfig, KafkaInputItem}
import com.hellowzk.light.spark.stages.StreamBaseInputWorker
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._

import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * <p>
 * 日期： 2020/7/10
 * <p>
 * 时间： 9:49
 * <p>
 * 星期： 星期五
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class KafkaInputWorker extends StreamBaseInputWorker {
  /**
   * 加载数据
   *
   * @param bean BaseInputConfig
   * @param ss   SparkSession
   */
  override def initDS(bean: BaseConfig)(implicit ss: SparkSession, ssc: StreamingContext): DStream[ConsumerRecord[String, String]] = {
    val kafkaInput = bean.asInstanceOf[KafkaInputConfig]
    kafkaInput.items.map(kafkaInputItem => {
      val kafkaParams: Map[String, Object] = getKafkaParam(kafkaInputItem)
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Seq(kafkaInputItem.topic), kafkaParams)
      )
    }).reduce(reduceDstream)
  }

  def reduceDstream(x: DStream[ConsumerRecord[String, String]], y: DStream[ConsumerRecord[String, String]]): DStream[ConsumerRecord[String, String]] = {
    x.union(y)
  }

  /**
   * 合并 kafka参数
   */
  private def getKafkaParam(item: KafkaInputItem): Map[String, Object] = {
    val defaultKafkaParams = getDefaultParam
    Option(item.brokers).foreach(v => defaultKafkaParams += ("bootstrap.servers" -> v))
    Option(item.autoCommit).filter(_ != null).foreach(v => defaultKafkaParams += ("enable.auto.commit" -> v))
    Option(item.offersetReset).filter(v => StringUtils.isNotBlank(v)).foreach(v => defaultKafkaParams += ("auto.offset.reset" -> v))
    Option(item.groupId).foreach(v => defaultKafkaParams += ("group.id" -> v))
    Option(item.params).filter(params => CollectionUtils.isNotEmpty(params)).foreach(kvs => {
      kvs.map(_.split("=", -1)).filter(_.length == 2).map(sp => (sp(0), sp(1)))
        .foreach { case (key, v) => defaultKafkaParams += (key -> v) }
    })

    defaultKafkaParams.foreach { case (key, v) =>
      logger.info(s"kafka params: $key -> $v")
    }
    defaultKafkaParams.toMap
  }

  /**
   * kafka 默认参数
   */
  private def getDefaultParam: mutable.Map[String, Object] = {
    scala.collection.mutable.Map[String, Object](
      "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "use_a_separate_group_id_for_each_stream",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
  }
}
