package com.hellowzk.light.spark.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.hellowzk.light.spark.uitils.{Logging, ResourceUtil}
import org.yaml.snakeyaml.Yaml

import scala.reflect.ClassTag

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:13
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：全局 cached 表信息存储
 * <p>
 * 作者： zhaokui
 *
 **/
trait ConfigLoader extends Logging {
  /**
   * 根据 Bean 解析yaml
   *
   * @param file yaml
   * @param cls  Bean
   * @tparam T Bean类型
   * @return yaml 解析后的 Bean
   */
  protected def load[T: ClassTag](file: String, cls: Class[T]): T = {
    val url = ResourceUtil.apply.get(file)
    logger.info(s"located config file path for $file: $url")
    val loader = new Yaml()
    loader.loadAs(url.openStream(), cls)
  }

  /**
   * yaml 文件转为 string
   *
   * @param file yaml file
   * @return
   */
  protected def loadYaml2String(file: String): String = {
    val mapper = new ObjectMapper(new YAMLFactory())
    val stream = this.getClass.getClassLoader.getResourceAsStream(file)
    val str = mapper.readValue(stream, classOf[Object])
    val objectMapper = new ObjectMapper()
    objectMapper.writeValueAsString(str)
  }
}
