package com.hellowzk.light.spark.beans

import java.lang.reflect.Field
import java.util

import com.hellowzk.light.spark.uitils.{ConfigException, Logging}
import org.apache.commons.collections4.{CollectionUtils, Predicate}
import org.apache.commons.lang3.StringUtils

import scala.beans.BeanProperty
import scala.collection.JavaConversions._

/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 11:14
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
class BaseConfig extends Logging with Serializable {
  @BeanProperty
  var tag = NodeTypes.root.toString

  @BeanProperty
  var name: String = _

  @BeanProperty
  var `type`: String = _

  @BeanProperty
  var workerClass: String = _

  @BeanProperty
  var cache: Boolean = false

  @BeanProperty
  var store: Boolean = false

  @BeanProperty
  var show: Integer = 0

  @BeanProperty
  var partations: Integer = 0

  /**
   * 检查必填项，扩展支持的组件时需要实现
   */
  def doCheck(): Unit = {
    setFieldDefault()
    checkNoneIsBlank()
    checkAnyIsNotBlank()

  }

  protected def checkNoneIsBlank(): Unit = {}

  protected def checkAnyIsNotBlank(): Unit = {}

  protected def setFieldDefault(): Unit = {}

  /**
   * 检查必填项，必须全部不为 null，有为 null 的，抛出异常
   *
   * @param requireFields 要检查的字段
   */
  protected def validateNoneIsBlank(requireFields: String*): Unit = {
    val fields = zipField2Values()
    requireFields.foreach(fieldName => {
      val field = getValueByFieldName(fields, fieldName)
      field.setAccessible(true)
      val value = field.get(this)
      // 检验 必填
      val requireMsg = s"In node '${this.tag}', '$fieldName' is required in item '${this.name}'!"
      require(value != null, requireMsg)
      // 字符串必填
      val fieldTypeIsCollection = field.getType.isAssignableFrom(classOf[java.util.Collection[Object]])
      if (fieldTypeIsCollection) {
        val objects = value.asInstanceOf[util.Collection[Object]]
        require(!objects.isEmpty, requireMsg)
      } else if (fieldName == "clazz") {
        // 校验 class 是否存在
        checkClassExist(value.toString)
      }
    })
  }

  /**
   * 检查参数，不能全部为空
   *
   * @param requireOne 其中一个字段必须有值
   */
  protected def validateAnyIsNotBlank(requireOne: String*): Unit = {
    val fields = zipField2Values()
    val currentTag = this.tag
    val currentName = this.name
    val currentInstanse = this
    val filterd = CollectionUtils.select(requireOne, new Predicate[String]() {
      override def evaluate(fieldName: String): Boolean = {
        val field = getValueByFieldName(fields, fieldName)
        require(field != null, s"In node '$currentTag, checked field '$currentName', this field isn't a member variable in config bean '${this.getClass.getName}', contact the developer!")
        field.setAccessible(true)
        val value = field.get(currentInstanse)
        var isNotNull: Boolean = false
        if (field.getType.isAssignableFrom(classOf[java.util.Collection[Object]])) {
          val objects = value.asInstanceOf[util.Collection[Object]]
          isNotNull = !objects.isEmpty
        } else {
          isNotNull = !(value == null)
        }
        isNotNull
      }
    })
    Option(filterd).foreach(lst => require(CollectionUtils.isNotEmpty(lst), s"In node '${this.tag}', '${requireOne.mkString(",")}' is required one in item '${this.name}'!"))
  }

  /**
   * 根据字段名获取字段的值值
   *
   * @param fields    fields
   * @param fieldName fieldName
   * @return
   */
  private def getValueByFieldName(fields: Map[String, Field], fieldName: String): Field = {
    val field = fields.getOrElse(fieldName, null)
    require(field != null, s"In node '${this.tag}, checked field '$fieldName', this field isn't a member variable in config bean '${this.getClass.getName}', contact the developer!")
    field
  }

  /**
   * 获取当前类的所有属性和值
   *
   * @return
   */
  private def zipField2Values(): Map[String, Field] = {
    nameCheck()
    val fields = scala.collection.mutable.HashMap.empty[String, Field]
    var classObj: Class[_ <: Any] = this.getClass
    while (classObj.getName != classOf[Object].getName) {
      classObj.getDeclaredFields.map(field => {
        fields += (field.getName -> field)
      })
      classObj = classObj.getSuperclass()
    }
    fields.toMap
  }

  def nameCheck(): Unit = {
    require(StringUtils.isNotBlank(this.name), s"In node '${this.tag}', 'name' is required in item!")
    //    require(StringUtils.isNotBlank(this.`type`), s"In node '${this.tag}', 'type' is required in item '${this.name}'!")
  }

  def checkClassExist(clazz: String): Unit = {
    try {
      this.getClass.getClassLoader.loadClass(clazz)
    } catch {
      case e: Exception =>
        logger.error("load class error.", e)
        throw new ConfigException(s"In node '${this.tag}', item '${this.name}': ${e.getClass.getName}, ${e.getMessage}")
    }
  }

  def getDefinedTables(): List[String] = {
    List.empty[String]
  }

}
