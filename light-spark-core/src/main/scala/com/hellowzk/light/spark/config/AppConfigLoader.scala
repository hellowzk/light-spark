package com.hellowzk.light.spark.config

import com.alibaba.fastjson.serializer.SerializeFilter
import com.alibaba.fastjson.{JSON, JSONObject}
import com.hellowzk.light.spark.ConfigMapping
import com.hellowzk.light.spark.beans.transform.BaseTransformConfig
import com.hellowzk.light.spark.beans._
import com.hellowzk.light.spark.constants.{AppConstants, SysConstants}
import com.hellowzk.light.spark.uitils.{AppUtil, ConfigException, DateUtil, ReflectUtils}
import org.apache.commons.collections4.CollectionUtils
import org.apache.commons.lang3.StringUtils

import scala.collection.JavaConversions._
import scala.collection.mutable
/**
 * <p>
 * 日期： 2020/5/19
 * <p>
 * 时间： 15:13
 * <p>
 * 星期： 星期二
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object AppConfigLoader extends ConfigLoader {

  // 存放定义的表名，检查是否重复定义

  // 是否有需要 persist
  var hasPersists = false

  /**
   * 加载指定配置
   *
   * @param file file
   * @return
   */
  def loadAppConfig(file: String): BusinessConfig = {
    loadDefaultConfig()
    logger.info(s"start process $file.")
    val appConfigStr = loadYaml2String(file)
    val strFormated = AppUtil.formatVariableStr(appConfigStr)
    var busconfig = JSON.parseObject(strFormated, classOf[BusinessConfig])
    // check name and type
    configNameCheck(busconfig.inputs)
    configNameCheck(busconfig.processes)
    configNameCheck(busconfig.outputs)
    // merge variables
    val variables = mergeVariables(busconfig)
    initVariableMap(variables)
    loggerVariables()
    // replace
    val variableReplaced = variableReplace(strFormated, variables)

    // 变量替换后，重新生成 bean
    busconfig = JSON.parseObject(variableReplaced, classOf[BusinessConfig])
    string2Config(variableReplaced, busconfig)
    checkPersist(busconfig)
    // 个性化配置存储
    SysConstants.SYS_SPARK_CONFIG.putAll(parseSparkConfStrings(busconfig.getEnvs.getSpark))
    busconfig
  }

  /**
   * 加载默认配置文件中的配置
   */
  def loadDefaultConfig(): Unit = {
    val defaultFile = "default-config.yaml"
    val stream = this.getClass.getClassLoader.getResourceAsStream(defaultFile)
    if (null != stream) {
      val defaultStr = loadYaml2String(defaultFile)
      val busconfig = JSON.parseObject(defaultStr, classOf[BusinessConfig])
      SysConstants.SYS_SPARK_CONFIG.putAll(parseSparkConfStrings(busconfig.getEnvs.getSpark))
      SysConstants.SYS_DEFALUT_VARIABLES.putAll(busconfig.getConstansMap)

      AppConstants.variables.putAll(SysConstants.SYS_DEFALUT_VARIABLES)
      logger.info(s"load default config $defaultFile success.")
    }
  }

  /**
   * 检查 persist 相关配置
   *
   * @param busconfig
   */
  private def checkPersist(busconfig: BusinessConfig): Unit = {
    if (hasPersists) {
      val persistType = busconfig.getPersistType
      persistType match {
        case t if t == PersistTypes.hive.toString =>
          require(StringUtils.isNotBlank(busconfig.getPersistHiveDb), s"persistType is '$t', 'persistHiveDb' is need.")
        case t if t == PersistTypes.hdfs.toString =>
          require(StringUtils.isNotBlank(busconfig.getPersistDir), s"persistType is '$t', 'persistDir' is need.")
        case t if StringUtils.isBlank(t) =>
          throw new ConfigException(s"'store' is defined in yaml file, node 'persistType' is need.")
        case t =>
          throw new ConfigException(s"Unsupport persistType '$t'.")
      }
    }
  }

  /**
   * String 形式的 yaml 转 javaBean
   */
  private def string2Config(string: String, bean: BusinessConfig): Unit = {
    val jsonObject = JSON.parseObject(string)
    boxItems(bean.inputs, bean, jsonObject)
    boxItems(bean.processes, bean, jsonObject)
    boxItems(bean.outputs, bean, jsonObject)
    // 判断是否为 streaming
    Option(bean.inputs).foreach(inputs => {
      val kafkaInputs = inputs.filter(_.`type` == InputTypes.kafka.toString)
      bean.isStreaming = kafkaInputs.nonEmpty
      require(!bean.isStreaming || bean.streamBatchSeconds > 0, s"${bean.configFile} input contains kafka, sparkstreaming batch time 'streamBatchTime' require!")
      require(kafkaInputs.isEmpty || kafkaInputs.size == 1, s"${bean.configFile} kafka input max define 1 times.")
    })
  }

  /**
   * 封装 input process output 具体配置
   */
  private def boxItems(items: java.util.List[_ <: BaseConfig], bean: BusinessConfig, jsonObject: JSONObject): Unit = {
    Option(items).filter(lst => CollectionUtils.isNotEmpty(lst))
      .foreach(lst => {
        val nodeType = items.head.tag
        lst.clear()
        val list = jsonObject.getJSONArray(nodeType)
        for (i <- list.indices) {
          val jsonObject = list.getJSONObject(i)
          var theType = jsonObject.getString("type")
          val configClass = nodeType match {
            // inputs
            case nodeType if nodeType == NodeTypes.inputs.toString =>
              ConfigMapping.getInputConfigClass(theType)
            // processes
            case nodeType if nodeType == NodeTypes.processes.toString =>
              val config = JSON.parseObject(JSON.toJSONString(jsonObject, new Array[SerializeFilter](0)), classOf[BaseTransformConfig])
              config.doCheck()
              theType = if (StringUtils.isNotBlank(config.clazz))
                ProcessTypes.clazz.toString
              else
                ProcessTypes.sql.toString
              ConfigMapping.getProcessConfigClass(theType)
            // outputs
            case nodeType if nodeType == NodeTypes.outputs.toString =>
              ConfigMapping.getOutputConfigClass(theType)
          }
          require(configClass != null, s"Unsuport type '$theType', in $nodeType: '${jsonObject.getString("name")}'!")
          val configBean = JSON.parseObject(JSON.toJSONString(jsonObject, new Array[SerializeFilter](0)), configClass)
          configBean.doCheck()

          if (StringUtils.equalsAny(nodeType, NodeTypes.inputs.toString, NodeTypes.processes.toString)) {
            // 检查定义的表名是否重复
            checkTablesExist(configBean.getDefinedTables())
            // 是否需要 persist
            if (configBean.getStore) {
              hasPersists = true
            }
          }
          lst.add(JSON.parseObject(JSON.toJSONString(jsonObject, new Array[SerializeFilter](0)), configClass))
        }
      })
  }

  /**
   * 检查 name type
   *
   * @param item item
   * @tparam T
   */
  private def configNameCheck[T <: BaseConfig](item: java.util.List[T]): Unit = {
    Option(item).filter(inputs => CollectionUtils.isNotEmpty(inputs))
      .foreach(imputs => {
        imputs.foreach(input => input.nameCheck())
      })
  }

  /**
   * 初始化变量，变量值引用其他变量、时间表达式，进行替换
   *
   * @param variables
   */
  def initVariableMap(variables: scala.collection.mutable.Map[String, String]): Unit = {
    var flag = false
    variables.foreach { case (k, v) =>
      var replaceV = v
      for ((vk, vv) <- AppUtil.getVariableExprMap(v)) {
        if (vv == k) {
          throw new ConfigException(s"Illegal definitions '$vv' in variable '$vv'.")
        }
        if (variables.containsKey(vv) && !variables(vv).contains("${")) {
          flag = true
          replaceV = replaceV.replace(vk, variables(vv))
        }
        AppUtil.regexFindWithRule(SysConstants.apply.DATE_REGEX, vv).filter(!_.contains("${"))
          .foreach { expr =>
            flag = true
            replaceV = replaceV.replace(vk, DateUtil.parseExprByString(expr))
          }
      }
      variables.put(k, replaceV)
    }
    if (flag) {
      initVariableMap(variables)
    }

  }

  /**
   * 打印变量到日志
   */
  private def loggerVariables(): Unit = {
    AppConstants.variables.toList.sortBy(_._1).foreach { case (k, v) =>
      logger.info(s"variable - $k -> $v.")
    }
  }

  /**
   * 替换配置文件中 变量表达式 可替换的值
   *
   * @param template  template
   * @param variables variables
   */
  def variableReplace(template: String, variables: scala.collection.mutable.Map[String, String]): String = {
    var flag = false
    var afterReplace = template
    val vMap = AppUtil.getVariableExprMap(template)
    vMap.foreach { case (k, v) =>
      AppUtil.regexFindWithRule(SysConstants.apply.DATE_REGEX, v).filter(!_.contains("${"))
        .foreach { expr =>
          flag = true
          afterReplace = afterReplace.replace(k, DateUtil.parseExprByString(expr))
        }
      if (variables.containsKey(v)) {
        flag = true
        afterReplace = afterReplace.replace(k, variables(v))
      }
    }
    if (flag) {
      afterReplace = variableReplace(afterReplace, variables)
    }
    afterReplace
  }


  /**
   * 合并所有定义的常量到一个 map，包括系统预定义以及用户自定义
   *
   * @param businessConfig 常量类
   * @return
   */
  private def mergeVariables(businessConfig: BusinessConfig): mutable.Map[String, String] = {
    val constansClass = businessConfig.constansCls
    val sysClass = classOf[SysConstants].getName
    val appClass = classOf[AppConstants].getName
    val sysVariableMap = ReflectUtils.apply.getFieldAsMap(sysClass)
    val appVariableMap = ReflectUtils.apply.getFieldAsMap(appClass)
    val variables = scala.collection.mutable.Map.empty[String, String]

    /**
     * 常量类中的常量名不能冲突
     */
    Option(sysVariableMap.keySet intersect appVariableMap.keySet)
      .filter(_.nonEmpty).filter(_.diff(AppConstants.apply.SONAR_VARIABLE).nonEmpty).foreach(set =>
      throw new ConfigException(s"""Conflict variables '${set.mkString(",")}' in '$sysClass' and  '$appClass'."""))
    variables ++= sysVariableMap ++= appVariableMap
    Option(variables).filter(v => StringUtils.isNoneBlank(constansClass)).foreach(v => {
      val customVariableMap = ReflectUtils.apply.getFieldAsMap(constansClass)
      Option(variables.keySet intersect customVariableMap.keySet)
        .filter(_.nonEmpty).filter(_.diff(AppConstants.apply.SONAR_VARIABLE).nonEmpty).foreach(set =>
        throw new ConfigException(s"""conflict variables '${set.mkString(",")}' in '$constansClass', please rename them."""))
      variables ++= customVariableMap
    })

    Option(businessConfig.constansMap).filter(map => null != map && map.nonEmpty).foreach(map => {
      Option(variables.keySet intersect map.keySet)
        .filter(_.nonEmpty).filter(_.diff(AppConstants.apply.SONAR_VARIABLE).nonEmpty).foreach(set =>
        throw new ConfigException(s"""conflict variables '${set.mkString(",")}' in 'constansMap', please rename them."""))
      variables ++= map
    })

    AppConstants.variables.putAll(variables)
    AppConstants.variables
  }

  /**
   * 判断class是否存在
   *
   * @param clazz      clazz
   * @param node       用于打印日志
   * @param itemName   用于打印日志
   * @param configFile 用于打印日志
   */
  private def checkClassExistStatus(clazz: String, node: String, itemName: String, configFile: String): Unit = {
    try {
      this.getClass.getClassLoader.loadClass(clazz)
    } catch {
      case e: Exception =>
        logger.error("load class error.", e)
        throw new ConfigException(s"from node '$node' in '$configFile', for item '$itemName': ${e.getClass.getName}, ${e.getMessage}")
    }
  }

  private def checkTablesExist(tables: List[String]): Unit = {
    tables.foreach(table => checkTableExist(table))
  }

  private def checkTableExist(table: String): Unit = {
    val beforeSize = SysConstants.SYS_DEFINED_TABLES.size
    SysConstants.SYS_DEFINED_TABLES.add(table)
    val afterSize = SysConstants.SYS_DEFINED_TABLES.size
    if (afterSize == beforeSize) {
      throw new Exception(s"defined duplicated table '$table' in yaml file.")
    }
  }

  private def parseSparkConfStrings(list: java.util.List[String]) = {
    val map = scala.collection.mutable.Map.empty[String, String]
    list.map(_.split("=", -1))
      .filter(_.length == 2)
      .map(cls => (cls(0), cls(1)))
      .foreach { case (key, v) => map.put(key, v) }
    map
  }
}
