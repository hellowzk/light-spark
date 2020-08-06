package com.hellowzk.light.spark.function

import java.lang.reflect.Method

import com.hellowzk.light.spark.uitils.{ConfigException, Logging}
import org.apache.spark.sql.catalyst.JavaTypeInference
import org.apache.spark.sql.types.DataType

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
object GenerateFunction extends Serializable with Logging {
  /**
   * 根据源码生成 udf
   *
   * @param func
   * @return
   */
  def apply(func: String): (AnyRef, Array[DataType], DataType) = {
    generateFunctionByText(func)
  }

  /**
   * 根据 className 和 method 生成
   *
   * @param funcClass  className
   * @param methodName method
   * @return
   */
  def apply(funcClass: String, methodName: String): (AnyRef, Array[DataType], DataType) = {
    generateFunctionByClass(funcClass, methodName)
  }

  //获取方法的参数类型及返回类型
  private def getFunctionReturnTypeByFunText(func: String): (Array[DataType], DataType) = {
    val classInfo = ClassCreateUtils(func)
    val method = classInfo.defaultMethod
    getFunctionReturnType(method)
  }

  private def getFunctionReturnType(method: Method): (Array[DataType], DataType) = {
    val dataType = JavaTypeInference.inferDataType(method.getReturnType)._1
    (method.getParameterTypes.map(JavaTypeInference.inferDataType).map(_._1), dataType)
  }

  private def generateFunctionByText(func: String): (AnyRef, Array[DataType], DataType) = {
    val (argumentTypes, returnType) = getFunctionReturnTypeByFunText(func)
    val instance = ClassCreateUtils(func).instance
    val method = ClassCreateUtils(func).methods("apply")
    (generateFunction(instance, method, argumentTypes.length), argumentTypes, returnType)
  }

  private def generateFunctionByClass(funcClass: String, methodName: String): (AnyRef, Array[DataType], DataType) = {
    val classObj = Class.forName(funcClass)
    val instance = try {
      classObj.newInstance()
    } catch {
      case e: Throwable => null
    }
    val methods = classObj.getDeclaredMethods.filter(_.getName == methodName)
    if (methods.isEmpty) {
      throw new ConfigException(s"in udf class '$funcClass', method '$methodName' not exist.")
    }
    val method = methods.head
    val (argumentTypes, returnType) = getFunctionReturnType(method)
    (generateFunction(instance, method, argumentTypes.length), argumentTypes, returnType)
  }

  //生成22个Function
  private def generateFunction(instance: Any, method: Method, argumentsNum: Int): AnyRef = {
    argumentsNum match {
      case 0 => new (() => Any) with Serializable {
        override def apply(): Any = {
          try {
            method.invoke(instance)
          } catch {
            case e: Exception =>
              logger.error(e.getMessage, e)
          }
        }
      }
      case 1 => new (Object => Any) with Serializable {
        override def apply(v1: Object): Any = {
          try {
            method.invoke(instance, v1)
          } catch {
            case e: Exception =>
              logger.error(e.getMessage, e)
              null
          }
        }
      }
      case 2 => new ((Object, Object) => Any) with Serializable {
        override def apply(v1: Object, v2: Object): Any = {
          try {
            method.invoke(instance, v1, v2)
          } catch {
            case e: Exception =>
              logger.error(e.getMessage, e)
              null
          }
        }
      }
      case 3 => new ((Object, Object, Object) => Any) with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object): Any = {
          try {
            method.invoke(instance, v1, v2, v3)
          } catch {
            case e: Exception =>
              logger.error(e.getMessage, e)
              null
          }
        }
      }
      case 4 => new ((Object, Object, Object, Object) => Any) with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object): Any = {
          try {
            method.invoke(instance, v1, v2, v3, v4)
          } catch {
            case e: Exception =>
              logger.error(e.getMessage, e)
              null
          }
        }
      }
      case 5 => new ((Object, Object, Object, Object, Object) => Any) with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object): Any = {
          try {
            method.invoke(instance, v1, v2, v3, v4, v5)
          } catch {
            case e: Exception =>
              logger.error(e.getMessage, e)
              null
          }
        }
      }
      case 6 => new ((Object, Object, Object, Object, Object, Object) => Any) with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object): Any = {
          try {
            method.invoke(instance, v1, v2, v3, v4, v5, v6)
          } catch {
            case e: Exception =>
              logger.error(e.getMessage, e)
              null
          }
        }
      }
      case 7 => new ((Object, Object, Object, Object, Object, Object, Object) => Any) with Serializable {
        override def apply(v1: Object, v2: Object, v3: Object, v4: Object, v5: Object, v6: Object, v7: Object): Any = {
          try {
            method.invoke(instance, v1, v2, v3, v4, v5, v6, v7)
          } catch {
            case e: Exception =>
              logger.error(e.getMessage, e)
              null
          }
        }
      }
    }
  }
}