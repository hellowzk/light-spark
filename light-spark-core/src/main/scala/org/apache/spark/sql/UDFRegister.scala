package org.apache.spark.sql

import com.hellowzk.light.spark.function.GenerateFunction
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUDF}

import scala.util.Try

/**
 * <p>
 * 日期： 2020/7/10
 * <p>
 * 时间： 11:10
 * <p>
 * 星期： 星期五
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 *
 **/
object UDFRegister {
  /**
   * 把源码注册成 udf, 如<br>
   * def apply(name:String)=name+" => hi"
   *
   * @param funcName 注册后 udf 名称
   * @param funcText 源码
   * @param ss
   */
  def register(funcName: String, funcText: String)(implicit ss: SparkSession): Unit = {
    val (fun, argumentTypes, returnType) = GenerateFunction(funcText)

    val inputTypes = Try(argumentTypes.toList).toOption

    def builder(e: Seq[Expression]) = ScalaUDF(fun, returnType, e, inputTypes.getOrElse(Nil), Some(funcName))
    //        val udf = UserDefinedFunction(fun, returnType, inputTypes)
    //        ss.udf.register(funcName,udf)
    ss.sessionState.functionRegistry.registerFunction(funcName, builder)

  }

  /**
   * 把 class 中的 method 注册为 udf
   *
   * @param funcName   注册后 udf 名称
   * @param funcClass  要注册的 class 类
   * @param methodName 要注册的 method
   * @param ss
   */
  def register(funcName: String, funcClass: String, methodName: String)(implicit ss: SparkSession): Unit = {
    val (fun, argumentTypes, returnType) = GenerateFunction(funcClass, methodName)

    val inputTypes = Try(argumentTypes.toList).toOption

    //    val udf = UserDefinedFunction(fun, returnType, inputTypes)
    //    ss.udf.register(funcName, udf)
    def builder(e: Seq[Expression]) = ScalaUDF(fun, returnType, e, inputTypes.getOrElse(Nil), Some(funcName))

    ss.sessionState.functionRegistry.registerFunction(funcName, builder)
  }
}
