/**
 * @author <a href="mailto:liuxiaolin6@cmbc.com.cn">Liu,Xiaolin</a>
 */
package com.hellowzk.light.spark.uitils

import org.slf4j.LoggerFactory

/**
 * <p>
 * 日期： 2019/11/22
 * <p>
 * 时间： 15:07
 * <p>
 * 星期：
 * <p>
 * 描述：通用日志
 * <p>
 * 作者： zhaokui
 *
 **/
trait Logging extends Serializable {
  val logger = LoggerFactory.getLogger(this.getClass.getCanonicalName.stripSuffix("$"))
}
