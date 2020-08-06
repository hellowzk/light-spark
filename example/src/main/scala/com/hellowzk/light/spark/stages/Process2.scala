package com.hellowzk.light.spark.stages

import com.hellowzk.light.spark.beans.transform.CustomTransformConfig
import com.hellowzk.light.spark.stages.custom.CustomBaseTransform
import org.apache.spark.sql.SparkSession

/**
 * <p>
 * 日期： 2020/7/6
 * <p>
 * 时间： 17:43
 * <p>
 * 星期： 星期一
 * <p>
 * 描述：
 * <p>
 * 作者： zhaokui
 **/
class Process2 extends CustomBaseTransform {
  override def doProcess(bean: CustomTransformConfig)(implicit ss: SparkSession): Unit = {
    val rdd = getRDDByTable("table2").map(_.toSeq.map(_.toString)).map(cols => (cols(0), cols(1), cols(2), "ttt"))
    import ss.implicits._
    rdd.toDF("count", "gend", "date", "added").createOrReplaceTempView(bean.getName)
  }
}
